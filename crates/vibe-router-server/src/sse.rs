//! Streaming: upstream chunks → client SSE response.
//!
//! Responsibilities, in order:
//!   * relay each chunk, rewriting its `model` field so the client sees the
//!     model string *it asked for* (a combo name stays a combo name) — the
//!     same contract the non-streaming path fulfils;
//!   * accumulate usage from the stream (last non-zero report wins, else an
//!     estimate — flagged as such all the way into storage);
//!   * detect client disconnect and stop relaying (the upstream pump notices
//!     its receiver dropped and tears the connection down);
//!   * terminate cleanly with `data: [DONE]`.
//!
//! The relay is channel-driven: frames are pushed into a bounded
//! `mpsc::Sender`, so backpressure is real (a slow client pauses the read
//! instead of buffering the whole stream) and a dropped receiver *is* the
//! disconnect signal — a failed send stops the relay immediately.
//!
//! A mid-stream upstream failure is terminal — the client already received
//! bytes, so failover is no longer possible. The failure is logged, usage is
//! still recorded with whatever was streamed, and the stream simply ends
//! without `[DONE]` so well-behaved clients notice truncation.

use vibe_router_core::{Candidate, StreamEvent, UpstreamResponse, Usage};
use vibe_router_translate::{encode_frame, UsageAccumulator, DONE_TOKEN};

/// What happened when the stream finished.
pub struct StreamOutcome {
    pub usage: Usage,
    /// True when the client got a complete stream ending in `[DONE]`.
    pub completed: bool,
    /// Data events sent to the client (for logging; not usage).
    pub events_sent: u32,
    /// Encoded frames for stream caching.
    pub recorded_frames: Vec<Vec<u8>>,
}

/// Stream `response` into `out`, then report the outcome.
///
/// `out` carries encoded SSE frames (`data: …\n\n`). A send error means the
/// receiver is gone — the client disconnected — and stops the relay.
/// `requested_model` is the client's original model string, echoed into every
/// chunk.
pub async fn relay(
    candidate: &Candidate,
    response: UpstreamResponse,
    requested_model: &str,
    out: tokio::sync::mpsc::Sender<Vec<u8>>,
) -> StreamOutcome {
    let UpstreamResponse::Stream { mut events, .. } = response else {
        // Callers only invoke this for streams; anything else is a bug.
        tracing::error!("relay called with a non-stream response");
        return StreamOutcome {
            usage: Usage::default(),
            completed: false,
            events_sent: 0,
            recorded_frames: Vec::new(),
        };
    };

    let mut outcome = StreamOutcome {
        usage: Usage::default(),
        completed: false,
        events_sent: 0,
        recorded_frames: Vec::new(),
    };
    let mut acc = UsageAccumulator::new();

    while let Some(event) = events.recv().await {
        match event {
            StreamEvent::Chunk(data) => {
                if data.trim() == DONE_TOKEN {
                    // Terminate: pass [DONE] through untouched.
                    let frame = encode_frame(DONE_TOKEN);
                    outcome.recorded_frames.push(frame.clone());
                    if out.send(frame).await.is_ok() {
                        outcome.completed = true;
                    }
                    break;
                }
                acc.observe(&data);
                outcome.events_sent += 1;
                let payload = rewrite_model(&data, requested_model);
                let frame = encode_frame(&payload);
                outcome.recorded_frames.push(frame.clone());
                if out.send(frame).await.is_err() {
                    // Client gone: stop relaying. Dropping `events` tears the
                    // upstream connection down via the pump task.
                    tracing::debug!(candidate = %candidate, "client disconnected mid-stream");
                    break;
                }
            }
            StreamEvent::Done => {
                // Upstream closed without [DONE]: synthesize it so the
                // client's parser sees a well-formed stream end.
                let frame = encode_frame(DONE_TOKEN);
                outcome.recorded_frames.push(frame.clone());
                if out.send(frame).await.is_ok() {
                    outcome.completed = true;
                }
                break;
            }
            StreamEvent::Error(message) => {
                // Terminal: bytes already went out; failover is impossible.
                tracing::warn!(candidate = %candidate, "stream failed mid-flight: {message}");
                break;
            }
        }
    }
    outcome.usage = acc.finish();
    outcome
}

/// Rewrite a chunk's `model` field to the client's requested string.
///
/// Chunks that fail to parse pass through untouched: dropping a chunk over a
/// cosmetic rewrite would corrupt the client's view of the conversation far
/// worse than a mismatched model label.
fn rewrite_model(data: &str, requested_model: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(data) {
        Ok(mut v) if v.is_object() => {
            v["model"] = serde_json::Value::String(requested_model.to_string());
            v.to_string()
        }
        _ => data.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn chunk(content: &str) -> String {
        format!(
            r#"{{"id":"c1","object":"chat.completion.chunk","model":"hidden/upstream","choices":[{{"index":0,"delta":{{"content":"{content}"}},"finish_reason":null}}]}}"#
        )
    }

    /// Drive `relay` with a scripted stream and collect the frames it sends.
    async fn run(events: Vec<StreamEvent>, requested_model: &str) -> (Vec<String>, StreamOutcome) {
        let (etx, erx) = tokio::sync::mpsc::channel::<StreamEvent>(16);
        for e in events {
            etx.send(e).await.unwrap();
        }
        drop(etx);
        let resp = UpstreamResponse::Stream {
            status: 200,
            events: erx,
        };
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(16);
        // The collector holds only rx; dropping tx in the collector's scope
        // is how a "disconnect" is simulated by tests that want one.
        let collector = tokio::spawn(async move {
            let mut frames = Vec::new();
            while let Some(bytes) = rx.recv().await {
                frames.push(String::from_utf8_lossy(&bytes).trim().to_string());
            }
            frames
        });
        let outcome = relay(&Candidate::direct("a/x"), resp, requested_model, tx).await;
        // rx is drained by the collector; give it a moment to finish.
        let frames = tokio::time::timeout(Duration::from_secs(5), collector)
            .await
            .expect("collector finished")
            .expect("collector ok");
        (frames, outcome)
    }

    #[tokio::test]
    async fn relays_chunks_with_model_rewritten() {
        let (frames, outcome) = run(
            vec![
                StreamEvent::Chunk(chunk("Hel")),
                StreamEvent::Chunk(chunk("lo")),
                StreamEvent::Chunk(
                    r#"{"choices":[],"usage":{"prompt_tokens":4,"completion_tokens":2}}"#.into(),
                ),
                StreamEvent::Chunk(DONE_TOKEN.into()),
            ],
            "my-combo",
        )
        .await;
        assert!(outcome.completed);
        assert_eq!(outcome.events_sent, 3);
        // Model rewritten to what the client asked for.
        assert!(frames[0].contains("\"model\":\"my-combo\""), "{frames:?}");
        assert!(!frames[0].contains("hidden/upstream"), "{frames:?}");
        // [DONE] forwarded as the terminal frame.
        assert_eq!(frames.last().unwrap(), "data: [DONE]");
        // Usage reported, not estimated.
        assert_eq!(outcome.usage, Usage::reported(4, 2));
    }

    #[tokio::test]
    async fn upstream_done_without_marker_is_synthesized() {
        let (frames, outcome) = run(
            vec![StreamEvent::Chunk(chunk("hi")), StreamEvent::Done],
            "a/x",
        )
        .await;
        assert!(outcome.completed);
        assert!(frames.iter().any(|s| s.contains("[DONE]")));
    }

    #[tokio::test]
    async fn mid_stream_error_is_terminal_and_records_partial() {
        let (frames, outcome) = run(
            vec![
                StreamEvent::Chunk(chunk("par")),
                StreamEvent::Error("upstream exploded".into()),
            ],
            "a/x",
        )
        .await;
        assert!(!outcome.completed, "no [DONE] after a mid-stream failure");
        assert_eq!(outcome.events_sent, 1);
        // No usage reported → estimated from what streamed.
        assert!(outcome.usage.estimated);
        assert_eq!(outcome.usage.completion_tokens, 0); // "par" = 3 chars / 4
        assert_eq!(frames.len(), 1, "only the pre-failure chunk went out");
    }

    #[tokio::test]
    async fn client_disconnect_stops_relay() {
        // One frame of buffer, then the receiver goes away.
        let (etx, erx) = tokio::sync::mpsc::channel::<StreamEvent>(16);
        tokio::spawn(async move {
            for i in 0..50 {
                let _ = etx.send(StreamEvent::Chunk(chunk(&format!("c{i}")))).await;
            }
            let _ = etx.send(StreamEvent::Chunk(DONE_TOKEN.into())).await;
        });
        let resp = UpstreamResponse::Stream {
            status: 200,
            events: erx,
        };
        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
        drop(rx); // client is already gone
        let outcome = relay(&Candidate::direct("a/x"), resp, "a/x", tx).await;
        assert!(!outcome.completed);
        // The very first send fails; at most the buffered first chunk counts.
        assert!(outcome.events_sent <= 1, "{}", outcome.events_sent);
    }

    #[tokio::test]
    async fn unparseable_chunk_passes_through_untouched() {
        let (frames, outcome) = run(
            vec![
                StreamEvent::Chunk("not-json{{".into()),
                StreamEvent::Chunk(DONE_TOKEN.into()),
            ],
            "a/x",
        )
        .await;
        assert!(outcome.completed);
        assert!(
            frames.iter().any(|s| s.contains("not-json{{")),
            "unparseable chunks must be forwarded, not dropped: {frames:?}"
        );
    }

    #[tokio::test]
    async fn empty_stream_reports_zero_and_incomplete() {
        let (frames, outcome) = run(vec![], "a/x").await;
        assert!(!outcome.completed);
        assert_eq!(outcome.events_sent, 0);
        assert!(frames.is_empty());
        assert!(outcome.usage.is_zero());
    }
}
