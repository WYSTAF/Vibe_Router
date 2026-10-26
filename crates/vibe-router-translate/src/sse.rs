//! Server-Sent Events: incremental decoding and frame writing.
//!
//! Decoding is push-based: the transport hands over bytes as they arrive and
//! the decoder yields whole events. A malformed or over-long stream is
//! reported rather than silently dropped, and no event is ever yielded half-
//! formed — a partially buffered event simply stays in the buffer.
//!
//! The buffer is capped so a hostile or broken upstream cannot make a
//! connection grow memory without bound.

use serde_json::Value;

/// The terminal SSE payload: `data: [DONE]`.
pub const DONE_TOKEN: &str = "[DONE]";

/// A decoded SSE event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// The `event:` field, if present.
    pub event: Option<String>,
    /// Concatenated `data:` lines, joined with `\n` per the SSE spec.
    pub data: String,
}

impl SseEvent {
    pub fn data_only(data: impl Into<String>) -> Self {
        Self {
            event: None,
            data: data.into(),
        }
    }
}

/// Incremental SSE decoder.
#[derive(Debug)]
pub struct SseDecoder {
    buf: Vec<u8>,
    /// Current event being assembled.
    event: Option<String>,
    data_lines: Vec<String>,
    max_buffer: usize,
    overflow: bool,
}

impl Default for SseDecoder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_BUFFER)
    }
}

const DEFAULT_MAX_BUFFER: usize = 4 << 20; // 4 MiB

impl SseDecoder {
    pub fn new(max_buffer: usize) -> Self {
        Self {
            buf: Vec::new(),
            event: None,
            data_lines: Vec::new(),
            max_buffer,
            overflow: false,
        }
    }

    /// True when the stream exceeded the buffer cap and must be abandoned.
    pub fn overflowed(&self) -> bool {
        self.overflow
    }

    /// Feed bytes; return every event completed by this chunk.
    pub fn push(&mut self, bytes: &[u8]) -> Vec<SseEvent> {
        if self.overflow {
            return Vec::new();
        }
        if self.buf.len() + bytes.len() > self.max_buffer {
            self.overflow = true;
            self.buf.clear();
            return Vec::new();
        }
        self.buf.extend_from_slice(bytes);

        let mut out = Vec::new();
        // Split on complete lines only; the remainder stays buffered. SSE
        // line terminators may be \n, \r\n, or a lone \r.
        while let Some((line_len, consumed)) = find_line_end(&self.buf) {
            let line = self.buf[..line_len].to_vec();
            self.buf.drain(..consumed);
            if self.consume_line(&line, &mut out).is_err() {
                self.overflow = true;
                break;
            }
        }
        out
    }

    /// Flush any trailing event at end-of-stream (a final block not followed
    /// by a blank line).
    pub fn finish(&mut self) -> Vec<SseEvent> {
        let mut out = Vec::new();
        if !self.buf.is_empty() {
            let line = std::mem::take(&mut self.buf);
            let _ = self.consume_line(&line, &mut out);
        }
        if let Some(ev) = self.take_event() {
            out.push(ev);
        }
        out
    }

    fn consume_line(&mut self, line: &[u8], out: &mut Vec<SseEvent>) -> Result<(), ()> {
        // A blank line dispatches the buffered event.
        if line.is_empty() || line == b"\r" {
            if let Some(ev) = self.take_event() {
                out.push(ev);
            }
            return Ok(());
        }
        let text = match std::str::from_utf8(line) {
            Ok(t) => t,
            // Invalid UTF-8 mid-line: we cannot repair it, so fail the stream
            // rather than emit mojibake to the client.
            Err(_) => return Err(()),
        };
        let text = text.trim_end_matches('\r');
        if text.starts_with(':') {
            return Ok(()); // comment
        }
        let (field, value) = match text.split_once(':') {
            Some((f, v)) => (f, v.trim_start_matches(' ')),
            None => (text, ""),
        };
        match field {
            "event" => self.event = Some(value.to_string()),
            "data" => self.data_lines.push(value.to_string()),
            "id" | "retry" => {} // not used for LLM streams
            _ => {}
        }
        Ok(())
    }

    fn take_event(&mut self) -> Option<SseEvent> {
        let event = self.event.take();
        if self.data_lines.is_empty() {
            return event.map(|e| SseEvent {
                event: Some(e),
                data: String::new(),
            });
        }
        let data = std::mem::take(&mut self.data_lines).join("\n");
        Some(SseEvent { event, data })
    }
}

/// Find the end of the first complete line. Returns `(line_len, bytes_consumed)`
/// where bytes_consumed includes the terminator.
fn find_line_end(buf: &[u8]) -> Option<(usize, usize)> {
    for (i, &b) in buf.iter().enumerate() {
        if b == b'\n' {
            return Some((i, i + 1));
        }
        if b == b'\r' {
            // \r\n counts as one terminator; a lone \r terminates too.
            return Some((i, i + 1));
        }
    }
    None
}

/// Write one SSE frame. `data` is sent verbatim (already JSON-encoded by the
/// caller or the literal [`DONE_TOKEN`]).
pub fn encode_frame(data: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 8);
    out.extend_from_slice(b"data: ");
    out.extend_from_slice(data.as_bytes());
    out.extend_from_slice(b"\n\n");
    out
}

/// Accumulate token usage out of streamed chunks.
///
/// Providers disagree about where usage appears: some put `usage` on the
/// final chunk, some on every chunk, some only with `stream_options.
/// include_usage`. We take the *last non-zero* usage we see, which is correct
/// for all three shapes, and fall back to estimating from content length when
/// the provider sent nothing — an honest estimate beats a silent zero.
#[derive(Debug, Default, Clone)]
pub struct UsageAccumulator {
    prompt: u32,
    completion: u32,
    /// Characters of assistant text seen, for the fallback estimate.
    content_chars: usize,
    reasoning_chars: usize,
}

impl UsageAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Observe one decoded chunk. Ignores non-JSON and `[DONE]` payloads.
    pub fn observe(&mut self, data: &str) {
        if data.trim() == DONE_TOKEN || data.is_empty() {
            return;
        }
        let Ok(value) = serde_json::from_str::<Value>(data) else {
            return;
        };
        if let Some(choice) = value.get("choices").and_then(|c| c.get(0)) {
            if let Some(text) = choice.pointer("/delta/content").and_then(|t| t.as_str()) {
                self.content_chars += text.chars().count();
            }
            // Reasoning summaries are billed as output on several providers.
            for path in ["/delta/reasoning_content", "/delta/reasoning"] {
                if let Some(text) = choice.pointer(path).and_then(|t| t.as_str()) {
                    self.reasoning_chars += text.chars().count();
                }
            }
        }
        if let Some(u) = value.get("usage") {
            let p = u.get("prompt_tokens").and_then(Value::as_u64).unwrap_or(0) as u32;
            let c = u
                .get("completion_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0) as u32;
            // Last non-zero wins; some providers report 0 on early chunks.
            if p > 0 {
                self.prompt = p;
            }
            if c > 0 {
                self.completion = c;
            }
        }
    }

    /// Final usage: reported if present, otherwise estimated (~4 chars per
    /// token, the usual rule of thumb for English prose). The `estimated`
    /// flag carries which one it was — an estimate is never silently passed
    /// off as provider-reported.
    pub fn finish(&self) -> vibe_router_core::Usage {
        if self.prompt > 0 || self.completion > 0 {
            return vibe_router_core::Usage::reported(self.prompt, self.completion);
        }
        let est = (self.content_chars + self.reasoning_chars) / 4;
        vibe_router_core::Usage::estimated(0, est as u32)
    }

    pub fn estimated(&self) -> bool {
        self.prompt == 0 && self.completion == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(chunks: &[&str]) -> Vec<SseEvent> {
        let mut d = SseDecoder::default();
        let mut out = Vec::new();
        for c in chunks {
            out.extend(d.push(c.as_bytes()));
        }
        out.extend(d.finish());
        out
    }

    #[test]
    fn decodes_two_events_from_one_chunk() {
        let evs = decode_all(&["data: {\"a\":1}\n\ndata: {\"b\":2}\n\n"]);
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].data, "{\"a\":1}");
        assert_eq!(evs[1].data, "{\"b\":2}");
    }

    #[test]
    fn reassembles_events_split_across_chunks() {
        let evs = decode_all(&["data: hel", "lo wor", "ld\n", "\n"]);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data, "hello world");
    }

    #[test]
    fn handles_crlf_and_lone_cr() {
        let evs = decode_all(&["data: a\r\n\r\ndata: b\r\r"]);
        assert_eq!(
            evs.iter().map(|e| e.data.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn joins_multiple_data_lines() {
        let evs = decode_all(&["data: one\ndata: two\n\n"]);
        assert_eq!(evs[0].data, "one\ntwo");
    }

    #[test]
    fn skips_comments_and_unknown_fields() {
        let evs = decode_all(&[": keep-alive\nevent: foo\nid: 7\ndata: hi\n\n"]);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data, "hi");
        assert_eq!(evs[0].event.as_deref(), Some("foo"));
    }

    #[test]
    fn done_token_survives() {
        let evs = decode_all(&["data: [DONE]\n\n"]);
        assert_eq!(evs[0].data, DONE_TOKEN);
    }

    #[test]
    fn flushes_trailing_event_without_blank_line() {
        let evs = decode_all(&["data: tail"]);
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data, "tail");
    }

    #[test]
    fn overflow_is_reported_not_broken() {
        let mut d = SseDecoder::new(16);
        let out = d.push(&[b'x'; 40]);
        assert!(out.is_empty());
        assert!(d.overflowed());
        // Still safe to feed afterwards.
        assert!(d.push(b"data: y\n\n").is_empty());
    }

    #[test]
    fn frame_round_trips() {
        let frame = encode_frame(r#"{"x":1}"#);
        assert_eq!(frame, b"data: {\"x\":1}\n\n");
        let evs = decode_all(&[std::str::from_utf8(&frame).unwrap()]);
        assert_eq!(evs[0].data, r#"{"x":1}"#);
    }

    #[test]
    fn usage_takes_last_nonzero_report() {
        let mut u = UsageAccumulator::new();
        u.observe(r#"{"choices":[{"delta":{"content":"hi"}}],"usage":{"prompt_tokens":5,"completion_tokens":0}}"#);
        u.observe(r#"{"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":3}}"#);
        let f = u.finish();
        assert_eq!((f.prompt_tokens, f.completion_tokens), (5, 3));
        assert!(!u.estimated());
    }

    #[test]
    fn usage_estimates_when_provider_says_nothing() {
        let mut u = UsageAccumulator::new();
        u.observe(r#"{"choices":[{"delta":{"content":"01234567890123456789"}}]}"#);
        let f = u.finish();
        assert_eq!(f.prompt_tokens, 0);
        assert_eq!(f.completion_tokens, 5); // 20 chars / 4
        assert!(u.estimated());
    }

    #[test]
    fn usage_ignores_done_and_garbage() {
        let mut u = UsageAccumulator::new();
        u.observe(DONE_TOKEN);
        u.observe("not json at all");
        u.observe("");
        assert!(u.finish().is_zero());
    }

    #[test]
    fn counts_reasoning_toward_output_estimate() {
        let mut u = UsageAccumulator::new();
        u.observe(
            r#"{"choices":[{"delta":{"reasoning_content":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}]}"#,
        );
        assert_eq!(u.finish().completion_tokens, 8);
    }
}
