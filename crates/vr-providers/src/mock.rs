//! Hermetic mock upstream for tests: a loopback HTTP/1.1 server.
//!
//! No external services, no fixed ports, no flakiness: every test binds an
//! ephemeral port. A mock serves its scripted replies in order (the last one
//! repeats forever), captures every request, and closes each connection after
//! its response (`connection: close`). SSE replies are written as properly
//! framed `transfer-encoding: chunked` bodies so the transport's decoder is
//! exercised through the real socket path.
//!
//! Compiled in with the `test-mock` feature; never part of a shipped build.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// One scripted answer the mock can give.
#[derive(Clone)]
pub enum Reply {
    /// Fixed status + JSON body.
    Json(u16, String),
    /// SSE stream: `chunks` framed as chunked-encoding bodies. With
    /// `silence`, the mock hangs after sending them instead of closing
    /// (stall/first-chunk timeout territory).
    Sse {
        chunks: Vec<String>,
        silence: Option<Duration>,
    },
    /// Read the request, then hang without answering.
    Hang,
}

/// A running mock server: answers requests from its queue in order.
pub struct MockUpstream {
    addr: std::net::SocketAddr,
    captured: Arc<Mutex<Vec<Vec<u8>>>>,
    server: Option<JoinHandle<()>>,
}

impl Drop for MockUpstream {
    fn drop(&mut self) {
        if let Some(h) = self.server.take() {
            h.abort();
        }
    }
}

impl MockUpstream {
    fn start(listener: TcpListener, mut replies: VecDeque<Reply>) -> Self {
        let addr = listener.local_addr().expect("addr");
        let captured = Arc::new(Mutex::new(Vec::new()));
        let cap = captured.clone();
        let server = tokio::spawn(async move {
            loop {
                let (sock, _) = match listener.accept().await {
                    Ok(s) => s,
                    Err(_) => return,
                };
                let cap = cap.clone();
                // Repeat the last scripted reply forever rather than running
                // out mid-test when a provider retries unexpectedly.
                let reply = replies.pop_front().unwrap_or_else(|| {
                    replies
                        .back()
                        .cloned()
                        .expect("mock started with no replies")
                });
                if replies.is_empty() {
                    replies.push_back(reply.clone());
                }
                tokio::spawn(async move {
                    let request = run_one(sock, reply).await;
                    cap.lock().push(request);
                });
            }
        });
        Self {
            addr,
            captured,
            server: Some(server),
        }
    }

    /// One JSON reply (repeats for subsequent requests).
    pub async fn json(status: u16, reason: &str, body: &str) -> Self {
        let _ = reason; // status line reason mirrors body semantics
        Self::once(Reply::Json(status, body.to_string())).await
    }

    /// An SSE stream of `chunks` (each an exact wire payload).
    pub async fn sse(chunks: Vec<String>) -> Self {
        Self::once(Reply::Sse {
            chunks,
            silence: None,
        })
        .await
    }

    /// Headers + `chunks`, then silence instead of closing the stream.
    pub async fn sse_then_silence(chunks: Vec<String>, silence: Duration) -> Self {
        Self::once(Reply::Sse {
            chunks,
            silence: Some(silence),
        })
        .await
    }

    /// Accept, read, then hang without ever responding.
    pub async fn blackhole() -> Self {
        Self::once(Reply::Hang).await
    }

    /// Serve a scripted sequence of replies; the last one repeats forever.
    pub async fn sequence(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        Self::start(listener, VecDeque::from(replies))
    }

    async fn once(reply: Reply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        Self::start(listener, VecDeque::from(vec![reply]))
    }

    /// Full URL the transport should be pointed at.
    pub fn url(&self) -> String {
        format!("http://{}/v1/chat/completions", self.addr)
    }

    /// Wait until the nth request (0-based) has been served; return its text.
    pub async fn request_text(&mut self, nth: usize) -> String {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let found = {
                let texts = self.captured.lock();
                if texts.len() > nth {
                    Some(String::from_utf8_lossy(&texts[nth]).into_owned())
                } else {
                    None
                }
            };
            if let Some(text) = found {
                return text;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("mock never served request #{nth}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Wait until exactly `n` requests have been served, then return the
    /// count — the assertion primitive for "failover happened once".
    pub async fn wait_for_requests(&self, n: usize) -> usize {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let count = self.captured.lock().len();
            if count >= n {
                return count;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("mock served only {count} of {n} expected requests");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub fn request_count(&self) -> usize {
        self.captured.lock().len()
    }
}

async fn run_one(mut sock: tokio::net::TcpStream, reply: Reply) -> Vec<u8> {
    let request = read_request(&mut sock).await;
    match reply {
        Reply::Json(status, body) => {
            let reason = match status {
                200 => "OK",
                400 => "Bad Request",
                401 => "Unauthorized",
                403 => "Forbidden",
                404 => "Not Found",
                429 => "Too Many Requests",
                500 => "Internal Server Error",
                502 => "Bad Gateway",
                503 => "Service Unavailable",
                _ => "Status",
            };
            let response = http_response(status, reason, &body);
            let _ = sock.write_all(&response).await;
        }
        Reply::Sse { chunks, silence } => {
            let header = b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-cache\r\nconnection: close\r\ntransfer-encoding: chunked\r\n\r\n";
            if sock.write_all(header).await.is_ok() {
                for c in &chunks {
                    // The chunk length in the frame is bytes, not chars.
                    let frame = format!("{:x}\r\n{}\r\n", c.len(), c);
                    if sock.write_all(frame.as_bytes()).await.is_err() {
                        break;
                    }
                    let _ = sock.flush().await;
                }
                if let Some(s) = silence {
                    tokio::time::sleep(s).await;
                } else {
                    let _ = sock.write_all(b"0\r\n\r\n").await;
                }
            }
        }
        Reply::Hang => {
            // Never respond, never close: the client's timeouts must save it.
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    }
    let _ = sock.shutdown().await;
    request
}

fn http_response(status: u16, reason: &str, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

/// Read until the end of headers and the advertised body. Capped so a broken
/// test can never wedge on an endless body.
async fn read_request(sock: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = sock.read(&mut tmp).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 200_000 {
            break;
        }
        if let Some(head_end) = find_sub(&buf, b"\r\n\r\n") {
            // Parse content-length if present and wait for the full body.
            let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
            let want = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if buf.len() >= head_end + 4 + want {
                break;
            }
        }
    }
    buf
}

fn find_sub(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mock_serves_and_captures_requests_in_order() {
        let mut m = MockUpstream::sequence(vec![
            Reply::Json(503, "overloaded".into()),
            Reply::Json(200, r#"{"ok":true}"#.into()),
        ])
        .await;
        let client = reqwest::Client::new();
        let r1 = client
            .post(m.url())
            .json(&serde_json::json!({"n": 1}))
            .send()
            .await
            .unwrap();
        assert_eq!(r1.status(), 503);
        let r2 = client
            .post(m.url())
            .json(&serde_json::json!({"n": 2}))
            .send()
            .await
            .unwrap();
        assert_eq!(r2.status(), 200);
        let r3 = client
            .post(m.url())
            .json(&serde_json::json!({"n": 3}))
            .send()
            .await
            .unwrap();
        assert_eq!(r3.status(), 200, "the last scripted reply repeats");
        assert_eq!(m.wait_for_requests(3).await, 3);
        let first = m.request_text(0).await;
        assert!(first.contains("\"n\":1"), "{first}");
        let second = m.request_text(1).await;
        assert!(second.contains("\"n\":2"), "{second}");
    }

    #[tokio::test]
    async fn sse_reply_streams_frames() {
        let mut m = MockUpstream::sse(vec![
            "data: {\"a\":1}\n\n".to_string(),
            "data: [DONE]\n\n".to_string(),
        ])
        .await;
        let resp = reqwest::Client::new()
            .post(m.url())
            .json(&serde_json::json!({"model": "m1"}))
            .send()
            .await
            .unwrap();
        let text = resp.text().await.unwrap();
        assert!(text.contains("data: {\"a\":1}"), "{text}");
        assert!(text.ends_with("data: [DONE]\n\n"), "{text}");
        let _ = m.request_text(0).await;
    }
}
