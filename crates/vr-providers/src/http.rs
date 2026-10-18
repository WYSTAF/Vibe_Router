//! The generic OpenAI-compatible HTTP transport.
//!
//! One implementation of [`UpstreamClient`] serves every configured provider
//! — that is the point of the provider abstraction: providers are *data*
//! (`ProviderConfig` records), and this code path knows nothing about which
//! brand sits at a base URL. Custom provider behaviour, when it ever arrives,
//! is a different `UpstreamClient` selected at the server's wiring point;
//! nothing here changes.
//!
//! Secrets are resolved at request time from their `SecretRef` — an env var
//! is re-read on every call, so a rotated key is picked up without a reload.
//!
//! Timeout semantics (v0.1, documented honestly):
//!   * non-streaming: the whole response (headers + body) must arrive within
//!     `connect_timeout` — reqwest gives us no header-only timeout, and a
//!     stalled body is the same failure as a stalled header;
//!   * streaming: `first_chunk_timeout` covers time to the first SSE byte,
//!     `chunk_stall_timeout` the gap between subsequent chunks, with no total
//!     cap (a long generation is not a fault).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;
use vr_core::{
    Auth, HeaderScheme, SecretRef, StreamEvent, TransportError, TransportErrorKind, UpstreamClient,
    UpstreamRequest, UpstreamResponse, Usage,
};
use vr_translate::SseDecoder;

use crate::Registry;

/// How many buffered chunks a slow client may lag behind the upstream before
/// backpressure pauses the read. The server drains this channel into the
/// client socket.
const STREAM_BUFFER: usize = 32;

pub struct HttpUpstream {
    client: reqwest::Client,
    registry: Arc<Registry>,
}

impl HttpUpstream {
    pub fn new(registry: Arc<Registry>) -> Self {
        Self::with_client(registry, default_client())
    }

    /// Build on an existing client so integration tests (and any embedding
    /// binary) can share one pool.
    pub fn with_client(registry: Arc<Registry>, client: reqwest::Client) -> Self {
        Self { client, registry }
    }

    fn build_request(
        &self,
        req: &UpstreamRequest,
        key: Option<&vr_core::SecretRef>,
    ) -> Result<reqwest::RequestBuilder, TransportError> {
        let provider = self.registry.provider(&req.provider_id).ok_or_else(|| {
            transport(
                TransportErrorKind::Connect,
                format!("provider `{}` vanished from registry", req.provider_id),
            )
        })?;
        let t = &provider.transport;
        let mut builder = self.client.post(&t.base_url).json(&req.body);
        for (name, value) in &t.headers {
            builder = builder.header(name, value);
        }

        // Credential: a pool-selected key when given, else the provider's
        // single configured secret (or none).
        let (secret, scheme) = match (&provider.auth, key) {
            (Auth::None, _) => return Ok(builder),
            (Auth::Bearer { .. }, Some(k)) => (resolve_secret(k)?, HeaderScheme::Bearer),
            (Auth::Bearer { key_pool }, None) => {
                let Some(k) = key_pool.first() else {
                    return Ok(builder);
                };
                (resolve_secret(k)?, HeaderScheme::Bearer)
            }
            (Auth::Header { scheme, .. }, Some(k)) => (resolve_secret(k)?, *scheme),
            (
                Auth::Header {
                    key_pool, scheme, ..
                },
                None,
            ) => {
                let Some(k) = key_pool.first() else {
                    return Ok(builder);
                };
                (resolve_secret(k)?, *scheme)
            }
        };

        match (&provider.auth, scheme) {
            (Auth::Bearer { .. }, _) => {
                builder = builder.header("Authorization", format!("Bearer {secret}"));
            }
            (Auth::Header { name, .. }, HeaderScheme::Raw) => {
                builder = builder.header(name, secret);
            }
            (Auth::Header { name, .. }, HeaderScheme::Bearer) => {
                builder = builder.header(name, format!("Bearer {secret}"));
            }
            (Auth::None, _) => {}
        }
        Ok(builder)
    }

    async fn execute_json(
        &self,
        req: &UpstreamRequest,
        key: Option<&vr_core::SecretRef>,
    ) -> UpstreamResponse {
        let builder = match self.build_request(req, key) {
            Ok(b) => b,
            Err(e) => return UpstreamResponse::Transport(e),
        };
        let sent = tokio::time::timeout(req.connect_timeout, builder.send()).await;
        let response = match sent {
            Err(_) => {
                return UpstreamResponse::Transport(transport(
                    TransportErrorKind::Timeout,
                    "upstream did not return response headers in time",
                ))
            }
            Ok(Err(e)) => return UpstreamResponse::Transport(map_reqwest_error(&e)),
            Ok(Ok(r)) => r,
        };
        let status = response.status().as_u16();
        // Non-streaming: read the whole body within the same budget. A
        // provider that stalls mid-body is indistinguishable from one that
        // stalls before headers, and the fallback answer is the same.
        let text = match tokio::time::timeout(req.connect_timeout, response.text()).await {
            Err(_) => {
                return UpstreamResponse::Transport(transport(
                    TransportErrorKind::Timeout,
                    "upstream stalled while sending the response body",
                ))
            }
            Ok(Err(e)) => return UpstreamResponse::Transport(map_reqwest_error(&e)),
            Ok(Ok(t)) => t,
        };
        let body: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            // Keep the raw text: the classifier's text rules may still match
            // a provider's own wording inside an HTML or plain error page.
            Err(_) => serde_json::Value::String(truncate_text(&text)),
        };
        let usage = extract_usage(&body);
        UpstreamResponse::Json {
            status,
            body,
            usage,
        }
    }

    async fn execute_stream(
        &self,
        req: &UpstreamRequest,
        key: Option<&vr_core::SecretRef>,
    ) -> UpstreamResponse {
        let builder = match self.build_request(req, key) {
            Ok(b) => b,
            Err(e) => return UpstreamResponse::Transport(e),
        };
        let response = match tokio::time::timeout(req.connect_timeout, builder.send()).await {
            Err(_) => {
                return UpstreamResponse::Transport(transport(
                    TransportErrorKind::Timeout,
                    "upstream did not return response headers in time",
                ))
            }
            Ok(Err(e)) => return UpstreamResponse::Transport(map_reqwest_error(&e)),
            Ok(Ok(r)) => r,
        };
        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            // An error before the first byte: safe to classify and fail over.
            let text = response.text().await.unwrap_or_default();
            let body: serde_json::Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(_) => serde_json::Value::String(truncate_text(&text)),
            };
            return UpstreamResponse::Json {
                status,
                body,
                usage: None,
            };
        }

        // Good stream: hand back a channel; the pump task reads bytes and
        // yields decoded SSE payloads. When the reader drops the channel
        // (client disconnect), the pump stops and the response is dropped —
        // which closes the upstream connection. That is fail-fast on
        // disconnect, not a leak.
        let (tx, rx) = mpsc::channel(STREAM_BUFFER);
        tokio::spawn(pump_stream(
            response,
            tx,
            req.first_chunk_timeout,
            req.chunk_stall_timeout,
        ));
        UpstreamResponse::Stream { status, events: rx }
    }
}

/// HTTP 429 is the signal to cool a specific key and try the next one.
fn is_rate_limited(resp: &UpstreamResponse) -> bool {
    matches!(resp, UpstreamResponse::Json { status: 429, .. })
}

#[async_trait]
impl UpstreamClient for HttpUpstream {
    async fn execute(&self, req: UpstreamRequest) -> UpstreamResponse {
        // v0.1: the translator is the identity for openai->openai, but
        // request-shaping belongs here so stream_options (usage on the final
        // chunk) is applied exactly once, on the way out.
        let mut body = vr_translate::translate_request(
            vr_core::WireFormat::OpenAi,
            vr_core::WireFormat::OpenAi,
            req.body.clone(),
            req.stream,
        );
        if req.stream {
            body["stream"] = serde_json::Value::Bool(true);
        }
        let req = UpstreamRequest { body, ..req };

        let provider = match self.registry.provider(&req.provider_id) {
            Some(p) => p,
            None => {
                return UpstreamResponse::Transport(transport(
                    TransportErrorKind::Connect,
                    format!("provider `{}` vanished from registry", req.provider_id),
                ))
            }
        };

        // No pool (no credential, or an empty one): single attempt as before.
        if provider.key_pool.is_empty() {
            return self.run_once(&req, None).await;
        }

        // Multi-key pool: try healthy keys in order. A 429 cools that key and
        // we immediately retry with the next one — the client never sees the
        // rate-limit error unless *every* key is exhausted or cooling.
        let mut attempted: Vec<usize> = Vec::new();
        let mut last_rate_limited: Option<UpstreamResponse> = None;

        loop {
            let Some((idx, secret_ref)) = provider.key_pool.select_healthy_except(&attempted)
            else {
                // No healthy key left. Return the last 429 so the router's
                // classifier can cool the candidate and fail over; if we never
                // saw one, report exhausted credentials.
                return last_rate_limited.unwrap_or_else(|| {
                    UpstreamResponse::Transport(transport(
                        TransportErrorKind::NoCredential,
                        "all API keys in the pool are cooling down or exhausted".to_string(),
                    ))
                });
            };
            attempted.push(idx);

            let resp = self.run_once(&req, Some(&secret_ref)).await;
            provider.key_pool.release_in_flight(idx);

            if is_rate_limited(&resp) {
                provider.key_pool.mark_cooldown(idx, None);
                tracing::warn!(
                    provider = %provider.id,
                    key_index = idx,
                    "upstream 429: key cooled down, retrying with next key"
                );
                last_rate_limited = Some(resp);
                continue;
            }

            provider.key_pool.mark_success(idx);
            return resp;
        }
    }
}

impl HttpUpstream {
    async fn run_once(
        &self,
        req: &UpstreamRequest,
        key: Option<&vr_core::SecretRef>,
    ) -> UpstreamResponse {
        if req.stream {
            self.execute_stream(req, key).await
        } else {
            self.execute_json(req, key).await
        }
    }
}

/// Copy bytes off the upstream, decode SSE, forward payloads.
async fn pump_stream(
    mut response: reqwest::Response,
    tx: mpsc::Sender<StreamEvent>,
    first_chunk_timeout: Duration,
    stall_timeout: Duration,
) {
    let mut decoder = SseDecoder::default();
    let mut first = true;
    loop {
        let budget = if first {
            first_chunk_timeout
        } else {
            stall_timeout
        };
        let next = tokio::time::timeout(budget, response.chunk()).await;
        let chunk = match next {
            Err(_) => {
                let _ = tx
                    .send(StreamEvent::Error(if first {
                        "upstream sent no data before the first-chunk timeout".to_string()
                    } else {
                        "upstream stream stalled past the chunk timeout".to_string()
                    }))
                    .await;
                return;
            }
            Ok(Err(e)) => {
                let _ = tx
                    .send(StreamEvent::Error(format!("upstream read failed: {e}")))
                    .await;
                return;
            }
            Ok(Ok(None)) => break, // EOF
            Ok(Ok(Some(bytes))) => bytes,
        };
        first = false;
        for ev in decoder.push(&chunk) {
            if forward(&tx, ev.data).await.is_err() {
                return; // receiver gone: client disconnected
            }
        }
        if decoder.overflowed() {
            let _ = tx
                .send(StreamEvent::Error(
                    "upstream SSE stream exceeded the buffer cap".to_string(),
                ))
                .await;
            return;
        }
    }
    for ev in decoder.finish() {
        if forward(&tx, ev.data).await.is_err() {
            return;
        }
    }
    let _ = tx.send(StreamEvent::Done).await;
}

async fn forward(tx: &mpsc::Sender<StreamEvent>, data: String) -> Result<(), ()> {
    tx.send(StreamEvent::Chunk(data)).await.map_err(|_| ())
}

fn resolve_secret(r: &SecretRef) -> Result<String, TransportError> {
    match r {
        SecretRef::Env(name) => std::env::var(name).map_err(|_| {
            transport(
                TransportErrorKind::NoCredential,
                format!("environment variable `{name}` is not set"),
            )
        }),
        SecretRef::File(path) => std::fs::read_to_string(path)
            .map(|s| s.trim().to_string())
            .map_err(|e| {
                transport(
                    TransportErrorKind::NoCredential,
                    format!("cannot read secret file `{path}`: {e}"),
                )
            }),
        SecretRef::Literal(value) => Ok(value.clone()),
        SecretRef::Empty => Err(transport(
            TransportErrorKind::NoCredential,
            "provider requires a secret but none is configured",
        )),
    }
}

fn extract_usage(body: &serde_json::Value) -> Option<Usage> {
    let u = body.get("usage")?;
    let p = u.get("prompt_tokens").and_then(serde_json::Value::as_u64);
    let c = u
        .get("completion_tokens")
        .and_then(serde_json::Value::as_u64);
    match (p, c) {
        (Some(p), Some(c)) => Some(Usage::reported(p as u32, c as u32)),
        _ => None,
    }
}

fn map_reqwest_error(e: &reqwest::Error) -> TransportError {
    // Everything that is not an explicit timeout we issued above is a
    // connect-path failure (DNS, refused, TLS, pool). reqwest distinguishes
    // more kinds than we route differently in v0.1, so one arm is honest.
    transport(TransportErrorKind::Connect, e.to_string())
}

fn transport(kind: TransportErrorKind, message: impl Into<String>) -> TransportError {
    TransportError {
        kind,
        message: message.into(),
    }
}

fn truncate_text(s: &str) -> String {
    const MAX: usize = 2000;
    if s.len() <= MAX {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(MAX).collect();
        out.push('…');
        out
    }
}

/// Shared client builder: one pool, no implicit timeouts (budgets are
/// enforced explicitly at every await below).
pub fn default_client() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(8)
        .build()
        .expect("reqwest client")
}
