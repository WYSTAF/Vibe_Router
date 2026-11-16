//! The generic upstream HTTP transport.
//!
//! One implementation of [`UpstreamClient`] serves every configured provider.
//! Providers are *data* (`ProviderConfig` records); this code path knows nothing
//! about which brand sits at a base URL. It honors the provider's configured
//! wire format (OpenAI or Anthropic), routes through a sticky tagged proxy when
//! one is registered, and rotates a multi-key pool on 429.
//!
//! Secrets are resolved at request time from their `SecretRef` — an env var is
//! re-read on every call, so a rotated key is picked up without a reload.
//!
//! Timeout semantics:
//!   * non-streaming: the whole response (headers + body) must arrive within
//!     `connect_timeout`;
//!   * streaming: `first_chunk_timeout` covers time to the first SSE byte,
//!     `chunk_stall_timeout` the gap between subsequent chunks.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use vibe_router_core::{
    Auth, HeaderScheme, SecretRef, StreamEvent, TransportError, TransportErrorKind, UpstreamClient,
    UpstreamRequest, UpstreamResponse, Usage, WireFormat,
};
use vibe_router_translate::SseDecoder;

use crate::proxy::{ProxyEntry, ProxyTag};
use crate::Registry;

/// How many buffered chunks a slow client may lag behind the upstream before
/// backpressure pauses the read.
const STREAM_BUFFER: usize = 32;

pub struct HttpUpstream {
    client: reqwest::Client,
    registry: Arc<Registry>,
    /// Per-proxy-URL clients (reqwest binds a proxy at client build time).
    proxy_clients: Mutex<HashMap<String, reqwest::Client>>,
}

impl HttpUpstream {
    pub fn new(registry: Arc<Registry>) -> Self {
        Self::with_client(registry, default_client())
    }

    pub fn with_client(registry: Arc<Registry>, client: reqwest::Client) -> Self {
        Self {
            client,
            registry,
            proxy_clients: Mutex::new(HashMap::new()),
        }
    }

    /// A client bound to a forward proxy, cached per URL. Falls back to the
    /// direct client if the proxy URL is unusable.
    fn client_for_proxy(&self, url: &str) -> reqwest::Client {
        let mut cache = self.proxy_clients.lock();
        if let Some(c) = cache.get(url) {
            return c.clone();
        }
        let proxy = match reqwest::Proxy::all(url) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(proxy = url, "invalid proxy URL, using direct: {e}");
                return self.client.clone();
            }
        };
        match reqwest::Client::builder()
            .pool_max_idle_per_host(8)
            .proxy(proxy)
            .build()
        {
            Ok(c) => {
                cache.insert(url.to_string(), c.clone());
                c
            }
            Err(e) => {
                tracing::warn!(proxy = url, "cannot build proxy client, using direct: {e}");
                self.client.clone()
            }
        }
    }

    /// Resolve (client, request URL, optional reverse-relay target) for a proxy.
    /// `cf-worker` entries are reverse relays (X-Target-URL); other tags are
    /// forward proxies bound into the client.
    fn route_for(
        &self,
        proxy: Option<&ProxyEntry>,
        base_url: &str,
    ) -> (reqwest::Client, String, Option<String>) {
        match proxy {
            None => (self.client.clone(), base_url.to_string(), None),
            Some(p) if p.tag == ProxyTag::CfWorker => (
                self.client.clone(),
                p.url.clone(),
                Some(base_url.to_string()),
            ),
            Some(p) => (self.client_for_proxy(&p.url), base_url.to_string(), None),
        }
    }

    fn build_request(
        &self,
        client: &reqwest::Client,
        req: &UpstreamRequest,
        key: Option<&SecretRef>,
        url: &str,
        relay_target: Option<&str>,
    ) -> Result<reqwest::RequestBuilder, TransportError> {
        let provider = self.registry.provider(&req.provider_id).ok_or_else(|| {
            transport(
                TransportErrorKind::Connect,
                format!("provider `{}` vanished from registry", req.provider_id),
            )
        })?;
        let t = &provider.transport;
        let mut builder = client.post(url).json(&req.body);
        for (name, value) in &t.headers {
            builder = builder.header(name, value);
        }
        if let Some(target) = relay_target {
            builder = builder.header("X-Target-URL", target);
        }

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
        client: &reqwest::Client,
        req: &UpstreamRequest,
        key: Option<&SecretRef>,
        url: &str,
        relay_target: Option<&str>,
        format: WireFormat,
    ) -> UpstreamResponse {
        let builder = match self.build_request(client, req, key, url, relay_target) {
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
            Err(_) => serde_json::Value::String(truncate_text(&text)),
        };

        // A successful Anthropic response is converted to OpenAI shape so the
        // router's usage extraction and classification stay format-agnostic.
        if (200..300).contains(&status) && format == WireFormat::Anthropic {
            let converted = vibe_router_translate::anthropic_to_openai_response(&body, &req.model);
            let usage = extract_usage(&converted);
            return UpstreamResponse::Json {
                status,
                body: converted,
                usage,
            };
        }

        let usage = extract_usage(&body);
        UpstreamResponse::Json {
            status,
            body,
            usage,
        }
    }

    async fn execute_stream(
        &self,
        client: &reqwest::Client,
        req: &UpstreamRequest,
        key: Option<&SecretRef>,
        url: &str,
        relay_target: Option<&str>,
        format: WireFormat,
    ) -> UpstreamResponse {
        let builder = match self.build_request(client, req, key, url, relay_target) {
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
            // An error before the first byte: bound the body read, then classify.
            let text = match tokio::time::timeout(req.connect_timeout, response.text()).await {
                Ok(Ok(t)) => t,
                _ => String::new(),
            };
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

        let (tx, rx) = mpsc::channel(STREAM_BUFFER);
        tokio::spawn(pump_stream(
            response,
            tx,
            req.first_chunk_timeout,
            req.chunk_stall_timeout,
            format,
            req.model.clone(),
        ));
        UpstreamResponse::Stream { status, events: rx }
    }
}

/// HTTP 429 is the signal to cool a specific key (short backoff) and try the
/// next one. Pre-SSE streaming errors arrive as `Json`, so this covers both the
/// non-streaming and streaming paths.
fn is_rate_limited(resp: &UpstreamResponse) -> bool {
    matches!(resp, UpstreamResponse::Json { status: 429, .. })
}

/// HTTP 401/403 is a *key* fault (a dead/invalid credential), not a request
/// fault: retire that key for the long auth cooldown and rotate to the next
/// healthy key. Like 429, pre-SSE streaming errors arrive as `Json`, so both
/// paths are covered. Request-scoped errors (400/404/422) are neither key
/// faults nor rate limits — they fail fast without rotating keys.
fn is_key_fault(resp: &UpstreamResponse) -> bool {
    matches!(
        resp,
        UpstreamResponse::Json {
            status: 401 | 403,
            ..
        }
    )
}

/// A stable identifier for sticky proxy affinity, derived from the credential.
fn secret_identifier(secret: &SecretRef) -> String {
    match secret {
        SecretRef::Env(name) => format!("env:{name}"),
        SecretRef::File(path) => format!("file:{path}"),
        SecretRef::Literal(value) => value.clone(),
        SecretRef::Empty => String::new(),
    }
}

#[async_trait]
impl UpstreamClient for HttpUpstream {
    async fn execute(&self, req: UpstreamRequest) -> UpstreamResponse {
        let provider = match self.registry.provider(&req.provider_id) {
            Some(p) => p,
            None => {
                return UpstreamResponse::Transport(transport(
                    TransportErrorKind::Connect,
                    format!("provider `{}` vanished from registry", req.provider_id),
                ))
            }
        };
        let format = provider.transport.format;

        // Shape the outgoing body for the provider's wire format.
        let mut body = if format == WireFormat::Anthropic {
            vibe_router_translate::openai_to_anthropic_request(&req.body)
        } else {
            vibe_router_translate::translate_request(
                WireFormat::OpenAi,
                WireFormat::OpenAi,
                req.body.clone(),
                req.stream,
            )
        };
        if req.stream {
            body["stream"] = serde_json::Value::Bool(true);
        }
        let req = UpstreamRequest { body, ..req };
        let base_url = provider.transport.base_url.clone();

        // No pool (no credential, or an empty one): single attempt, still
        // routed through a sticky proxy when one is registered.
        if provider.key_pool.is_empty() {
            let proxy = self.registry.proxies().select_sticky(&provider.id, None);
            let (client, url, relay) = self.route_for(proxy.as_ref(), &base_url);
            return self
                .run_once(&client, &req, None, &url, relay.as_deref(), format)
                .await;
        }

        // Multi-key pool: try healthy keys in order, each through its sticky
        // proxy. A 429 (short backoff) or 401/403 (long auth cooldown) cools
        // that specific key and we retry the next one transparently. Only when
        // every key is exhausted does the last key-fault status surface.
        let mut attempted: Vec<usize> = Vec::new();
        let mut last_key_error: Option<UpstreamResponse> = None;

        loop {
            let Some((idx, secret_ref)) = provider.key_pool.select_healthy_except(&attempted)
            else {
                return last_key_error.unwrap_or_else(|| {
                    UpstreamResponse::Transport(transport(
                        TransportErrorKind::NoCredential,
                        "all API keys in the pool are cooling down or exhausted".to_string(),
                    ))
                });
            };
            attempted.push(idx);

            let proxy = self
                .registry
                .proxies()
                .select_sticky(&secret_identifier(&secret_ref), None);
            let (client, url, relay) = self.route_for(proxy.as_ref(), &base_url);
            let resp = self
                .run_once(
                    &client,
                    &req,
                    Some(&secret_ref),
                    &url,
                    relay.as_deref(),
                    format,
                )
                .await;
            provider.key_pool.release_in_flight(idx);

            if is_rate_limited(&resp) {
                provider.key_pool.mark_cooldown(idx, None);
                tracing::warn!(
                    provider = %provider.id,
                    key_index = idx,
                    "upstream 429: key cooled down, retrying with next key"
                );
                last_key_error = Some(resp);
                continue;
            }

            if is_key_fault(&resp) {
                // Key fault: retire this key (long cooldown) and rotate. Log
                // only the key index — never the key material.
                provider.key_pool.mark_auth_fault(idx);
                tracing::warn!(
                    provider = %provider.id,
                    key_index = idx,
                    "upstream auth fault (401/403): key retired, retrying with next key"
                );
                last_key_error = Some(resp);
                continue;
            }

            // Only a 2xx resets the key's cooldown/backoff; a 5xx or a
            // request-scoped 4xx must not clear it.
            if matches!(&resp, UpstreamResponse::Json { status, .. } if (200..300).contains(status))
                || matches!(&resp, UpstreamResponse::Stream { .. })
            {
                provider.key_pool.mark_success(idx);
            }
            return resp;
        }
    }
}

impl HttpUpstream {
    async fn run_once(
        &self,
        client: &reqwest::Client,
        req: &UpstreamRequest,
        key: Option<&SecretRef>,
        url: &str,
        relay_target: Option<&str>,
        format: WireFormat,
    ) -> UpstreamResponse {
        if req.stream {
            self.execute_stream(client, req, key, url, relay_target, format)
                .await
        } else {
            self.execute_json(client, req, key, url, relay_target, format)
                .await
        }
    }
}

/// Copy bytes off the upstream, decode SSE, forward payloads (converting
/// Anthropic events to OpenAI chunks when the upstream speaks Anthropic).
async fn pump_stream(
    mut response: reqwest::Response,
    tx: mpsc::Sender<StreamEvent>,
    first_chunk_timeout: Duration,
    stall_timeout: Duration,
    format: WireFormat,
    model: String,
) {
    let mut decoder = SseDecoder::default();
    let mut anth_stream = vibe_router_translate::AnthropicToOpenAiStream::new(model);
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
            Ok(Ok(None)) => break,
            Ok(Ok(Some(bytes))) => bytes,
        };
        first = false;
        for ev in decoder.push(&chunk) {
            if format == WireFormat::Anthropic {
                let is_stop = ev.data.contains("\"message_stop\"");
                for openai_chunk in anth_stream.push(&ev.data) {
                    if forward(&tx, openai_chunk).await.is_err() {
                        return;
                    }
                }
                if is_stop && forward(&tx, "[DONE]".to_string()).await.is_err() {
                    return;
                }
            } else if forward(&tx, ev.data).await.is_err() {
                return;
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
        if format == WireFormat::Anthropic {
            for openai_chunk in anth_stream.push(&ev.data) {
                if forward(&tx, openai_chunk).await.is_err() {
                    return;
                }
            }
        } else if forward(&tx, ev.data).await.is_err() {
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

/// Shared client builder: one pool, no implicit timeouts (budgets are enforced
/// explicitly at every await).
pub fn default_client() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(8)
        .build()
        .expect("reqwest client")
}
