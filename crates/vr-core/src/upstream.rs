//! The upstream-call abstraction.
//!
//! `vr-routing` drives requests through the [`UpstreamClient`] trait defined
//! here, so the routing core never names `reqwest` or any concrete provider.
//! `vr-providers` supplies the only v0.1 implementation (generic
//! OpenAI-compatible HTTP); a custom provider with its own protocol is a
//! second implementation registered by provider id — the extension point
//! stays open without the core knowing about it.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

/// One fully-prepared attempt against a specific provider.
///
/// Built by the routing driver: `body` is the client request with `model`
/// already rewritten to the upstream id, and the timeouts come from config.
#[derive(Debug, Clone)]
pub struct UpstreamRequest {
    pub provider_id: String,
    /// Upstream model id (after any alias rewrite).
    pub model: String,
    pub body: Value,
    pub stream: bool,
    /// Abort if upstream response headers do not arrive in time.
    pub connect_timeout: Duration,
    /// Abort if the first streamed chunk does not arrive in time.
    pub first_chunk_timeout: Duration,
    /// Abort if chunks stall mid-stream for longer than this.
    pub chunk_stall_timeout: Duration,
}

/// Token accounting extracted from a completed response.
///
/// The `estimated` flag exists because an estimate must never be presented
/// as authoritative: providers that do not report usage (or report it only
/// on streams we did not read to the end) leave us counting characters, and
/// logs and the UI have to be able to tell the two apart.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    /// True when these numbers were estimated by us rather than reported by
    /// the provider. Prompt tokens are almost always 0 when estimated — we
    /// only see the outgoing body, not the provider's tokenization.
    pub estimated: bool,
}

impl Usage {
    pub fn is_zero(&self) -> bool {
        self.prompt_tokens == 0 && self.completion_tokens == 0
    }

    /// Usage exactly as the provider reported it.
    pub fn reported(prompt_tokens: u32, completion_tokens: u32) -> Self {
        Self {
            prompt_tokens,
            completion_tokens,
            estimated: false,
        }
    }

    /// Usage we inferred ourselves; callers that display totals must say so.
    pub fn estimated(prompt_tokens: u32, completion_tokens: u32) -> Self {
        Self {
            prompt_tokens,
            completion_tokens,
            estimated: true,
        }
    }
}

/// A single event on a streaming response.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// One SSE `data:` payload, already unwrapped (JSON text or `[DONE]`).
    Chunk(String),
    /// Upstream finished normally.
    Done,
    /// The stream failed mid-flight. Failover is no longer possible — the
    /// client has already received bytes — so this is terminal for the
    /// request and must be surfaced.
    Error(String),
}

/// How an upstream call failed at the transport level (no HTTP status).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportError {
    pub kind: TransportErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportErrorKind {
    /// Could not connect / DNS failure.
    Connect,
    /// Any timeout budget was exceeded.
    Timeout,
    /// The provider has no credential configured (a config problem, treated
    /// as a transient fallback: cools the candidate, shows in the UI).
    NoCredential,
}

/// The outcome of one upstream attempt.
pub enum UpstreamResponse {
    /// A complete JSON answer. `status` is the upstream HTTP status; 2xx
    /// means success.
    Json {
        status: u16,
        body: Value,
        usage: Option<Usage>,
    },
    /// Upstream accepted the request and is streaming. `status` is 2xx.
    /// The receiver yields chunks until [`StreamEvent::Done`] or
    /// [`StreamEvent::Error`]. The channel is bounded on purpose: it applies
    /// backpressure to a slow reader and, when the receiver is dropped
    /// (client disconnect), the producer sees it and tears the upstream
    /// connection down.
    Stream {
        status: u16,
        events: tokio::sync::mpsc::Receiver<StreamEvent>,
    },
    /// Never reached a parseable HTTP response.
    Transport(TransportError),
}

impl std::fmt::Debug for UpstreamResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpstreamResponse::Json { status, .. } => write!(f, "Json(status={status})"),
            UpstreamResponse::Stream { status, .. } => write!(f, "Stream(status={status})"),
            UpstreamResponse::Transport(e) => f.debug_tuple("Transport").field(e).finish(),
        }
    }
}

/// Executes prepared requests. Implementations must not decide routing
/// policy — they perform one attempt and report what happened.
#[async_trait]
pub trait UpstreamClient: Send + Sync {
    async fn execute(&self, req: UpstreamRequest) -> UpstreamResponse;
}
