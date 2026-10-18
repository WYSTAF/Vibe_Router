//! The failover engine.
//!
//! One pass: reserve candidates through health, attempt them in order,
//! interpret failures through the classifier, and either return a response,
//! hand back a client error, or fail over. Rate limiting participates in the
//! same walk: a full window makes a candidate unreservable, and if *only*
//! window-limited models remain we wait for the earliest reopen rather than
//! returning 429 — waiting beats provoking.
//!
//! Note on model ids: a candidate's `model` is `provider/upstream-model`, and
//! the upstream part may itself contain slashes (`openrouter/anthropic/claude-
//! sonnet-4`). Everything here splits on the **first** slash only.

use std::sync::Arc;
use std::time::Instant;

use tokio::time::sleep;
use vr_core::{
    Candidate, ErrorClassifier, ModelRegistry, Outcome, Resolution, RouterError, RouterResult,
    TransportErrorKind, UpstreamClient, UpstreamRequest, UpstreamResponse, Usage,
};
use vr_health::{HealthView, Usability};

use crate::policy::Policy;

/// A completed route: what to send to the client, and what we learned.
#[derive(Debug)]
pub struct Routed {
    /// The winning candidate (for logging/usage attribution).
    pub candidate: Candidate,
    /// The provider id that served it.
    pub provider: String,
    pub response: UpstreamResponse,
    /// Usage, when the response was non-streaming. Streaming usage is
    /// extracted by the server after the stream drains.
    pub usage: Option<Usage>,
    /// How many candidates were tried before the one that succeeded
    /// (0 = the first candidate served it).
    pub fell_back_past: usize,
}

/// The router. Generic over the concrete implementations the server wires in.
pub struct Router<R, H, C, U>
where
    R: ModelRegistry,
    H: HealthView,
    C: ErrorClassifier,
    U: UpstreamClient,
{
    registry: Arc<R>,
    health: Arc<H>,
    classifier: Arc<C>,
    upstream: Arc<U>,
    policy: Policy,
}

impl<R, H, C, U> Router<R, H, C, U>
where
    R: ModelRegistry,
    H: HealthView,
    C: ErrorClassifier,
    U: UpstreamClient,
{
    pub fn new(
        registry: Arc<R>,
        health: Arc<H>,
        classifier: Arc<C>,
        upstream: Arc<U>,
        policy: Policy,
    ) -> Self {
        Self {
            registry,
            health,
            classifier,
            upstream,
            policy,
        }
    }

    pub fn health(&self) -> &Arc<H> {
        &self.health
    }

    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// Route `body` (a client request already validated) to a provider.
    pub async fn route(&self, body: serde_json::Value) -> RouterResult<Routed> {
        let requested = body
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if requested.is_empty() {
            return Err(RouterError::BadRequest("missing `model` field".to_string()));
        }
        let stream = body
            .get("stream")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let mut candidates = match self.registry.resolve(&requested)? {
            Resolution::Single(c) => vec![c],
            Resolution::Chain(cs) if cs.is_empty() => {
                return Err(RouterError::ModelNotFound {
                    requested,
                    detail: "combo is empty".into(),
                })
            }
            Resolution::Chain(cs) => cs,
        };

        // Vibe-Link Judge Retry: rotate candidates so retry attempts the next fallback candidate
        if body
            .get("_vibe_retry")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
            && candidates.len() > 1
        {
            candidates.rotate_left(1);
        }

        let deadline = Instant::now() + self.policy.request_budget;
        let mut reopen: Option<Instant> = None;
        let mut last_status: u16 = 0;
        let mut last_detail = String::from("no candidate was available");

        for pass in 0..2u32 {
            let mut reserved: Vec<Candidate> = Vec::with_capacity(candidates.len());
            reopen = None;

            for cand in &candidates {
                if Instant::now() >= deadline {
                    break;
                }
                match self.health.reserve(cand) {
                    Usability::Ready => reserved.push(cand.clone()),
                    Usability::Cooldown(t) | Usability::RateLimited(t) => {
                        reopen = Some(match reopen {
                            Some(x) => x.min(t),
                            None => t,
                        });
                    }
                }
            }

            for (i, cand) in reserved.iter().enumerate() {
                if Instant::now() >= deadline {
                    last_status = 504;
                    last_detail = "request budget exhausted".to_string();
                    break;
                }
                match self.attempt(cand, &body, stream, deadline).await {
                    Ok(mut routed) => {
                        routed.fell_back_past = i;
                        return Ok(routed);
                    }
                    // A client error stops the chain: the next provider would
                    // see the same bad request, so do not punish them for it.
                    Err(err @ RouterError::Upstream { .. }) => return Err(err),
                    Err(err) => {
                        last_status = err.status();
                        last_detail = err.message();
                        tracing::warn!(candidate = %cand, "falling over: {err}");
                    }
                }
            }

            // Nothing succeeded. If every candidate was blocked only by a
            // rate-limit window that reopens inside the budget, wait rather
            // than returning 429 — that is "wait instead of provoking"
            // applied across the whole chain.
            if pass == 0 {
                if let Some(until) = reopen {
                    let wait = until.saturating_duration_since(Instant::now());
                    if !wait.is_zero() && Instant::now() + wait < deadline {
                        tracing::info!("all candidates rate-limited; waiting {wait:?}");
                        sleep(wait).await;
                        continue;
                    }
                }
            }
            break;
        }

        if last_status == 429 || (last_status == 0 && reopen.is_some()) {
            return Err(RouterError::RateLimited {
                requested,
                retry_after_secs: self.retry_after(&candidates),
                detail: last_detail,
            });
        }
        // Propagate a chain-wide timeout as 504 — masking it as 503 would
        // hide "the upstream never answered" behind "try another provider".
        if last_status == 504 {
            return Err(RouterError::Timeout {
                requested,
                detail: last_detail,
            });
        }
        Err(RouterError::AllUnavailable {
            requested,
            detail: last_detail,
        })
    }

    /// Attempt one candidate: same-candidate retries per the classifier's
    /// table, then classify the final failure and record it into health.
    async fn attempt(
        &self,
        cand: &Candidate,
        body: &serde_json::Value,
        stream: bool,
        deadline: Instant,
    ) -> RouterResult<Routed> {
        let (provider, upstream_model) =
            split_model(&cand.model).ok_or_else(|| RouterError::ModelNotFound {
                requested: cand.model.clone(),
                detail: "malformed `provider/model` id".into(),
            })?;

        // Rewrite the model id for the upstream; the server layer restores the
        // client's own string in the response body.
        let mut upstream_body = body.clone();
        upstream_body["model"] = serde_json::Value::String(upstream_model.to_string());

        let req = UpstreamRequest {
            provider_id: provider.to_string(),
            model: upstream_model.to_string(),
            body: upstream_body,
            stream,
            connect_timeout: self.policy.connect_timeout,
            first_chunk_timeout: self.policy.first_chunk_timeout,
            chunk_stall_timeout: self.policy.chunk_stall_timeout,
        };

        let mut failures = 0u32;
        // Assigned on every non-Ok path through the loop before the read
        // below the loop; left uninitialized until then.
        let mut status;
        let mut detail;
        let mut timed_out;

        loop {
            if Instant::now() >= deadline {
                timed_out = true;
                status = 504;
                detail = "request budget exhausted".to_string();
                break;
            }

            let response = self.upstream.execute(req.clone()).await;
            match classify_response(&response) {
                ResponseClass::Ok => {
                    let usage = match &response {
                        UpstreamResponse::Json { usage, .. } => *usage,
                        _ => None,
                    };
                    self.health.record(cand, Outcome::Success);
                    return Ok(Routed {
                        candidate: cand.clone(),
                        provider: provider.to_string(),
                        response,
                        usage,
                        fell_back_past: 0,
                    });
                }
                ResponseClass::Fail { status: s, body: m } => {
                    status = s;
                    detail = m;
                    timed_out = false;
                }
                ResponseClass::Transport { timeout, message } => {
                    status = if timeout { 504 } else { 502 };
                    detail = message;
                    timed_out = timeout;
                }
            }

            failures += 1;
            // Retry the *same* candidate only while the retry table says to.
            if let Some(hint) = self.classifier.retry_for(status) {
                if failures <= hint.attempts {
                    let delay = hint.delay;
                    if Instant::now() + delay >= deadline {
                        timed_out = true;
                        break;
                    }
                    tracing::debug!(
                        candidate = %cand,
                        "retry {} of {} in {delay:?} (status {status})",
                        failures,
                        hint.attempts
                    );
                    sleep(delay).await;
                    continue;
                }
            }
            break;
        }

        // Out of retries (or none configured): classify, record, report.
        let outcome = if timed_out {
            Outcome::TimedOut
        } else {
            let prev_level = self.health.backoff_level(&cand.model);
            self.classifier.classify(status, &detail, prev_level)
        };
        self.health.record(cand, outcome.clone());

        match outcome {
            Outcome::ClientError { status } => Err(RouterError::Upstream {
                status,
                message: truncate(&detail),
            }),
            Outcome::TimedOut => Err(RouterError::Timeout {
                requested: cand.model.clone(),
                detail: truncate(&detail),
            }),
            Outcome::Fallback(f) => Err(RouterError::BadGateway {
                provider: provider.to_string(),
                detail: format!(
                    "{} (upstream {}): {}",
                    cand.model,
                    f.status,
                    truncate(&detail)
                ),
            }),
            Outcome::Success => unreachable!("a success returns before classification"),
        }
    }

    /// The soonest any candidate reopens, reported honestly to the client.
    fn retry_after(&self, candidates: &[Candidate]) -> Option<u32> {
        let mut earliest: Option<Instant> = None;
        for cand in candidates {
            match self.health.peek(cand) {
                Usability::Cooldown(t) | Usability::RateLimited(t) => {
                    earliest = Some(match earliest {
                        Some(x) => x.min(t),
                        None => t,
                    });
                }
                Usability::Ready => {}
            }
        }
        earliest.map(|t| t.saturating_duration_since(Instant::now()).as_secs().max(1) as u32)
    }
}

/// `provider/upstream-model` split on the **first** slash.
fn split_model(model: &str) -> Option<(&str, &str)> {
    let (provider, rest) = model.split_once('/')?;
    if provider.is_empty() || rest.is_empty() {
        return None;
    }
    Some((provider, rest))
}

/// How an upstream response maps onto the attempt loop's control flow.
enum ResponseClass {
    /// 2xx — deliver it.
    Ok,
    /// An HTTP error: status + the message extracted from the body.
    Fail { status: u16, body: String },
    /// Never got a parseable HTTP response.
    Transport { timeout: bool, message: String },
}

fn classify_response(resp: &UpstreamResponse) -> ResponseClass {
    match resp {
        UpstreamResponse::Json { status, body, .. } => {
            if (200..300).contains(status) {
                ResponseClass::Ok
            } else {
                ResponseClass::Fail {
                    status: *status,
                    body: extract_error_message(body),
                }
            }
        }
        UpstreamResponse::Stream { .. } => ResponseClass::Ok,
        UpstreamResponse::Transport(e) => ResponseClass::Transport {
            timeout: matches!(e.kind, TransportErrorKind::Timeout),
            message: format!("{}: {}", transport_kind(e.kind), e.message),
        },
    }
}

fn transport_kind(kind: TransportErrorKind) -> &'static str {
    match kind {
        TransportErrorKind::Connect => "connect",
        TransportErrorKind::Timeout => "timeout",
        TransportErrorKind::NoCredential => "no-credential",
    }
}

/// Pull `error.message` out of an OpenAI-shaped error body, falling back to a
/// compact rendering so the classifier's text rules still see the provider's
/// own wording ("overloaded", "quota exceeded", …).
fn extract_error_message(body: &serde_json::Value) -> String {
    if let Some(m) = body.pointer("/error/message").and_then(|m| m.as_str()) {
        return m.to_string();
    }
    if let Some(s) = body.as_str() {
        return s.to_string();
    }
    body.to_string()
}

fn truncate(s: &str) -> String {
    const MAX: usize = 300;
    if s.chars().count() <= MAX {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(MAX).collect();
        out.push('…');
        out
    }
}

#[cfg(test)]
mod tests;
