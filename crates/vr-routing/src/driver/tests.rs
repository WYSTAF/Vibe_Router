//! Mock-driven unit tests for the routing engine: every layer of the loop is
//! exercised without a network, a config file, or a real provider.
//!
//! The mocks are shared with the integration tests via `pub` re-export from
//! the crate root (`vr_routing::testing`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use vr_core::{
    Candidate, ErrorClassifier, ModelInfo, ModelRegistry, Outcome, Resolution, RetryHint,
    RouterError, TransportError, TransportErrorKind, UpstreamClient, UpstreamRequest,
    UpstreamResponse, Usage,
};
use vr_health::NoopHealth;

use super::{Routed, Router};
use crate::policy::Policy;

// -- mock registry ---------------------------------------------------------

pub struct MapRegistry {
    chains: HashMap<String, Vec<String>>,
}

impl MapRegistry {
    pub fn new(chains: HashMap<String, Vec<String>>) -> Self {
        Self { chains }
    }
}

impl ModelRegistry for MapRegistry {
    fn resolve(&self, model: &str) -> Result<Resolution, RouterError> {
        if let Some(chain) = self.chains.get(model) {
            let cands: Vec<Candidate> = chain
                .iter()
                .enumerate()
                .map(|(i, m)| Candidate::combo(model, m).with_index(i))
                .collect();
            return Ok(Resolution::Chain(cands));
        }
        if model.contains('/') {
            return Ok(Resolution::Single(Candidate::direct(model)));
        }
        Err(RouterError::ModelNotFound {
            requested: model.to_string(),
            detail: "unknown model or combo".into(),
        })
    }

    fn list(&self) -> Vec<ModelInfo> {
        Vec::new()
    }
}

// -- mock upstream ---------------------------------------------------------

/// Scripted responses per model: each call pops the next scripted answer.
pub struct ScriptedUpstream {
    scripts: Mutex<HashMap<String, Vec<Script>>>,
    pub calls: AtomicUsize,
}

#[derive(Clone, Debug)]
pub enum Script {
    Json(u16, serde_json::Value),
    Transport { timeout: bool, msg: String },
}

impl ScriptedUpstream {
    pub fn new(scripts: HashMap<String, Vec<Script>>) -> Self {
        Self {
            scripts: Mutex::new(scripts),
            calls: AtomicUsize::new(0),
        }
    }

    fn next(&self, model: &str) -> Script {
        let mut g = self.scripts.lock();
        let list = g.get_mut(model).expect("no script for model");
        if list.len() > 1 {
            list.remove(0)
        } else {
            list[0].clone() // repeat the last scripted answer
        }
    }
}

#[async_trait::async_trait]
impl UpstreamClient for ScriptedUpstream {
    async fn execute(&self, req: UpstreamRequest) -> UpstreamResponse {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.next(&req.model) {
            Script::Json(status, body) => UpstreamResponse::Json {
                status,
                body,
                usage: Some(Usage::reported(1, 2)),
            },
            Script::Transport { timeout, msg } => UpstreamResponse::Transport(TransportError {
                kind: if timeout {
                    TransportErrorKind::Timeout
                } else {
                    TransportErrorKind::Connect
                },
                message: msg,
            }),
        }
    }
}

// -- mock classifier -------------------------------------------------------

/// Default table: 400/404 are client errors; 429 and 5xx fall back; 502 also
/// retries once at 1ms.
pub struct TestClassifier;

impl ErrorClassifier for TestClassifier {
    fn classify(&self, status: u16, _body: &str, _prev: u32) -> Outcome {
        match status {
            400 | 404 => Outcome::ClientError { status },
            _ => Outcome::fallback(status),
        }
    }
    fn retry_for(&self, status: u16) -> Option<RetryHint> {
        if status == 502 {
            Some(RetryHint {
                attempts: 1,
                delay: Duration::from_millis(1),
            })
        } else {
            None
        }
    }
}

// -- helpers ----------------------------------------------------------------

fn ok_body(model: &str) -> serde_json::Value {
    serde_json::json!({
        "id": "cmpl-1", "object": "chat.completion", "model": model,
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"},
                     "finish_reason": "stop"}],
    })
}

fn err_body(msg: &str) -> serde_json::Value {
    serde_json::json!({"error": {"message": msg}})
}

fn router_with(
    chains: HashMap<String, Vec<String>>,
    scripts: HashMap<String, Vec<Script>>,
    policy: Policy,
) -> Router<MapRegistry, NoopHealth, TestClassifier, ScriptedUpstream> {
    let registry = Arc::new(MapRegistry::new(chains));
    let upstream = Arc::new(ScriptedUpstream::new(scripts));
    Router::new(
        registry,
        Arc::new(NoopHealth),
        Arc::new(TestClassifier),
        upstream,
        policy,
    )
}

fn quick_policy() -> Policy {
    Policy {
        request_budget: Duration::from_secs(5),
        ..Policy::default()
    }
}

async fn route_ok(r: &Router<MapRegistry, NoopHealth, TestClassifier, ScriptedUpstream>) -> Routed {
    let body = serde_json::json!({"model": "p/m", "messages": []});
    r.route(body).await.expect("route should succeed")
}

// -- tests -------------------------------------------------------------------

#[tokio::test]
async fn first_candidate_success_short_circuits() {
    let mut chains = HashMap::new();
    chains.insert("p/m".to_string(), vec!["p/m".to_string()]);
    let mut scripts = HashMap::new();
    scripts.insert("m".to_string(), vec![Script::Json(200, ok_body("m"))]);
    let r = router_with(chains, scripts, quick_policy());
    let routed = route_ok(&r).await;
    assert_eq!(routed.provider, "p");
    assert_eq!(routed.fell_back_past, 0);
    assert!(routed.usage.is_some());
}

#[tokio::test]
async fn combo_walks_to_second_candidate_on_503() {
    let mut chains = HashMap::new();
    chains.insert(
        "chain".to_string(),
        vec!["a/x".to_string(), "b/y".to_string()],
    );
    let mut scripts = HashMap::new();
    scripts.insert(
        "x".to_string(),
        vec![Script::Json(503, err_body("overloaded"))],
    );
    scripts.insert("y".to_string(), vec![Script::Json(200, ok_body("y"))]);
    let r = router_with(chains, scripts, quick_policy());
    let body = serde_json::json!({"model": "chain", "messages": []});
    let routed = r.route(body).await.unwrap();
    assert_eq!(routed.provider, "b");
    assert_eq!(routed.fell_back_past, 1);
}

#[tokio::test]
async fn same_candidate_retries_before_falling_over() {
    let mut chains = HashMap::new();
    chains.insert("p/m".to_string(), vec!["p/m".to_string()]);
    let mut scripts = HashMap::new();
    // first attempt 502 (retryable), second 200.
    scripts.insert(
        "m".to_string(),
        vec![
            Script::Json(502, err_body("bad gateway")),
            Script::Json(200, ok_body("m")),
        ],
    );
    let r = router_with(chains, scripts, quick_policy());
    let routed = route_ok(&r).await;
    assert_eq!(routed.fell_back_past, 0);
}

#[tokio::test]
async fn client_error_stops_the_chain() {
    let mut chains = HashMap::new();
    chains.insert(
        "chain".to_string(),
        vec!["a/x".to_string(), "b/y".to_string()],
    );
    let mut scripts = HashMap::new();
    scripts.insert(
        "x".to_string(),
        vec![Script::Json(400, err_body("bad params"))],
    );
    scripts.insert("y".to_string(), vec![Script::Json(200, ok_body("y"))]);
    let r = router_with(chains, scripts, quick_policy());
    let body = serde_json::json!({"model": "chain", "messages": []});
    let err = r.route(body).await.unwrap_err();
    // Passed through with the upstream's own status and message.
    assert_eq!(err.status(), 400);
    assert_eq!(err.message(), "bad params");
}

#[tokio::test]
async fn transport_failure_falls_over() {
    let mut chains = HashMap::new();
    chains.insert(
        "chain".to_string(),
        vec!["a/x".to_string(), "b/y".to_string()],
    );
    let mut scripts = HashMap::new();
    scripts.insert(
        "x".to_string(),
        vec![Script::Transport {
            timeout: false,
            msg: "connection refused".into(),
        }],
    );
    scripts.insert("y".to_string(), vec![Script::Json(200, ok_body("y"))]);
    let r = router_with(chains, scripts, quick_policy());
    let body = serde_json::json!({"model": "chain", "messages": []});
    let routed = r.route(body).await.unwrap();
    assert_eq!(routed.provider, "b");
}

#[tokio::test]
async fn all_fail_reports_all_unavailable() {
    let mut chains = HashMap::new();
    chains.insert(
        "chain".to_string(),
        vec!["a/x".to_string(), "b/y".to_string()],
    );
    let mut scripts = HashMap::new();
    scripts.insert("x".to_string(), vec![Script::Json(503, err_body("nope"))]);
    scripts.insert("y".to_string(), vec![Script::Json(500, err_body("nope"))]);
    let r = router_with(chains, scripts, quick_policy());
    let body = serde_json::json!({"model": "chain", "messages": []});
    let err = r.route(body).await.unwrap_err();
    assert_eq!(err.status(), 503);
    assert!(err.message().contains("chain"));
}

#[tokio::test]
async fn unknown_model_is_not_found() {
    let mut chains = HashMap::new();
    chains.insert("real".to_string(), vec!["p/m".to_string()]);
    let r = router_with(chains, HashMap::new(), quick_policy());
    let body = serde_json::json!({"model": "nope", "messages": []});
    let err = r.route(body).await.unwrap_err();
    assert_eq!(err.status(), 404);
}

#[tokio::test]
async fn provider_models_with_inner_slashes_split_on_first_only() {
    // "openrouter/anthropic/claude-sonnet-4" -> provider "openrouter",
    // upstream id "anthropic/claude-sonnet-4".
    let mut chains = HashMap::new();
    chains.insert(
        "or/x".to_string(),
        vec!["openrouter/anthropic/claude-sonnet-4".to_string()],
    );
    let mut scripts = HashMap::new();
    scripts.insert(
        "anthropic/claude-sonnet-4".to_string(),
        vec![Script::Json(200, ok_body("x"))],
    );
    let r = router_with(chains, scripts, quick_policy());
    let body = serde_json::json!({"model": "or/x", "messages": []});
    let routed = r.route(body).await.unwrap();
    assert_eq!(routed.provider, "openrouter");
}

#[tokio::test]
async fn timed_out_candidate_falls_over_and_times_out_chain() {
    let mut chains = HashMap::new();
    chains.insert("p/m".to_string(), vec!["p/m".to_string()]);
    let mut scripts = HashMap::new();
    scripts.insert(
        "m".to_string(),
        vec![Script::Transport {
            timeout: true,
            msg: "stalled".into(),
        }],
    );
    let r = router_with(chains, scripts, quick_policy());
    let body = serde_json::json!({"model": "p/m", "messages": []});
    let err = r.route(body).await.unwrap_err();
    assert_eq!(err.status(), 504);
}
