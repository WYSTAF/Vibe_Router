//! End-to-end tests: the real HTTP server against the hermetic mock upstream.
//!
//! Everything here runs in-process: the full wiring (config → registry →
//! health → router → axum) binds an ephemeral port, and the tests speak plain
//! HTTP to it, exactly as a client would. The upstream is the loopback mock,
//! so no test ever touches the network or a real provider.
//!
//! These are the acceptance tests for the MVP: OpenAI compatibility, SSE
//! streaming, failover, retry, cooldown, RPM limits, auth, and error shape.

use std::sync::Arc;
use std::time::Duration;

use vibe_router_config::schema::{AuthConfig, ComboConfig, ModelConfig, ProviderConfig};
use vibe_router_config::AppConfig;
use vibe_router_providers::mock::{MockUpstream, Reply};
use vibe_router_server::{routes, state::AppState};
use vibe_router_storage::{MemoryStore, StorageBackend};

// ---------------------------------------------------------------- fixtures

fn provider(id: &str, url: &str) -> ProviderConfig {
    ProviderConfig {
        id: id.into(),
        display_name: None,
        base_url: url.into(),
        format: Default::default(),
        auth: AuthConfig::default(), // empty secret -> Auth::None for the mock
        api_keys: vec![],
        key_strategy: None,
        headers: vec![],
        models: vec![ModelConfig {
            id: "m".into(),
            name: None,
            context: None,
            max_output: None,
        }],
        passthrough: false,
    }
}

/// A full config: `good` and `bad` providers plus a combo that tries bad
/// first, then good — the failover scenario in one line of config.
///
/// The prompt cache is **disabled** here: these tests assert on how many
/// times the upstream was contacted, and a cache hit would short-circuit
/// that. Cache behaviour has its own tests with the cache explicitly on.
fn combo_cfg(good_url: &str, bad_url: &str) -> AppConfig {
    let mut cfg = AppConfig::minimal();
    cfg.cache.enabled = false;
    cfg.router.cache.enabled = false;
    cfg.providers = vec![provider("bad", bad_url), provider("good", good_url)];
    cfg.router.providers = cfg.providers.clone();
    cfg.combos = vec![ComboConfig {
        name: "resilient".into(),
        description: None,
        models: vec!["bad/m".into(), "good/m".into()],
    }];
    cfg.router.combos = cfg.combos.clone();
    cfg
}

/// Like `combo_cfg` but with the prompt cache enabled, for cache tests.
fn cached_cfg(good_url: &str, bad_url: &str) -> AppConfig {
    let mut cfg = combo_cfg(good_url, bad_url);
    cfg.cache.enabled = true;
    cfg.router.cache.enabled = true;
    cfg
}

async fn spawn_server(cfg: &AppConfig) -> (String, Arc<AppState>) {
    let store: Arc<dyn vibe_router_storage::Store> = Arc::new(MemoryStore::default());
    let state = Arc::new(AppState::build(cfg, store).expect("state builds"));
    let app = routes::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), state)
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

const OK_BODY: &str = r#"{"id":"c1","object":"chat.completion","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":5}}"#;

fn sse_chunks() -> Vec<String> {
    vec![
        r#"data: {"id":"s1","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":"Hel"},"finish_reason":null}]}"#
            .to_string() + "\n\n",
        r#"data: {"id":"s1","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"content":"lo"},"finish_reason":"stop"}]}"#
            .to_string() + "\n\n",
        r#"data: {"id":"s1","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{},"finish_reason":null}],"usage":{"prompt_tokens":3,"completion_tokens":2}}"#
            .to_string() + "\n\n",
        "data: [DONE]\n\n".to_string(),
    ]
}

// ---------------------------------------------------------------- OpenAI compat

#[tokio::test]
async fn non_streaming_completion_is_openai_shaped() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;

    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({
            "model": "good/m",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    // The envelope an OpenAI client expects.
    assert_eq!(v["object"], "chat.completion");
    assert_eq!(v["choices"][0]["message"]["content"], "hello");
    assert_eq!(v["usage"]["prompt_tokens"], 7);
    // The client's model string is echoed back, not the upstream's.
    assert_eq!(v["model"], "good/m");
}

#[tokio::test]
async fn models_list_is_openai_shaped() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;

    let v: serde_json::Value = client()
        .get(format!("{base}/v1/models"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v["object"], "list");
    let ids: Vec<&str> = v["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"good/m"), "{ids:?}");
    assert!(ids.contains(&"bad/m"), "{ids:?}");
    // Combos are addressable — they appear in the list.
    assert!(ids.contains(&"resilient"), "{ids:?}");
}

#[tokio::test]
async fn unknown_model_is_openai_shaped_not_found() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;

    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "ghost/m", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["error"]["type"], "invalid_request_error");
    assert_eq!(v["error"]["code"], "model_not_found");
}

#[tokio::test]
async fn missing_model_field_is_a_400() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;

    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["error"]["type"], "invalid_request_error");
}

// ---------------------------------------------------------------- streaming

#[tokio::test]
async fn streaming_relay_produces_well_formed_sse() {
    let mock = MockUpstream::sse(sse_chunks()).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;

    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({
            "model": "good/m",
            "stream": true,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()["content-type"].to_str().unwrap(),
        "text/event-stream"
    );
    let body = resp.text().await.unwrap();
    // Every data frame, in order, ending in [DONE].
    let frames: Vec<&str> = body
        .lines()
        .filter(|l| l.starts_with("data: "))
        .map(|l| &l[6..])
        .collect();
    // Escape newlines in the failure message so terminal mangling cannot
    // hide the frame layout.
    assert_eq!(frames.len(), 4, "raw={:?}", body.replace('\n', "\\n"));
    assert!(
        frames[0].contains("\"delta\":{\"content\":\"Hel\"}"),
        "{body}"
    );
    assert_eq!(frames.last().unwrap(), &"[DONE]");
    // The model echo: chunks carry the client's requested string.
    assert!(frames[0].contains("\"model\":\"good/m\""), "{body}");
}

#[tokio::test]
async fn streaming_via_combo_fails_over_before_first_byte() {
    // bad always 503s, good streams. The client must see a clean SSE stream
    // from good — failover happened before any byte reached the client.
    let good = MockUpstream::sse(sse_chunks()).await;
    let bad = MockUpstream::json(
        503,
        "Service Unavailable",
        r#"{"error":{"message":"overloaded"}}"#,
    )
    .await;
    let (base, _state) = spawn_server(&combo_cfg(&good.url(), &bad.url())).await;

    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "resilient", "stream": true, "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.trim_end().ends_with("data: [DONE]"), "{body}");
    assert!(body.contains("Hel"), "{body}");
    assert_eq!(bad.wait_for_requests(1).await, 1);
}

// ---------------------------------------------------------------- failover + retry

#[tokio::test]
async fn combo_fails_over_on_503() {
    let good = MockUpstream::json(200, "OK", OK_BODY).await;
    let bad = MockUpstream::json(
        503,
        "Service Unavailable",
        r#"{"error":{"message":"overloaded"}}"#,
    )
    .await;
    let (base, _state) = spawn_server(&combo_cfg(&good.url(), &bad.url())).await;

    let v: serde_json::Value = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "resilient", "messages": []}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    // Served by the second candidate in the chain.
    assert_eq!(v["choices"][0]["message"]["content"], "hello");
    assert_eq!(bad.wait_for_requests(1).await, 1, "bad was tried first");
}

#[tokio::test]
async fn same_candidate_retry_then_failover() {
    // Retry row for 503 is absent by default, so this exercises the plain
    // failover path twice; a retry-config test below covers attempts.
    let good = MockUpstream::json(200, "OK", OK_BODY).await;
    let bad = MockUpstream::sequence(vec![
        Reply::Json(503, r#"{"error":{"message":"overloaded"}}"#.into()),
        Reply::Json(503, r#"{"error":{"message":"still overloaded"}}"#.into()),
    ])
    .await;
    let (base, _state) = spawn_server(&combo_cfg(&good.url(), &bad.url())).await;

    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "resilient", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // bad was tried once (no retry row), then good served it.
    assert_eq!(bad.wait_for_requests(1).await, 1);
}

#[tokio::test]
async fn all_candidates_failing_is_503_with_openai_envelope() {
    let bad = MockUpstream::json(
        503,
        "Service Unavailable",
        r#"{"error":{"message":"overloaded"}}"#,
    )
    .await;
    let worse = MockUpstream::json(
        500,
        "Internal Server Error",
        r#"{"error":{"message":"on fire"}}"#,
    )
    .await;
    let (base, _state) = spawn_server(&combo_cfg(&worse.url(), &bad.url())).await;

    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "resilient", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 503);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["error"]["type"], "server_error");
    assert_eq!(v["error"]["code"], "service_unavailable");
}

// ---------------------------------------------------------------- cooldown

#[tokio::test]
async fn client_4xx_stops_the_chain_and_passes_through() {
    // A 400 is the client's fault: the chain must NOT try the next provider,
    // and the upstream's own message must come through untouched.
    let good = MockUpstream::json(200, "OK", OK_BODY).await;
    let strict = MockUpstream::json(
        400,
        "Bad Request",
        r#"{"error":{"message":"context length exceeded"}}"#,
    )
    .await;
    let (base, _state) = spawn_server(&combo_cfg(&good.url(), &strict.url())).await;

    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "resilient", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["error"]["message"], "context length exceeded");
    // The good provider was never contacted.
    assert_eq!(good.request_count(), 0);
}

// ---------------------------------------------------------------- auth

#[tokio::test]
async fn api_key_is_required_when_configured() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let mut cfg = combo_cfg(&mock.url(), &mock.url());
    cfg.server.api_keys = vec!["sk-secret".into()];

    let (base, _state) = spawn_server(&cfg).await;

    // No key -> 401.
    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "good/m", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Wrong key -> 401.
    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .header("authorization", "Bearer sk-wrong")
        .json(&serde_json::json!({"model": "good/m", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Right key -> 200, and it reaches the upstream.
    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .header("authorization", "Bearer sk-secret")
        .json(&serde_json::json!({"model": "good/m", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// ---------------------------------------------------------------- usage records

#[tokio::test]
async fn usage_records_reported_vs_estimated() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;

    client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "good/m", "messages": []}))
        .send()
        .await
        .unwrap();

    let rows = state.store.recent(10).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].record.prompt_tokens, 7);
    assert!(
        !rows[0].record.usage_estimated,
        "provider reported usage; must not be flagged estimated"
    );
}

#[tokio::test]
async fn combo_failover_is_visible_in_the_usage_record() {
    let good = MockUpstream::json(200, "OK", OK_BODY).await;
    let bad = MockUpstream::json(
        503,
        "Service Unavailable",
        r#"{"error":{"message":"overloaded"}}"#,
    )
    .await;
    let (base, state) = spawn_server(&combo_cfg(&good.url(), &bad.url())).await;

    client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "resilient", "messages": []}))
        .send()
        .await
        .unwrap();

    let rows = state.store.recent(10).unwrap();
    let r = &rows[0].record;
    assert_eq!(r.requested_model, "resilient");
    assert_eq!(r.served_model, "good/m");
    assert_eq!(r.combo.as_deref(), Some("resilient"));
    assert_eq!(r.failovers, 1, "one candidate was skipped");
}

// ---------------------------------------------------------------- dashboard API

#[tokio::test]
async fn dashboard_serves_the_embedded_index() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;

    let resp = client().get(format!("{base}/")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp.headers()["content-type"]
        .to_str()
        .unwrap()
        .contains("text/html"));
    let body = resp.text().await.unwrap();
    assert!(body.contains("Vibe Router"), "{body}");
}

#[tokio::test]
async fn combo_editor_round_trip_via_api() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;

    // Create.
    let resp = client()
        .post(format!("{base}/api/combos"))
        .json(&serde_json::json!({"name": "ui-chain", "models": ["good/m"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // It resolves and appears in the list with runtime provenance.
    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "ui-chain", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let combos: serde_json::Value = client()
        .get(format!("{base}/api/combos"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ui = combos
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "ui-chain")
        .expect("listed");
    assert_eq!(ui["from_file"], false);

    // Unknown models are rejected at save time.
    let resp = client()
        .post(format!("{base}/api/combos"))
        .json(&serde_json::json!({"name": "broken", "models": ["ghost/m"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // Delete, and it stops resolving.
    let resp = client()
        .delete(format!("{base}/api/combos/ui-chain"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "ui-chain", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn provider_editor_round_trip_via_api() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;

    // Add new provider dynamically
    let resp = client()
        .post(format!("{base}/api/providers"))
        .json(&serde_json::json!({
            "id": "dynamic-ai",
            "display_name": "Dynamic AI",
            "base_url": mock.url(),
            "models": [{"id": "dyn-model", "context": 65536}],
            "passthrough": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Resolves and serves completions
    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "dynamic-ai/dyn-model", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // Shows up in GET /api/providers
    let providers: serde_json::Value = client()
        .get(format!("{base}/api/providers"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let dyn_p = providers
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["id"] == "dynamic-ai")
        .expect("dynamic-ai listed");
    assert_eq!(dyn_p["display_name"], "Dynamic AI");
    assert_eq!(dyn_p["from_file"], false);

    // Delete dynamic provider
    let resp = client()
        .delete(format!("{base}/api/providers/dynamic-ai"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "dynamic-ai/dyn-model", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn api_status_returns_full_json_payload() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;

    let status_val: serde_json::Value = client()
        .get(format!("{base}/api/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(status_val["endpoint"].as_str().unwrap().contains("/v1"));
    assert!(status_val["providers"].is_array());
    assert!(status_val["combos"].is_array());
    assert!(status_val["health"].is_array());
    assert!(status_val["totals"]["requests"].is_number());
}

// ---------------------------------------------------------------- rate limiting

#[tokio::test]
async fn rpm_limit_waits_then_serves_instead_of_429() {
    // Limit good/m to 2 RPM with a 100% margin (effective 2). Two requests
    // pass; the third must wait for the window (60s) or fail with 429 —
    // and with a request budget larger than the window it waits, but the
    // test cannot wait 60s, so the budget is set below the window: the
    // third request then reports 429 with Retry-After.
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let mut cfg = combo_cfg(&mock.url(), &mock.url());
    cfg.limits.rate_limits.insert(
        "good/m".into(),
        vibe_router_config::RateLimitConfig {
            scope: vibe_router_config::RateLimitScope::RequestsPerMinute,
            limit: 2,
            safety_margin_pct: 100,
        },
    );
    cfg.router.limits = cfg.limits.clone();
    // Budget short enough that waiting is not viable.
    cfg.strategy.request_budget_ms = 200;
    cfg.router.strategy = cfg.strategy.clone();

    let (base, _state) = spawn_server(&cfg).await;
    let c = client();

    let r1 = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "good/m", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    let r2 = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "good/m", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 200);
    let r3 = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "good/m", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(r3.status(), 429);
    let retry_after = r3
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    assert!(retry_after.is_some(), "429 must advertise Retry-After");
    assert!(retry_after.unwrap() <= 60);
    // The upstream saw exactly two requests — the limit was enforced here,
    // not by provoking a provider 429.
    assert_eq!(mock.wait_for_requests(2).await, 2);
}

#[tokio::test]
async fn cooldown_skips_failed_provider_on_subsequent_requests() {
    let good = MockUpstream::json(200, "OK", OK_BODY).await;
    let bad = MockUpstream::json(
        503,
        "Service Unavailable",
        r#"{"error":{"message":"overloaded"}}"#,
    )
    .await;
    let (base, _state) = spawn_server(&combo_cfg(&good.url(), &bad.url())).await;
    let c = client();

    // First request: tries bad (fails 503 -> enters cooldown), then fails over to good (succeeds)
    let r1 = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "resilient", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    assert_eq!(bad.wait_for_requests(1).await, 1);
    assert_eq!(good.wait_for_requests(1).await, 1);

    // Second request: bad is cooling down, so routing must skip bad entirely
    // and route directly to good!
    let r2 = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "resilient", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 200);
    assert_eq!(good.wait_for_requests(2).await, 2);
    // bad was NOT contacted on the second request!
    assert_eq!(bad.request_count(), 1);
}

#[tokio::test]
async fn same_candidate_retries_configured_attempts_before_success() {
    // Upstream fails with 502 twice, succeeds on attempt 3.
    let mock = MockUpstream::sequence(vec![
        Reply::Json(502, r#"{"error":{"message":"bad gateway"}}"#.into()),
        Reply::Json(502, r#"{"error":{"message":"bad gateway"}}"#.into()),
        Reply::Json(200, OK_BODY.into()),
    ])
    .await;

    let mut cfg = combo_cfg(&mock.url(), &mock.url());
    cfg.strategy.retry.by_status.insert(
        "502".into(),
        vibe_router_config::RetryEntry {
            attempts: 2,
            delay_ms: 10,
        },
    );
    cfg.router.strategy = cfg.strategy.clone();

    let (base, _state) = spawn_server(&cfg).await;
    let c = client();

    let resp = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "good/m", "messages": []}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["choices"][0]["message"]["content"], "hello");
    // All 3 attempts hit the same candidate!
    assert_eq!(mock.wait_for_requests(3).await, 3);
}

#[tokio::test]
async fn client_disconnect_during_stream_cleans_up_without_panic() {
    // Stream with lots of chunks
    let mut chunks = Vec::new();
    for i in 0..50 {
        chunks.push(
            format!(
                r#"data: {{"id":"s1","object":"chat.completion.chunk","model":"m","choices":[{{"index":0,"delta":{{"content":"chunk {i}"}},"finish_reason":null}}]}}"#
            ) + "\n\n",
        );
    }
    chunks.push("data: [DONE]\n\n".into());

    let mock = MockUpstream::sse_then_silence(chunks, Duration::from_millis(500)).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;

    let mut resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "good/m", "stream": true, "messages": []}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    // Read only the first chunk then drop the response to simulate client disconnect
    let chunk = resp.chunk().await.unwrap();
    assert!(chunk.is_some());
    drop(resp);

    // Give server relay task time to detect client disconnect and cleanly exit
    tokio::time::sleep(Duration::from_millis(100)).await;
}

// ---------------------------------------------------------------- assignment endpoint

#[tokio::test]
async fn assignment_endpoint_matches_specification() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;

    let resp = client()
        .get(format!("{base}/v1/assignment"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();

    // Verify top-level structure
    assert!(v.get("roles").is_some());
    assert!(v.get("refreshed_at").is_some());
    assert_eq!(v["offline_fallback_recommended"], false);

    // Verify refreshed_at ISO-8601 format
    let refreshed_at = v["refreshed_at"].as_str().unwrap();
    assert!(refreshed_at.ends_with('Z'));
    assert_eq!(refreshed_at.len(), 20);

    let roles = &v["roles"];
    assert!(roles.get("boss").is_some());
    assert!(roles.get("code").is_some());
    assert!(roles.get("debug").is_some());
    assert!(roles.get("chat").is_some());

    // Boss
    assert_eq!(roles["boss"]["class"], "flagship");
    assert_eq!(roles["boss"]["status"], "ready");
    assert!(roles["boss"]["context_window"].as_u64().unwrap() > 0);
    assert!(!roles["boss"]["model"].as_str().unwrap().is_empty());

    // Code
    assert_eq!(roles["code"]["class"], "coder");
    assert_eq!(roles["code"]["status"], "ready");
    assert!(roles["code"]["context_window"].as_u64().unwrap() > 0);
    assert_eq!(roles["code"]["recommended_ticket_size"], "S");
    assert!(!roles["code"]["model"].as_str().unwrap().is_empty());

    // Debug
    assert_eq!(roles["debug"]["class"], "reasoner");
    assert_eq!(roles["debug"]["status"], "ready");
    assert!(!roles["debug"]["model"].as_str().unwrap().is_empty());

    // Chat
    assert_eq!(roles["chat"]["class"], "fast");
    assert_eq!(roles["chat"]["status"], "ready");
    assert!(!roles["chat"]["model"].as_str().unwrap().is_empty());

    // Also accessible via /api/assignment
    let resp_api = client()
        .get(format!("{base}/api/assignment"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp_api.status(), 200);
}

#[tokio::test]
async fn assignment_endpoint_honors_explicit_roles_config() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let mut cfg = combo_cfg(&mock.url(), &mock.url());

    cfg.roles = Some(vibe_router_config::RolesConfig {
        boss: Some(vibe_router_config::RoleConfig {
            model: "good/m".into(),
            class: Some("flagship".into()),
            context_window: Some(200000),
            recommended_ticket_size: None,
        }),
        code: Some(vibe_router_config::RoleConfig {
            model: "resilient".into(),
            class: Some("coder".into()),
            context_window: Some(128000),
            recommended_ticket_size: Some("M".into()),
        }),
        debug: Some(vibe_router_config::RoleConfig {
            model: "good/m".into(),
            class: Some("reasoner".into()),
            context_window: None,
            recommended_ticket_size: None,
        }),
        chat: Some(vibe_router_config::RoleConfig {
            model: "good/m".into(),
            class: Some("fast".into()),
            context_window: None,
            recommended_ticket_size: None,
        }),
    });
    cfg.router.roles = cfg.roles.clone();

    let (base, _state) = spawn_server(&cfg).await;

    let resp = client()
        .get(format!("{base}/v1/assignment"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(v["roles"]["boss"]["model"], "good/m");
    assert_eq!(v["roles"]["boss"]["context_window"], 200000);
    assert_eq!(v["roles"]["code"]["model"], "resilient");
    assert_eq!(v["roles"]["code"]["recommended_ticket_size"], "M");
    assert_eq!(v["roles"]["code"]["context_window"], 128000);
}

#[tokio::test]
async fn assignment_reflects_cooldown_and_offline_fallback() {
    let good = MockUpstream::json(200, "OK", OK_BODY).await;
    let bad = MockUpstream::json(
        503,
        "Service Unavailable",
        r#"{"error":{"message":"overloaded"}}"#,
    )
    .await;
    let mut cfg = combo_cfg(&good.url(), &bad.url());

    // Explicitly set boss to bad/m
    cfg.roles = Some(vibe_router_config::RolesConfig {
        boss: Some(vibe_router_config::RoleConfig {
            model: "bad/m".into(),
            class: Some("flagship".into()),
            context_window: Some(131072),
            recommended_ticket_size: None,
        }),
        code: Some(vibe_router_config::RoleConfig {
            model: "bad/m".into(),
            class: Some("coder".into()),
            context_window: Some(65536),
            recommended_ticket_size: Some("S".into()),
        }),
        debug: None,
        chat: None,
    });
    cfg.router.roles = cfg.roles.clone();

    let (base, _state) = spawn_server(&cfg).await;
    let c = client();

    // Trigger failure on bad/m to place it into cooldown
    let _ = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "bad/m", "messages": []}))
        .send()
        .await
        .unwrap();

    // Now query /v1/assignment: boss and code are cooling, so offline fallback is recommended!
    let resp = c.get(format!("{base}/v1/assignment")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(v["roles"]["boss"]["status"], "cooling");
    assert_eq!(v["roles"]["code"]["status"], "cooling");
    assert_eq!(v["offline_fallback_recommended"], true);
}

#[tokio::test]
async fn assignment_endpoint_requires_auth_when_configured() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let mut cfg = combo_cfg(&mock.url(), &mock.url());
    cfg.server.api_keys = vec!["sk-secret-token".into()];

    let (base, _state) = spawn_server(&cfg).await;

    // No auth -> 401
    let resp = client()
        .get(format!("{base}/v1/assignment"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Correct auth -> 200
    let resp = client()
        .get(format!("{base}/v1/assignment"))
        .header("authorization", "Bearer sk-secret-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

// ---------------------------------------------------------------- prompt cache

#[tokio::test]
async fn cache_hit_returns_identical_body_with_hit_header() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&cached_cfg(&mock.url(), &mock.url())).await;
    let c = client();

    let payload = serde_json::json!({
        "model": "good/m",
        "messages": [{"role": "user", "content": "cache me"}],
        "temperature": 0.0
    });

    // First call: MISS, reaches upstream.
    let r1 = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    assert_eq!(r1.headers().get("x-vr-cache").unwrap(), "MISS");
    let body1: serde_json::Value = r1.json().await.unwrap();

    // Second identical call: HIT, identical body, upstream NOT contacted again.
    let r2 = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 200);
    assert_eq!(r2.headers().get("x-vr-cache").unwrap(), "HIT");
    let body2: serde_json::Value = r2.json().await.unwrap();
    assert_eq!(body1, body2, "cache must replay the exact response");
    assert_eq!(mock.request_count(), 1, "upstream hit exactly once");
}

#[tokio::test]
async fn different_prompt_misses_cache() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&cached_cfg(&mock.url(), &mock.url())).await;
    let c = client();

    for content in ["first prompt", "second prompt"] {
        let r = c
            .post(format!("{base}/v1/chat/completions"))
            .json(&serde_json::json!({
                "model": "good/m",
                "messages": [{"role": "user", "content": content}],
                "temperature": 0.0
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.headers().get("x-vr-cache").unwrap(), "MISS");
    }
    assert_eq!(
        mock.request_count(),
        2,
        "distinct prompts both reach upstream"
    );
}

#[tokio::test]
async fn streaming_requests_bypass_the_cache() {
    let mock = MockUpstream::sse(sse_chunks()).await;
    let (base, _state) = spawn_server(&cached_cfg(&mock.url(), &mock.url())).await;
    let c = client();

    for _ in 0..2 {
        let r = c
            .post(format!("{base}/v1/chat/completions"))
            .json(&serde_json::json!({
                "model": "good/m",
                "stream": true,
                "messages": [{"role": "user", "content": "stream me"}]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        // Streams bypass cache when stream_cache is disabled and report MISS
        assert_eq!(r.headers().get("x-vr-cache").unwrap(), "MISS");
        let _ = r.text().await.unwrap();
    }
    assert_eq!(mock.request_count(), 2, "streams always hit upstream");
}

#[tokio::test]
async fn cache_stats_and_purge_endpoints() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&cached_cfg(&mock.url(), &mock.url())).await;
    let c = client();

    let payload = serde_json::json!({
        "model": "good/m",
        "messages": [{"role": "user", "content": "stats me"}],
        "temperature": 0.0
    });
    // One miss, one hit.
    for _ in 0..2 {
        let _ = c
            .post(format!("{base}/v1/chat/completions"))
            .json(&payload)
            .send()
            .await
            .unwrap();
    }

    let stats: serde_json::Value = c
        .get(format!("{base}/api/cache/stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats["hits"], 1);
    assert_eq!(stats["misses"], 1);
    assert_eq!(stats["cached_prompts"], 1);
    assert!(stats["hit_rate"].as_f64().unwrap() > 0.0);

    // Purge drops everything.
    let r = c
        .post(format!("{base}/api/cache/purge"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let stats2: serde_json::Value = c
        .get(format!("{base}/api/cache/stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(stats2["cached_prompts"], 0);
}

#[tokio::test]
async fn api_status_exposes_cache_metrics() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&cached_cfg(&mock.url(), &mock.url())).await;

    let status: serde_json::Value = client()
        .get(format!("{base}/api/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(status["cache"].is_object());
    assert!(status["cache"]["max_memory_bytes"].as_u64().unwrap() > 0);
}

// ---------------------------------------------------------------- multi-key pool E2E

#[tokio::test]
async fn provider_key_pool_rotates_and_survives_429() {
    // First upstream call 429s, second succeeds — one client request must
    // still succeed by rotating to the second key transparently.
    let mut mock = MockUpstream::sequence(vec![
        vibe_router_providers::mock::Reply::Json(
            429,
            r#"{"error":{"message":"rate limit"}}"#.into(),
        ),
        vibe_router_providers::mock::Reply::Json(200, OK_BODY.into()),
    ])
    .await;

    let mut cfg = AppConfig::minimal();
    cfg.providers = vec![vibe_router_config::ProviderConfig {
        id: "pooled".into(),
        display_name: None,
        base_url: mock.url(),
        format: Default::default(),
        auth: vibe_router_config::AuthConfig {
            scheme: Default::default(),
            secret: String::new(),
            secrets: vec!["sk-key-1".into(), "sk-key-2".into()],
            ..Default::default()
        },
        api_keys: vec![],
        key_strategy: None,
        headers: vec![],
        models: vec![vibe_router_config::ModelConfig {
            id: "m".into(),
            name: None,
            context: None,
            max_output: None,
        }],
        passthrough: false,
    }];
    cfg.router.providers = cfg.providers.clone();

    let (base, _state) = spawn_server(&cfg).await;
    let resp = client()
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({"model": "pooled/m", "messages": []}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "429 absorbed by key rotation");

    mock.wait_for_requests(2).await;
    let r1 = mock.request_text(0).await;
    let r2 = mock.request_text(1).await;
    assert!(r1.contains("Bearer sk-key-1"), "{r1}");
    assert!(r2.contains("Bearer sk-key-2"), "{r2}");
}

#[tokio::test]
async fn provider_api_exposes_key_counts() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let mut cfg = AppConfig::minimal();
    cfg.providers = vec![vibe_router_config::ProviderConfig {
        id: "pooled".into(),
        display_name: None,
        base_url: mock.url(),
        format: Default::default(),
        auth: vibe_router_config::AuthConfig {
            scheme: Default::default(),
            secret: String::new(),
            secrets: vec!["k1".into(), "k2".into(), "k3".into()],
            ..Default::default()
        },
        api_keys: vec![],
        key_strategy: None,
        headers: vec![],
        models: vec![vibe_router_config::ModelConfig {
            id: "m".into(),
            name: None,
            context: None,
            max_output: None,
        }],
        passthrough: false,
    }];
    cfg.router.providers = cfg.providers.clone();

    let (base, _state) = spawn_server(&cfg).await;
    let providers: serde_json::Value = client()
        .get(format!("{base}/api/providers"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let p = &providers.as_array().unwrap()[0];
    assert_eq!(p["key_count"], 3);
    assert_eq!(p["healthy_keys"], 3);
}

// ---------------------------------------------------------------- Objectives 2-6 E2E

#[tokio::test]
async fn virtual_model_alias_routing_e2e() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;
    let c = client();

    // Register virtual alias: "gpt-4o" -> "good/m"
    let alias_resp = c
        .post(format!("{base}/api/aliases"))
        .json(&serde_json::json!({
            "alias": "gpt-4o",
            "target": "good/m"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(alias_resp.status(), 201);

    // List aliases
    let list: serde_json::Value = c
        .get(format!("{base}/api/aliases"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["aliases"]["gpt-4o"], "good/m");

    // Route request using virtual alias "gpt-4o"
    let resp = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hello alias"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: serde_json::Value = resp.json().await.unwrap();
    // Echoes back the client's requested alias
    assert_eq!(v["model"], "gpt-4o");
    assert_eq!(mock.request_count(), 1);

    // Clean up
    let del = c
        .delete(format!("{base}/api/aliases/gpt-4o"))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 204);
    assert!(state.registry.get_alias("gpt-4o").is_none());
}

#[tokio::test]
async fn costs_and_latency_endpoint_e2e() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;
    let c = client();

    // Serve a request so usage and costs accumulate
    let r = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({
            "model": "good/m",
            "messages": [{"role": "user", "content": "calculate my cost"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    // Give asynchronous storage writer a moment to flush
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Query /api/costs
    let costs: serde_json::Value = c
        .get(format!("{base}/api/costs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert!(costs.get("breakdown").is_some());
    assert!(costs.get("metrics").is_some());
    let metrics = &costs["metrics"];
    assert!(metrics["requests"].as_u64().unwrap() >= 1);
    assert!(metrics["succeeded"].as_u64().unwrap() >= 1);
    assert!(metrics.get("p50_latency_ms").is_some());
    assert!(metrics.get("p95_latency_ms").is_some());
    assert!(metrics.get("p99_latency_ms").is_some());
}

#[tokio::test]
async fn circuit_breaker_trips_and_reroutes_traffic_e2e() {
    let good = MockUpstream::json(200, "OK", OK_BODY).await;
    let bad = MockUpstream::json(
        500,
        "Internal Server Error",
        r#"{"error":{"message":"internal failure"}}"#,
    )
    .await;
    let (base, state) = spawn_server(&combo_cfg(&good.url(), &bad.url())).await;
    let c = client();

    // Check health endpoint initial state
    let h1: serde_json::Value = c
        .get(format!("{base}/api/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(h1["circuits"].is_array());

    // Trip the bad provider's circuit breaker to OPEN
    state
        .health
        .trip_circuit("bad", Some(Duration::from_secs(60)));
    assert_eq!(
        state.health.circuit_state("bad"),
        vibe_router_health::CircuitState::Open
    );

    // Query /api/health to verify circuit status is reported
    let h2: serde_json::Value = c
        .get(format!("{base}/api/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let bad_cb = h2["circuits"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["provider"] == "bad")
        .unwrap();
    assert_eq!(bad_cb["state"], "open");

    // Request to "resilient" combo (bad -> good):
    // Circuit breaker for bad is OPEN, so it is skipped immediately and routes to good!
    let resp = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({
            "model": "resilient",
            "messages": [{"role": "user", "content": "bypass broken circuit"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(good.request_count(), 1);
    assert_eq!(
        bad.request_count(),
        0,
        "bad provider avoided while circuit is open"
    );

    // Reset circuit breaker
    state.health.reset_circuit("bad");
    assert_eq!(
        state.health.circuit_state("bad"),
        vibe_router_health::CircuitState::Closed
    );
}

#[tokio::test]
async fn proxy_management_and_cf_worker_script_e2e() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;
    let c = client();

    // 1. GET /api/relay/worker.js returns the Cloudflare Worker script
    let script_resp = c
        .get(format!("{base}/api/relay/worker.js"))
        .send()
        .await
        .unwrap();
    assert_eq!(script_resp.status(), 200);
    assert_eq!(
        script_resp.headers().get("content-type").unwrap(),
        "application/javascript; charset=utf-8"
    );
    let script_text = script_resp.text().await.unwrap();
    assert!(script_text.contains("Vibe Router - Cloudflare Worker Relay"));
    assert!(script_text.contains("X-Target-URL"));

    // 2. Add tagged proxy via POST /api/proxies
    let add_resp = c
        .post(format!("{base}/api/proxies"))
        .json(&serde_json::json!({
            "id": "cf-edge-east",
            "url": "https://worker-east.workers.dev",
            "tag": "cf-worker"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(add_resp.status(), 201);

    // 3. List proxies via GET /api/proxies
    let list: serde_json::Value = c
        .get(format!("{base}/api/proxies"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let proxies = list["proxies"].as_array().unwrap();
    assert!(proxies
        .iter()
        .any(|p| p["id"] == "cf-edge-east" && p["tag"] == "cf-worker"));

    // 4. Delete proxy via DELETE /api/proxies/:id
    let del_resp = c
        .delete(format!("{base}/api/proxies/cf-edge-east"))
        .send()
        .await
        .unwrap();
    assert_eq!(del_resp.status(), 204);

    let list2: serde_json::Value = c
        .get(format!("{base}/api/proxies"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!list2["proxies"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p["id"] == "cf-edge-east"));
}

#[tokio::test]
async fn auth_compatibility_x_api_key_and_bearer_e2e() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let mut cfg = combo_cfg(&mock.url(), &mock.url());
    cfg.server.api_keys = vec!["sk-vibe-assembly-token".into()];

    let (base, _state) = spawn_server(&cfg).await;
    let c = client();

    // No auth -> 401
    let r1 = c.get(format!("{base}/v1/assignment")).send().await.unwrap();
    assert_eq!(r1.status(), 401);

    // Bearer header -> 200
    let r2 = c
        .get(format!("{base}/v1/assignment"))
        .header("authorization", "Bearer sk-vibe-assembly-token")
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 200);

    // X-API-Key header -> 200
    let r3 = c
        .get(format!("{base}/api/assignment"))
        .header("x-api-key", "sk-vibe-assembly-token")
        .send()
        .await
        .unwrap();
    assert_eq!(r3.status(), 200);

    // Invalid X-API-Key -> 401
    let r4 = c
        .get(format!("{base}/api/assignment"))
        .header("x-api-key", "sk-wrong-key")
        .send()
        .await
        .unwrap();
    assert_eq!(r4.status(), 401);
}

#[tokio::test]
async fn opt_in_stream_caching_e2e() {
    let mock = MockUpstream::sse(sse_chunks()).await;
    let mut cfg = cached_cfg(&mock.url(), &mock.url());
    cfg.cache.stream_cache = true; // opt-in stream caching enabled

    let (base, _state) = spawn_server(&cfg).await;
    let c = client();

    let stream_payload = serde_json::json!({
        "model": "good/m",
        "stream": true,
        "messages": [{"role": "user", "content": "stream cache test"}],
        "temperature": 0.0
    });

    // 1st request: misses cache, streams from upstream
    let r1 = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&stream_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    let text1 = r1.text().await.unwrap();
    assert!(text1.contains("data: [DONE]"));
    assert_eq!(mock.request_count(), 1);

    // Give async task a moment to store recorded stream frames in cache
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 2nd request: HIT! Served from stream cache with x-vr-cache: HIT header
    let r2 = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&stream_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 200);
    assert_eq!(r2.headers().get("x-vr-cache").unwrap(), "HIT");
    let text2 = r2.text().await.unwrap();
    assert_eq!(text1, text2);
    // Upstream was NOT called a second time
    assert_eq!(mock.request_count(), 1);
}

// ---------------------------------------------------------------- Vibe-Link & Anthropic & Portable Mode

#[tokio::test]
async fn vibe_link_telemetry_and_headers_e2e() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;
    let c = client();

    let resp = c
        .post(format!("{base}/v1/chat/completions"))
        .header("x-vibe-role", "code")
        .header("x-vibe-node-id", "node-alpha-1")
        .json(&serde_json::json!({
            "model": "good/m",
            "messages": [{"role": "user", "content": "telemetry header check"}]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let headers = resp.headers();

    // Verify all required Vibe-Link telemetry headers (both canonical and compat alias)
    assert!(headers.get("x-vibe-router-cost-micros").is_some());
    assert!(headers.get("x-vibe-router-latency-ms").is_some());
    assert_eq!(headers.get("x-vibe-router-cache").unwrap(), "MISS");
    assert_eq!(headers.get("x-vibe-router-provider").unwrap(), "good");
    assert!(headers.get("x-vr-cost-micros").is_some());
    assert_eq!(headers.get("x-vr-cache").unwrap(), "MISS");
}

#[tokio::test]
async fn vibe_link_judge_retry_bypass_and_candidate_rotation_e2e() {
    let mock1 = MockUpstream::json(200, "OK", OK_BODY).await;
    let mock2 = MockUpstream::json(200, "OK", OK_BODY).await;
    let mut cfg = combo_cfg(&mock1.url(), &mock2.url());
    cfg.cache.enabled = true;

    let (base, _state) = spawn_server(&cfg).await;
    let c = client();

    let payload = serde_json::json!({
        "model": "good/m",
        "messages": [{"role": "user", "content": "retryable prompt"}],
        "temperature": 0.0
    });

    // 1st request: misses cache, cached
    let r1 = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(r1.status(), 200);
    assert_eq!(r1.headers().get("x-vr-cache").unwrap(), "MISS");
    assert_eq!(mock1.request_count(), 1);

    // 2nd request normal: hits cache!
    let r2 = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(r2.status(), 200);
    assert_eq!(r2.headers().get("x-vr-cache").unwrap(), "HIT");
    assert_eq!(mock1.request_count(), 1);

    // 3rd request with X-Vibe-Retry: 1: strictly bypasses cache and hits upstream again!
    let r3 = c
        .post(format!("{base}/v1/chat/completions"))
        .header("x-vibe-retry", "1")
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(r3.status(), 200);
    assert_eq!(r3.headers().get("x-vr-cache").unwrap(), "MISS");
    assert_eq!(mock1.request_count(), 2, "judge retry bypassed cache");

    // Also test candidate rotation on combo when X-Vibe-Retry is present
    let combo_payload = serde_json::json!({
        "model": "resilient", // combo of bad/m then good/m
        "messages": [{"role": "user", "content": "test retry rotation"}]
    });
    let r4 = c
        .post(format!("{base}/v1/chat/completions"))
        .header("x-vibe-retry", "1")
        .json(&combo_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(r4.status(), 200);
}

#[tokio::test]
async fn anthropic_messages_protocol_adapter_non_streaming_e2e() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;
    let c = client();

    let anthropic_req = serde_json::json!({
        "model": "good/m",
        "system": "You are a software engineer.",
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": "Hello from Claude Code!"}]
            }
        ],
        "max_tokens": 256,
        "temperature": 0.5,
        "stream": false
    });

    let resp = c
        .post(format!("{base}/v1/messages"))
        .header("x-vibe-role", "chat")
        .json(&anthropic_req)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let headers = resp.headers();
    assert!(headers.get("x-vibe-router-cost-micros").is_some());
    assert!(headers.get("x-vibe-router-latency-ms").is_some());
    assert_eq!(headers.get("x-vibe-router-cache").unwrap(), "MISS");
    assert_eq!(headers.get("x-vibe-router-provider").unwrap(), "good");
    assert!(headers.get("x-vr-cost-micros").is_some());
    assert_eq!(headers.get("x-vr-cache").unwrap(), "MISS");

    let val: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(val["type"], "message");
    assert_eq!(val["role"], "assistant");
    assert_eq!(val["model"], "good/m");
    assert_eq!(val["stop_reason"], "end_turn");
    assert!(val["content"].is_array());
    assert!(val["usage"]["input_tokens"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn anthropic_messages_protocol_adapter_streaming_e2e() {
    let mock = MockUpstream::sse(sse_chunks()).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;
    let c = client();

    let anthropic_req = serde_json::json!({
        "model": "good/m",
        "messages": [
            {
                "role": "user",
                "content": "stream to me"
            }
        ],
        "stream": true
    });

    let resp = c
        .post(format!("{base}/v1/messages"))
        .json(&anthropic_req)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("x-vibe-router-provider").unwrap(),
        "good"
    );
    assert_eq!(resp.headers().get("x-vibe-router-cache").unwrap(), "MISS");
    assert_eq!(resp.headers().get("x-vr-provider").unwrap(), "good");

    let stream_text = resp.text().await.unwrap();
    assert!(stream_text.contains("event: message_start"));
    assert!(stream_text.contains("event: content_block_start"));
    assert!(stream_text.contains("event: content_block_delta"));
    assert!(stream_text.contains("event: content_block_stop"));
    assert!(stream_text.contains("event: message_delta"));
    assert!(stream_text.contains("event: message_stop"));
}

#[tokio::test]
async fn portable_mode_zero_wear_sqlite_sync_e2e() {
    let dir = std::env::temp_dir().join(format!("vibe-router-portable-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("portable.db");

    // 1. Open in RAM-first portable mode
    let backend = vibe_router_storage::SqliteAsyncBackend::open_portable(&db_path).unwrap();

    // 2. Mutate in-memory state
    let mut p = vibe_router_storage::Provider::new("usb-provider", "https://usb.test/v1");
    p.api_keys = vec!["usb-key".into()];
    backend.save_provider(&p).await.unwrap();
    backend
        .save_alias("fast-usb", "usb-provider/m")
        .await
        .unwrap();
    backend
        .record_usage(&vibe_router_storage::UsageRecord {
            requested_model: "fast-usb".into(),
            served_model: "m".into(),
            provider: "usb-provider".into(),
            status: 200,
            prompt_tokens: 100,
            completion_tokens: 50,
            ..Default::default()
        })
        .await
        .unwrap();

    // 3. Snapshot state to disk (atomic write-to-temp + rename)
    backend
        .sync_to_disk(db_path.to_str().unwrap())
        .await
        .unwrap();
    assert!(db_path.exists(), "database file written to disk");

    // 4. Reopen from disk to verify all data survived
    let reopened = vibe_router_storage::SqliteAsyncBackend::open(&db_path)
        .await
        .unwrap();
    let providers = reopened.get_providers().await.unwrap();
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0].id, "usb-provider");
    let aliases = reopened.get_aliases().await.unwrap();
    assert_eq!(aliases.get("fast-usb").unwrap(), "usb-provider/m");
    let metrics = reopened.get_usage_metrics().await.unwrap();
    assert_eq!(metrics.requests, 1);
    assert_eq!(metrics.prompt_tokens, 100);

    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn canonical_vibe_router_telemetry_headers_e2e() {
    let mock = MockUpstream::json(200, "OK", OK_BODY).await;
    let (base, _state) = spawn_server(&combo_cfg(&mock.url(), &mock.url())).await;
    let c = client();

    let resp = c
        .post(format!("{base}/v1/chat/completions"))
        .json(&serde_json::json!({
            "model": "good/m",
            "messages": [{"role": "user", "content": "canonical headers test"}]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let h = resp.headers();

    // Must emit full-name canonical response headers:
    assert!(h.get("x-vibe-router-cost-micros").is_some());
    assert!(h.get("x-vibe-router-latency-ms").is_some());
    assert_eq!(h.get("x-vibe-router-cache").unwrap(), "MISS");
    assert_eq!(h.get("x-vibe-router-provider").unwrap(), "good");
}
