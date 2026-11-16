//! vibe-router-providers integration tests: the HTTP transport against the hermetic
//! mock upstream, and the registry against config-shaped inputs.

use std::sync::Arc;
use std::time::Duration;

use crate::mock::MockUpstream;
use crate::{HttpUpstream, Registry};
use vibe_router_config::schema::{AuthConfig, ModelConfig, ProviderConfig};
use vibe_router_config::AppConfig;
use vibe_router_core::{
    SecretRef, StreamEvent, TransportErrorKind, UpstreamClient, UpstreamRequest, UpstreamResponse,
    Usage, WireFormat,
};

fn provider(id: &str, base_url: &str, secret: &str) -> ProviderConfig {
    ProviderConfig {
        id: id.into(),
        display_name: None,
        base_url: base_url.into(),
        format: Default::default(),
        auth: AuthConfig {
            scheme: Default::default(),
            secret: secret.into(),
            ..Default::default()
        },
        api_keys: vec![],
        key_strategy: None,
        headers: vec![("x-test".into(), "1".into())],
        models: vec![ModelConfig {
            id: "m1".into(),
            name: None,
            context: None,
            max_output: None,
        }],
        passthrough: false,
    }
}

/// A provider whose auth is a pool of literal keys (no env indirection), so
/// a test can assert on which key reached the wire.
fn pooled_provider(id: &str, base_url: &str, keys: Vec<&str>) -> ProviderConfig {
    ProviderConfig {
        id: id.into(),
        display_name: None,
        base_url: base_url.into(),
        format: Default::default(),
        auth: AuthConfig {
            scheme: Default::default(),
            secret: String::new(),
            secrets: keys.into_iter().map(|k| k.to_string()).collect(),
            ..Default::default()
        },
        api_keys: vec![],
        key_strategy: None,
        headers: vec![],
        models: vec![ModelConfig {
            id: "m1".into(),
            name: None,
            context: None,
            max_output: None,
        }],
        passthrough: false,
    }
}

fn registry_for(p: ProviderConfig) -> Arc<Registry> {
    let mut cfg = AppConfig::minimal();
    cfg.providers = vec![p];
    Arc::new(Registry::from_config(&cfg))
}

fn req(provider: &str, stream: bool, timeout: Duration) -> UpstreamRequest {
    UpstreamRequest {
        provider_id: provider.into(),
        model: "m1".into(),
        body: serde_json::json!({"model": "m1", "messages": [{"role":"user","content":"x"}]}),
        stream,
        connect_timeout: timeout,
        first_chunk_timeout: timeout,
        chunk_stall_timeout: timeout,
    }
}

fn upstream_for(mock: &MockUpstream) -> HttpUpstream {
    HttpUpstream::new(registry_for(provider("mock", &mock.url(), "")))
}

#[tokio::test]
async fn non_stream_success_carries_reported_usage() {
    let body = r#"{"id":"c1","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":7,"completion_tokens":3}}"#;
    let mut mock = MockUpstream::json(200, "OK", body).await;
    std::env::set_var("VIBE_ROUTER_TEST_KEY_A", "sk-test");
    let p = provider("mock", &mock.url(), "env:VIBE_ROUTER_TEST_KEY_A");
    let resp = HttpUpstream::new(registry_for(p))
        .execute(req("mock", false, Duration::from_secs(5)))
        .await;
    match resp {
        UpstreamResponse::Json {
            status,
            body,
            usage,
        } => {
            assert_eq!(status, 200);
            assert_eq!(body["id"], "c1");
            // Reported, never estimated: the flag must be false here.
            assert_eq!(usage, Some(Usage::reported(7, 3)));
            assert!(!usage.unwrap().estimated);
        }
        other => panic!("expected Json, got {other:?}"),
    }
    let _ = mock.request_text(0).await;
}

#[tokio::test]
async fn bearer_and_custom_headers_reach_the_wire() {
    let mut mock = MockUpstream::json(200, "OK", r#"{"ok":true}"#).await;
    std::env::set_var("VIBE_ROUTER_TEST_KEY_B", "sk-secret");
    let p = provider("mock", &mock.url(), "env:VIBE_ROUTER_TEST_KEY_B");
    let _ = HttpUpstream::new(registry_for(p))
        .execute(req("mock", false, Duration::from_secs(5)))
        .await;
    let text = mock.request_text(0).await;
    let lower = text.to_ascii_lowercase();
    assert!(lower.contains("authorization: bearer sk-secret"), "{text}");
    assert!(lower.contains("x-test: 1"), "{text}");
    assert!(text.contains("\"model\":\"m1\""), "{text}");
}

#[tokio::test]
async fn missing_secret_fails_before_any_connection() {
    // base_url points at a closed port: if we reached the network the
    // error kind would be Connect, not NoCredential.
    std::env::remove_var("VIBE_ROUTER_TEST_UNSET");
    let p = provider(
        "mock",
        "http://127.0.0.1:9/v1/chat/completions",
        "env:VIBE_ROUTER_TEST_UNSET",
    );
    let resp = HttpUpstream::new(registry_for(p))
        .execute(req("mock", false, Duration::from_secs(2)))
        .await;
    match resp {
        UpstreamResponse::Transport(e) => {
            assert_eq!(e.kind, TransportErrorKind::NoCredential);
            assert!(
                e.message.contains("VIBE_ROUTER_TEST_UNSET"),
                "{}",
                e.message
            );
        }
        other => panic!("expected Transport, got {other:?}"),
    }
}

#[tokio::test]
async fn file_secret_is_resolved() {
    let dir = std::env::temp_dir().join(format!("vibe-router-secret-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("key");
    std::fs::write(&path, "sk-from-file\n").unwrap();
    let mut mock = MockUpstream::json(200, "OK", r#"{"ok":true}"#).await;
    let p = provider("mock", &mock.url(), &format!("file:{}", path.display()));
    let _ = HttpUpstream::new(registry_for(p))
        .execute(req("mock", false, Duration::from_secs(5)))
        .await;
    let text = mock.request_text(0).await;
    assert!(text.contains("Bearer sk-from-file"), "{text}"); // trimmed on read
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn auth_none_sends_no_authorization_header() {
    let mut mock = MockUpstream::json(200, "OK", r#"{"ok":true}"#).await;
    let p = provider("mock", &mock.url(), ""); // empty secret -> Auth::None
    let resp = HttpUpstream::new(registry_for(p))
        .execute(req("mock", false, Duration::from_secs(5)))
        .await;
    assert!(matches!(resp, UpstreamResponse::Json { status: 200, .. }));
    let text = mock.request_text(0).await;
    assert!(
        !text.to_ascii_lowercase().contains("authorization"),
        "unexpected auth header in:\n{text}"
    );
}

#[tokio::test]
async fn upstream_error_status_passes_through_with_body() {
    let mock = MockUpstream::json(
        503,
        "Service Unavailable",
        r#"{"error":{"message":"overloaded"}}"#,
    )
    .await;
    let resp = upstream_for(&mock)
        .execute(req("mock", false, Duration::from_secs(5)))
        .await;
    match resp {
        UpstreamResponse::Json { status, body, .. } => {
            assert_eq!(status, 503);
            assert_eq!(body["error"]["message"], "overloaded");
        }
        other => panic!("expected Json, got {other:?}"),
    }
}

#[tokio::test]
async fn unparseable_body_is_kept_as_text_for_the_classifier() {
    // The provider lied about JSON; we must still hand the classifier its
    // wording so text rules ("rate limit", "overloaded") can match.
    let mock = MockUpstream::json(500, "Internal Server Error", "<html>overloaded</html>").await;
    let resp = upstream_for(&mock)
        .execute(req("mock", false, Duration::from_secs(5)))
        .await;
    match resp {
        UpstreamResponse::Json { status, body, .. } => {
            assert_eq!(status, 500);
            assert!(body.as_str().unwrap().contains("overloaded"), "{body}");
        }
        other => panic!("expected Json, got {other:?}"),
    }
}

#[tokio::test]
async fn unresolvable_host_maps_to_connect_error() {
    // `.invalid` (RFC 2606) can never resolve: a deterministic connect-path
    // failure with no network involved. Use a generous timeout so slow Windows
    // LLMNR/DNS suffix search timeouts under parallel test load never trigger
    // the request connect_timeout before the OS reports failure.
    let p = provider(
        "mock",
        "http://vibe-router-tests.invalid/v1/chat/completions",
        "",
    );
    let resp = HttpUpstream::new(registry_for(p))
        .execute(req("mock", false, Duration::from_secs(20)))
        .await;
    match resp {
        UpstreamResponse::Transport(e) => assert_eq!(e.kind, TransportErrorKind::Connect),
        other => panic!("expected Transport, got {other:?}"),
    }
}

#[tokio::test]
async fn silent_upstream_hits_the_connect_timeout() {
    let mock = MockUpstream::blackhole().await;
    let resp = upstream_for(&mock)
        .execute(req("mock", false, Duration::from_millis(250)))
        .await;
    match resp {
        UpstreamResponse::Transport(e) => assert_eq!(e.kind, TransportErrorKind::Timeout),
        other => panic!("expected Transport, got {other:?}"),
    }
}

#[tokio::test]
async fn stream_forwards_decoded_chunks_then_done() {
    let mock = MockUpstream::sse(vec![
        "data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n\n".into(),
        "data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\n".into(),
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2}}\n\n".into(),
        "data: [DONE]\n\n".into(),
    ])
    .await;
    let resp = upstream_for(&mock)
        .execute(req("mock", true, Duration::from_secs(5)))
        .await;
    let UpstreamResponse::Stream { mut events, status } = resp else {
        panic!("expected Stream, got {resp:?}");
    };
    assert_eq!(status, 200);
    let mut datas = Vec::new();
    loop {
        match events.recv().await {
            Some(StreamEvent::Chunk(d)) => datas.push(d),
            Some(StreamEvent::Done) => break,
            Some(StreamEvent::Error(e)) => panic!("unexpected stream error: {e}"),
            None => panic!("stream ended without Done"),
        }
    }
    assert_eq!(datas.len(), 4, "3 JSON chunks + [DONE]");
    assert!(datas[0].contains("Hel"));
    assert_eq!(datas[3], "[DONE]");
}

#[tokio::test]
async fn stream_stalled_before_first_chunk_yields_error_event() {
    // Headers arrive, no body bytes ever do within the budget.
    let mock = MockUpstream::sse_then_silence(vec![], Duration::from_millis(400)).await;
    let resp = upstream_for(&mock)
        .execute(req("mock", true, Duration::from_millis(150)))
        .await;
    let UpstreamResponse::Stream { mut events, .. } = resp else {
        panic!("expected Stream, got {resp:?}");
    };
    match events.recv().await {
        Some(StreamEvent::Error(_)) => {}
        other => panic!("expected Error, got {other:?}"),
    }
}

#[tokio::test]
async fn stream_mid_flight_stall_yields_error_event() {
    let mock = MockUpstream::sse_then_silence(
        vec!["data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n".into()],
        Duration::from_millis(400),
    )
    .await;
    let resp = upstream_for(&mock)
        .execute(req("mock", true, Duration::from_millis(150)))
        .await;
    let UpstreamResponse::Stream { mut events, .. } = resp else {
        panic!("expected Stream, got {resp:?}");
    };
    // First chunk delivers...
    assert!(matches!(events.recv().await, Some(StreamEvent::Chunk(_))));
    // ...then the stall surfaces as an Error, not a silent hang.
    assert!(matches!(events.recv().await, Some(StreamEvent::Error(_))));
}

#[tokio::test]
async fn stream_error_status_becomes_json_for_failover() {
    // 429 before any body: the router must see a classifiable JSON, not
    // a stream, so failover can happen.
    let mock = MockUpstream::json(
        429,
        "Too Many Requests",
        r#"{"error":{"message":"rate limit"}}"#,
    )
    .await;
    let resp = upstream_for(&mock)
        .execute(req("mock", true, Duration::from_secs(5)))
        .await;
    match resp {
        UpstreamResponse::Json { status, body, .. } => {
            assert_eq!(status, 429);
            assert_eq!(body["error"]["message"], "rate limit");
        }
        other => panic!("expected Json, got {other:?}"),
    }
}

#[tokio::test]
async fn streaming_requests_get_include_usage_added() {
    let mut mock = MockUpstream::json(200, "OK", r#"{"ok":true}"#).await;
    let resp = upstream_for(&mock)
        .execute(req("mock", true, Duration::from_secs(5)))
        .await;
    // Drain so the mock completes.
    if let UpstreamResponse::Stream { mut events, .. } = resp {
        while events.recv().await.is_some() {}
    }
    let text = mock.request_text(0).await;
    assert!(text.contains("\"include_usage\":true"), "{text}");
    assert!(text.contains("\"stream\":true"), "{text}");
}

#[tokio::test]
async fn registry_keeps_providers_openai_format_only() {
    let reg = registry_for(provider("mock", "http://x.invalid", ""));
    let prov = reg.provider("mock").unwrap();
    assert_eq!(prov.transport.format, WireFormat::OpenAi);
}

#[tokio::test]
async fn secret_ref_round_trips_through_registry() {
    std::env::set_var("VIBE_ROUTER_TEST_KEY_C", "x");
    let reg = registry_for(provider(
        "mock",
        "http://x.invalid",
        "env:VIBE_ROUTER_TEST_KEY_C",
    ));
    let prov = reg.provider("mock").unwrap();
    match &prov.auth {
        vibe_router_core::Auth::Bearer { key_pool } => {
            assert_eq!(
                key_pool.keys,
                vec![SecretRef::Env("VIBE_ROUTER_TEST_KEY_C".into())]
            )
        }
        other => panic!("expected Bearer, got {other:?}"),
    }
}

// ---------------------------------------------------------------- multi-key pool

#[tokio::test]
async fn pool_rotates_keys_round_robin_across_requests() {
    let mut mock = MockUpstream::json(200, "OK", r#"{"ok":true}"#).await;
    let p = pooled_provider("mock", &mock.url(), vec!["sk-one", "sk-two"]);
    let upstream = HttpUpstream::new(registry_for(p));

    // Three requests: key 1, key 2, key 1 (round-robin).
    for _ in 0..3 {
        let _ = upstream
            .execute(req("mock", false, Duration::from_secs(5)))
            .await;
    }
    mock.wait_for_requests(3).await;

    let first = mock.request_text(0).await;
    let second = mock.request_text(1).await;
    let third = mock.request_text(2).await;
    assert!(first.contains("Bearer sk-one"), "{first}");
    assert!(second.contains("Bearer sk-two"), "{second}");
    assert!(third.contains("Bearer sk-one"), "wraps around: {third}");
}

#[tokio::test]
async fn pool_cools_429_key_and_retries_with_next_key_transparently() {
    // First request 429s, second succeeds — the client sees only the success.
    let mut mock = MockUpstream::sequence(vec![
        crate::mock::Reply::Json(429, r#"{"error":{"message":"rate limit"}}"#.into()),
        crate::mock::Reply::Json(200, r#"{"ok":true}"#.into()),
    ])
    .await;
    let p = pooled_provider("mock", &mock.url(), vec!["sk-cooling", "sk-healthy"]);
    let upstream = HttpUpstream::new(registry_for(p));

    let resp = upstream
        .execute(req("mock", false, Duration::from_secs(5)))
        .await;
    match resp {
        UpstreamResponse::Json { status, .. } => {
            assert_eq!(status, 200, "429 must be absorbed, not surfaced")
        }
        other => panic!("expected Json, got {other:?}"),
    }
    mock.wait_for_requests(2).await;
    let first = mock.request_text(0).await;
    let second = mock.request_text(1).await;
    assert!(first.contains("Bearer sk-cooling"), "{first}");
    assert!(second.contains("Bearer sk-healthy"), "{second}");
}

#[tokio::test]
async fn pool_fails_closed_only_when_every_key_is_cooling() {
    // Every response is a 429: after trying both keys, the last 429 surfaces.
    let mock = MockUpstream::json(
        429,
        "Too Many Requests",
        r#"{"error":{"message":"rate limit"}}"#,
    )
    .await;
    let p = pooled_provider("mock", &mock.url(), vec!["sk-a", "sk-b"]);
    let upstream = HttpUpstream::new(registry_for(p));

    let resp = upstream
        .execute(req("mock", false, Duration::from_secs(5)))
        .await;
    match resp {
        UpstreamResponse::Json { status, .. } => {
            assert_eq!(status, 429, "all keys exhausted -> surface the 429")
        }
        other => panic!("expected Json 429, got {other:?}"),
    }
    // Both keys were attempted exactly once.
    assert_eq!(mock.wait_for_requests(2).await, 2);
}

#[tokio::test]
async fn cooling_key_is_skipped_on_subsequent_requests() {
    let mut mock = MockUpstream::sequence(vec![
        crate::mock::Reply::Json(429, r#"{"error":{"message":"rate limit"}}"#.into()),
        crate::mock::Reply::Json(200, r#"{"ok":true}"#.into()),
        crate::mock::Reply::Json(200, r#"{"ok":true}"#.into()),
        crate::mock::Reply::Json(200, r#"{"ok":true}"#.into()),
    ])
    .await;
    let p = pooled_provider("mock", &mock.url(), vec!["sk-first", "sk-second"]);
    let upstream = HttpUpstream::new(registry_for(p));

    // Request 1: sk-first 429s, sk-second succeeds (sk-first now cooling).
    let _ = upstream
        .execute(req("mock", false, Duration::from_secs(5)))
        .await;
    // Requests 2 and 3: only sk-second should be used.
    for _ in 0..2 {
        let _ = upstream
            .execute(req("mock", false, Duration::from_secs(5)))
            .await;
    }
    mock.wait_for_requests(4).await;

    let r2 = mock.request_text(1).await;
    let r3 = mock.request_text(2).await;
    let r4 = mock.request_text(3).await;
    assert!(r2.contains("Bearer sk-second"), "{r2}");
    assert!(r3.contains("Bearer sk-second"), "{r3}");
    assert!(r4.contains("Bearer sk-second"), "{r4}");
}

#[tokio::test]
async fn key_pool_reports_health_and_count() {
    let p = pooled_provider("mock", "http://x.invalid", vec!["k1", "k2", "k3"]);
    let reg = registry_for(p);
    let prov = reg.provider("mock").unwrap();
    assert_eq!(prov.key_pool.total_keys(), 3);
    assert_eq!(prov.key_pool.healthy_keys(), 3);
    prov.key_pool.mark_cooldown(0, None);
    assert_eq!(prov.key_pool.healthy_keys(), 2);
    assert!(prov.key_pool.earliest_cooldown().is_some());
}

// ---------------------------------------------------------------- auth-fault rotation

const OK_JSON: &str = r#"{"id":"c1","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}"#;

/// A 401 on the first pooled key rotates to the next healthy key (non-streaming),
/// and the bad key stays cooled on subsequent requests.
#[tokio::test]
async fn auth_fault_401_rotates_and_cools_key_non_streaming() {
    let mut mock = MockUpstream::sequence(vec![
        crate::mock::Reply::Json(401, r#"{"error":{"message":"invalid key"}}"#.into()),
        crate::mock::Reply::Json(200, OK_JSON.into()),
    ])
    .await;
    let p = pooled_provider("mock", &mock.url(), vec!["sk-bad", "sk-ok"]);
    let upstream = HttpUpstream::new(registry_for(p));

    let resp = upstream
        .execute(req("mock", false, Duration::from_secs(5)))
        .await;
    assert!(
        matches!(resp, UpstreamResponse::Json { status: 200, .. }),
        "{resp:?}"
    );
    assert!(mock.request_text(0).await.contains("Bearer sk-bad"));
    assert!(mock.request_text(1).await.contains("Bearer sk-ok"));

    // Bad key is cooling: the next request goes straight to the healthy key.
    let resp2 = upstream
        .execute(req("mock", false, Duration::from_secs(5)))
        .await;
    assert!(matches!(resp2, UpstreamResponse::Json { status: 200, .. }));
    assert!(mock.request_text(2).await.contains("Bearer sk-ok"));
}

/// Same rotation for a 403 key fault.
#[tokio::test]
async fn auth_fault_403_rotates_to_next_key() {
    let mut mock = MockUpstream::sequence(vec![
        crate::mock::Reply::Json(403, r#"{"error":{"message":"forbidden"}}"#.into()),
        crate::mock::Reply::Json(200, OK_JSON.into()),
    ])
    .await;
    let p = pooled_provider("mock", &mock.url(), vec!["sk-bad", "sk-ok"]);
    let upstream = HttpUpstream::new(registry_for(p));
    let resp = upstream
        .execute(req("mock", false, Duration::from_secs(5)))
        .await;
    assert!(
        matches!(resp, UpstreamResponse::Json { status: 200, .. }),
        "{resp:?}"
    );
    assert!(mock.request_text(1).await.contains("Bearer sk-ok"));
}

/// A pre-SSE 401 on a streaming request rotates keys too (error arrives as Json).
#[tokio::test]
async fn auth_fault_rotates_on_streaming_path() {
    let chunks = vec![
        "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n".to_string(),
        "data: [DONE]\n\n".to_string(),
    ];
    let mut mock = MockUpstream::sequence(vec![
        crate::mock::Reply::Json(401, r#"{"error":{"message":"invalid key"}}"#.into()),
        crate::mock::Reply::Sse {
            chunks,
            silence: None,
        },
    ])
    .await;
    let p = pooled_provider("mock", &mock.url(), vec!["sk-bad", "sk-ok"]);
    let upstream = HttpUpstream::new(registry_for(p));

    let resp = upstream
        .execute(req("mock", true, Duration::from_secs(5)))
        .await;
    match resp {
        UpstreamResponse::Stream { mut events, .. } => {
            let mut done = false;
            while let Some(ev) = events.recv().await {
                if matches!(ev, StreamEvent::Done) {
                    done = true;
                    break;
                }
            }
            assert!(
                done,
                "stream should complete after rotating to the healthy key"
            );
        }
        other => panic!("expected Stream after rotation, got {other:?}"),
    }
    assert!(mock.request_text(0).await.contains("Bearer sk-bad"));
    assert!(mock.request_text(1).await.contains("Bearer sk-ok"));
}

/// When every key returns 401, the last 401 surfaces immediately (no 120s wait).
#[tokio::test]
async fn all_keys_auth_fault_surfaces_fast() {
    let mock = MockUpstream::json(
        401,
        "Unauthorized",
        r#"{"error":{"message":"invalid key"}}"#,
    )
    .await;
    let p = pooled_provider("mock", &mock.url(), vec!["bad1", "bad2"]);
    let upstream = HttpUpstream::new(registry_for(p));

    let start = std::time::Instant::now();
    let resp = upstream
        .execute(req("mock", false, Duration::from_secs(5)))
        .await;
    let elapsed = start.elapsed();
    assert!(
        matches!(resp, UpstreamResponse::Json { status: 401, .. }),
        "{resp:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "must not wait out a cooldown: {elapsed:?}"
    );
}

/// Request-scoped errors (400/422) fail fast without rotating keys.
#[tokio::test]
async fn request_error_does_not_rotate_keys() {
    for status in [400u16, 422] {
        let mock = MockUpstream::json(
            status,
            "Bad Request",
            r#"{"error":{"message":"bad input"}}"#,
        )
        .await;
        let p = pooled_provider("mock", &mock.url(), vec!["k1", "k2"]);
        let upstream = HttpUpstream::new(registry_for(p));
        let resp = upstream
            .execute(req("mock", false, Duration::from_secs(5)))
            .await;
        assert!(
            matches!(resp, UpstreamResponse::Json { status: s, .. } if s == status),
            "{resp:?}"
        );
        // Exactly one upstream call: no key rotation on a request fault.
        assert_eq!(mock.wait_for_requests(1).await, 1);
        assert_eq!(mock.request_count(), 1);
    }
}

/// An auth-failed key receives the long auth cooldown (default 30 minutes).
#[tokio::test]
async fn auth_fault_key_gets_long_cooldown() {
    let p = pooled_provider("mock", "http://x.invalid", vec!["k1", "k2"]);
    let reg = registry_for(p);
    let prov = reg.provider("mock").unwrap();
    prov.key_pool.mark_auth_fault(0);
    assert_eq!(prov.key_pool.healthy_keys(), 1);
    let cd = prov.key_pool.earliest_cooldown().unwrap();
    assert!(
        cd >= Duration::from_secs(1700),
        "auth cooldown should be ~30m, got {cd:?}"
    );
    // Successful use clears it.
    prov.key_pool.mark_success(0);
    assert_eq!(prov.key_pool.healthy_keys(), 2);
}
