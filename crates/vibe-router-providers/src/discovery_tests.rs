//! Hermetic tests for Vibe Local node discovery: an ephemeral loopback UDP
//! responder plus the mock HTTP upstream stand in for real PC/LAN nodes. No
//! broadcast address is touched and no fixed port is bound.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::task::JoinHandle;
use vibe_router_config::AppConfig;
use vibe_router_core::{Auth, ModelSpec, SecretRef, WireFormat};

use crate::discovery::{discover, reconcile, DiscoveredNode, DiscoveryConfig};
use crate::mock::MockUpstream;
use crate::Registry;

const MODELS_JSON: &str = r#"{"data":[{"id":"local-llama"},{"id":"local-embed"}]}"#;

fn base_of(mock: &MockUpstream) -> String {
    mock.url()
        .trim_end_matches("/v1/chat/completions")
        .to_string()
}

/// A one-shot UDP responder: answers the first probe with `reply` (the node's
/// base URL), exactly as a Vibe Local node would.
async fn udp_responder(reply: String) -> (SocketAddr, JoinHandle<()>) {
    let sock = UdpSocket::bind("127.0.0.1:0").await.expect("bind udp");
    let addr = sock.local_addr().expect("udp addr");
    let handle = tokio::spawn(async move {
        let mut buf = [0u8; 256];
        if let Ok((_n, peer)) = sock.recv_from(&mut buf).await {
            let _ = sock.send_to(reply.as_bytes(), peer).await;
        }
    });
    (addr, handle)
}

fn cfg_with(udp_targets: Vec<SocketAddr>, http_fallbacks: Vec<String>) -> DiscoveryConfig {
    DiscoveryConfig {
        udp_targets,
        http_fallbacks,
        recv_window: Duration::from_millis(500),
        http_timeout: Duration::from_secs(2),
        // An unset, test-specific name so no real credential is read.
        token_env: "VIBE_LOCAL_TOKEN_UNSET_TEST".to_string(),
    }
}

#[tokio::test]
async fn udp_probe_discovers_node_and_ingests_models() {
    let mock = MockUpstream::json(200, "OK", MODELS_JSON).await;
    let base = base_of(&mock);
    let (addr, responder) = udp_responder(base.clone()).await;

    let cfg = cfg_with(vec![addr], vec![]);
    let nodes = discover(&cfg, &reqwest::Client::new()).await;
    responder.abort();

    assert_eq!(nodes.len(), 1, "exactly one node should answer the probe");
    let node = &nodes[0];
    assert_eq!(node.base_url, base);
    assert_eq!(node.provider_id, "vibe-local-127-0-0-1");
    let ids: Vec<&str> = node.models.iter().map(|m| m.id.as_str()).collect();
    assert_eq!(ids, vec!["local-llama", "local-embed"]);
}

#[tokio::test]
async fn http_fallback_discovers_when_udp_is_silent() {
    let mock = MockUpstream::json(200, "OK", MODELS_JSON).await;
    let base = base_of(&mock);

    // No UDP targets at all: the HTTP fallback alone must find the node, the
    // path taken when a host firewall drops UDP broadcasts.
    let cfg = cfg_with(vec![], vec![base.clone()]);
    let nodes = discover(&cfg, &reqwest::Client::new()).await;

    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].base_url, base);
    assert_eq!(nodes[0].models.len(), 2);
}

#[tokio::test]
async fn offline_fallback_is_skipped() {
    // Nothing is listening on this ephemeral port: bind then drop to reserve a
    // dead address, so the HTTP probe must fail and yield no node.
    let probe = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
    let dead = probe.local_addr().expect("addr");
    drop(probe);
    let base = format!("http://{dead}");

    let cfg = cfg_with(vec![], vec![base]);
    let nodes = discover(&cfg, &reqwest::Client::new()).await;
    assert!(
        nodes.is_empty(),
        "a non-responding endpoint is not registered"
    );
}

#[tokio::test]
async fn token_headers_attach_to_probe_and_registry() {
    // A unique env var isolates this test's credential from the real
    // VIBE_LOCAL_TOKEN and from every other test running in parallel.
    let token_env = "VIBE_LOCAL_TOKEN_DISC_HEADERS_TEST";
    std::env::set_var(token_env, "node-secret-abc");

    let mut mock = MockUpstream::json(200, "OK", MODELS_JSON).await;
    let base = base_of(&mock);
    let mut cfg = cfg_with(vec![], vec![base.clone()]);
    cfg.token_env = token_env.to_string();

    let nodes = discover(&cfg, &reqwest::Client::new()).await;
    assert_eq!(nodes.len(), 1);

    let wire = mock.request_text(0).await.to_lowercase();
    assert!(
        wire.contains("authorization: bearer node-secret-abc"),
        "probe must carry the bearer token: {wire}"
    );
    assert!(
        wire.contains("x-vibe-local-token: node-secret-abc"),
        "probe must carry the local-token header: {wire}"
    );

    // Reconcile must reference the env var, never the literal secret.
    let registry = Registry::from_config(&AppConfig::minimal());
    let mut tracked = HashSet::new();
    reconcile(&registry, &nodes, &mut tracked, token_env);
    let provider = registry
        .provider(&nodes[0].provider_id)
        .expect("registered");
    match &provider.auth {
        Auth::Bearer { key_pool } => {
            assert_eq!(key_pool.keys, vec![SecretRef::Env(token_env.to_string())]);
        }
        other => panic!("expected bearer env auth, got {other:?}"),
    }
    assert!(provider
        .transport
        .headers
        .contains(&("X-Vibe-Local-Token".to_string(), format!("env:{token_env}"))));
    assert!(
        !format!("{:?}", provider.transport.headers).contains("node-secret-abc"),
        "no literal credential may be stored in the registry"
    );

    std::env::remove_var(token_env);
}

#[tokio::test]
async fn reconcile_registers_passthrough_openai_provider() {
    let registry = Registry::from_config(&AppConfig::minimal());
    let node = DiscoveredNode {
        provider_id: "vibe-local".to_string(),
        base_url: "http://127.0.0.1:20129".to_string(),
        models: vec![ModelSpec {
            id: "local-llama".to_string(),
            name: None,
            context: None,
            max_output: None,
            capabilities: Vec::new(),
        }],
    };
    let mut tracked = HashSet::new();
    reconcile(
        &registry,
        &[node],
        &mut tracked,
        "VIBE_LOCAL_TOKEN_UNSET_TEST",
    );

    let provider = registry.provider("vibe-local").expect("registered");
    assert!(provider.passthrough);
    assert_eq!(provider.transport.format, WireFormat::OpenAi);
    assert_eq!(
        provider.transport.base_url,
        "http://127.0.0.1:20129/v1/chat/completions"
    );
    assert!(matches!(provider.auth, Auth::None));
    assert!(tracked.contains("vibe-local"));
}

#[tokio::test]
async fn reconcile_removes_stale_nodes() {
    let registry = Registry::from_config(&AppConfig::minimal());
    let node = DiscoveredNode {
        provider_id: "vibe-local-192-168-1-50".to_string(),
        base_url: "http://192.168.1.50:20129".to_string(),
        models: vec![],
    };
    let mut tracked = HashSet::new();
    reconcile(
        &registry,
        &[node],
        &mut tracked,
        "VIBE_LOCAL_TOKEN_UNSET_TEST",
    );
    assert!(registry.provider("vibe-local-192-168-1-50").is_some());

    // Next pass: the node stopped responding (offline / thermal pause).
    reconcile(&registry, &[], &mut tracked, "VIBE_LOCAL_TOKEN_UNSET_TEST");
    assert!(
        registry.provider("vibe-local-192-168-1-50").is_none(),
        "a stale node must be removed from the registry"
    );
    assert!(tracked.is_empty());
}
