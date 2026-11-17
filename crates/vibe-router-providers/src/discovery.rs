//! Automatic discovery of local "Vibe Local" worker nodes (this PC and LAN
//! pocket workers).
//!
//! Two complementary probes, neither requiring a heavy mDNS stack:
//!   * a UDP magic packet (`VIBE_DISCOVER_V1`) sent to loopback and the local
//!     broadcast address on port 20131; a node answers with its base URL,
//!   * an HTTP fallback (`GET <base>/v1/models`) for hosts whose firewall drops
//!     UDP broadcasts.
//!
//! Every responding endpoint is queried for its OpenAI-shaped model list and
//! reconciled into the [`Registry`] as a passthrough OpenAI runtime provider.
//! Nodes that stop responding are removed on the next pass.
//!
//! Probe targets are injectable so tests stay hermetic: they point at an
//! ephemeral loopback UDP responder and a [`crate::mock::MockUpstream`] instead
//! of touching the real broadcast address.
//!
//! Depends on: vibe-router-core, vibe-router-config (via the registry).

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use vibe_router_core::{
    Auth, KeyPool, KeySelectionStrategy, ModelSpec, SecretRef, Transport, WireFormat,
};

use crate::pool::ProviderKeyPool;
use crate::{ConfigProvider, Registry};

/// Magic payload a Vibe Local node recognises as a discovery probe.
pub const PROBE_MAGIC: &str = "VIBE_DISCOVER_V1";
/// UDP port nodes listen on for discovery probes.
pub const DEFAULT_UDP_PORT: u16 = 20131;
/// HTTP port a local node serves its OpenAI-compatible API on.
pub const DEFAULT_HTTP_PORT: u16 = 20129;
/// Environment variable holding the shared local-node token, when set.
pub const TOKEN_ENV: &str = "VIBE_LOCAL_TOKEN";

/// Where and how to probe. Defaults target the real local/LAN surfaces; tests
/// override the targets to keep discovery off the network.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// UDP destinations for the magic probe.
    pub udp_targets: Vec<SocketAddr>,
    /// Base URLs (`http://host:port`, no path) probed directly over HTTP.
    pub http_fallbacks: Vec<String>,
    /// How long to listen for UDP answers after sending the probes.
    pub recv_window: Duration,
    /// Per-request HTTP timeout when fetching a node's model list.
    pub http_timeout: Duration,
    /// Environment variable name read for the node token. Configurable so tests
    /// can isolate their credential from the real `VIBE_LOCAL_TOKEN`.
    pub token_env: String,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            udp_targets: vec![
                SocketAddr::from(([127, 0, 0, 1], DEFAULT_UDP_PORT)),
                SocketAddr::from(([255, 255, 255, 255], DEFAULT_UDP_PORT)),
            ],
            http_fallbacks: vec![format!("http://127.0.0.1:{DEFAULT_HTTP_PORT}")],
            recv_window: Duration::from_millis(400),
            http_timeout: Duration::from_secs(2),
            token_env: TOKEN_ENV.to_string(),
        }
    }
}

/// A responding node with its ingested model list.
#[derive(Debug, Clone)]
pub struct DiscoveredNode {
    /// Registry provider id (`vibe-local` or `vibe-local-<sanitized-host>`).
    pub provider_id: String,
    /// Base URL with no path, e.g. `http://192.168.1.50:20129`.
    pub base_url: String,
    /// Models advertised by `GET <base>/v1/models`.
    pub models: Vec<ModelSpec>,
}

/// Run one discovery pass: probe UDP targets and HTTP fallbacks, then fetch the
/// model list of every unique responding endpoint.
pub async fn discover(cfg: &DiscoveryConfig, client: &reqwest::Client) -> Vec<DiscoveredNode> {
    let mut bases: Vec<String> = Vec::new();

    if !cfg.udp_targets.is_empty() {
        if let Ok(sock) = UdpSocket::bind("0.0.0.0:0").await {
            let _ = sock.set_broadcast(true);
            for target in &cfg.udp_targets {
                let _ = sock.send_to(PROBE_MAGIC.as_bytes(), target).await;
            }
            let deadline = tokio::time::Instant::now() + cfg.recv_window;
            let mut buf = [0u8; 512];
            loop {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    break;
                }
                match tokio::time::timeout(deadline - now, sock.recv_from(&mut buf)).await {
                    Ok(Ok((n, _peer))) => {
                        if let Ok(text) = std::str::from_utf8(&buf[..n]) {
                            let text = text.trim().trim_end_matches('/');
                            if text.starts_with("http://") || text.starts_with("https://") {
                                bases.push(text.to_string());
                            }
                        }
                    }
                    _ => break,
                }
            }
        }
    }

    for base in &cfg.http_fallbacks {
        bases.push(base.trim_end_matches('/').to_string());
    }

    let mut seen = HashSet::new();
    bases.retain(|b| seen.insert(b.clone()));

    let token = std::env::var(&cfg.token_env).ok();
    let mut nodes = Vec::new();
    for base in bases {
        let mut req = client
            .get(format!("{base}/v1/models"))
            .timeout(cfg.http_timeout);
        if let Some(t) = &token {
            req = req
                .header("Authorization", format!("Bearer {t}"))
                .header("X-Vibe-Local-Token", t);
        }
        let Ok(resp) = req.send().await else {
            continue;
        };
        if !resp.status().is_success() {
            continue;
        }
        let Ok(json) = resp.json::<serde_json::Value>().await else {
            continue;
        };
        let Some(arr) = json.get("data").and_then(|d| d.as_array()) else {
            continue;
        };
        let models: Vec<ModelSpec> = arr
            .iter()
            .filter_map(|m| m.get("id").and_then(|i| i.as_str()))
            .map(|id| ModelSpec {
                id: id.to_string(),
                name: None,
                context: None,
                max_output: None,
                capabilities: Vec::new(),
            })
            .collect();
        nodes.push(DiscoveredNode {
            provider_id: provider_id_for(&base),
            base_url: base,
            models,
        });
    }
    nodes
}

/// Register discovered nodes as passthrough OpenAI runtime providers and remove
/// previously-discovered ids that no longer respond. `tracked` carries the ids
/// registered by earlier passes so staleness can be detected across scans.
pub fn reconcile(
    registry: &Registry,
    nodes: &[DiscoveredNode],
    tracked: &mut HashSet<String>,
    token_env: &str,
) {
    let token_set = std::env::var(token_env).is_ok();
    let mut current = HashSet::new();
    for node in nodes {
        current.insert(node.provider_id.clone());
        let (auth, key_pool, headers) = if token_set {
            let secret = SecretRef::Env(token_env.to_string());
            (
                Auth::Bearer {
                    key_pool: KeyPool::single(secret.clone()),
                },
                ProviderKeyPool::new(vec![secret], KeySelectionStrategy::RoundRobin),
                vec![("X-Vibe-Local-Token".to_string(), format!("env:{token_env}"))],
            )
        } else {
            (Auth::None, ProviderKeyPool::empty(), Vec::new())
        };
        registry.set_runtime_provider(ConfigProvider {
            id: node.provider_id.clone(),
            display_name: format!("Vibe Local ({})", node.base_url),
            transport: Transport {
                base_url: format!("{}/v1/chat/completions", node.base_url),
                format: WireFormat::OpenAi,
                headers,
            },
            auth,
            models: node.models.clone(),
            passthrough: true,
            key_pool: Arc::new(key_pool),
        });
    }

    for id in tracked.difference(&current) {
        registry.remove_runtime_provider(id);
    }
    *tracked = current;
}

/// Convenience for the serve loop: one pass with the default probe surfaces and
/// a fresh HTTP client, reconciled into `registry`.
pub async fn scan_and_reconcile(registry: &Registry, tracked: &mut HashSet<String>) {
    let cfg = DiscoveryConfig::default();
    let client = reqwest::Client::new();
    let nodes = discover(&cfg, &client).await;
    reconcile(registry, &nodes, tracked, &cfg.token_env);
}

/// `vibe-local` for the canonical loopback node, otherwise
/// `vibe-local-<sanitized-host>` for LAN workers.
fn provider_id_for(base: &str) -> String {
    let hostport = base.split("://").nth(1).unwrap_or(base);
    let hostport = hostport.split('/').next().unwrap_or(hostport);
    if hostport == format!("127.0.0.1:{DEFAULT_HTTP_PORT}")
        || hostport == format!("localhost:{DEFAULT_HTTP_PORT}")
    {
        return "vibe-local".to_string();
    }
    let host = hostport.split(':').next().unwrap_or(hostport);
    let sanitized: String = host
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    format!("vibe-local-{sanitized}")
}

#[cfg(test)]
mod tests {
    use super::provider_id_for;

    #[test]
    fn canonical_loopback_node_is_vibe_local() {
        assert_eq!(provider_id_for("http://127.0.0.1:20129"), "vibe-local");
        assert_eq!(provider_id_for("http://localhost:20129"), "vibe-local");
    }

    #[test]
    fn lan_node_uses_sanitized_host() {
        assert_eq!(
            provider_id_for("http://192.168.1.50:20129"),
            "vibe-local-192-168-1-50"
        );
        assert_eq!(
            provider_id_for("http://pocket.worker.lan:20129"),
            "vibe-local-pocket-worker-lan"
        );
    }

    #[test]
    fn non_default_loopback_port_is_not_canonical() {
        assert_eq!(
            provider_id_for("http://127.0.0.1:20999"),
            "vibe-local-127-0-0-1"
        );
    }
}
