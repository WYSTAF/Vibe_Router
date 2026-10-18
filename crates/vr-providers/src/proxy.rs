//! Smart proxy diversification, tagged proxy pools, and Cloudflare Worker Relay.
//!
//! Solves upstream mass-banning and proxy-bleed:
//!   * Tagged proxy pools (`cf-worker`, `datacenter`, `residential`, `direct`).
//!   * Sticky IP affinity hashing: `proxy_index = fnv1a(key_identifier) % matching_pool.len()`.
//!     Each API key consistently routes through the same proxy instance to avoid IP-hopping fraud flags.
//!   * Built-in Cloudflare Worker Reverse-Proxy Relay script ready to deploy.

use std::str::FromStr;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Ready-to-deploy Cloudflare Worker JavaScript reverse-proxy relay script.
pub const CLOUDFLARE_WORKER_SCRIPT: &str = r#"/**
 * Vibe Router - Cloudflare Worker Relay
 * High-performance edge reverse-proxy for LLM APIs with SSE streaming support.
 *
 * Usage:
 *   1. Create a Cloudflare Worker (Workers & Pages -> Create Application -> Quick edit).
 *   2. Paste this entire script into the Worker's code editor and deploy.
 *   3. Register the worker URL in Vibe Router as a proxy with tag `cf-worker`:
 *      vibe-router proxy add https://your-worker.your-subdomain.workers.dev --tag cf-worker
 *
 * Supported Headers:
 *   * X-Target-URL: Overrides the upstream destination (e.g. https://api.openai.com/v1/chat/completions).
 *   * Authorization / X-API-Key: Forwarded directly to upstream.
 */

export default {
  async fetch(request, env, ctx) {
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
          "Access-Control-Allow-Headers": "*",
          "Access-Control-Max-Age": "86400",
        },
      });
    }

    const url = new URL(request.url);
    let target = request.headers.get("X-Target-URL");
    if (!target) {
      target = "https://api.openai.com" + url.pathname + url.search;
    }

    const upstreamUrl = new URL(target);
    const newHeaders = new Headers(request.headers);
    newHeaders.delete("X-Target-URL");
    newHeaders.set("Host", upstreamUrl.host);

    const upstreamReq = new Request(upstreamUrl.toString(), {
      method: request.method,
      headers: newHeaders,
      body: request.body,
      redirect: "follow",
    });

    try {
      const response = await fetch(upstreamReq);
      const respHeaders = new Headers(response.headers);
      respHeaders.set("Access-Control-Allow-Origin", "*");
      respHeaders.set("X-Relayed-By", "Vibe-Router-Worker-Relay");

      return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: respHeaders,
      });
    } catch (err) {
      return new Response(JSON.stringify({
        error: {
          message: "Cloudflare Worker Relay upstream connect failed: " + err.message,
          type: "relay_error"
        }
      }), {
        status: 502,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        }
      });
    }
  }
};
"#;

/// Proxy tier tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProxyTag {
    CfWorker,
    Datacenter,
    Residential,
    Direct,
}

impl FromStr for ProxyTag {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "cf-worker" | "cf_worker" | "cfworker" => Ok(ProxyTag::CfWorker),
            "datacenter" | "dc" => Ok(ProxyTag::Datacenter),
            "residential" | "res" => Ok(ProxyTag::Residential),
            "direct" => Ok(ProxyTag::Direct),
            other => Err(format!("unknown proxy tag: `{other}`")),
        }
    }
}

impl std::fmt::Display for ProxyTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProxyTag::CfWorker => write!(f, "cf-worker"),
            ProxyTag::Datacenter => write!(f, "datacenter"),
            ProxyTag::Residential => write!(f, "residential"),
            ProxyTag::Direct => write!(f, "direct"),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_weight() -> u32 {
    1
}

/// A registered proxy endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyEntry {
    pub id: String,
    /// Upstream proxy URL, e.g. `http://proxy.internal:8080` or `https://worker.subdomain.workers.dev`
    pub url: String,
    pub tag: ProxyTag,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_weight")]
    pub weight: u32,
}

impl ProxyEntry {
    pub fn new(id: impl Into<String>, url: impl Into<String>, tag: ProxyTag) -> Self {
        Self {
            id: id.into(),
            url: url.into(),
            tag,
            enabled: true,
            weight: 1,
        }
    }
}

/// FNV-1a 64-bit hash for deterministic sticky affinity.
pub fn fnv1a_hash(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Deterministic sticky proxy index for a key identifier over a pool of size `pool_len`.
pub fn sticky_proxy_index(key_identifier: &str, pool_len: usize) -> usize {
    if pool_len == 0 {
        0
    } else {
        (fnv1a_hash(key_identifier) as usize) % pool_len
    }
}

/// Thread-safe manager for tagged proxies.
#[derive(Debug, Default, Clone)]
pub struct ProxyPool {
    proxies: Arc<RwLock<Vec<ProxyEntry>>>,
}

impl ProxyPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_proxies(proxies: Vec<ProxyEntry>) -> Self {
        Self {
            proxies: Arc::new(RwLock::new(proxies)),
        }
    }

    /// Add or update a proxy entry by id.
    pub fn add(&self, proxy: ProxyEntry) {
        let mut list = self.proxies.write();
        list.retain(|p| p.id != proxy.id);
        list.push(proxy);
    }

    /// Delete a proxy entry by id.
    pub fn remove(&self, id: &str) -> bool {
        let mut list = self.proxies.write();
        let len_before = list.len();
        list.retain(|p| p.id != id);
        list.len() < len_before
    }

    /// List all registered proxies.
    pub fn list(&self) -> Vec<ProxyEntry> {
        self.proxies.read().clone()
    }

    /// Number of active proxies.
    pub fn len(&self) -> usize {
        self.proxies.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.proxies.read().is_empty()
    }

    /// Auto-discover host environment proxies and local VPN ports (`10808`, `10809`, `7890`, `2080`).
    pub fn auto_discover_local_proxies(&self) {
        // 1. Host proxy environment variables
        let env_keys = [
            "HTTPS_PROXY",
            "https_proxy",
            "HTTP_PROXY",
            "http_proxy",
            "ALL_PROXY",
            "all_proxy",
        ];
        for key in &env_keys {
            if let Ok(val) = std::env::var(key) {
                let trimmed = val.trim();
                if !trimmed.is_empty() {
                    self.add(ProxyEntry {
                        id: format!("env-{key}"),
                        url: trimmed.to_string(),
                        tag: ProxyTag::Direct,
                        enabled: true,
                        weight: 1,
                    });
                    tracing::info!(env = key, url = trimmed, "discovered host proxy from env");
                    break;
                }
            }
        }

        // 2. Probe common local VPN ports (v2ray, Clash, Mihomo, Surge)
        let local_ports = [
            ("local-vpn-10808", "127.0.0.1:10808"),
            ("local-vpn-10809", "127.0.0.1:10809"),
            ("local-vpn-7890", "127.0.0.1:7890"),
            ("local-vpn-2080", "127.0.0.1:2080"),
        ];

        for (id, addr_str) in &local_ports {
            if let Ok(addr) = addr_str.parse() {
                if std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(50))
                    .is_ok()
                {
                    self.add(ProxyEntry {
                        id: id.to_string(),
                        url: format!("http://{addr_str}"),
                        tag: ProxyTag::Direct,
                        enabled: true,
                        weight: 1,
                    });
                    tracing::info!(
                        id = id,
                        addr = addr_str,
                        "discovered active local VPN proxy"
                    );
                }
            }
        }
    }

    /// Select a sticky proxy deterministically for `key_identifier` with optional tag whitelist.
    pub fn select_sticky(
        &self,
        key_identifier: &str,
        tag_whitelist: Option<&[ProxyTag]>,
    ) -> Option<ProxyEntry> {
        let list = self.proxies.read();
        let matching: Vec<&ProxyEntry> = list
            .iter()
            .filter(|p| p.enabled)
            .filter(|p| {
                if let Some(tags) = tag_whitelist {
                    tags.contains(&p.tag)
                } else {
                    true
                }
            })
            .collect();

        if matching.is_empty() {
            None
        } else {
            let idx = sticky_proxy_index(key_identifier, matching.len());
            Some(matching[idx].clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sticky_affinity_hashing_consistency() {
        let key_a = "sk-live-alpha-123456789";
        let key_b = "sk-live-beta-987654321";
        let pool_size = 5;

        let idx_a1 = sticky_proxy_index(key_a, pool_size);
        let idx_a2 = sticky_proxy_index(key_a, pool_size);
        let idx_b = sticky_proxy_index(key_b, pool_size);

        assert_eq!(
            idx_a1, idx_a2,
            "identical key must yield identical proxy index"
        );
        assert!(idx_a1 < pool_size);
        assert!(idx_b < pool_size);
    }

    #[test]
    fn test_proxy_pool_tag_filtering() {
        let pool = ProxyPool::new();
        pool.add(ProxyEntry::new(
            "p1",
            "http://worker.test",
            ProxyTag::CfWorker,
        ));
        pool.add(ProxyEntry::new(
            "p2",
            "http://dc.test",
            ProxyTag::Datacenter,
        ));
        pool.add(ProxyEntry::new(
            "p3",
            "http://res.test",
            ProxyTag::Residential,
        ));

        let key = "sk-test-key";

        // Filter only cf-worker
        let selected_cf = pool
            .select_sticky(key, Some(&[ProxyTag::CfWorker]))
            .unwrap();
        assert_eq!(selected_cf.id, "p1");
        assert_eq!(selected_cf.tag, ProxyTag::CfWorker);

        // Filter datacenter or residential
        let selected_other = pool
            .select_sticky(key, Some(&[ProxyTag::Datacenter, ProxyTag::Residential]))
            .unwrap();
        assert!(selected_other.id == "p2" || selected_other.id == "p3");

        // Filter tag with no matches
        let none = pool.select_sticky(key, Some(&[ProxyTag::Direct]));
        assert!(none.is_none());
    }

    #[test]
    fn test_proxy_tag_parsing() {
        assert_eq!("cf-worker".parse::<ProxyTag>().unwrap(), ProxyTag::CfWorker);
        assert_eq!(
            "datacenter".parse::<ProxyTag>().unwrap(),
            ProxyTag::Datacenter
        );
        assert_eq!(
            "residential".parse::<ProxyTag>().unwrap(),
            ProxyTag::Residential
        );
        assert_eq!("direct".parse::<ProxyTag>().unwrap(), ProxyTag::Direct);
        assert!("invalid".parse::<ProxyTag>().is_err());
    }

    #[test]
    fn test_cloudflare_worker_script_contains_essentials() {
        assert!(CLOUDFLARE_WORKER_SCRIPT.contains("X-Target-URL"));
        assert!(CLOUDFLARE_WORKER_SCRIPT.contains("Vibe Router - Cloudflare Worker Relay"));
        assert!(CLOUDFLARE_WORKER_SCRIPT.contains("export default"));
    }

    #[test]
    fn test_auto_discover_local_proxies_from_env() {
        std::env::set_var("HTTPS_PROXY", "http://127.0.0.1:9999");
        let pool = ProxyPool::new();
        pool.auto_discover_local_proxies();
        let list = pool.list();
        assert!(list.iter().any(|p| p.url == "http://127.0.0.1:9999"));
        std::env::remove_var("HTTPS_PROXY");
    }
}
