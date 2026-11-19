//! Remote model-catalog synchronization (the `Vibe_Radar` feed).
//!
//! A catalog is a JSON document advertising providers, their models, and a
//! `daily_free_chain` of `provider/model` entries. [`sync_remote_catalog`]
//! fetches it on a bounded budget and reconciles it into the [`Registry`]:
//!
//!   * **Credentials are preserved.** A provider id that already exists keeps
//!     its configured `transport` and `auth` (e.g. `env:OPENROUTER_API_KEY`);
//!     only newly advertised models are merged onto a runtime clone.
//!   * **New catalog-only providers are SSRF-guarded** — only public `http://`
//!     / `https://` hosts are accepted — and registered as anonymous,
//!     passthrough OpenAI providers.
//!   * **Stale entries are removed** when they disappear from the feed.
//!   * **`daily-free`** is upserted as a runtime combo from the resolvable
//!     entries of `daily_free_chain`, with a `free` alias when unset.
//!
//! Config remains the source of truth for auth and endpoints; the catalog only
//! fills in model metadata and free-tier chains. Reads are snapshot reads, so a
//! swap is atomic from the request path's point of view.
//!
//! Depends on: vibe-router-core, vibe-router-config (via the registry).

use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use vibe_router_core::{Auth, Combo, ModelRegistry, ModelSpec, Transport, WireFormat};

use crate::pool::ProviderKeyPool;
use crate::{ConfigProvider, Registry};

/// Hard cap on the catalog response body (2 MB).
pub const MAX_CATALOG_BYTES: usize = 2 * 1024 * 1024;
/// Bounded wall-clock budget for fetching the catalog.
pub const FETCH_TIMEOUT: Duration = Duration::from_secs(15);
/// Name of the auto-generated free-tier combo.
pub const DAILY_FREE_COMBO: &str = "daily-free";
/// Alias pointing at [`DAILY_FREE_COMBO`], registered only when unset.
pub const FREE_ALIAS: &str = "free";

/// The `Vibe_Radar` catalog feed.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RemoteCatalog {
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub providers: Vec<CatalogProvider>,
    #[serde(default)]
    pub daily_free_chain: Vec<String>,
}

/// A provider advertised by the catalog.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CatalogProvider {
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub base_url: String,
    #[serde(default)]
    pub models: Vec<CatalogModel>,
}

/// A model advertised by the catalog.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CatalogModel {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub context: Option<u32>,
    #[serde(default)]
    pub max_output: Option<u32>,
}

impl CatalogModel {
    fn to_spec(&self) -> ModelSpec {
        ModelSpec {
            id: self.id.clone(),
            name: self.name.clone(),
            context: self.context,
            max_output: self.max_output,
            capabilities: Vec::new(),
        }
    }
}

/// Fetch the catalog at `url` and reconcile it into `registry`. Returns the
/// number of providers registered or merged. `tracked_providers` carries the
/// catalog-registered ids from earlier passes so stale ones can be removed.
///
/// Non-fatal by contract: any fetch/parse failure returns `Err` and leaves the
/// registry untouched, so the serve loop keeps the previous snapshot.
pub async fn sync_remote_catalog(
    client: &reqwest::Client,
    registry: &Registry,
    url: &str,
    tracked_providers: &mut HashSet<String>,
) -> Result<usize, String> {
    let resp = client
        .get(url)
        .timeout(FETCH_TIMEOUT)
        .send()
        .await
        .map_err(|e| format!("catalog fetch failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("catalog returned HTTP {}", resp.status()));
    }
    if let Some(len) = resp.content_length() {
        if len as usize > MAX_CATALOG_BYTES {
            return Err(format!("catalog body exceeds {MAX_CATALOG_BYTES} byte cap"));
        }
    }
    let mut buf: Vec<u8> = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("catalog read failed: {e}"))?;
        if buf.len() + chunk.len() > MAX_CATALOG_BYTES {
            return Err(format!("catalog body exceeds {MAX_CATALOG_BYTES} byte cap"));
        }
        buf.extend_from_slice(&chunk);
    }
    let catalog: RemoteCatalog =
        serde_json::from_slice(&buf).map_err(|e| format!("catalog parse failed: {e}"))?;

    let mut current: HashSet<String> = HashSet::new();
    for cp in &catalog.providers {
        if registry.provider(&cp.id).is_some() {
            // Existing provider (file or runtime): overlay only the advertised
            // models. Its configured transport, auth, and key pool stay intact
            // and authoritative — a config hot reload is never masked. The full
            // advertised list is stored so repeated syncs are idempotent.
            let specs: Vec<ModelSpec> = cp.models.iter().map(CatalogModel::to_spec).collect();
            registry.set_catalog_models(&cp.id, specs);
            current.insert(cp.id.clone());
            continue;
        }

        if !catalog_url_is_safe(&cp.base_url) {
            tracing::warn!(
                provider = %cp.id,
                "catalog provider rejected: base_url is not a public http(s) host"
            );
            continue;
        }
        registry.set_runtime_provider(ConfigProvider {
            id: cp.id.clone(),
            display_name: cp.display_name.clone().unwrap_or_else(|| cp.id.clone()),
            transport: Transport {
                base_url: cp.base_url.clone(),
                format: WireFormat::OpenAi,
                headers: Vec::new(),
            },
            auth: Auth::None,
            models: cp.models.iter().map(CatalogModel::to_spec).collect(),
            passthrough: true,
            key_pool: Arc::new(ProviderKeyPool::empty()),
        });
        current.insert(cp.id.clone());
    }

    for id in tracked_providers.difference(&current) {
        registry.remove_runtime_provider(id);
        registry.remove_catalog_models(id);
    }
    let registered = current.len();
    *tracked_providers = current;

    // Auto-combo from the resolvable free-tier chain.
    let chain: Vec<String> = catalog
        .daily_free_chain
        .iter()
        .filter(|entry| registry.resolve(entry).is_ok())
        .cloned()
        .collect();
    if chain.is_empty() {
        registry.remove_runtime_combo(DAILY_FREE_COMBO);
    } else {
        registry.set_runtime_combo(Combo {
            name: DAILY_FREE_COMBO.to_string(),
            description: Some(
                "Auto-generated from the remote catalog daily_free_chain".to_string(),
            ),
            models: chain,
            strategy: Default::default(),
        });
        if registry.get_alias(FREE_ALIAS).is_none() {
            registry.set_alias(FREE_ALIAS, DAILY_FREE_COMBO);
        }
    }

    Ok(registered)
}

/// SSRF policy for catalog-only providers: only `http`/`https` URLs whose host
/// is not loopback, link-local, or private. Mirrors the admin-boundary guard;
/// local nodes are the job of [`crate::discovery`], not the public catalog.
fn catalog_url_is_safe(raw: &str) -> bool {
    let Ok(u) = url::Url::parse(raw) else {
        return false;
    };
    if !matches!(u.scheme(), "http" | "https") {
        return false;
    }
    let Some(host) = u.host_str() else {
        return false;
    };
    !host_is_blocked(host)
}

fn host_is_blocked(host: &str) -> bool {
    let h = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    if h == "localhost" || h.ends_with(".localhost") {
        return true;
    }
    if let Ok(ip) = h.parse::<IpAddr>() {
        return match ip {
            IpAddr::V4(v4) => {
                v4.is_loopback()
                    || v4.is_link_local()
                    || v4.is_private()
                    || v4.is_unspecified()
                    || v4.is_broadcast()
            }
            IpAddr::V6(v6) => {
                let seg = v6.segments();
                v6.is_loopback()
                    || v6.is_unspecified()
                    || (seg[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
                    || (seg[0] & 0xfe00) == 0xfc00 // fc00::/7 unique-local
            }
        };
    }
    false
}
