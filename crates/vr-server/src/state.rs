//! Server state: the wired-up stack.
//!
//! This is the *only* place in the binary that names concrete implementations:
//! the registry from `vr-providers`, the classifier from `vr-config`, the
//! HTTP transport, the health state and the store all meet here and nowhere
//! else. Routing stays generic, which is what keeps the dependency arrows
//! pointing one way.
//!
//! A config reload swaps an `Arc<AppRouter>` inside an `RwLock`. In-flight
//! requests hold their own clone of the Arc and finish on the old snapshot,
//! so a reload never interrupts one. Availability state (cooldowns, rate
//! windows) and the usage store survive reloads — they are facts about the
//! world, not about the config file.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use vr_config::{AppConfig, ConfigClassifier};
use vr_health::HealthState;
use vr_providers::{HttpUpstream, Registry};
use vr_routing::{Policy, Router};
use vr_storage::{RuntimeCombo, Store};

/// The monomorphized router for this binary.
pub type AppRouter = Router<Registry, HealthState, ConfigClassifier, HttpUpstream>;

pub struct AppState {
    /// Swapped atomically on reload; requests clone the Arc at entry.
    router: RwLock<Arc<AppRouter>>,
    /// The single shared registry: the router resolves against it and the
    /// HTTP transport reads transports from it, so an in-place `reload`
    /// updates both views at once.
    pub registry: Arc<Registry>,
    pub health: Arc<HealthState>,
    pub store: Arc<dyn Store>,
    pub backend: Arc<dyn vr_storage::StorageBackend>,
    /// Auth may be re-resolved from a reloaded config.
    pub auth: Mutex<Auth>,
    /// `host:port` the server is (or will be) bound to, for display.
    pub bind_addr: String,
    pub config_path: Option<String>,
    /// Configured role assignments, if specified in configuration.
    pub roles: RwLock<Option<vr_config::RolesConfig>>,
    /// In-memory LRU prompt/response cache.
    pub cache: Arc<vr_cache::PromptCache>,
    /// Whether the cache is consulted at all (`[cache] enabled = true`).
    pub cache_enabled: bool,
    /// Whether streaming responses may be cached (`[cache] stream_cache = true`).
    pub stream_cache_enabled: bool,
    /// Hardware profile and runtime mode (VPS, Portable, Workstation).
    pub hardware_profile: vr_core::HardwareProfile,
}

/// Client-facing API key checking.
#[derive(Debug, Clone)]
pub enum Auth {
    /// No keys configured: open service, intended for loopback use.
    Open,
    Keys(Vec<String>),
}

impl Auth {
    /// Resolve configured keys plus any `api_key_env` names into the active
    /// set. A named-but-missing env var is a startup error rather than a
    /// silent hole in the auth check.
    pub fn from_config(cfg: &AppConfig) -> Result<Self, String> {
        let mut keys: Vec<String> = cfg
            .server
            .api_keys
            .iter()
            .filter(|k| !k.trim().is_empty())
            .cloned()
            .collect();
        for name in &cfg.server.api_key_env {
            match std::env::var(name) {
                Ok(v) if !v.trim().is_empty() => keys.push(v),
                Ok(_) => return Err(format!("`{name}` is set but empty")),
                Err(_) => {
                    return Err(format!(
                        "`server.api_key_env` names `{name}`, which is not set"
                    ))
                }
            }
        }

        // Vibe-Link zero-401 handshake: check shared environment variables
        for env_key in &["VIBE_ROUTER_TOKEN", "VIBE_ROUTER_API_KEY"] {
            if let Ok(v) = std::env::var(env_key) {
                let trimmed = v.trim();
                if !trimmed.is_empty() && !keys.contains(&trimmed.to_string()) {
                    keys.push(trimmed.to_string());
                }
            }
        }

        Ok(if keys.is_empty() {
            Auth::Open
        } else {
            Auth::Keys(keys)
        })
    }

    pub fn enabled(&self) -> bool {
        matches!(self, Auth::Keys(_))
    }

    /// Constant-time check against configured keys or shared `.vibe_router_token`.
    pub fn verify(&self, presented: Option<&str>) -> bool {
        let Auth::Keys(keys) = self else {
            return true; // open mode
        };
        let Some(presented) = presented else {
            return false;
        };
        let mut matched = false;
        for key in keys {
            if ct_eq(key.as_bytes(), presented.as_bytes()) {
                matched = true;
            }
        }
        if !matched {
            // Check shared .vibe_router_token file for local tool handshake
            if let Ok(token) = std::fs::read_to_string(".vibe_router_token") {
                let trimmed = token.trim();
                if !trimmed.is_empty() && ct_eq(trimmed.as_bytes(), presented.as_bytes()) {
                    matched = true;
                }
            }
        }
        matched
    }
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

impl AppState {
    /// Build the whole stack from a loaded config.
    pub fn build(cfg: &AppConfig, store: Arc<dyn Store>) -> Result<Self, String> {
        let auth = Auth::from_config(cfg)?;
        let health = Arc::new(HealthState::new(
            Duration::from_millis(cfg.limits.cooldown.transient_ms),
            Duration::from_millis(cfg.limits.cooldown.max_ms),
        ));
        apply_rate_limits(&health, cfg);
        let registry = Arc::new(Registry::from_config(cfg));
        // Restore runtime combos persisted by a previous run so the UI's
        // editor survives restarts (they are runtime state, not file state).
        let persisted: Vec<RuntimeCombo> = store.list_combos().unwrap_or_default();
        for rc in persisted {
            registry.set_runtime_combo(vr_core::Combo {
                name: rc.name,
                description: rc.description,
                models: rc.models,
                strategy: Default::default(),
            });
        }
        // Restore runtime providers persisted by a previous run.
        let persisted_providers = store.list_providers().unwrap_or_default();
        for rp in persisted_providers {
            let p_cfg = provider_from_runtime(&rp);
            registry.set_runtime_provider(vr_providers::build_provider(&p_cfg));
        }
        let hardware_profile = vr_core::HardwareProfile::detect(None);
        registry.proxies().auto_discover_local_proxies();

        let backend_path = if cfg.storage.sqlite_path.trim().is_empty() {
            ":memory:"
        } else {
            cfg.storage.sqlite_path.as_str()
        };

        let backend: Arc<dyn vr_storage::StorageBackend> =
            if hardware_profile.memory_first_storage && backend_path != ":memory:" {
                Arc::new(
                    vr_storage::SqliteAsyncBackend::open_portable(backend_path)
                        .map_err(|e| e.to_string())?,
                )
            } else {
                Arc::new(
                    vr_storage::SqliteAsyncBackend::open_sync(backend_path)
                        .map_err(|e| e.to_string())?,
                )
            };

        let router = Arc::new(build_router(cfg, registry.clone(), health.clone()));

        let cache_mb = if cfg.cache.max_memory_mb != 64 && cfg.cache.max_memory_mb != 256 {
            cfg.cache.max_memory_mb
        } else {
            hardware_profile.cache_capacity_mb
        };

        let cache = Arc::new(vr_cache::PromptCache::new(
            cache_mb * 1024 * 1024,
            Duration::from_secs(cfg.cache.ttl_seconds),
        ));

        if auth.enabled() {
            let token_path = std::path::Path::new(".vibe_router_token");
            if !token_path.exists() {
                if let Auth::Keys(keys) = &auth {
                    if let Some(first) = keys.first() {
                        let _ = std::fs::write(token_path, first);
                    }
                }
            }
        }

        Ok(Self {
            router: RwLock::new(router),
            registry,
            health,
            store,
            backend,
            auth: Mutex::new(auth),
            bind_addr: cfg.bind_addr(),
            config_path: None,
            roles: RwLock::new(cfg.roles.clone()),
            cache,
            cache_enabled: cfg.cache.enabled,
            stream_cache_enabled: cfg.cache.stream_cache,
            hardware_profile,
        })
    }

    pub fn with_backend(mut self, backend: Arc<dyn vr_storage::StorageBackend>) -> Self {
        self.backend = backend;
        self
    }

    pub fn with_config_path(mut self, path: Option<String>) -> Self {
        self.config_path = path;
        self
    }

    pub fn router(&self) -> Arc<AppRouter> {
        self.router.read().clone()
    }

    /// Rebuild from fresh config: auth re-resolved, rate limits re-applied,
    /// registry reloaded in place (the transport and router both point at
    /// that one Arc), then the router swapped so classifier rules and policy
    /// budgets take effect. Health and storage are deliberately untouched.
    pub fn swap_config(&self, cfg: &AppConfig) -> Result<(), String> {
        *self.auth.lock() = Auth::from_config(cfg)?;
        apply_rate_limits(&self.health, cfg);
        *self.roles.write() = cfg.roles.clone();
        self.registry.reload(cfg);
        let router = Arc::new(build_router(
            cfg,
            self.registry.clone(),
            self.health.clone(),
        ));
        *self.router.write() = router;
        Ok(())
    }
}

/// v0.1 supports one rate-limit scope: fixed-window RPM per `provider/model`.
/// Matching on the enum is exhaustive *on purpose*: when RPD/TPM/concurrency
/// join `RateLimitScope`, this is the line that fails to compile and forces
/// the accounting decision, instead of new scopes silently doing nothing.
fn apply_rate_limits(health: &HealthState, cfg: &AppConfig) {
    use vr_config::RateLimitScope;
    for (model, limit) in &cfg.limits.rate_limits {
        match limit.scope {
            RateLimitScope::RequestsPerMinute => {
                health.set_rpm(model, limit.limit, limit.safety_margin_pct);
            }
        }
    }
}

fn build_router(cfg: &AppConfig, registry: Arc<Registry>, health: Arc<HealthState>) -> AppRouter {
    let classifier = Arc::new(ConfigClassifier::new(
        &cfg.limits,
        &cfg.strategy,
        cfg.router.error_rules.clone(),
    ));
    let upstream = Arc::new(HttpUpstream::new(registry.clone()));
    Router::new(
        registry,
        health,
        classifier,
        upstream,
        policy_from_config(cfg),
    )
}

fn policy_from_config(cfg: &AppConfig) -> Policy {
    Policy {
        request_budget: Duration::from_millis(cfg.strategy.request_budget_ms),
        connect_timeout: Duration::from_millis(cfg.strategy.connect_timeout_ms),
        first_chunk_timeout: Duration::from_millis(cfg.strategy.first_chunk_timeout_ms),
        chunk_stall_timeout: Duration::from_millis(cfg.strategy.stall_timeout_ms),
        default_retry_delay: DEFAULT_RETRY_DELAY,
    }
}

/// Floor between same-candidate retries when the retry table gives no delay.
const DEFAULT_RETRY_DELAY: Duration = Duration::from_millis(500);

pub fn provider_from_runtime(rp: &vr_storage::RuntimeProvider) -> vr_config::ProviderConfig {
    vr_config::ProviderConfig {
        id: rp.id.clone(),
        display_name: rp.display_name.clone(),
        base_url: rp.base_url.clone(),
        format: vr_config::WireFormatSerde(rp.format.clone().unwrap_or_else(|| "openai".into())),
        auth: vr_config::AuthConfig {
            scheme: match rp.auth_scheme.as_deref().unwrap_or("Bearer") {
                "Bearer" | "bearer" => vr_config::AuthScheme::Bearer,
                custom => vr_config::AuthScheme::Custom {
                    header_name: custom.into(),
                },
            },
            secret: rp.auth_secret.clone().unwrap_or_default(),
            secrets: Vec::new(),
            api_keys: rp.api_keys.clone(),
            strategy: rp.key_strategy.clone(),
        },
        api_keys: rp.api_keys.clone(),
        key_strategy: rp.key_strategy.clone(),
        headers: rp.headers.clone(),
        models: rp
            .models
            .iter()
            .map(|m| vr_config::ModelConfig {
                id: m.id.clone(),
                name: m.name.clone(),
                context: m.context,
                max_output: m.max_output,
            })
            .collect(),
        passthrough: rp.passthrough,
    }
}

pub fn runtime_from_provider_config(p: &vr_config::ProviderConfig) -> vr_storage::RuntimeProvider {
    vr_storage::RuntimeProvider {
        id: p.id.clone(),
        display_name: p.display_name.clone(),
        base_url: p.base_url.clone(),
        format: Some(p.format.0.clone()),
        auth_scheme: Some(match &p.auth.scheme {
            vr_config::AuthScheme::Bearer => "Bearer".into(),
            vr_config::AuthScheme::Custom { header_name } => header_name.clone(),
        }),
        auth_secret: Some(p.auth.secret.clone()),
        api_keys: if !p.api_keys.is_empty() {
            p.api_keys.clone()
        } else if !p.auth.api_keys.is_empty() {
            p.auth.api_keys.clone()
        } else {
            p.auth.secrets.clone()
        },
        key_strategy: p.key_strategy.clone(),
        headers: p.headers.clone(),
        models: p
            .models
            .iter()
            .map(|m| vr_storage::RuntimeModel {
                id: m.id.clone(),
                name: m.name.clone(),
                context: m.context,
                max_output: m.max_output,
            })
            .collect(),
        passthrough: p.passthrough,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> AppConfig {
        AppConfig::minimal()
    }

    #[test]
    fn open_mode_accepts_anything() {
        let auth = Auth::from_config(&cfg()).unwrap();
        assert!(!auth.enabled());
        assert!(auth.verify(None));
        assert!(auth.verify(Some("garbage")));
    }

    #[test]
    fn configured_key_is_required() {
        let mut c = cfg();
        c.server.api_keys = vec!["sk-good".into()];
        let auth = Auth::from_config(&c).unwrap();
        assert!(auth.enabled());
        assert!(!auth.verify(None));
        assert!(!auth.verify(Some("sk-bad")));
        assert!(auth.verify(Some("sk-good")));
    }

    #[test]
    fn env_named_but_unset_is_a_startup_error() {
        std::env::remove_var("VR_TEST_NO_SUCH_KEY");
        let mut c = cfg();
        c.server.api_key_env = vec!["VR_TEST_NO_SUCH_KEY".into()];
        let err = Auth::from_config(&c).unwrap_err();
        assert!(err.contains("VR_TEST_NO_SUCH_KEY"), "{err}");
    }

    #[test]
    fn empty_configured_keys_fall_through_to_open() {
        let mut c = cfg();
        c.server.api_keys = vec!["".into(), "   ".into()];
        let auth = Auth::from_config(&c).unwrap();
        assert!(!auth.enabled());
    }

    #[tokio::test]
    async fn build_restores_persisted_runtime_combos() {
        // A store holding a combo from a "previous run".
        let store: Arc<dyn Store> = Arc::new(vr_storage::MemoryStore::default());
        store
            .upsert_combo(&RuntimeCombo {
                name: "restored".into(),
                description: None,
                models: vec!["a/x".into()],
            })
            .unwrap();
        let mut c = cfg();
        c.providers = vec![vr_config::schema::ProviderConfig {
            id: "a".into(),
            display_name: None,
            base_url: "http://127.0.0.1:1/v1/chat/completions".into(),
            format: Default::default(),
            auth: Default::default(),
            api_keys: vec![],
            key_strategy: None,
            headers: vec![],
            models: vec![vr_config::schema::ModelConfig {
                id: "x".into(),
                name: None,
                context: None,
                max_output: None,
            }],
            passthrough: false,
        }];
        c.router.providers = c.providers.clone();
        let state = AppState::build(&c, store).unwrap();
        assert!(state.registry.combo("restored").is_some());
        // And it resolves.
        use vr_core::ModelRegistry;
        assert!(state.registry.resolve("restored").is_ok());
    }
}
