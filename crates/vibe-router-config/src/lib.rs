//! Configuration for Vibe Router.
//!
//! Static configuration lives in TOML; secrets are resolved from the
//! environment at request time and never appear in config objects. The file is
//! watched and re-applied on change without dropping in-flight requests.
//!
//! Depends on: vibe-router-core. Nothing else.

#![forbid(unsafe_code)]

mod classify;
mod error;
mod file;
pub mod schema;

pub use classify::ConfigClassifier;
pub use error::{ConfigError, ConfigResult};

#[cfg(feature = "watch")]
pub use file::watch;
pub use file::ConfigSource;
pub use schema::{
    AuthConfig, AuthScheme, CacheConfig, ComboConfig, ErrorRule, ErrorRuleMatch, LimitsConfig,
    ModelConfig, ProviderConfig, RateLimitConfig, RateLimitScope, RetryConfig, RetryEntry,
    RoleConfig, RolesConfig, RouterConfig, ServerConfig, StorageConfig, StrategyConfig,
    WireFormatSerde,
};

/// A fully loaded, validated configuration plus the secret environment.
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub router: RouterConfig,
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub limits: LimitsConfig,
    pub strategy: StrategyConfig,
    pub cache: CacheConfig,
    pub providers: Vec<ProviderConfig>,
    pub combos: Vec<ComboConfig>,
    pub roles: Option<RolesConfig>,
    /// Where the TOML was loaded from (for display in `status`).
    pub source_path: Option<String>,
}

impl AppConfig {
    /// Load from a TOML file, resolving `env:` secret references against the
    /// process environment.
    pub fn load(path: &str) -> ConfigResult<Self> {
        let router = RouterConfig::load(path)?;
        let source_path = Some(path.to_string());
        Ok(Self {
            server: router.server.clone(),
            storage: router.storage.clone(),
            limits: router.limits.clone(),
            strategy: router.strategy.clone(),
            cache: router.cache.clone(),
            providers: router.providers.clone(),
            combos: router.combos.clone(),
            roles: router.roles.clone(),
            router,
            source_path,
        })
    }

    /// Parse from inline TOML text (used by tests and `config validate`).
    pub fn parse_str(toml: &str) -> ConfigResult<Self> {
        let router = RouterConfig::parse_str(toml)?;
        Ok(Self {
            server: router.server.clone(),
            storage: router.storage.clone(),
            limits: router.limits.clone(),
            strategy: router.strategy.clone(),
            cache: router.cache.clone(),
            providers: router.providers.clone(),
            combos: router.combos.clone(),
            roles: router.roles.clone(),
            router,
            source_path: None,
        })
    }

    /// Minimum viable config: one provider, one model, no auth. For the
    /// `--no-config` path and for unit tests that only exercise routing.
    pub fn minimal() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            limits: LimitsConfig::default(),
            strategy: StrategyConfig::default(),
            cache: CacheConfig::default(),
            providers: Vec::new(),
            combos: Vec::new(),
            roles: None,
            router: RouterConfig::default(),
            source_path: None,
        }
    }

    /// Bind address for the HTTP server.
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }
}
