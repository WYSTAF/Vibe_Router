//! The configuration schema.
//!
//! Everything behaviour-shaped is data here, not code: error classification,
//! retry policy, cooldown parameters and rate limits are all editable without
//! touching routing logic.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use vibe_router_core::WireFormat;

/// Root of the TOML file.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RouterConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub strategy: StrategyConfig,
    #[serde(default)]
    pub providers: Vec<ProviderConfig>,
    #[serde(default)]
    pub combos: Vec<ComboConfig>,
    /// Ordered error-classification rules. First match wins, text before status.
    #[serde(default)]
    pub error_rules: Vec<ErrorRule>,
    /// Optional role assignments for orchestration tools (e.g. Vibe Assembly).
    #[serde(default)]
    pub roles: Option<RolesConfig>,
    /// In-memory prompt & response LRU cache.
    #[serde(default)]
    pub cache: CacheConfig,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            limits: LimitsConfig::default(),
            strategy: StrategyConfig::default(),
            providers: Vec::new(),
            combos: Vec::new(),
            // A sensible baseline: rate limits and overload fall back; request
            // errors do not. See ARCHITECTURE.md.
            error_rules: default_error_rules(),
            roles: None,
            cache: CacheConfig::default(),
        }
    }
}

impl RouterConfig {
    pub fn load(path: &str) -> ConfigResult<Self> {
        let raw = std::fs::read_to_string(path).map_err(|e| ConfigError::read(path, e))?;
        let mut cfg: Self = toml::from_str(&raw).map_err(|e| ConfigError::parse(path, e))?;
        if cfg.error_rules.is_empty() {
            cfg.error_rules = default_error_rules();
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// Parse from TOML text (used by tests, `config validate`, and the
    /// watcher). Named `parse_str`, not `from_str`, deliberately: it takes
    /// a whole document, not a token — `std::str::FromStr` would be the
    /// wrong trait for it.
    pub fn parse_str(raw: &str) -> ConfigResult<Self> {
        let mut cfg: Self = toml::from_str(raw).map_err(|e| ConfigError::parse("<inline>", e))?;
        if cfg.error_rules.is_empty() {
            cfg.error_rules = default_error_rules();
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// Reject structurally broken config before the server starts.
    fn validate(&self) -> ConfigResult<()> {
        if self.server.port == 0 {
            return Err(ConfigError::invalid("server.port must be > 0"));
        }
        // Provider ids are the namespace everything else keys off.
        let mut seen = std::collections::HashSet::new();
        for p in &self.providers {
            if p.id.trim().is_empty() {
                return Err(ConfigError::invalid("a provider has an empty id"));
            }
            if !seen.insert(p.id.clone()) {
                return Err(ConfigError::invalid(format!(
                    "duplicate provider id: `{}`",
                    p.id
                )));
            }
        }
        // Combo model strings must resolve to a known provider/model,
        // otherwise a typo only surfaces at request time. Providers that
        // declare no model list are gateways: any model passes validation.
        let known: std::collections::HashSet<String> = self
            .providers
            .iter()
            .flat_map(|p| p.models.iter().map(move |m| format!("{}/{}", p.id, m.id)))
            .collect();
        for c in &self.combos {
            if c.name.trim().is_empty() {
                return Err(ConfigError::invalid("a combo has an empty name"));
            }
            if c.models.is_empty() {
                return Err(ConfigError::invalid(format!(
                    "combo `{}` has no models",
                    c.name
                )));
            }
            for m in &c.models {
                if self.model_known(m, &known).is_none() {
                    return Err(ConfigError::unknown_model(&c.name, m));
                }
            }
        }
        if let Some(roles) = &self.roles {
            let check_role = |r: &Option<RoleConfig>, name: &str| -> ConfigResult<()> {
                if let Some(rc) = r {
                    if rc.model.trim().is_empty() {
                        return Err(ConfigError::invalid(format!(
                            "role `{name}` has an empty model"
                        )));
                    }
                    let is_combo = self.combos.iter().any(|c| c.name == rc.model);
                    if !is_combo && self.model_known(&rc.model, &known).is_none() {
                        return Err(ConfigError::invalid(format!(
                            "role `{name}` refers to unknown model or combo `{}`",
                            rc.model
                        )));
                    }
                }
                Ok(())
            };
            check_role(&roles.boss, "boss")?;
            check_role(&roles.code, "code")?;
            check_role(&roles.debug, "debug")?;
            check_role(&roles.chat, "chat")?;
        }
        Ok(())
    }

    /// Does `provider/model` resolve? Returns the provider id when yes.
    /// A provider with no declared models (or `passthrough = true`) accepts
    /// any model name; a provider with a list accepts only its members.
    pub fn model_known(
        &self,
        qualified: &str,
        known: &std::collections::HashSet<String>,
    ) -> Option<String> {
        let (provider, rest) = qualified.split_once('/')?;
        if rest.is_empty() {
            return None;
        }
        let p = self.providers.iter().find(|p| p.id == provider)?;
        if p.models.is_empty() || p.passthrough || known.contains(qualified) {
            Some(provider.to_string())
        } else {
            None
        }
    }

    /// The full known-model set, for building registries.
    pub fn known_models(&self) -> std::collections::HashSet<String> {
        self.providers
            .iter()
            .flat_map(|p| p.models.iter().map(move |m| format!("{}/{}", p.id, m.id)))
            .collect()
    }

    /// Does `qualified` resolve against this config? Used by combo validation
    /// and by the registry when it is built from a fresh config file.
    pub fn resolve_provider_of(&self, qualified: &str) -> Option<String> {
        let known = self.known_models();
        self.model_known(qualified, &known)
    }
}

fn default_error_rules() -> Vec<ErrorRule> {
    vec![
        // Text rules first — they catch wording a bare status would miss.
        ErrorRule {
            match_: ErrorRuleMatch::Text("rate limit".into()),
            fallback: true,
            backoff: true,
            cooldown_ms: None,
        },
        ErrorRule {
            match_: ErrorRuleMatch::Text("too many requests".into()),
            fallback: true,
            backoff: true,
            cooldown_ms: None,
        },
        ErrorRule {
            match_: ErrorRuleMatch::Text("quota exceeded".into()),
            fallback: true,
            backoff: true,
            cooldown_ms: None,
        },
        ErrorRule {
            match_: ErrorRuleMatch::Text("overloaded".into()),
            fallback: true,
            backoff: true,
            cooldown_ms: None,
        },
        ErrorRule {
            match_: ErrorRuleMatch::Text("capacity".into()),
            fallback: true,
            backoff: true,
            cooldown_ms: None,
        },
        // Then status rules.
        ErrorRule {
            match_: ErrorRuleMatch::Status(401),
            fallback: true,
            backoff: false,
            cooldown_ms: Some(120_000),
        },
        ErrorRule {
            match_: ErrorRuleMatch::Status(403),
            fallback: true,
            backoff: false,
            cooldown_ms: Some(120_000),
        },
        ErrorRule {
            match_: ErrorRuleMatch::Status(429),
            fallback: true,
            backoff: true,
            cooldown_ms: None,
        },
        ErrorRule {
            match_: ErrorRuleMatch::Status(503),
            fallback: true,
            backoff: false,
            cooldown_ms: Some(5_000),
        },
        ErrorRule {
            match_: ErrorRuleMatch::Status(502),
            fallback: true,
            backoff: false,
            cooldown_ms: Some(3_000),
        },
    ]
}

// ---------------------------------------------------------------------------
// Server / storage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    /// Optional API keys accepted on `Authorization: Bearer`. When empty the
    /// server is open on loopback — fine for a single-user VPS, not for
    /// exposure.
    #[serde(default)]
    pub api_keys: Vec<String>,
    /// Where to look for keys if not listed above (env var names).
    #[serde(default)]
    pub api_key_env: Vec<String>,
    #[serde(default = "default_log_dir")]
    pub log_dir: String,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}
fn default_port() -> u16 {
    20128
}
fn default_log_dir() -> String {
    "logs".to_string()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            api_keys: Vec::new(),
            api_key_env: Vec::new(),
            log_dir: default_log_dir(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    /// SQLite path for usage rows and runtime state. Empty disables persistence.
    #[serde(default = "default_db_path")]
    pub sqlite_path: String,
}

fn default_db_path() -> String {
    "vibe-router.db".to_string()
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            sqlite_path: default_db_path(),
        }
    }
}

// ---------------------------------------------------------------------------
// Cache: in-memory LRU prompt & response cache
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CacheConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_cache_memory_mb")]
    pub max_memory_mb: usize,
    #[serde(default = "default_cache_ttl_secs")]
    pub ttl_seconds: u64,
    #[serde(default)]
    pub stream_cache: bool,
}

fn default_cache_memory_mb() -> usize {
    256
}

fn default_cache_ttl_secs() -> u64 {
    1800 // 30 minutes
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_memory_mb: default_cache_memory_mb(),
            ttl_seconds: default_cache_ttl_secs(),
            stream_cache: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Limits: cooldown + backoff + rate limits
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct LimitsConfig {
    #[serde(default)]
    pub cooldown: CooldownConfig,
    #[serde(default)]
    pub backoff: BackoffConfig,
    /// Per-model rate limits, keyed by `provider/model`. The scope of each
    /// entry is chosen by its own config, so the map can hold a simple RPM
    /// limit for one model and a TPM limit for another.
    #[serde(default)]
    pub rate_limits: HashMap<String, RateLimitConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CooldownConfig {
    /// Cooldown applied to a candidate that failed without a rule-specific one.
    #[serde(default = "default_transient_ms")]
    pub transient_ms: u64,
    /// Never cool down longer than this, even if a provider asks for hours.
    #[serde(default = "default_max_ms")]
    pub max_ms: u64,
}

fn default_transient_ms() -> u64 {
    30_000
}
fn default_max_ms() -> u64 {
    30 * 60_000
}

impl Default for CooldownConfig {
    fn default() -> Self {
        Self {
            transient_ms: default_transient_ms(),
            max_ms: default_max_ms(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BackoffConfig {
    /// Base delay for the first exponential backoff step.
    #[serde(default = "default_backoff_base")]
    pub base_ms: u64,
    /// Ceiling for a single backoff delay.
    #[serde(default = "default_backoff_max")]
    pub max_ms: u64,
    /// Maximum number of successive doublings.
    #[serde(default = "default_backoff_levels")]
    pub max_levels: u32,
}

fn default_backoff_base() -> u64 {
    2_000
}
fn default_backoff_max() -> u64 {
    5 * 60_000
}
fn default_backoff_levels() -> u32 {
    15
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base_ms: default_backoff_base(),
            max_ms: default_backoff_max(),
            max_levels: default_backoff_levels(),
        }
    }
}

/// Which dimension a rate limit counts.
///
/// v0.1 implements [`RateLimitScope::RequestsPerMinute`] only. The others are
/// reserved: the enum is deliberately exhaustive so adding RPD/TPM/concurrency
/// is a compile-driven audit of every accounting site, not a silent default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum RateLimitScope {
    /// Requests per minute.
    RequestsPerMinute,
    // Reserved for later — do not implement in v0.1:
    //   RequestsPerDay, TokensPerMinute, TokensPerDay, Concurrency
}

/// A rate limit for one model.
///
/// The shape supports the future dimensions (per-minute/per-day, requests/
/// tokens, provider-reported vs configured) without any of them existing yet —
/// v0.1 fills in only `scope` + `limit` + `safety_margin`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RateLimitConfig {
    pub scope: RateLimitScope,
    /// The raw limit before the safety margin is applied.
    pub limit: u32,
    /// Fraction of the limit we refuse to exceed, in hundredths: 90 means use
    /// at most 90% of the declared limit. Waiting beats provoking a 429.
    #[serde(default = "default_safety_margin")]
    pub safety_margin_pct: u32,
}

fn default_safety_margin() -> u32 {
    90
}

impl RateLimitConfig {
    /// Effective limit after the safety margin. A limit of 100 at 90% is 90.
    pub fn effective(&self) -> u32 {
        self.limit.saturating_mul(self.safety_margin_pct.min(100)) / 100
    }
}

// ---------------------------------------------------------------------------
// Strategy: retry + failover policy
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StrategyConfig {
    /// Default retry attempts per candidate, keyed by status code.
    #[serde(default)]
    pub retry: RetryConfig,
    /// Ceiling on total wall-clock spent on one request across all candidates.
    #[serde(default = "default_request_budget_ms")]
    pub request_budget_ms: u64,
    /// Time-to-first-byte budget per candidate.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Budget for the first streamed chunk (prefill / time-to-first-token).
    #[serde(default = "default_first_chunk_timeout_ms")]
    pub first_chunk_timeout_ms: u64,
    /// Once tokens are flowing, how long a stall is tolerated.
    #[serde(default = "default_stall_timeout_ms")]
    pub stall_timeout_ms: u64,
}

fn default_request_budget_ms() -> u64 {
    180_000
}
fn default_connect_timeout_ms() -> u64 {
    60_000
}
fn default_first_chunk_timeout_ms() -> u64 {
    200_000
}
fn default_stall_timeout_ms() -> u64 {
    360_000
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            retry: RetryConfig::default(),
            request_budget_ms: default_request_budget_ms(),
            connect_timeout_ms: default_connect_timeout_ms(),
            first_chunk_timeout_ms: default_first_chunk_timeout_ms(),
            stall_timeout_ms: default_stall_timeout_ms(),
        }
    }
}

/// Per-status retry attempts and delay. Separated from failover: retry asks
/// "try this candidate again", failover asks "try the next one".
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct RetryConfig {
    /// `{ status: { attempts, delay_ms } }`. Absent status = no retry.
    #[serde(default)]
    pub by_status: HashMap<String, RetryEntry>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetryEntry {
    pub attempts: u32,
    pub delay_ms: u64,
}

// ---------------------------------------------------------------------------
// Error classification rules
// ---------------------------------------------------------------------------

/// One rule in the ordered classification table.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ErrorRule {
    #[serde(rename = "match")]
    pub match_: ErrorRuleMatch,
    /// Cool this candidate and move to the next.
    #[serde(default = "default_true")]
    pub fallback: bool,
    /// Use exponential backoff rather than a fixed cooldown.
    #[serde(default)]
    pub backoff: bool,
    /// Fixed cooldown when `backoff` is false.
    pub cooldown_ms: Option<u64>,
}

fn default_true() -> bool {
    true
}

/// What a rule matches. Text rules are checked before status rules.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum ErrorRuleMatch {
    /// Case-insensitive substring of the error body.
    Text(String),
    /// An HTTP status code.
    Status(u16),
}

// ---------------------------------------------------------------------------
// Providers and models
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProviderConfig {
    /// Unique kebab-case id used in `provider/model` strings.
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub base_url: String,
    #[serde(default)]
    pub format: WireFormatSerde,
    #[serde(default)]
    pub auth: AuthConfig,
    /// Direct API keys list on provider (alternative / addition to auth.secret / auth.api_keys)
    #[serde(default)]
    pub api_keys: Vec<String>,
    /// Key selection strategy: "round-robin" (default) or "least-loaded"
    #[serde(default)]
    pub key_strategy: Option<String>,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub models: Vec<ModelConfig>,
    /// When true, the provider forwards model ids it has not declared
    /// (a gateway pattern: the upstream decides). When false — the
    /// default — unknown models on this provider are rejected at
    /// resolution time rather than being tried and failed.
    #[serde(default)]
    pub passthrough: bool,
}

/// Serde-compatible wrapper around [`vibe_router_core::WireFormat`].
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(transparent)]
pub struct WireFormatSerde(pub String);

impl From<WireFormatSerde> for WireFormat {
    fn from(v: WireFormatSerde) -> Self {
        v.0.parse().unwrap_or(WireFormat::OpenAi)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModelConfig {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub context: Option<u32>,
    #[serde(default)]
    pub max_output: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct AuthConfig {
    /// `bearer` (default) or a custom header name.
    #[serde(default)]
    pub scheme: AuthScheme,
    /// Single secret: `env:MY_KEY`, `file:/path`, raw key, or empty for no auth.
    #[serde(default)]
    pub secret: String,
    /// Multiple secrets / keys for rotation: `["env:KEY1", "env:KEY2", "sk-live-xxx"]`.
    #[serde(default)]
    pub secrets: Vec<String>,
    /// Alternative alias for multiple keys.
    #[serde(default)]
    pub api_keys: Vec<String>,
    /// Selection strategy: "round-robin" (default) or "least-loaded".
    #[serde(default)]
    pub strategy: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub enum AuthScheme {
    /// `Authorization: Bearer <secret>`
    #[default]
    Bearer,
    /// Custom header named by `header_name`.
    Custom { header_name: String },
}

impl AuthConfig {
    /// Resolve into a vibe-router-core [`Auth`] with multi-key pool support.
    pub fn resolve_with_extras(
        &self,
        extra_keys: &[String],
        extra_strategy: Option<&str>,
    ) -> vibe_router_core::Auth {
        let mut all_raw = Vec::new();
        if !self.secret.trim().is_empty() {
            all_raw.push(self.secret.trim().to_string());
        }
        for s in &self.secrets {
            let trimmed = s.trim();
            if !trimmed.is_empty() && !all_raw.contains(&trimmed.to_string()) {
                all_raw.push(trimmed.to_string());
            }
        }
        for s in &self.api_keys {
            let trimmed = s.trim();
            if !trimmed.is_empty() && !all_raw.contains(&trimmed.to_string()) {
                all_raw.push(trimmed.to_string());
            }
        }
        for s in extra_keys {
            let trimmed = s.trim();
            if !trimmed.is_empty() && !all_raw.contains(&trimmed.to_string()) {
                all_raw.push(trimmed.to_string());
            }
        }

        if all_raw.is_empty() {
            return vibe_router_core::Auth::None;
        }

        let strategy_str = extra_strategy
            .or(self.strategy.as_deref())
            .unwrap_or("round-robin");
        let strategy = vibe_router_core::KeySelectionStrategy::parse(strategy_str);

        let secret_refs: Vec<vibe_router_core::SecretRef> = all_raw
            .into_iter()
            .map(|s| vibe_router_core::SecretRef::parse(&s))
            .filter(|r| !matches!(r, vibe_router_core::SecretRef::Empty))
            .collect();

        if secret_refs.is_empty() {
            return vibe_router_core::Auth::None;
        }

        let key_pool = vibe_router_core::KeyPool::new(secret_refs, strategy);

        match self.scheme {
            AuthScheme::Bearer => vibe_router_core::Auth::Bearer { key_pool },
            AuthScheme::Custom { ref header_name } => vibe_router_core::Auth::Header {
                name: header_name.clone(),
                scheme: vibe_router_core::HeaderScheme::Raw,
                key_pool,
            },
        }
    }

    pub fn resolve(&self) -> vibe_router_core::Auth {
        self.resolve_with_extras(&[], None)
    }
}

impl ProviderConfig {
    /// The wire-format transport record the executor builds on.
    pub fn transport(&self) -> vibe_router_core::Transport {
        vibe_router_core::Transport {
            base_url: self.base_url.clone(),
            format: self.format.clone().into(),
            headers: self.headers.clone(),
        }
    }

    pub fn auth(&self) -> vibe_router_core::Auth {
        self.auth
            .resolve_with_extras(&self.api_keys, self.key_strategy.as_deref())
    }

    /// Declared models as core specs.
    pub fn model_specs(&self) -> Vec<vibe_router_core::ModelSpec> {
        self.models
            .iter()
            .map(|m| vibe_router_core::ModelSpec {
                id: m.id.clone(),
                name: m.name.clone(),
                context: m.context,
                max_output: m.max_output,
                capabilities: Vec::new(),
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Combos
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ComboConfig {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub models: Vec<String>,
}

// ---------------------------------------------------------------------------
// Roles (Orchestration role assignments, e.g. for Vibe Assembly)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct RolesConfig {
    pub boss: Option<RoleConfig>,
    pub code: Option<RoleConfig>,
    pub debug: Option<RoleConfig>,
    pub chat: Option<RoleConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RoleConfig {
    pub model: String,
    #[serde(default)]
    pub class: Option<String>,
    #[serde(default)]
    pub context_window: Option<u32>,
    #[serde(default)]
    pub recommended_ticket_size: Option<String>,
}

use crate::error::{ConfigError, ConfigResult};
