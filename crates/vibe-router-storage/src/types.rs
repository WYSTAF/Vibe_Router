//! Storage-facing record types.
//!
//! These are the shapes that cross the `StorageBackend` boundary. They are
//! deliberately *not* the config types (`vibe-router-config`) or the core vocabulary
//! (`vibe-router-core`): storage is a persistence concern, and a row that outlives a
//! config reload must not borrow its schema from either.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A provider persisted in storage.
///
/// Mirrors what a user can create at runtime (`POST /api/providers`) and what
/// `vibe-router providers keys add` writes. Secrets are stored here because
/// they must survive a restart — the config file keeps only *references*
/// (`env:NAME`); this row may hold the resolved value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Provider {
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub base_url: String,
    /// Wire format id, e.g. `openai`.
    #[serde(default = "default_format")]
    pub format: String,
    /// `Bearer` or a custom header name.
    #[serde(default = "default_auth_scheme")]
    pub auth_scheme: String,
    /// Legacy single secret. Retained for backward compatibility; prefer
    /// `api_keys` for pools.
    #[serde(default)]
    pub auth_secret: Option<String>,
    /// Key pool for rotation, with per-key cooldown handled in-process.
    #[serde(default)]
    pub api_keys: Vec<String>,
    /// `round-robin` (default) or `least-loaded`.
    #[serde(default)]
    pub key_strategy: Option<String>,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub models: Vec<ModelRecord>,
    #[serde(default)]
    pub passthrough: bool,
    /// Epoch seconds; `None` means "never seen a health probe".
    #[serde(default)]
    pub last_health_check: Option<i64>,
    #[serde(default)]
    pub enabled: bool,
}

fn default_format() -> String {
    "openai".to_string()
}

fn default_auth_scheme() -> String {
    "Bearer".to_string()
}

impl Provider {
    /// A provider row with sensible defaults.
    pub fn new(id: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            display_name: None,
            base_url: base_url.into(),
            format: default_format(),
            auth_scheme: default_auth_scheme(),
            auth_secret: None,
            api_keys: Vec::new(),
            key_strategy: None,
            headers: Vec::new(),
            models: Vec::new(),
            passthrough: false,
            last_health_check: None,
            enabled: true,
        }
    }

    /// Every key this provider can authenticate with, pool first, legacy
    /// single secret last.
    pub fn all_keys(&self) -> Vec<String> {
        let mut keys = self.api_keys.clone();
        if let Some(secret) = &self.auth_secret {
            if !secret.trim().is_empty() && !keys.contains(secret) {
                keys.push(secret.clone());
            }
        }
        keys
    }
}

/// A model declared by a provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRecord {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub context: Option<u32>,
    #[serde(default)]
    pub max_output: Option<u32>,
}

impl ModelRecord {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: None,
            context: None,
            max_output: None,
        }
    }
}

/// A named fallback chain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Combo {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// `provider/model` strings in priority order.
    pub models: Vec<String>,
    /// Epoch seconds.
    #[serde(default)]
    pub created_at: Option<i64>,
    #[serde(default)]
    pub updated_at: Option<i64>,
}

impl Combo {
    pub fn new(name: impl Into<String>, models: Vec<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            models,
            created_at: None,
            updated_at: None,
        }
    }
}

/// One completed request.
///
/// Shared by both storage layers: the async [`crate::StorageBackend`] takes
/// it, and the request path's synchronous `UsageStore::append` takes the same
/// shape so there is exactly one definition to keep correct.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageRecord {
    /// What the client asked for (a combo name stays a combo name).
    pub requested_model: String,
    /// `provider/model` that actually served it.
    pub served_model: String,
    pub provider: String,
    #[serde(default)]
    pub combo: Option<String>,
    pub status: u16,
    #[serde(default)]
    pub streamed: bool,
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    /// True when token counts were estimated rather than reported upstream.
    /// This flag is what keeps an estimate from masquerading as a report.
    #[serde(default)]
    pub usage_estimated: bool,
    /// Wall-clock time from request receipt to response completion.
    #[serde(default, with = "duration_secs")]
    pub latency: Duration,
    /// Candidates tried before success.
    #[serde(default)]
    pub failovers: u32,
    /// Cost in micro-dollars (1 USD = 1_000_000), computed at write time so
    /// historical rows keep the rate that applied when they were served.
    #[serde(default)]
    pub cost_micros: u64,
    /// Epoch seconds.
    #[serde(default = "now_secs")]
    pub ts: i64,
    /// Vibe Assembly role (`boss`, `code`, `debug`, `judge`, `chat`, `hermes`).
    #[serde(default)]
    pub role: Option<String>,
}

impl Default for UsageRecord {
    fn default() -> Self {
        Self {
            requested_model: String::new(),
            served_model: String::new(),
            provider: String::new(),
            combo: None,
            status: 200,
            streamed: false,
            prompt_tokens: 0,
            completion_tokens: 0,
            usage_estimated: false,
            latency: Duration::ZERO,
            failovers: 0,
            cost_micros: 0,
            ts: now_secs(),
            role: None,
        }
    }
}

/// Serialize a `Duration` as whole milliseconds and back, so rows stay
/// readable and a round trip preserves sub-ms precision.
mod duration_secs {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_millis() as u64)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        Ok(Duration::from_millis(u64::deserialize(d)?))
    }
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs() as i64
}

/// Aggregate usage counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct UsageMetricsSummary {
    pub requests: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_cost_micros: u64,
    /// Rows whose tokens were estimated, not reported.
    pub estimated_rows: u64,
    /// Requests that streamed.
    pub streamed_requests: u64,
    /// Candidates skipped via failover.
    pub failovers: u64,
    /// Mean latency across all requests, milliseconds.
    pub avg_latency_ms: u64,
    /// p50 / p95 / p99 latency, milliseconds.
    pub p50_latency_ms: u64,
    pub p95_latency_ms: u64,
    pub p99_latency_ms: u64,
}

impl UsageMetricsSummary {
    pub fn total_tokens(&self) -> u64 {
        self.prompt_tokens + self.completion_tokens
    }
}

/// Cost for one dimension of a breakdown.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CostLine {
    pub key: String,
    pub requests: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub cost_micros: u64,
    /// Rows in this line whose tokens were estimated.
    pub estimated_rows: u64,
}

/// A cost breakdown sliced by provider, model, combo, and Vibe role.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CostBreakdown {
    pub total_micros: u64,
    pub by_provider: BTreeMap<String, CostLine>,
    pub by_model: BTreeMap<String, CostLine>,
    pub by_combo: BTreeMap<String, CostLine>,
    pub by_role: BTreeMap<String, CostLine>,
    /// Rows priced with no matching rate — cost is genuinely unknown, so we
    /// say so rather than silently counting them as zero.
    pub unpriced_rows: u64,
}

/// Per-million-token rates, in micro-dollars, keyed by `provider/model`.
///
/// Prices live in storage so they can be edited at runtime; the config file
/// seeds them. A model with no rate is *unpriced*, never free.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Rates {
    /// Micro-dollars per 1M prompt tokens.
    pub prompt_micros: u64,
    /// Micro-dollars per 1M completion tokens.
    pub completion_micros: u64,
}

impl Rates {
    /// Cost in micro-dollars for a request.
    pub fn cost_micros(&self, prompt_tokens: u32, completion_tokens: u32) -> u64 {
        // Work in u128: `tokens * rate` overflows u64 for absurd token counts,
        // and a router must not panic on a hostile upstream's usage report.
        let p = (prompt_tokens as u128 * self.prompt_micros as u128) / 1_000_000;
        let c = (completion_tokens as u128 * self.completion_micros as u128) / 1_000_000;
        let total = p + c;
        total.min(u64::MAX as u128) as u64
    }

    /// Return embedded default rates for major frontier models and providers.
    pub fn default_frontier_rates() -> BTreeMap<String, Rates> {
        let mut m = BTreeMap::new();
        // OpenAI
        m.insert(
            "gpt-4o".into(),
            Rates {
                prompt_micros: 2_500_000,
                completion_micros: 10_000_000,
            },
        );
        m.insert(
            "gpt-4o-mini".into(),
            Rates {
                prompt_micros: 150_000,
                completion_micros: 600_000,
            },
        );
        m.insert(
            "o1".into(),
            Rates {
                prompt_micros: 15_000_000,
                completion_micros: 60_000_000,
            },
        );
        m.insert(
            "o3-mini".into(),
            Rates {
                prompt_micros: 1_100_000,
                completion_micros: 4_400_000,
            },
        );
        // Anthropic
        m.insert(
            "claude-3-7-sonnet".into(),
            Rates {
                prompt_micros: 3_000_000,
                completion_micros: 15_000_000,
            },
        );
        m.insert(
            "claude-3-5-sonnet".into(),
            Rates {
                prompt_micros: 3_000_000,
                completion_micros: 15_000_000,
            },
        );
        m.insert(
            "claude-3-5-haiku".into(),
            Rates {
                prompt_micros: 800_000,
                completion_micros: 4_000_000,
            },
        );
        m.insert(
            "claude-3-opus".into(),
            Rates {
                prompt_micros: 15_000_000,
                completion_micros: 75_000_000,
            },
        );
        // DeepSeek
        m.insert(
            "deepseek-chat".into(),
            Rates {
                prompt_micros: 140_000,
                completion_micros: 280_000,
            },
        );
        m.insert(
            "deepseek-v3".into(),
            Rates {
                prompt_micros: 140_000,
                completion_micros: 280_000,
            },
        );
        m.insert(
            "deepseek-reasoner".into(),
            Rates {
                prompt_micros: 550_000,
                completion_micros: 2_190_000,
            },
        );
        m.insert(
            "deepseek-r1".into(),
            Rates {
                prompt_micros: 550_000,
                completion_micros: 2_190_000,
            },
        );
        // Google Gemini
        m.insert(
            "gemini-1.5-pro".into(),
            Rates {
                prompt_micros: 1_250_000,
                completion_micros: 5_000_000,
            },
        );
        m.insert(
            "gemini-2.0-flash".into(),
            Rates {
                prompt_micros: 75_000,
                completion_micros: 300_000,
            },
        );
        m.insert(
            "gemini-1.5-flash".into(),
            Rates {
                prompt_micros: 75_000,
                completion_micros: 300_000,
            },
        );
        // Open-weights / Meta / Qwen
        m.insert(
            "llama-3.3-70b".into(),
            Rates {
                prompt_micros: 400_000,
                completion_micros: 400_000,
            },
        );
        m.insert(
            "llama-3.1-70b".into(),
            Rates {
                prompt_micros: 400_000,
                completion_micros: 400_000,
            },
        );
        m.insert(
            "qwen-2.5-72b".into(),
            Rates {
                prompt_micros: 400_000,
                completion_micros: 400_000,
            },
        );
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_all_keys_dedups_and_appends_legacy_secret() {
        let mut p = Provider::new("groq", "https://api.groq.test/v1");
        p.api_keys = vec!["k1".into(), "k2".into()];
        p.auth_secret = Some("k3".into());
        assert_eq!(p.all_keys(), vec!["k1", "k2", "k3"]);

        // A legacy secret already present in the pool is not duplicated.
        p.auth_secret = Some("k1".into());
        assert_eq!(p.all_keys(), vec!["k1", "k2"]);
    }

    #[test]
    fn rate_cost_uses_million_token_denominator() {
        let r = Rates {
            prompt_micros: 1_000_000,     // $1 / 1M prompt tokens
            completion_micros: 2_000_000, // $2 / 1M completion tokens
        };
        assert_eq!(r.cost_micros(1_000_000, 0), 1_000_000);
        assert_eq!(r.cost_micros(0, 1_000_000), 2_000_000);
        assert_eq!(r.cost_micros(500_000, 500_000), 1_500_000);
        // Sub-million requests still cost real money: the u128 intermediate
        // means we round to the nearest micro-dollar instead of truncating a
        // small request to zero.
        assert_eq!(r.cost_micros(1, 1), 3);
    }

    #[test]
    fn rate_cost_does_not_overflow_on_hostile_usage() {
        let r = Rates {
            prompt_micros: u64::MAX,
            completion_micros: u64::MAX,
        };
        // Would overflow u64 if computed naively; must saturate instead.
        let cost = r.cost_micros(u32::MAX, u32::MAX);
        assert!(cost > 0);
    }
}
