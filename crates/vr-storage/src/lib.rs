//! Storage for Vibe Router: one async trait, two backends.
//!
//! # Two layers, deliberately
//!
//! * [`StorageBackend`] — the async, remote-capable persistence API
//!   (providers, combos, aliases, usage, cost). SQLite implements it today;
//!   PostgreSQL will be a new module, not a change to any caller.
//! * [`UsageStore`] / [`ComboStore`] — the *synchronous* traits the request
//!   path already uses. `append` must never await: a slow disk must not add
//!   latency to a response. The SQLite backend serves these from the same
//!   connection it writes through.
//!
//! # What is persisted
//!
//! Static configuration stays in TOML. Storage holds what must outlive a
//! restart and must be editable at runtime: runtime-authored providers and
//! combos, key pools, model aliases, pricing rates, and the usage log.
//!
//! Depends on: vr-core.

#![forbid(unsafe_code)]

mod backend;
mod memory;
mod sqlite;
mod sqlite_backend;
mod types;

pub use backend::{BackendError, BackendResult, StorageBackend};
pub use memory::MemoryStore;
pub use sqlite::SqliteStore;
pub use sqlite_backend::SqliteBackend as SqliteAsyncBackend;
pub use types::{
    Combo, CostBreakdown, CostLine, ModelRecord, Provider, Rates, UsageMetricsSummary, UsageRecord,
};

use std::path::Path;
use std::sync::Arc;

use thiserror::Error;

/// A provider authored at runtime (via API, CLI, or dashboard), keyed by id.
///
/// This is the *legacy* shape kept for the synchronous request-path traits.
/// It is intentionally a near-subset of [`Provider`]; new code should use the
/// async [`StorageBackend`] and [`Provider`] instead.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeProvider {
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub base_url: String,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub auth_scheme: Option<String>,
    #[serde(default)]
    pub auth_secret: Option<String>,
    /// Multiple API keys for pool rotation.
    #[serde(default)]
    pub api_keys: Vec<String>,
    /// "round-robin" (default) or "least-loaded".
    #[serde(default)]
    pub key_strategy: Option<String>,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub models: Vec<RuntimeModel>,
    #[serde(default)]
    pub passthrough: bool,
}

/// A model declared inside a runtime provider.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeModel {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub context: Option<u32>,
    #[serde(default)]
    pub max_output: Option<u32>,
}

/// A combo authored at runtime, keyed by name.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RuntimeCombo {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// `provider/model` strings in priority order.
    pub models: Vec<String>,
}

/// Rows as read back out of the store (record + identity + timestamp).
#[derive(Debug, Clone)]
pub struct StoredUsage {
    pub id: i64,
    /// Unix milliseconds when the row was written.
    pub ts_ms: i64,
    pub record: UsageRecord,
}

/// Aggregate counters for the status endpoint.
#[derive(Debug, Clone, Copy, Default)]
pub struct Totals {
    pub requests: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub estimated_usage_rows: u64,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("sqlite: {0}")]
    Sqlite(String),
    #[error("io: {0}")]
    Io(String),
    #[error("usage worker stopped")]
    Closed,
    #[error("backend: {0}")]
    Backend(String),
}

pub type StorageResult<T> = Result<T, StorageError>;

/// Usage logging for the request path.
///
/// Synchronous on purpose: `append` must never block a response on a disk
/// write. The backend buffers and commits in the background.
pub trait UsageStore: Send + Sync {
    fn append(&self, record: UsageRecord);
    fn recent(&self, limit: u32) -> StorageResult<Vec<StoredUsage>>;
    fn totals(&self) -> StorageResult<Totals>;
}

/// Runtime-authored providers, keyed by id.
pub trait ProviderStore: Send + Sync {
    fn list_providers(&self) -> StorageResult<Vec<RuntimeProvider>>;
    fn upsert_provider(&self, provider: &RuntimeProvider) -> StorageResult<()>;
    fn delete_provider(&self, id: &str) -> StorageResult<bool>;
}

/// Runtime-authored combos.
pub trait ComboStore: Send + Sync {
    fn list_combos(&self) -> StorageResult<Vec<RuntimeCombo>>;
    fn upsert_combo(&self, combo: &RuntimeCombo) -> StorageResult<()>;
    fn delete_combo(&self, name: &str) -> StorageResult<bool>;
}

/// One backend serving all synchronous facets.
pub trait Store: UsageStore + ComboStore + ProviderStore {}
impl<T: UsageStore + ComboStore + ProviderStore> Store for T {}

/// Open the configured synchronous backend: SQLite at `path`, or in-memory
/// when empty.
pub fn open(path: &str) -> StorageResult<Arc<dyn Store>> {
    if path.trim().is_empty() {
        return Ok(Arc::new(MemoryStore::default()));
    }
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| StorageError::Io(e.to_string()))?;
        }
    }
    Ok(Arc::new(SqliteStore::open(path)?))
}

/// Open the async backend at `path` (`:memory:` for a throwaway store).
pub async fn open_backend(path: impl AsRef<Path>) -> BackendResult<Arc<dyn StorageBackend>> {
    Ok(Arc::new(SqliteAsyncBackend::open(path).await?))
}
