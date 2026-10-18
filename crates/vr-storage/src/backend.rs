//! The storage backend trait and its error type.
//!
//! One async trait over both local SQLite and remote PostgreSQL. The
//! implementations differ in *where* rows live, never in what a row means, so
//! adding a backend is a new module rather than a change to any caller.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::types::{Combo, CostBreakdown, Provider, Rates, UsageMetricsSummary, UsageRecord};

/// Storage failures, surfaced to callers with enough context to act on.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("storage backend unavailable: {0}")]
    Unavailable(String),

    #[error("not found: {entity} `{id}`")]
    NotFound { entity: &'static str, id: String },

    #[error("invalid data: {0}")]
    Invalid(String),

    #[error("storage io error: {0}")]
    Io(String),

    #[error("query failed: {0}")]
    Query(String),
}

impl From<BackendError> for crate::StorageError {
    fn from(e: BackendError) -> Self {
        crate::StorageError::Backend(e.to_string())
    }
}

pub type BackendResult<T> = Result<T, BackendError>;

/// A persistence backend.
///
/// Every method is `async` so a remote (PostgreSQL) implementation is a
/// drop-in replacement for the local one: same calls, same semantics, no
/// caller learns which it is holding.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    // -- providers ---------------------------------------------------------
    async fn get_providers(&self) -> BackendResult<Vec<Provider>>;
    async fn save_provider(&self, provider: &Provider) -> BackendResult<()>;
    async fn delete_provider(&self, id: &str) -> BackendResult<()>;

    // -- combos ------------------------------------------------------------
    async fn get_combos(&self) -> BackendResult<Vec<Combo>>;
    async fn save_combo(&self, combo: &Combo) -> BackendResult<()>;
    async fn delete_combo(&self, id: &str) -> BackendResult<()>;

    // -- aliases -----------------------------------------------------------
    /// `alias -> target model id`, for `/v1/models`-level short names.
    async fn get_aliases(&self) -> BackendResult<HashMap<String, String>>;
    async fn save_alias(&self, alias: &str, target: &str) -> BackendResult<()>;
    async fn delete_alias(&self, alias: &str) -> BackendResult<bool>;

    // -- usage -------------------------------------------------------------
    async fn record_usage(&self, stat: &UsageRecord) -> BackendResult<()>;
    async fn get_usage_metrics(&self) -> BackendResult<UsageMetricsSummary>;

    // -- cost --------------------------------------------------------------
    async fn get_cost_breakdown(&self) -> BackendResult<CostBreakdown>;

    /// Per-million-token rates, keyed by `provider/model` or a bare model id.
    ///
    /// Pricing is part of the backend, not of the router: a cost breakdown is
    /// only meaningful if the rate that produced it is persisted next to the
    /// rows, so historical spend stays reproducible after a price edit.
    async fn set_rate(&self, model: &str, rates: Rates) -> BackendResult<()>;

    /// Sync in-memory state to disk (for RAM-first zero-wear portable mode).
    async fn sync_to_disk(&self, _path: &str) -> BackendResult<()> {
        Ok(())
    }
}
