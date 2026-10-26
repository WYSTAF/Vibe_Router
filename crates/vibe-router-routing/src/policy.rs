//! Routing tunables, sourced from config.

use std::time::Duration;

/// Per-request and per-attempt budgets. Plain durations so `vibe-router-routing` does
/// not depend on `vibe-router-config`.
#[derive(Debug, Clone)]
pub struct Policy {
    /// Total wall-clock a single client request may spend across all
    /// candidates, including any wait for a rate-limit window.
    pub request_budget: Duration,
    /// Abort an attempt if upstream response headers do not arrive in time.
    pub connect_timeout: Duration,
    /// Abort a stream if the first chunk does not arrive in time.
    pub first_chunk_timeout: Duration,
    /// Abort a stream if chunks stall mid-flight for longer than this.
    pub chunk_stall_timeout: Duration,
    /// How long to sleep between same-candidate retries when the classifier
    /// gives no delay. Not used in v0.1's default table (retry delays come
    /// from config), kept as the floor for a zero-config retry.
    pub default_retry_delay: Duration,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            request_budget: Duration::from_secs(180),
            connect_timeout: Duration::from_secs(60),
            first_chunk_timeout: Duration::from_secs(200),
            chunk_stall_timeout: Duration::from_secs(360),
            default_retry_delay: Duration::from_millis(500),
        }
    }
}
