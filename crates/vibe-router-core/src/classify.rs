//! Classification vocabulary: how an upstream failure is interpreted.
//!
//! The *rules* are config (`vibe-router-config` builds a [`ErrorClassifier`] from the
//! TOML table); the *decision shape* is core vocabulary so `vibe-router-routing` can
//! consume a classifier without depending on the config crate.
//!
//! A classifier returns an [`Outcome`] directly: for a fallback that is an
//! [`Outcome::Fallback`] carrying the cooldown and backoff level the rules
//! decided, so the routing loop hands one value to health accounting with no
//! intermediate representation to keep in sync.

use std::time::Duration;

use crate::Outcome;

/// Attempts and delay for retrying the *same* candidate before giving up on
/// it and falling over. Kept separate from [`Outcome`] because retry is a
/// per-candidate concern decided by a different table than failover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryHint {
    pub attempts: u32,
    pub delay: Duration,
}

/// Classifies upstream failures and knows the same-candidate retry policy.
///
/// Implementations must be pure (no I/O, no state) so the routing loop stays
/// deterministic and the classifier is trivially testable.
pub trait ErrorClassifier: Send + Sync {
    /// Decide from HTTP status + error text, given the candidate's current
    /// exponential-backoff level.
    fn classify(&self, status: u16, body_text: &str, prev_level: u32) -> Outcome;

    /// Same-candidate retry policy for a status, if any.
    fn retry_for(&self, status: u16) -> Option<RetryHint>;
}
