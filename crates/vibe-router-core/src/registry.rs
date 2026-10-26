//! Resolution vocabulary: turning a client's `model` string into work.
//!
//! The registry is a trait so `vibe-router-routing` never imports `vibe-router-providers` or
//! config; the server wires concrete implementations in at startup.

use crate::{Candidate, Resolution, RouterError};

/// One model as advertised by `GET /v1/models`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelInfo {
    /// The id a client should send (`provider/model`, or a combo name).
    pub id: String,
    /// Which provider serves it (for `owned_by`).
    pub owned_by: String,
}

/// Resolves client model strings to ordered candidate chains.
pub trait ModelRegistry: Send + Sync {
    /// Resolve `model` to a chain: combo names expand to their candidates,
    /// `provider/model` strings to a single candidate. Unknown ids return
    /// [`RouterError::ModelNotFound`].
    fn resolve(&self, model: &str) -> Result<Resolution, RouterError>;

    /// All addressable models (every concrete `provider/model` plus every
    /// combo name), for `GET /v1/models`.
    fn list(&self) -> Vec<ModelInfo>;

    /// The provider id serving a candidate, parsed from `provider/model`.
    fn provider_of(&self, candidate: &Candidate) -> Option<String> {
        let (provider, _) = candidate.model.split_once('/')?;
        Some(provider.to_string())
    }
}
