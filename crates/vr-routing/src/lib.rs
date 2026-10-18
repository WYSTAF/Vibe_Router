//! The decision path: resolve → select → attempt → react.
//!
//! This crate knows *how* to route and nothing about *who* it routes to. It
//! consumes the core traits only: [`ModelRegistry`] to expand a model string,
//! [`HealthView`] to ask what is usable (and book the slot),
//! [`ErrorClassifier`] to interpret a failed attempt, and [`UpstreamClient`]
//! to perform it. The server wires concrete implementations in at startup —
//! which is what keeps "add a provider without touching the core" structural:
//! nothing here can name a provider.
//!
//! Three layers, deliberately separate (conflating them is how cooldown
//! behaviour becomes unreasonable):
//!   * **Retry** — "try this candidate again" (per-status attempt table).
//!   * **Failover** — "try the next candidate" (fallback-eligible outcomes).
//!   * **Health** — "is this candidate even eligible" (cooldowns, RPM window).
//!
//! Depends on: vr-core, vr-health.

#![forbid(unsafe_code)]

mod driver;
mod policy;

pub use driver::{Routed, Router};
pub use policy::Policy;
