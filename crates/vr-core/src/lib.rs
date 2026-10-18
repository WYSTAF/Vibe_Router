//! Core vocabulary for Vibe Router.
//!
//! Zero logic, zero I/O. Everything downstream is expressed in these types so
//! that neither side of the routing/provider boundary has to depend on the
//! other.
//!
//! Dependency direction is strictly downward: `vr-core` depends on nothing.

#![forbid(unsafe_code)]

pub mod candidate;
pub mod classify;
pub mod combo;
pub mod error;
pub mod format;
pub mod model;
pub mod provider;
pub mod registry;
pub mod runtime;
pub mod upstream;

pub use candidate::{Candidate, Resolution};
pub use classify::{ErrorClassifier, RetryHint};
pub use combo::{Combo, ComboStrategy};
pub use error::{Fallback, Outcome, RouterError, RouterResult};
pub use format::WireFormat;
pub use model::{Capability, Model, ModelSpec};
pub use provider::{
    Auth, HeaderScheme, KeyPool, KeySelectionStrategy, Provider, ProviderKind, SecretRef, Transport,
};
pub use registry::{ModelInfo, ModelRegistry};
pub use runtime::{
    expand_portable_path, normalize_to_portable, setup_portable_temp, HardwareProfile,
    RuntimeProfile,
};
pub use upstream::{
    StreamEvent, TransportError, TransportErrorKind, UpstreamClient, UpstreamRequest,
    UpstreamResponse, Usage,
};
