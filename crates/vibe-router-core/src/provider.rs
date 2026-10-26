//! The provider abstraction.
//!
//! A provider is anything that can serve model requests. Built-in providers are
//! built from config records by `vibe-router-providers`; anything with genuinely custom
//! behaviour implements this trait in its own crate and registers itself.

use crate::{Model, ModelSpec};

/// What a provider serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProviderKind {
    /// Chat completions (text, vision, tool use).
    Chat,
    /// Text embeddings.
    Embedding,
}

/// How a request is authenticated against an upstream.
///
/// Supports multi-key pools for rotation, failover, and rate-limit cooldown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Auth {
    /// `Authorization: Bearer <key>` — the OpenAI-compatible default.
    Bearer { key_pool: KeyPool },
    /// A custom header, e.g. `x-api-key: <key>`.
    Header {
        name: String,
        scheme: HeaderScheme,
        key_pool: KeyPool,
    },
    /// No credential needed (local, free, or anonymous endpoints).
    None,
}

impl Auth {
    pub fn bearer_single(secret_ref: SecretRef) -> Self {
        Auth::Bearer {
            key_pool: KeyPool::single(secret_ref),
        }
    }

    pub fn bearer_pool(keys: Vec<SecretRef>, strategy: KeySelectionStrategy) -> Self {
        Auth::Bearer {
            key_pool: KeyPool::new(keys, strategy),
        }
    }

    pub fn header_single(
        name: impl Into<String>,
        scheme: HeaderScheme,
        secret_ref: SecretRef,
    ) -> Self {
        Auth::Header {
            name: name.into(),
            scheme,
            key_pool: KeyPool::single(secret_ref),
        }
    }

    pub fn header_pool(
        name: impl Into<String>,
        scheme: HeaderScheme,
        keys: Vec<SecretRef>,
        strategy: KeySelectionStrategy,
    ) -> Self {
        Auth::Header {
            name: name.into(),
            scheme,
            key_pool: KeyPool::new(keys, strategy),
        }
    }

    pub fn key_pool(&self) -> Option<&KeyPool> {
        match self {
            Auth::Bearer { key_pool } | Auth::Header { key_pool, .. } => Some(key_pool),
            Auth::None => None,
        }
    }

    pub fn key_count(&self) -> usize {
        self.key_pool().map_or(0, |kp| kp.len())
    }
}

/// Strategy for selecting keys from a provider's key pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeySelectionStrategy {
    #[default]
    RoundRobin,
    LeastLoaded,
    Weighted,
}

impl KeySelectionStrategy {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "least-loaded" | "least_loaded" => KeySelectionStrategy::LeastLoaded,
            "weighted" => KeySelectionStrategy::Weighted,
            _ => KeySelectionStrategy::RoundRobin,
        }
    }
}

/// A pool of secret references configured for a provider.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyPool {
    pub keys: Vec<SecretRef>,
    pub strategy: KeySelectionStrategy,
}

impl KeyPool {
    pub fn empty() -> Self {
        Self {
            keys: Vec::new(),
            strategy: KeySelectionStrategy::RoundRobin,
        }
    }

    pub fn single(secret_ref: SecretRef) -> Self {
        Self {
            keys: vec![secret_ref],
            strategy: KeySelectionStrategy::RoundRobin,
        }
    }

    pub fn new(keys: Vec<SecretRef>, strategy: KeySelectionStrategy) -> Self {
        Self { keys, strategy }
    }

    pub fn first(&self) -> Option<&SecretRef> {
        self.keys.first()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }
}

/// The credential prefix used by an [`Auth`] scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderScheme {
    /// Send the value as-is.
    Raw,
    /// Prefix the value with `Bearer `.
    Bearer,
}

/// A resolvable reference to a secret. The value is looked up at request time
/// so secrets never live in config objects or in the database.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SecretRef {
    /// Read from the process environment.
    Env(String),
    /// Read from a file on disk (mode 0600 on unix).
    File(String),
    /// Direct literal string (e.g. from runtime SQLite store or test config).
    Literal(String),
    /// No credential available.
    Empty,
}

impl SecretRef {
    pub fn parse(s: &str) -> Self {
        let s = s.trim();
        if s.is_empty() {
            SecretRef::Empty
        } else if let Some(rest) = s.strip_prefix("env:") {
            SecretRef::Env(rest.to_string())
        } else if let Some(rest) = s.strip_prefix("file:") {
            SecretRef::File(rest.to_string())
        } else {
            SecretRef::Literal(s.to_string())
        }
    }
}

/// How requests reach an upstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transport {
    /// Fully-qualified endpoint, e.g.
    /// `https://api.openrouter.ai/api/v1/chat/completions`.
    pub base_url: String,
    /// Wire format the upstream speaks natively.
    pub format: crate::WireFormat,
    /// Additional static headers (fingerprinting, beta flags, …).
    pub headers: Vec<(String, String)>,
}

/// The core abstraction. Implementations must be cheap to clone: they are held
/// behind an `Arc` and shared across the request path.
pub trait Provider: Send + Sync {
    /// Stable, unique, kebab-case identifier (`openrouter`).
    fn id(&self) -> &str;

    fn kind(&self) -> ProviderKind;

    /// Human-readable name for the UI.
    fn display_name(&self) -> &str;

    /// Where requests go.
    fn transport(&self) -> &Transport;

    /// How requests are authenticated.
    fn auth(&self) -> &Auth;

    /// Models this provider declares statically.
    fn models(&self) -> &[ModelSpec];

    /// Convenience: find a declared [`Model`] by id.
    fn find_model(&self, id: &str) -> Option<Model> {
        self.models()
            .iter()
            .find(|m| m.id == id)
            .map(|spec| Model::new(self.id(), spec))
    }
}

impl PartialEq for dyn Provider {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}
impl Eq for dyn Provider {}
