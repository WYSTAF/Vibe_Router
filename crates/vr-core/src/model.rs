//! Model identity and static metadata.

/// A model as declared by a provider in configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelSpec {
    /// Model id as sent to the upstream (`anthropic/claude-sonnet-4`).
    pub id: String,
    /// Human-readable name for the UI.
    pub name: Option<String>,
    /// Maximum context window in tokens, if known.
    pub context: Option<u32>,
    /// Maximum output tokens, if known.
    pub max_output: Option<u32>,
    /// Input modalities the model accepts. Absent means "unknown" — the
    /// request is allowed through and the upstream decides.
    pub capabilities: Vec<Capability>,
}

/// A capability a model may advertise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {
    Vision,
    AudioInput,
    VideoInput,
    Pdf,
}

/// A resolved model: a [`ModelSpec`] bound to the provider that serves it.
///
/// This is the "fully-qualified name" of something a request can target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Model {
    pub provider: String,
    pub id: String,
    pub spec: ModelSpec,
}

impl Model {
    pub fn new(provider: &str, spec: &ModelSpec) -> Self {
        Self {
            provider: provider.to_string(),
            id: spec.id.clone(),
            spec: spec.clone(),
        }
    }

    /// The `provider/model` string a client uses to address this model.
    pub fn qualified(&self) -> String {
        format!("{}/{}", self.provider, self.id)
    }
}

impl std::fmt::Display for Model {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.qualified())
    }
}
