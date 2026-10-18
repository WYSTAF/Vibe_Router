//! Named fallback chains.

use crate::Candidate;

/// A user-defined ordered list of models.
///
/// Requesting the combo name walks the list: try each candidate, move to the
/// next on any fallback-eligible error. This is the whole value proposition —
/// a cost ladder such as `subscription -> cheap -> free` expressed as data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Combo {
    /// Name a client sends as the `model` field.
    pub name: String,
    /// Human-readable description.
    pub description: Option<String>,
    /// Candidates in priority order; first is preferred.
    pub models: Vec<String>,
    /// Which selection strategy this combo uses.
    pub strategy: ComboStrategy,
}

/// How a combo orders its candidates per request.
///
/// v0.1 ships [`ComboStrategy::Fallback`] only. Others are reserved extension
/// points: the enum is exhaustive on purpose so adding one is a compile-driven
/// audit of every selection site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ComboStrategy {
    /// Try in order, move on on a fallback-eligible error. The default.
    #[default]
    Fallback,
    // Reserved, deliberately unimplemented in v0.1:
    //   RoundRobin { sticky: u32 }
    //   Fusion { judge: Option<String> }
}

impl Combo {
    pub fn new(name: impl Into<String>, models: Vec<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            models,
            strategy: ComboStrategy::Fallback,
        }
    }

    /// Expand the combo into candidates, preserving order.
    pub fn candidates(&self) -> Vec<Candidate> {
        self.models
            .iter()
            .map(|m| Candidate::combo(&self.name, m))
            .collect()
    }
}

impl std::fmt::Display for Combo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> [{}]", self.name, self.models.join(", "))
    }
}
