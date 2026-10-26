//! The unit the routing core operates on.

use crate::ComboStrategy;

/// One place a request could be sent, with enough context to fail over.
///
/// The candidate list is materialised *before* execution: failover is then a
/// plain loop over an already-decided list, so the hot path carries no policy
/// lookups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// `provider/model` — fully qualified, after alias resolution.
    pub model: String,
    /// The combo this candidate came from, if any. Used for round-robin state
    /// and for logging which chain produced a response.
    pub combo: Option<String>,
    /// Position in the originating list (0-based). Lets a response say
    /// "served on attempt 3 of 5" without re-deriving the chain.
    pub index: usize,
    pub strategy: ComboStrategy,
}

impl Candidate {
    pub fn direct(model: &str) -> Self {
        Self {
            model: model.to_string(),
            combo: None,
            index: 0,
            strategy: ComboStrategy::Fallback,
        }
    }

    pub fn combo(name: &str, model: &str) -> Self {
        Self {
            model: model.to_string(),
            combo: Some(name.to_string()),
            index: 0,
            strategy: ComboStrategy::Fallback,
        }
    }

    pub fn with_index(mut self, index: usize) -> Self {
        self.index = index;
        self
    }

    /// True when this candidate is not the first in its chain.
    pub fn is_fallback(&self) -> bool {
        self.index > 0
    }
}

impl std::fmt::Display for Candidate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.combo {
            Some(name) => write!(f, "{} ({}#{})", self.model, name, self.index),
            None => write!(f, "{}", self.model),
        }
    }
}

/// The outcome of resolving a model string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// A single model, addressed directly.
    Single(Candidate),
    /// A combo expanded into an ordered candidate list.
    Chain(Vec<Candidate>),
}

impl Resolution {
    pub fn into_candidates(self) -> Vec<Candidate> {
        match self {
            Resolution::Single(c) => vec![c],
            Resolution::Chain(mut cs) => {
                for (i, c) in cs.iter_mut().enumerate() {
                    c.index = i;
                }
                cs
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Resolution::Single(_) => 1,
            Resolution::Chain(c) => c.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Resolution::Chain(c) if c.is_empty())
    }
}
