//! Errors shaped for the OpenAI-compatible surface, plus the classification
//! vocabulary every failure resolves into.
//!
//! Every failure mode is classified into an [`Outcome`] with its cooldown
//! attached, so the routing loop carries exactly one value from the
//! classifier to the health record — no intermediate struct, no drift between
//! "what we decided" and "what we stored".

use std::time::Duration;

/// Every error the router can surface to a client or log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterError {
    /// 400 — the request as written cannot be served.
    BadRequest(String),
    /// 401 — missing or invalid API key.
    Unauthorized(String),
    /// 403 — valid key, not permitted.
    Forbidden(String),
    /// 404 — the model string resolved to nothing.
    ModelNotFound { requested: String, detail: String },
    /// 429 — every candidate is rate-limited or cooling down.
    RateLimited {
        requested: String,
        retry_after_secs: Option<u32>,
        detail: String,
    },
    /// 502 — an upstream returned something we could not parse.
    BadGateway { provider: String, detail: String },
    /// 503 — all candidates failed and none is a client error.
    AllUnavailable { requested: String, detail: String },
    /// 504 — every candidate exceeded its timeout budget.
    Timeout { requested: String, detail: String },
    /// 500 — an internal invariant was violated.
    Internal(String),
    /// An upstream 4xx that is the *client's* problem: passed through with
    /// the upstream's status and message, since re-writing it here would
    /// hide what the provider actually said.
    Upstream { status: u16, message: String },
}

impl RouterError {
    /// The HTTP status a handler should send.
    pub fn status(&self) -> u16 {
        match self {
            RouterError::BadRequest(_) => 400,
            RouterError::Unauthorized(_) => 401,
            RouterError::Forbidden(_) => 403,
            RouterError::ModelNotFound { .. } => 404,
            RouterError::RateLimited { .. } => 429,
            RouterError::BadGateway { .. } => 502,
            RouterError::AllUnavailable { .. } => 503,
            RouterError::Timeout { .. } => 504,
            RouterError::Internal(_) => 500,
            RouterError::Upstream { status, .. } => *status,
        }
    }

    /// The OpenAI error `type` field for this error.
    pub fn error_type(&self) -> &'static str {
        match self {
            RouterError::BadRequest(_) => "invalid_request_error",
            RouterError::Unauthorized(_) => "authentication_error",
            RouterError::Forbidden(_) => "permission_error",
            RouterError::ModelNotFound { .. } => "invalid_request_error",
            RouterError::RateLimited { .. } => "rate_limit_error",
            RouterError::BadGateway { .. } => "server_error",
            RouterError::AllUnavailable { .. } => "server_error",
            RouterError::Timeout { .. } => "server_error",
            RouterError::Internal(_) => "server_error",
            RouterError::Upstream { status, .. } => {
                if *status >= 500 {
                    "server_error"
                } else {
                    "invalid_request_error"
                }
            }
        }
    }

    /// The OpenAI error `code` field for this error.
    pub fn code(&self) -> &'static str {
        match self {
            RouterError::BadRequest(_) => "bad_request",
            RouterError::Unauthorized(_) => "invalid_api_key",
            RouterError::Forbidden(_) => "insufficient_quota",
            RouterError::ModelNotFound { .. } => "model_not_found",
            RouterError::RateLimited { .. } => "rate_limit_exceeded",
            RouterError::BadGateway { .. } => "bad_gateway",
            RouterError::AllUnavailable { .. } => "service_unavailable",
            RouterError::Timeout { .. } => "gateway_timeout",
            RouterError::Internal(_) => "internal_server_error",
            RouterError::Upstream { .. } => "upstream_error",
        }
    }

    /// Human-readable message, safe to hand back to the client.
    pub fn message(&self) -> String {
        match self {
            RouterError::BadRequest(m) => m.clone(),
            RouterError::Unauthorized(m) => m.clone(),
            RouterError::Forbidden(m) => m.clone(),
            RouterError::ModelNotFound { requested, detail } => {
                format!("Model `{requested}` not found: {detail}")
            }
            RouterError::RateLimited {
                requested, detail, ..
            } => format!("Model `{requested}` is rate-limited: {detail}"),
            RouterError::BadGateway { provider, detail } => {
                format!("Upstream provider `{provider}` returned an invalid response: {detail}")
            }
            RouterError::AllUnavailable { requested, detail } => {
                format!("No candidate for `{requested}` could serve the request: {detail}")
            }
            RouterError::Timeout { requested, detail } => {
                format!("Request for `{requested}` timed out: {detail}")
            }
            RouterError::Internal(m) => m.clone(),
            RouterError::Upstream { message, .. } => message.clone(),
        }
    }

    /// `Retry-After` seconds this error advertises, if any.
    pub fn retry_after_secs(&self) -> Option<u32> {
        match self {
            RouterError::RateLimited {
                retry_after_secs, ..
            } => *retry_after_secs,
            _ => None,
        }
    }

    /// Serialize into the OpenAI error envelope.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "error": {
                "message": self.message(),
                "type": self.error_type(),
                "code": self.code(),
                "param": null,
            }
        })
    }
}

impl std::fmt::Display for RouterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.status(), self.message())
    }
}

impl std::error::Error for RouterError {}

/// Result alias used across crates.
pub type RouterResult<T> = Result<T, RouterError>;

/// The result of attempting one candidate — the single value that flows from
/// the classifier into health accounting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// 2xx, response delivered. Resets the model's failure state.
    Success,
    /// Step aside and try the next candidate. Carries the cooldown decision
    /// so `vr-health` stores what was decided, unchanged.
    Fallback(Fallback),
    /// Request-scoped error: the provider is fine, hand this back.
    ClientError { status: u16 },
    /// No usable response within the timeout budget.
    TimedOut,
}

/// Why a candidate should step aside, and for how long.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fallback {
    pub status: u16,
    /// `None` means "no rule-specific duration; the health store applies its
    /// transient default".
    pub cooldown: Option<Duration>,
    /// `Some(n)` stores the new exponential-backoff level; `None` means "do
    /// not touch the stored level".
    pub backoff_level: Option<u32>,
}

impl Fallback {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            cooldown: None,
            backoff_level: None,
        }
    }

    pub fn with_cooldown(mut self, d: Duration) -> Self {
        self.cooldown = Some(d);
        self
    }

    pub fn with_backoff(mut self, level: u32) -> Self {
        self.backoff_level = Some(level);
        self
    }
}

impl Outcome {
    pub fn fallback(status: u16) -> Self {
        Outcome::Fallback(Fallback::new(status))
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Outcome::Success)
    }

    /// The status code to surface if nothing else works.
    pub fn status(&self) -> u16 {
        match self {
            Outcome::Success => 200,
            Outcome::Fallback(f) => f.status,
            Outcome::ClientError { status } => *status,
            Outcome::TimedOut => 504,
        }
    }
}
