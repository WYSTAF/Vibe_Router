//! Availability state: cooldowns, rate-limit windows, backoff levels, circuit breakers.
//!
//! Answers two questions on the request path — "is this candidate usable
//! right now?" and, if so, "book the slot" — and records outcomes afterwards.
//! Owns no execution and no policy: the *duration* of a cooldown is decided
//! by the classifier (config) and merely stored here.
//!
//! Includes a per-provider Circuit Breaker:
//!   * Tripped to OPEN after N consecutive 5xx errors or timeouts.
//!   * Automatically transitions to HALF_OPEN after recovery cooldown (e.g. 30s).
//!   * Recovers to CLOSED on success.
//!   * Reroutes traffic to healthy fallbacks when OPEN.
//!
//! Depends on: vr-core.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use vr_core::{Candidate, Outcome};

mod ratelimit;

pub use ratelimit::{FixedWindowRpm, ObservedLimit, RateLimiter, Usability};

/// State of a provider's circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "closed"),
            CircuitState::Open => write!(f, "open"),
            CircuitState::HalfOpen => write!(f, "half-open"),
        }
    }
}

/// Circuit Breaker tracking consecutive upstream 5xx/timeout failures.
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    pub consecutive_failures: u32,
    pub failure_threshold: u32,
    pub state: CircuitState,
    pub tripped_until: Option<Instant>,
    pub recovery_cooldown: Duration,
    pub last_latency_ms: Option<u64>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, recovery_cooldown: Duration) -> Self {
        Self {
            consecutive_failures: 0,
            failure_threshold: failure_threshold.max(1),
            state: CircuitState::Closed,
            tripped_until: None,
            recovery_cooldown,
            last_latency_ms: None,
        }
    }

    pub fn current_state(&mut self) -> CircuitState {
        if self.state == CircuitState::Open {
            if let Some(until) = self.tripped_until {
                if Instant::now() >= until {
                    self.state = CircuitState::HalfOpen;
                }
            }
        }
        self.state
    }

    pub fn can_execute(&mut self) -> bool {
        match self.current_state() {
            CircuitState::Closed => true,
            CircuitState::HalfOpen => true,
            CircuitState::Open => false,
        }
    }

    pub fn record_success(&mut self, latency_ms: Option<u64>) {
        self.consecutive_failures = 0;
        self.state = CircuitState::Closed;
        self.tripped_until = None;
        if let Some(lat) = latency_ms {
            self.last_latency_ms = Some(lat);
        }
    }

    pub fn record_failure(&mut self, is_5xx_or_timeout: bool) {
        if is_5xx_or_timeout {
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            if self.consecutive_failures >= self.failure_threshold {
                self.state = CircuitState::Open;
                self.tripped_until = Some(Instant::now() + self.recovery_cooldown);
            }
        }
    }
}

/// Snapshot of circuit breaker state for monitoring and UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitSnapshot {
    pub provider: String,
    pub state: String,
    pub consecutive_failures: u32,
    pub tripped_until_ms: Option<u64>,
    pub last_latency_ms: Option<u64>,
}

/// Per-`provider/model` availability state.
#[derive(Debug, Clone, Default)]
struct Entry {
    cooldown_until: Option<Instant>,
    backoff_level: u32,
    last_error: Option<LastError>,
    rpm: FixedWindowRpm,
}

#[derive(Clone)]
pub struct HealthState {
    inner: Arc<RwLock<HashMap<String, Entry>>>,
    circuits: Arc<RwLock<HashMap<String, CircuitBreaker>>>,
    circuit_threshold: u32,
    circuit_cooldown: Duration,
    transient: Duration,
    max_cooldown: Duration,
}

impl Default for HealthState {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            circuits: Arc::new(RwLock::new(HashMap::new())),
            circuit_threshold: 3,
            circuit_cooldown: Duration::from_secs(30),
            transient: Duration::from_secs(30),
            max_cooldown: Duration::from_secs(30 * 60),
        }
    }
}

fn provider_of(model: &str) -> &str {
    model.split_once('/').map(|(p, _)| p).unwrap_or(model)
}

impl HealthState {
    /// Build with tunables (plain durations, so this crate does not depend on `vr-config`).
    pub fn new(transient: Duration, max_cooldown: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            circuits: Arc::new(RwLock::new(HashMap::new())),
            circuit_threshold: 3,
            circuit_cooldown: Duration::from_secs(30),
            transient,
            max_cooldown: max_cooldown.max(Duration::from_millis(1)),
        }
    }

    /// Build with custom circuit breaker thresholds.
    pub fn with_circuit_breaker(
        transient: Duration,
        max_cooldown: Duration,
        failure_threshold: u32,
        recovery_cooldown: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            circuits: Arc::new(RwLock::new(HashMap::new())),
            circuit_threshold: failure_threshold.max(1),
            circuit_cooldown: recovery_cooldown,
            transient,
            max_cooldown: max_cooldown.max(Duration::from_millis(1)),
        }
    }

    /// Apply a configured RPM limit to a model.
    pub fn set_rpm(&self, model: &str, limit: u32, safety_margin_pct: u32) {
        let mut g = self.inner.write();
        let e = g.entry(model.to_string()).or_default();
        e.rpm = FixedWindowRpm::with_margin(limit, safety_margin_pct);
    }

    /// Ask whether the model may serve a request now, and if so, consume one slot.
    /// Checks provider Circuit Breaker first; if OPEN, trips to Cooldown.
    pub fn reserve(&self, model: &str) -> Usability {
        let provider = provider_of(model);
        {
            let mut cg = self.circuits.write();
            let cb = cg.entry(provider.to_string()).or_insert_with(|| {
                CircuitBreaker::new(self.circuit_threshold, self.circuit_cooldown)
            });
            if !cb.can_execute() {
                if let Some(until) = cb.tripped_until {
                    return Usability::Cooldown(until);
                }
            }
        }

        let mut g = self.inner.write();
        let e = g.entry(model.to_string()).or_default();
        if let Some(until) = e.cooldown_until {
            if until > Instant::now() {
                return Usability::Cooldown(until);
            }
            e.cooldown_until = None;
        }
        e.rpm.reserve()
    }

    /// Non-consuming usability read (for `Retry-After` reporting).
    pub fn peek(&self, model: &str) -> Usability {
        let provider = provider_of(model);
        {
            let mut cg = self.circuits.write();
            if let Some(cb) = cg.get_mut(provider) {
                if !cb.can_execute() {
                    if let Some(until) = cb.tripped_until {
                        return Usability::Cooldown(until);
                    }
                }
            }
        }

        let g = self.inner.read();
        let now = Instant::now();
        let Some(e) = g.get(model) else {
            return Usability::Ready;
        };
        if let Some(until) = e.cooldown_until.filter(|t| *t > now) {
            return Usability::Cooldown(until);
        }
        e.rpm.peek()
    }

    /// Record the outcome of an attempted candidate.
    pub fn record(&self, model: &str, outcome: Outcome) {
        let provider = provider_of(model);
        {
            let mut cg = self.circuits.write();
            let cb = cg.entry(provider.to_string()).or_insert_with(|| {
                CircuitBreaker::new(self.circuit_threshold, self.circuit_cooldown)
            });
            match &outcome {
                Outcome::Success => cb.record_success(None),
                Outcome::Fallback(f) if f.status >= 500 => cb.record_failure(true),
                Outcome::TimedOut => cb.record_failure(true),
                _ => cb.record_failure(false),
            }
        }

        let mut g = self.inner.write();
        let e = g.entry(model.to_string()).or_default();
        let now = Instant::now();
        match outcome {
            Outcome::Success => {
                e.cooldown_until = None;
                e.backoff_level = 0;
                e.last_error = None;
            }
            Outcome::Fallback(f) => {
                let cooldown = f.cooldown.unwrap_or(self.transient);
                e.cooldown_until = Some(now + capped(cooldown, self.max_cooldown));
                if let Some(level) = f.backoff_level {
                    e.backoff_level = level;
                }
                e.last_error = Some(LastError {
                    status: f.status,
                    at: now,
                });
            }
            Outcome::ClientError { status } => {
                e.last_error = Some(LastError { status, at: now });
            }
            Outcome::TimedOut => {
                e.cooldown_until = Some(now + capped(self.transient, self.max_cooldown));
                e.last_error = Some(LastError {
                    status: 504,
                    at: now,
                });
            }
        }
    }

    /// Record provider latency in milliseconds.
    pub fn record_provider_latency(&self, provider: &str, latency_ms: u64) {
        let mut cg = self.circuits.write();
        let cb = cg
            .entry(provider.to_string())
            .or_insert_with(|| CircuitBreaker::new(self.circuit_threshold, self.circuit_cooldown));
        cb.last_latency_ms = Some(latency_ms);
    }

    /// Current circuit breaker state for a provider.
    pub fn circuit_state(&self, provider: &str) -> CircuitState {
        let mut cg = self.circuits.write();
        cg.entry(provider.to_string())
            .or_insert_with(|| CircuitBreaker::new(self.circuit_threshold, self.circuit_cooldown))
            .current_state()
    }

    /// Trip a provider's circuit breaker manually.
    pub fn trip_circuit(&self, provider: &str, duration: Option<Duration>) {
        let mut cg = self.circuits.write();
        let dur = duration.unwrap_or(self.circuit_cooldown);
        let cb = cg
            .entry(provider.to_string())
            .or_insert_with(|| CircuitBreaker::new(self.circuit_threshold, self.circuit_cooldown));
        cb.state = CircuitState::Open;
        cb.consecutive_failures = cb.failure_threshold;
        cb.tripped_until = Some(Instant::now() + dur);
    }

    /// Reset a provider's circuit breaker to CLOSED.
    pub fn reset_circuit(&self, provider: &str) {
        let mut cg = self.circuits.write();
        if let Some(cb) = cg.get_mut(provider) {
            cb.record_success(None);
        }
    }

    /// Collect circuit breaker snapshots across all tracked providers.
    pub fn circuit_snapshots(&self) -> Vec<CircuitSnapshot> {
        let now = Instant::now();
        let mut cg = self.circuits.write();
        cg.iter_mut()
            .map(|(p, cb)| {
                let state = cb.current_state().to_string();
                let tripped_until_ms = cb
                    .tripped_until
                    .and_then(|t| t.checked_duration_since(now).map(|d| d.as_millis() as u64));
                CircuitSnapshot {
                    provider: p.clone(),
                    state,
                    consecutive_failures: cb.consecutive_failures,
                    tripped_until_ms,
                    last_latency_ms: cb.last_latency_ms,
                }
            })
            .collect()
    }

    /// Clear a model's cooldown (used by `providers test` and the UI).
    pub fn clear(&self, model: &str) {
        let mut g = self.inner.write();
        if let Some(e) = g.get_mut(model) {
            e.cooldown_until = None;
            e.backoff_level = 0;
        }
    }

    /// Snapshot for the UI and `status`.
    pub fn snapshot(&self) -> Vec<ModelHealth> {
        let g = self.inner.read();
        let now = Instant::now();
        g.iter()
            .map(|(model, e)| ModelHealth {
                model: model.clone(),
                cooling_until: e.cooldown_until.filter(|t| *t > now),
                backoff_level: e.backoff_level,
                rpm_used: e.rpm.used(),
                rpm_limit: e.rpm.limit(),
                last_error: e.last_error.clone(),
            })
            .collect()
    }

    /// Current stored backoff level for a model.
    pub fn backoff_level(&self, model: &str) -> u32 {
        self.inner
            .read()
            .get(model)
            .map(|e| e.backoff_level)
            .unwrap_or(0)
    }
}

fn capped(d: Duration, max: Duration) -> Duration {
    if d > max {
        max
    } else {
        d
    }
}

#[derive(Debug, Clone)]
pub struct LastError {
    pub status: u16,
    pub at: Instant,
}

#[derive(Debug, Clone)]
pub struct ModelHealth {
    pub model: String,
    pub cooling_until: Option<Instant>,
    pub backoff_level: u32,
    pub rpm_used: u32,
    pub rpm_limit: u32,
    pub last_error: Option<LastError>,
}

impl ModelHealth {
    pub fn is_cooling(&self) -> bool {
        self.cooling_until.is_some()
    }
}

pub trait HealthView: Send + Sync {
    fn reserve(&self, candidate: &Candidate) -> Usability;
    fn peek(&self, candidate: &Candidate) -> Usability;
    fn record(&self, candidate: &Candidate, outcome: Outcome);
    fn backoff_level(&self, model: &str) -> u32;
}

impl HealthView for HealthState {
    fn reserve(&self, candidate: &Candidate) -> Usability {
        self.reserve(&candidate.model)
    }
    fn peek(&self, candidate: &Candidate) -> Usability {
        self.peek(&candidate.model)
    }
    fn record(&self, candidate: &Candidate, outcome: Outcome) {
        self.record(&candidate.model, outcome)
    }
    fn backoff_level(&self, model: &str) -> u32 {
        self.backoff_level(model)
    }
}

#[derive(Debug, Clone, Default)]
pub struct NoopHealth;

impl HealthView for NoopHealth {
    fn reserve(&self, _candidate: &Candidate) -> Usability {
        Usability::Ready
    }
    fn peek(&self, _candidate: &Candidate) -> Usability {
        Usability::Ready
    }
    fn record(&self, _candidate: &Candidate, _outcome: Outcome) {}
    fn backoff_level(&self, _model: &str) -> u32 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn circuit_breaker_trips_to_open_after_threshold_5xx() {
        let h = HealthState::with_circuit_breaker(
            Duration::from_secs(5),
            Duration::from_secs(60),
            3,
            Duration::from_millis(50),
        );

        assert_eq!(h.circuit_state("alpha"), CircuitState::Closed);
        assert!(h.reserve("alpha/m1").is_ready());

        // 1st 503 error
        h.record("alpha/m1", Outcome::fallback(503));
        assert_eq!(h.circuit_state("alpha"), CircuitState::Closed);

        // 2nd 503 error
        h.record("alpha/m1", Outcome::fallback(502));
        assert_eq!(h.circuit_state("alpha"), CircuitState::Closed);

        // 3rd failure: TimedOut -> trips to OPEN!
        h.record("alpha/m1", Outcome::TimedOut);
        assert_eq!(h.circuit_state("alpha"), CircuitState::Open);

        // While OPEN, reserve returns Cooldown (rerouting traffic!)
        assert!(!h.reserve("alpha/m1").is_ready());
        assert!(!h.reserve("alpha/m2").is_ready());

        // A different provider is unaffected
        assert!(h.reserve("beta/m1").is_ready());
    }

    #[test]
    fn circuit_breaker_half_open_and_recovery() {
        let h = HealthState::with_circuit_breaker(
            Duration::from_millis(20),
            Duration::from_secs(60),
            2,
            Duration::from_millis(40),
        );

        // Trip provider beta to OPEN
        h.record("beta/m", Outcome::fallback(500));
        h.record("beta/m", Outcome::fallback(500));
        assert_eq!(h.circuit_state("beta"), CircuitState::Open);

        // Wait past recovery cooldown
        std::thread::sleep(Duration::from_millis(60));

        // Transitions to HALF_OPEN and allows probe request
        assert_eq!(h.circuit_state("beta"), CircuitState::HalfOpen);
        assert!(h.reserve("beta/m").is_ready());

        // On success, transitions back to CLOSED!
        h.record("beta/m", Outcome::Success);
        assert_eq!(h.circuit_state("beta"), CircuitState::Closed);
    }

    #[test]
    fn fallback_with_cooldown_locks_model_out() {
        let h = HealthState::new(Duration::from_secs(1), Duration::from_secs(10));
        h.record(
            "p/m",
            Outcome::Fallback(vr_core::Fallback::new(429).with_cooldown(Duration::from_secs(5))),
        );
        assert!(matches!(h.reserve("p/m"), Usability::Cooldown(_)));
        assert!(matches!(h.reserve("p/other"), Usability::Ready));
    }

    #[test]
    fn cooldown_expiry_restores_usability() {
        let h = HealthState::new(Duration::from_millis(30), Duration::from_secs(1));
        h.record("p/m", Outcome::fallback(503));
        assert!(!h.reserve("p/m").is_ready());
        std::thread::sleep(Duration::from_millis(60));
        assert!(h.reserve("p/m").is_ready());
    }

    #[test]
    fn success_clears_state() {
        let h = HealthState::new(Duration::from_secs(10), Duration::from_secs(60));
        h.record("p/m", Outcome::fallback(500));
        h.record("p/m", Outcome::Success);
        assert!(h.reserve("p/m").is_ready());
        assert_eq!(h.backoff_level("p/m"), 0);
    }

    #[test]
    fn client_error_does_not_cool_down() {
        let h = HealthState::new(Duration::from_secs(10), Duration::from_secs(60));
        h.record("p/m", Outcome::ClientError { status: 400 });
        assert!(h.reserve("p/m").is_ready());
    }

    #[test]
    fn max_cooldown_caps_long_lockouts() {
        let h = HealthState::new(Duration::from_secs(1), Duration::from_millis(40));
        h.record(
            "p/m",
            Outcome::Fallback(vr_core::Fallback::new(429).with_cooldown(Duration::from_secs(3600))),
        );
        std::thread::sleep(Duration::from_millis(80));
        assert!(h.reserve("p/m").is_ready());
    }

    #[test]
    fn rpm_reserves_slots_and_blocks_at_margin() {
        let h = HealthState::default();
        h.set_rpm("p/m", 10, 100);
        for _ in 0..10 {
            assert!(h.reserve("p/m").is_ready());
        }
        assert!(matches!(h.reserve("p/m"), Usability::RateLimited(_)));
        assert!(h.reserve("p/other").is_ready());
    }

    #[test]
    fn peek_does_not_consume_slots() {
        let h = HealthState::default();
        h.set_rpm("p/m", 2, 100);
        assert!(h.peek("p/m").is_ready());
        assert!(h.reserve("p/m").is_ready());
        assert!(h.reserve("p/m").is_ready());
        assert!(!h.reserve("p/m").is_ready());
        assert!(matches!(h.peek("p/m"), Usability::RateLimited(_)));
    }
}
