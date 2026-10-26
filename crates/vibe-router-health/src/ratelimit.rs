//! Rate limiting.
//!
//! v0.1 implements exactly one mechanism: fixed-window RPM with a configurable
//! safety margin. The trait is written so that RPD, TPM, TPD, concurrency,
//! provider-reported limits and per-model margins can be added later as
//! additional implementations — none of them exist yet, and the enum below is
//! deliberately exhaustive so adding a dimension is a compile-driven audit of
//! every accounting site rather than a silent default.
//!
//! Policy: when a window is full we *wait* rather than provoking a 429, if
//! waiting can succeed within the request budget. Only when the wait would
//! exceed the budget do we fall back to the next candidate.

use std::time::{Duration, Instant};

/// Whether a candidate may be used right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Usability {
    /// Good to go.
    Ready,
    /// In cooldown until this instant; try another candidate.
    Cooldown(Instant),
    /// Rate-limit window is full until this instant.
    RateLimited(Instant),
}

impl Usability {
    pub fn is_ready(&self) -> bool {
        matches!(self, Usability::Ready)
    }

    /// Time until this candidate becomes usable again, if not ready.
    pub fn wait_for(&self) -> Option<Duration> {
        match self {
            Usability::Ready => None,
            Usability::Cooldown(t) | Usability::RateLimited(t) => {
                t.checked_duration_since(Instant::now())
            }
        }
    }
}

/// A rate-limit accounting strategy for one model.
///
/// Extension point for future dimensions. v0.1 has one implementor.
pub trait RateLimiter: Send + Sync {
    /// Book one slot. Implementations must not book when capacity is full.
    fn reserve(&mut self) -> Usability;
    /// Read usability without booking (for honest `Retry-After`).
    fn peek(&self) -> Usability;
    fn used(&self) -> u32;
    fn limit(&self) -> u32;
}

/// Fixed-window requests-per-minute.
///
/// Chosen over sliding-window because it needs no retained request history —
/// one counter per model, which is the whole RAM story we care about on a VPS.
#[derive(Debug, Clone)]
pub struct FixedWindowRpm {
    /// Effective limit after the safety margin was applied.
    effective_limit: u32,
    /// Requests started in the current minute.
    count: u32,
    /// Start of the current minute.
    window_start: Instant,
}

impl FixedWindowRpm {
    /// Apply the caller's own safety margin (from config) to a declared limit.
    pub fn with_margin(declared_limit: u32, safety_margin_pct: u32) -> Self {
        let pct = safety_margin_pct.clamp(1, 100);
        let effective_limit = declared_limit.saturating_mul(pct) / 100;
        Self {
            effective_limit: effective_limit.max(1),
            count: 0,
            window_start: Instant::now(),
        }
    }

    fn rollover(&mut self, now: Instant) {
        // A minute has elapsed: start a fresh window.
        if now.duration_since(self.window_start) >= WINDOW {
            self.count = 0;
            self.window_start = now;
        }
    }

    fn reopen_at(&self) -> Instant {
        self.window_start + WINDOW
    }
}

const WINDOW: Duration = Duration::from_secs(60);

impl RateLimiter for FixedWindowRpm {
    fn reserve(&mut self) -> Usability {
        let now = Instant::now();
        self.rollover(now);
        if self.count < self.effective_limit {
            self.count += 1;
            return Usability::Ready;
        }
        // Window is full. Report when it rolls over so the caller can decide
        // whether waiting is worthwhile.
        Usability::RateLimited(self.reopen_at())
    }

    fn peek(&self) -> Usability {
        let now = Instant::now();
        if now.duration_since(self.window_start) >= WINDOW {
            return Usability::Ready; // the window would roll over on use
        }
        if self.count < self.effective_limit {
            Usability::Ready
        } else {
            Usability::RateLimited(self.reopen_at())
        }
    }

    fn used(&self) -> u32 {
        self.count
    }

    fn limit(&self) -> u32 {
        self.effective_limit
    }
}

impl Default for FixedWindowRpm {
    fn default() -> Self {
        // Unbounded by default: no configured limit means no accounting.
        Self {
            effective_limit: u32::MAX,
            count: 0,
            window_start: Instant::now(),
        }
    }
}

/// Extension point for later: provider-reported limits observed at runtime
/// (e.g. `x-ratelimit-remaining` headers).
///
/// Deliberately not implemented in v0.1. When it exists, the configured limit
/// becomes a floor and observations refine it.
pub enum ObservedLimit {
    /// Requests remaining in the current window, as reported by the provider.
    Remaining(u32),
    /// When the provider says the window resets.
    ResetsAt(Instant),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admits_until_effective_limit_then_blocks() {
        let mut r = FixedWindowRpm::with_margin(100, 90);
        assert_eq!(r.limit(), 90);
        for _ in 0..90 {
            assert!(matches!(r.reserve(), Usability::Ready));
        }
        // 91st within the same minute is refused.
        assert!(matches!(r.reserve(), Usability::RateLimited(_)));
    }

    #[test]
    fn margin_never_yields_zero_capacity() {
        // A limit of 1 at 1% margin would be 0.01 -> clamp to at least 1.
        let r = FixedWindowRpm::with_margin(1, 1);
        assert_eq!(r.limit(), 1);
    }

    #[test]
    fn peek_does_not_consume() {
        let mut r = FixedWindowRpm::with_margin(2, 100);
        assert!(r.peek().is_ready());
        assert!(r.reserve().is_ready());
        assert!(r.reserve().is_ready());
        assert!(!r.reserve().is_ready());
        assert!(!r.peek().is_ready());
    }

    #[test]
    fn unbounded_by_default() {
        let mut r = FixedWindowRpm::default();
        for _ in 0..1000 {
            assert!(matches!(r.reserve(), Usability::Ready));
        }
    }
}
