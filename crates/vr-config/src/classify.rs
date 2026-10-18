//! Error classification: the rule table as a decision function.
//!
//! This is the "behaviour is configuration" principle applied to failure:
//! whether an upstream error means *fall over* or *hand it back* is decided
//! by an ordered table defined in TOML, not by code.
//!
//! Evaluation order is the semantics:
//!   1. Text rules, in declaration order (a `429` whose body says
//!      "quota exceeded" should back off exponentially, not sleep 5s).
//!   2. Status rules, in declaration order.
//!   3. Built-in fallbacks: non-account 4xx are request-scoped (do NOT fall
//!      over, do NOT cool down — that would remove a healthy connection from
//!      rotation and hide the real cause); anything else is transient.
//!
//! The classifier returns an [`Outcome`] carrying the cooldown and backoff
//! level it decided, so the routing loop passes one value to health accounting
//! with no intermediate representation to keep in sync.

use std::time::Duration;

use vr_core::{ErrorClassifier, Fallback, Outcome, RetryHint};

use crate::schema::{ErrorRule, ErrorRuleMatch, LimitsConfig, RetryConfig, StrategyConfig};

/// Classifier backed by the config rule table.
#[derive(Debug, Clone)]
pub struct ConfigClassifier {
    rules: Vec<ErrorRule>,
    transient_ms: u64,
    max_ms: u64,
    backoff_base_ms: u64,
    backoff_max_ms: u64,
    backoff_max_levels: u32,
    retry: RetryConfig,
}

impl ConfigClassifier {
    pub fn new(limits: &LimitsConfig, strategy: &StrategyConfig, rules: Vec<ErrorRule>) -> Self {
        Self {
            rules,
            transient_ms: limits.cooldown.transient_ms,
            max_ms: limits.cooldown.max_ms,
            backoff_base_ms: limits.backoff.base_ms,
            backoff_max_ms: limits.backoff.max_ms,
            backoff_max_levels: limits.backoff.max_levels,
            retry: strategy.retry.clone(),
        }
    }

    fn verdict(&self, rule: &ErrorRule, status: u16, prev_level: u32) -> Outcome {
        if !rule.fallback {
            return Outcome::ClientError { status };
        }
        if rule.backoff {
            let level = (prev_level + 1).min(self.backoff_max_levels);
            let delay = self
                .backoff_base_ms
                .saturating_mul(1u64 << level.saturating_sub(1).min(20))
                .min(self.backoff_max_ms)
                .max(1);
            return Outcome::Fallback(
                Fallback::new(status)
                    .with_cooldown(self.capped(Duration::from_millis(delay)))
                    .with_backoff(level),
            );
        }
        let cooldown = rule
            .cooldown_ms
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(self.transient_ms));
        Outcome::Fallback(
            Fallback::new(status)
                .with_cooldown(self.capped(cooldown))
                .with_backoff(0),
        )
    }

    fn builtin(&self, status: u16) -> Outcome {
        // Account-scoped 4xx keep their configured rules (401/402/403/429);
        // a bare 4xx that matched nothing is the caller's problem.
        if (400..500).contains(&status) && !matches!(status, 401 | 402 | 403 | 429) {
            return Outcome::ClientError { status };
        }
        Outcome::Fallback(
            Fallback::new(status)
                .with_cooldown(self.capped(Duration::from_millis(self.transient_ms)))
                .with_backoff(0),
        )
    }

    fn capped(&self, d: Duration) -> Duration {
        let ms = d.as_millis() as u64;
        Duration::from_millis(ms.min(self.max_ms.max(1)))
    }
}

impl ErrorClassifier for ConfigClassifier {
    fn classify(&self, status: u16, body_text: &str, prev_level: u32) -> Outcome {
        let lower = body_text.to_ascii_lowercase();

        for rule in &self.rules {
            if let ErrorRuleMatch::Text(needle) = &rule.match_ {
                if !lower.is_empty() && lower.contains(&needle.to_ascii_lowercase()) {
                    return self.verdict(rule, status, prev_level);
                }
            }
        }
        for rule in &self.rules {
            if let ErrorRuleMatch::Status(s) = &rule.match_ {
                if *s == status {
                    return self.verdict(rule, status, prev_level);
                }
            }
        }
        self.builtin(status)
    }

    fn retry_for(&self, status: u16) -> Option<RetryHint> {
        let entry = self.retry.by_status.get(&status.to_string())?;
        if entry.attempts == 0 {
            return None;
        }
        Some(RetryHint {
            attempts: entry.attempts,
            delay: Duration::from_millis(entry.delay_ms),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{BackoffConfig, CooldownConfig};

    fn limits() -> LimitsConfig {
        LimitsConfig {
            cooldown: CooldownConfig {
                transient_ms: 30_000,
                max_ms: 30 * 60_000,
            },
            backoff: BackoffConfig {
                base_ms: 2_000,
                max_ms: 300_000,
                max_levels: 15,
            },
            rate_limits: Default::default(),
        }
    }

    fn classifier(rules: Vec<ErrorRule>) -> ConfigClassifier {
        ConfigClassifier::new(&limits(), &StrategyConfig::default(), rules)
    }

    fn rules() -> Vec<ErrorRule> {
        vec![
            ErrorRule {
                match_: ErrorRuleMatch::Text("rate limit".into()),
                fallback: true,
                backoff: true,
                cooldown_ms: None,
            },
            ErrorRule {
                match_: ErrorRuleMatch::Status(401),
                fallback: true,
                backoff: false,
                cooldown_ms: Some(120_000),
            },
            ErrorRule {
                match_: ErrorRuleMatch::Status(503),
                fallback: true,
                backoff: false,
                cooldown_ms: Some(5_000),
            },
        ]
    }

    fn fb(o: &Outcome) -> &Fallback {
        match o {
            Outcome::Fallback(f) => f,
            other => panic!("expected a Fallback outcome, got {other:?}"),
        }
    }

    #[test]
    fn text_rule_wins_over_status_and_backs_off() {
        let c = classifier(rules());
        let o = c.classify(429, "Rate limit exceeded", 2);
        assert_eq!(o.status(), 429);
        let f = fb(&o);
        assert_eq!(f.backoff_level, Some(3));
        assert_eq!(f.cooldown, Some(Duration::from_millis(8_000)));
    }

    #[test]
    fn status_rule_applies_when_no_text_rule_matches() {
        let c = classifier(rules());
        let o = c.classify(503, "upstream went away", 0);
        let f = fb(&o);
        assert_eq!(f.cooldown, Some(Duration::from_millis(5_000)));
        assert_eq!(f.backoff_level, Some(0));
    }

    #[test]
    fn unmatched_4xx_is_a_client_error() {
        let c = classifier(rules());
        let o = c.classify(400, "invalid parameter", 0);
        assert_eq!(o, Outcome::ClientError { status: 400 });
    }

    #[test]
    fn unmatched_5xx_falls_back_on_transient_cooldown() {
        let c = classifier(rules());
        let o = c.classify(502, "", 0);
        assert_eq!(fb(&o).cooldown, Some(Duration::from_millis(30_000)));
    }

    #[test]
    fn backoff_level_saturates_and_cooldown_caps() {
        let c = classifier(rules());
        let o = c.classify(429, "rate limit: slow down", 14);
        let f = fb(&o);
        assert_eq!(f.backoff_level, Some(15));
        assert_eq!(f.cooldown, Some(Duration::from_millis(300_000)));
    }

    #[test]
    fn status_401_rule_cools_down_for_two_minutes() {
        let c = classifier(rules());
        let o = c.classify(401, "bad key", 0);
        assert_eq!(fb(&o).cooldown, Some(Duration::from_millis(120_000)));
    }

    #[test]
    fn explicit_non_fallback_rule_wins() {
        let c = classifier(vec![ErrorRule {
            match_: ErrorRuleMatch::Status(404),
            fallback: false,
            backoff: false,
            cooldown_ms: None,
        }]);
        assert_eq!(
            c.classify(404, "model not found", 0),
            Outcome::ClientError { status: 404 }
        );
    }
}
