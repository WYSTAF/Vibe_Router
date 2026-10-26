//! Multi-key pool rotation, health tracking, and 429 auto-cooldown.
//!
//! Supports:
//!   * Round-Robin and Least-Loaded selection strategies.
//!   * Automatic 429 rate limit cooldown per key (default 60s).
//!   * Instant retry with the next available healthy key in the pool.
//!   * Dynamic key addition via API or CLI.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use vibe_router_core::{KeySelectionStrategy, SecretRef};

/// State of an individual API key in the provider pool.
#[derive(Debug, Clone)]
pub struct KeySlot {
    pub secret_ref: SecretRef,
    pub cooldown_until: Option<Instant>,
    pub in_flight: usize,
    pub total_calls: u64,
    pub total_429s: u64,
    pub weight: u32,
    pub consecutive_429s: u32,
}

/// Thread-safe manager for a provider's collection of API keys.
#[derive(Debug)]
pub struct ProviderKeyPool {
    slots: Mutex<Vec<KeySlot>>,
    strategy: KeySelectionStrategy,
    rr_counter: AtomicUsize,
    default_cooldown: Duration,
}

impl Default for ProviderKeyPool {
    fn default() -> Self {
        Self::empty()
    }
}

impl ProviderKeyPool {
    pub fn new(keys: Vec<SecretRef>, strategy: KeySelectionStrategy) -> Self {
        let slots = keys
            .into_iter()
            .map(|secret_ref| KeySlot {
                secret_ref,
                cooldown_until: None,
                in_flight: 0,
                total_calls: 0,
                total_429s: 0,
                weight: 1,
                consecutive_429s: 0,
            })
            .collect();

        Self {
            slots: Mutex::new(slots),
            strategy,
            rr_counter: AtomicUsize::new(0),
            default_cooldown: Duration::from_secs(60),
        }
    }

    pub fn with_weights(keys: Vec<(SecretRef, u32)>, strategy: KeySelectionStrategy) -> Self {
        let slots = keys
            .into_iter()
            .map(|(secret_ref, weight)| KeySlot {
                secret_ref,
                cooldown_until: None,
                in_flight: 0,
                total_calls: 0,
                total_429s: 0,
                weight: weight.max(1),
                consecutive_429s: 0,
            })
            .collect();

        Self {
            slots: Mutex::new(slots),
            strategy,
            rr_counter: AtomicUsize::new(0),
            default_cooldown: Duration::from_secs(60),
        }
    }

    pub fn with_cooldown(
        keys: Vec<SecretRef>,
        strategy: KeySelectionStrategy,
        default_cooldown: Duration,
    ) -> Self {
        let mut pool = Self::new(keys, strategy);
        pool.default_cooldown = default_cooldown;
        pool
    }

    pub fn empty() -> Self {
        Self::new(Vec::new(), KeySelectionStrategy::RoundRobin)
    }

    pub fn is_empty(&self) -> bool {
        self.slots.lock().is_empty()
    }

    pub fn total_keys(&self) -> usize {
        self.slots.lock().len()
    }

    /// Number of keys not currently in rate-limit cooldown.
    pub fn healthy_keys(&self) -> usize {
        let now = Instant::now();
        self.slots
            .lock()
            .iter()
            .filter(|s| s.cooldown_until.is_none_or(|t| now >= t))
            .count()
    }

    /// Select the next healthy key according to strategy, excluding already-attempted keys.
    pub fn select_healthy_except(&self, exclude: &[usize]) -> Option<(usize, SecretRef)> {
        let mut slots = self.slots.lock();
        let now = Instant::now();

        let healthy_indices: Vec<usize> = slots
            .iter()
            .enumerate()
            .filter_map(|(i, s)| {
                if exclude.contains(&i) {
                    return None;
                }
                if s.cooldown_until.is_none_or(|until| now >= until) {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        if healthy_indices.is_empty() {
            return None;
        }

        let chosen_idx = match self.strategy {
            KeySelectionStrategy::RoundRobin => {
                let pick = self.rr_counter.fetch_add(1, Ordering::Relaxed) % healthy_indices.len();
                healthy_indices[pick]
            }
            KeySelectionStrategy::LeastLoaded => *healthy_indices
                .iter()
                .min_by_key(|&&i| (slots[i].in_flight, slots[i].total_calls))
                .expect("non-empty"),
            KeySelectionStrategy::Weighted => {
                let total_weight: u32 = healthy_indices
                    .iter()
                    .map(|&i| slots[i].weight.max(1))
                    .sum();
                if total_weight == 0 {
                    healthy_indices[0]
                } else {
                    let pick =
                        (self.rr_counter.fetch_add(1, Ordering::Relaxed) as u32) % total_weight;
                    let mut acc = 0;
                    let mut chosen = healthy_indices[0];
                    for &idx in &healthy_indices {
                        acc += slots[idx].weight.max(1);
                        if pick < acc {
                            chosen = idx;
                            break;
                        }
                    }
                    chosen
                }
            }
        };

        slots[chosen_idx].in_flight += 1;
        slots[chosen_idx].total_calls += 1;
        Some((chosen_idx, slots[chosen_idx].secret_ref.clone()))
    }

    /// Check if any healthy key remains that is not in `exclude`.
    pub fn has_healthy_except(&self, exclude: &[usize]) -> bool {
        let slots = self.slots.lock();
        let now = Instant::now();
        slots.iter().enumerate().any(|(i, s)| {
            !exclude.contains(&i) && s.cooldown_until.is_none_or(|until| now >= until)
        })
    }

    /// Release an in-flight request counter for a key.
    pub fn release_in_flight(&self, idx: usize) {
        let mut slots = self.slots.lock();
        if let Some(slot) = slots.get_mut(idx) {
            slot.in_flight = slot.in_flight.saturating_sub(1);
        }
    }

    /// Mark a key in cooldown with exponential backoff on repeated HTTP 429s.
    pub fn mark_cooldown(&self, idx: usize, duration: Option<Duration>) {
        let mut slots = self.slots.lock();
        if let Some(slot) = slots.get_mut(idx) {
            let base = duration.unwrap_or(self.default_cooldown);
            slot.consecutive_429s = slot.consecutive_429s.saturating_add(1);
            let shift = (slot.consecutive_429s.saturating_sub(1)).min(6);
            let factor = 1u32 << shift;
            let exp_dur = (base * factor).min(Duration::from_secs(900));
            slot.cooldown_until = Some(Instant::now() + exp_dur);
            slot.total_429s += 1;
        }
    }

    /// Mark a key as succeeded, clearing pending cooldown and resetting consecutive 429 backoff.
    pub fn mark_success(&self, idx: usize) {
        let mut slots = self.slots.lock();
        if let Some(slot) = slots.get_mut(idx) {
            slot.cooldown_until = None;
            slot.consecutive_429s = 0;
        }
    }

    /// Add a new key to the pool if not already present.
    pub fn add_key(&self, secret_ref: SecretRef) {
        self.add_key_with_weight(secret_ref, 1);
    }

    /// Add a new key with explicit weight.
    pub fn add_key_with_weight(&self, secret_ref: SecretRef, weight: u32) {
        let mut slots = self.slots.lock();
        if !slots.iter().any(|s| s.secret_ref == secret_ref) {
            slots.push(KeySlot {
                secret_ref,
                cooldown_until: None,
                in_flight: 0,
                total_calls: 0,
                total_429s: 0,
                weight: weight.max(1),
                consecutive_429s: 0,
            });
        }
    }

    /// Earliest remaining cooldown duration among keys in cooldown.
    pub fn earliest_cooldown(&self) -> Option<Duration> {
        let slots = self.slots.lock();
        let now = Instant::now();
        slots
            .iter()
            .filter_map(|s| s.cooldown_until)
            .filter(|&t| t > now)
            .map(|t| t.duration_since(now))
            .min()
    }

    /// All secret references currently in the pool.
    pub fn all_keys(&self) -> Vec<SecretRef> {
        self.slots
            .lock()
            .iter()
            .map(|s| s.secret_ref.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rotation_distribution_round_robin() {
        let keys = vec![
            SecretRef::Literal("key-1".into()),
            SecretRef::Literal("key-2".into()),
            SecretRef::Literal("key-3".into()),
        ];
        let pool = ProviderKeyPool::new(keys, KeySelectionStrategy::RoundRobin);

        let mut counts = vec![0, 0, 0];
        for _ in 0..12 {
            let (idx, _) = pool.select_healthy_except(&[]).unwrap();
            counts[idx] += 1;
            pool.release_in_flight(idx);
        }

        assert_eq!(
            counts,
            vec![4, 4, 4],
            "each key picked uniformly in round-robin"
        );
    }

    #[test]
    fn test_429_cooldown_transition() {
        let keys = vec![
            SecretRef::Literal("key-A".into()),
            SecretRef::Literal("key-B".into()),
        ];
        let pool = ProviderKeyPool::new(keys, KeySelectionStrategy::RoundRobin);

        // Pick key 0
        let (idx1, _) = pool.select_healthy_except(&[]).unwrap();
        pool.release_in_flight(idx1);

        // Put key 0 in cooldown for 60s
        pool.mark_cooldown(idx1, Some(Duration::from_secs(60)));
        assert_eq!(pool.healthy_keys(), 1);

        // Next 5 requests must only pick key 1
        for _ in 0..5 {
            let (idx, _) = pool.select_healthy_except(&[]).unwrap();
            assert_ne!(idx, idx1, "cooling key must not be selected");
            pool.release_in_flight(idx);
        }
    }

    #[tokio::test]
    async fn test_recovery_after_cooldown() {
        let keys = vec![SecretRef::Literal("key-only".into())];
        let pool = ProviderKeyPool::new(keys, KeySelectionStrategy::RoundRobin);

        let (idx, _) = pool.select_healthy_except(&[]).unwrap();
        pool.release_in_flight(idx);

        // Put in short 30ms cooldown
        pool.mark_cooldown(idx, Some(Duration::from_millis(30)));
        assert!(
            pool.select_healthy_except(&[]).is_none(),
            "in cooldown -> None"
        );

        // Wait past cooldown
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Key has recovered!
        assert_eq!(pool.healthy_keys(), 1);
        let (rec_idx, _) = pool.select_healthy_except(&[]).unwrap();
        assert_eq!(rec_idx, idx);
    }

    #[test]
    fn test_least_loaded_strategy() {
        let keys = vec![
            SecretRef::Literal("k1".into()),
            SecretRef::Literal("k2".into()),
        ];
        let pool = ProviderKeyPool::new(keys, KeySelectionStrategy::LeastLoaded);

        // First pick: k1 (in_flight becomes 1)
        let (idx1, _) = pool.select_healthy_except(&[]).unwrap();
        assert_eq!(idx1, 0);

        // Second pick while k1 is still in flight: picks k2!
        let (idx2, _) = pool.select_healthy_except(&[]).unwrap();
        assert_eq!(idx2, 1);

        // Release k1
        pool.release_in_flight(idx1);

        // Third pick: k1 has in_flight=0, k2 has in_flight=1 -> picks k1!
        let (idx3, _) = pool.select_healthy_except(&[]).unwrap();
        assert_eq!(idx3, 0);
    }

    #[test]
    fn test_all_keys_in_cooldown_returns_none() {
        let keys = vec![
            SecretRef::Literal("k1".into()),
            SecretRef::Literal("k2".into()),
        ];
        let pool = ProviderKeyPool::new(keys, KeySelectionStrategy::RoundRobin);

        pool.mark_cooldown(0, Some(Duration::from_secs(60)));
        pool.mark_cooldown(1, Some(Duration::from_secs(60)));

        assert_eq!(pool.healthy_keys(), 0);
        assert!(pool.select_healthy_except(&[]).is_none());
        assert!(pool.earliest_cooldown().is_some());
    }

    #[test]
    fn test_weighted_strategy_distribution() {
        let keys = vec![
            (SecretRef::Literal("heavy".into()), 3),
            (SecretRef::Literal("light".into()), 1),
        ];
        let pool = ProviderKeyPool::with_weights(keys, KeySelectionStrategy::Weighted);

        let mut counts = [0, 0];
        for _ in 0..16 {
            let (idx, _) = pool.select_healthy_except(&[]).unwrap();
            counts[idx] += 1;
            pool.release_in_flight(idx);
        }

        // Heavy has weight 3, light has weight 1 -> total weight 4.
        // Over 16 requests: heavy=12, light=4.
        assert_eq!(counts[0], 12);
        assert_eq!(counts[1], 4);
    }

    #[test]
    fn test_exponential_backoff_cooldown_on_repeated_429() {
        let keys = vec![SecretRef::Literal("k".into())];
        let pool = ProviderKeyPool::new(keys, KeySelectionStrategy::RoundRobin);

        // 1st 429: base 60s
        pool.mark_cooldown(0, Some(Duration::from_secs(10)));
        let cd1 = pool.earliest_cooldown().unwrap().as_secs();
        assert!((9..=10).contains(&cd1));

        // 2nd 429: 10 * 2 = 20s
        pool.mark_cooldown(0, Some(Duration::from_secs(10)));
        let cd2 = pool.earliest_cooldown().unwrap().as_secs();
        assert!((18..=20).contains(&cd2));

        // 3rd 429: 10 * 4 = 40s
        pool.mark_cooldown(0, Some(Duration::from_secs(10)));
        let cd3 = pool.earliest_cooldown().unwrap().as_secs();
        assert!((38..=40).contains(&cd3));

        // Success resets consecutive counter
        pool.mark_success(0);
        assert_eq!(pool.healthy_keys(), 1);

        // Next 429 resets back to base 10s
        pool.mark_cooldown(0, Some(Duration::from_secs(10)));
        let cd4 = pool.earliest_cooldown().unwrap().as_secs();
        assert!((9..=10).contains(&cd4));
    }
}
