//! In-memory LRU prompt and response cache for Vibe Router.
//!
//! Features:
//!   * Key hashing: SHA-256 of `(model_id, temperature, top_p, tools, messages_json)`.
//!   * Bounded memory consumption with LRU eviction (default 256MB).
//!   * Time-to-live (TTL) expiration per cached item.
//!   * Real-time metrics: hits, misses, evictions, memory_bytes, hit rate.
//!   * Zero-cost purging and thread-safe concurrent access.
//!   * Opt-in Stream Caching: Cleanly stores completed SSE stream frames.

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use vibe_router_core::Usage;

/// Compute deterministic cache key: SHA-256 of `(model_id, temperature, top_p, tools, messages_json)`.
pub fn compute_cache_key(
    model_id: &str,
    temperature: Option<f64>,
    top_p: Option<f64>,
    tools: Option<&serde_json::Value>,
    messages: &serde_json::Value,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(model_id.as_bytes());
    hasher.update(b":");
    if let Some(t) = temperature {
        hasher.update(format!("{:.4}", t).as_bytes());
    } else {
        hasher.update(b"default");
    }
    hasher.update(b":");
    if let Some(p) = top_p {
        hasher.update(format!("{:.4}", p).as_bytes());
    } else {
        hasher.update(b"default");
    }
    hasher.update(b":");
    if let Some(t) = tools {
        let tool_str = serde_json::to_string(t).unwrap_or_default();
        hasher.update(tool_str.as_bytes());
    } else {
        hasher.update(b"none");
    }
    hasher.update(b":");
    let msg_str = serde_json::to_string(messages).unwrap_or_default();
    hasher.update(msg_str.as_bytes());
    let hash = hasher.finalize();
    hash.iter().fold(String::with_capacity(64), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Convenience helper for simple prompts without tools or top_p.
pub fn compute_simple_cache_key(
    model_id: &str,
    temperature: Option<f64>,
    messages: &serde_json::Value,
) -> String {
    compute_cache_key(model_id, temperature, None, None, messages)
}

/// A cached prompt completion response (non-streaming JSON).
#[derive(Debug, Clone)]
pub struct CachedResponse {
    pub body: serde_json::Value,
    pub usage: Option<Usage>,
    pub created_at: Instant,
    pub approx_bytes: usize,
}

/// A cached SSE stream response (opt-in stream caching).
#[derive(Debug, Clone)]
pub struct CachedStream {
    pub frames: Vec<Vec<u8>>,
    pub usage: Option<Usage>,
    pub created_at: Instant,
    pub approx_bytes: usize,
}

/// Live cache statistics.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub stream_hits: u64,
    pub memory_bytes: usize,
    pub max_memory_bytes: usize,
    pub cached_prompts: usize,
    pub cached_streams: usize,
    pub hit_rate: f64,
}

struct Node<T> {
    key: String,
    value: T,
    approx_bytes: usize,
    created_at: Instant,
    prev: Option<usize>,
    next: Option<usize>,
}

struct LRUInner<T> {
    map: HashMap<String, usize>,
    nodes: Vec<Option<Node<T>>>,
    free_indices: Vec<usize>,
    head: Option<usize>, // Most recently used
    tail: Option<usize>, // Least recently used
    current_memory_bytes: usize,
    max_memory_bytes: usize,
    ttl: Duration,
}

impl<T: Clone> LRUInner<T> {
    fn new(max_memory_bytes: usize, ttl: Duration) -> Self {
        Self {
            map: HashMap::new(),
            nodes: Vec::new(),
            free_indices: Vec::new(),
            head: None,
            tail: None,
            current_memory_bytes: 0,
            max_memory_bytes,
            ttl,
        }
    }

    fn remove_node(&mut self, idx: usize) {
        let (prev, next) = {
            let node = self.nodes[idx].as_ref().expect("node exists");
            (node.prev, node.next)
        };

        if let Some(p) = prev {
            if let Some(n) = self.nodes[p].as_mut() {
                n.next = next;
            }
        } else {
            self.head = next;
        }

        if let Some(n) = next {
            if let Some(node) = self.nodes[n].as_mut() {
                node.prev = prev;
            }
        } else {
            self.tail = prev;
        }

        if let Some(node) = self.nodes[idx].as_mut() {
            node.prev = None;
            node.next = None;
        }
    }

    fn push_front(&mut self, idx: usize) {
        let old_head = self.head;
        if let Some(node) = self.nodes[idx].as_mut() {
            node.prev = None;
            node.next = old_head;
        }
        if let Some(h) = old_head {
            if let Some(node) = self.nodes[h].as_mut() {
                node.prev = Some(idx);
            }
        } else {
            self.tail = Some(idx);
        }
        self.head = Some(idx);
    }

    fn detach_and_delete(&mut self, idx: usize) -> T {
        self.remove_node(idx);
        let node = self.nodes[idx].take().expect("node exists");
        self.map.remove(&node.key);
        self.current_memory_bytes = self.current_memory_bytes.saturating_sub(node.approx_bytes);
        self.free_indices.push(idx);
        node.value
    }

    fn get(&mut self, key: &str, evictions: &AtomicU64) -> Option<T> {
        let &idx = self.map.get(key)?;
        let is_expired = {
            let node = self.nodes[idx].as_ref().expect("valid index");
            node.created_at.elapsed() > self.ttl
        };

        if is_expired {
            self.detach_and_delete(idx);
            evictions.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        self.remove_node(idx);
        self.push_front(idx);

        Some(self.nodes[idx].as_ref().unwrap().value.clone())
    }

    fn put(&mut self, key: String, value: T, approx_bytes: usize, evictions: &AtomicU64) {
        if approx_bytes > self.max_memory_bytes {
            return;
        }

        if let Some(&old_idx) = self.map.get(&key) {
            self.detach_and_delete(old_idx);
        }

        while self.current_memory_bytes + approx_bytes > self.max_memory_bytes {
            if let Some(tail_idx) = self.tail {
                self.detach_and_delete(tail_idx);
                evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }

        let node = Node {
            key: key.clone(),
            value,
            approx_bytes,
            created_at: Instant::now(),
            prev: None,
            next: None,
        };

        let idx = if let Some(free_idx) = self.free_indices.pop() {
            self.nodes[free_idx] = Some(node);
            free_idx
        } else {
            self.nodes.push(Some(node));
            self.nodes.len() - 1
        };

        self.push_front(idx);
        self.current_memory_bytes += approx_bytes;
        self.map.insert(key, idx);
    }

    fn clear(&mut self) {
        self.map.clear();
        self.nodes.clear();
        self.free_indices.clear();
        self.head = None;
        self.tail = None;
        self.current_memory_bytes = 0;
    }
}

/// Thread-safe in-memory LRU prompt cache supporting non-streaming and opt-in streaming.
#[derive(Clone)]
pub struct PromptCache {
    inner: Arc<Mutex<LRUInner<CachedResponse>>>,
    stream_inner: Arc<Mutex<LRUInner<CachedStream>>>,
    hits: Arc<AtomicU64>,
    misses: Arc<AtomicU64>,
    evictions: Arc<AtomicU64>,
    stream_hits: Arc<AtomicU64>,
}

impl Default for PromptCache {
    fn default() -> Self {
        // Default 256MB capacity and 30m TTL
        Self::new(256 * 1024 * 1024, Duration::from_secs(1800))
    }
}

impl PromptCache {
    pub fn new(max_memory_bytes: usize, ttl: Duration) -> Self {
        // Split capacity 75% for JSON responses, 25% for streams
        let json_cap = (max_memory_bytes * 3) / 4;
        let stream_cap = max_memory_bytes - json_cap;
        Self {
            inner: Arc::new(Mutex::new(LRUInner::new(json_cap, ttl))),
            stream_inner: Arc::new(Mutex::new(LRUInner::new(stream_cap, ttl))),
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            evictions: Arc::new(AtomicU64::new(0)),
            stream_hits: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Retrieve a cached response if present and unexpired.
    pub fn get(&self, key: &str) -> Option<CachedResponse> {
        let mut g = self.inner.lock();
        if let Some(hit) = g.get(key, &self.evictions) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(hit)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert or update a cached response.
    pub fn put(&self, key: String, body: serde_json::Value, usage: Option<Usage>) {
        let body_str = serde_json::to_string(&body).unwrap_or_default();
        let approx_bytes = key.len() + body_str.len() + 128;
        let item = CachedResponse {
            body,
            usage,
            created_at: Instant::now(),
            approx_bytes,
        };
        let mut g = self.inner.lock();
        g.put(key, item, approx_bytes, &self.evictions);
    }

    /// Retrieve a cached SSE stream if present and unexpired.
    pub fn get_stream(&self, key: &str) -> Option<CachedStream> {
        let mut g = self.stream_inner.lock();
        if let Some(hit) = g.get(key, &self.evictions) {
            self.stream_hits.fetch_add(1, Ordering::Relaxed);
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(hit)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Cache a clean, completed SSE stream.
    pub fn put_stream(&self, key: String, frames: Vec<Vec<u8>>, usage: Option<Usage>) {
        let frame_bytes: usize = frames.iter().map(|f| f.len()).sum();
        let approx_bytes = key.len() + frame_bytes + 128;
        let item = CachedStream {
            frames,
            usage,
            created_at: Instant::now(),
            approx_bytes,
        };
        let mut g = self.stream_inner.lock();
        g.put(key, item, approx_bytes, &self.evictions);
    }

    /// Clear all cached items (both non-streaming and streaming).
    pub fn purge(&self) {
        self.inner.lock().clear();
        self.stream_inner.lock().clear();
    }

    /// Collect current cache statistics.
    pub fn stats(&self) -> CacheStats {
        let g1 = self.inner.lock();
        let g2 = self.stream_inner.lock();
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };

        CacheStats {
            hits,
            misses,
            evictions: self.evictions.load(Ordering::Relaxed),
            stream_hits: self.stream_hits.load(Ordering::Relaxed),
            memory_bytes: g1.current_memory_bytes + g2.current_memory_bytes,
            max_memory_bytes: g1.max_memory_bytes + g2.max_memory_bytes,
            cached_prompts: g1.map.len(),
            cached_streams: g2.map.len(),
            hit_rate,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_body(msg: &str) -> serde_json::Value {
        serde_json::json!({
            "id": "cmpl-1",
            "choices": [{"message": {"role": "assistant", "content": msg}}]
        })
    }

    #[test]
    fn cache_key_determinism() {
        let msgs1 = serde_json::json!([{"role": "user", "content": "hello"}]);
        let msgs2 = serde_json::json!([{"role": "user", "content": "hello"}]);
        let msgs3 = serde_json::json!([{"role": "user", "content": "world"}]);

        let k1 = compute_simple_cache_key("gpt-4", Some(0.7), &msgs1);
        let k2 = compute_simple_cache_key("gpt-4", Some(0.7), &msgs2);
        let k3 = compute_simple_cache_key("gpt-4", Some(0.7), &msgs3);
        let k4 = compute_simple_cache_key("gpt-4", Some(0.0), &msgs1);
        let k5 = compute_simple_cache_key("gpt-3.5", Some(0.7), &msgs1);

        assert_eq!(k1, k2, "identical inputs produce identical hash");
        assert_ne!(k1, k3, "different messages produce different hash");
        assert_ne!(k1, k4, "different temperature produces different hash");
        assert_ne!(k1, k5, "different model produces different hash");
        assert_eq!(k1.len(), 64, "SHA-256 hex string is 64 characters");
    }

    #[test]
    fn cache_key_tools_and_top_p_differentiation() {
        let msgs = serde_json::json!([{"role": "user", "content": "solve"}]);
        let tools1 = serde_json::json!([{"type": "function", "function": {"name": "calculator"}}]);
        let tools2 = serde_json::json!([{"type": "function", "function": {"name": "search"}}]);

        let k_plain = compute_cache_key("gpt-4o", Some(0.7), Some(0.9), None, &msgs);
        let k_tools1 = compute_cache_key("gpt-4o", Some(0.7), Some(0.9), Some(&tools1), &msgs);
        let k_tools2 = compute_cache_key("gpt-4o", Some(0.7), Some(0.9), Some(&tools2), &msgs);
        let k_top_p2 = compute_cache_key("gpt-4o", Some(0.7), Some(0.95), None, &msgs);

        assert_ne!(
            k_plain, k_tools1,
            "tools vs plain must produce distinct keys"
        );
        assert_ne!(
            k_tools1, k_tools2,
            "different tool lists produce distinct keys"
        );
        assert_ne!(k_plain, k_top_p2, "different top_p produces distinct keys");
    }

    #[test]
    fn exact_hit() {
        let cache = PromptCache::new(1024 * 1024, Duration::from_secs(60));
        let key = "test_key".to_string();
        let body = test_body("hello from cache");

        assert!(cache.get(&key).is_none());
        assert_eq!(cache.stats().misses, 1);
        assert_eq!(cache.stats().hits, 0);

        cache.put(key.clone(), body.clone(), Some(Usage::reported(10, 5)));

        let hit = cache.get(&key);
        assert!(hit.is_some());
        let cached = hit.unwrap();
        assert_eq!(cached.body, body);
        assert_eq!(cached.usage, Some(Usage::reported(10, 5)));
        assert_eq!(cache.stats().hits, 1);
        assert_eq!(cache.stats().cached_prompts, 1);
    }

    #[test]
    fn stream_caching_round_trip() {
        let cache = PromptCache::new(1024 * 1024, Duration::from_secs(60));
        let key = "stream_key".to_string();
        let frames = vec![
            b"data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}\n\n".to_vec(),
            b"data: [DONE]\n\n".to_vec(),
        ];

        assert!(cache.get_stream(&key).is_none());
        assert_eq!(cache.stats().misses, 1);

        cache.put_stream(key.clone(), frames.clone(), Some(Usage::reported(5, 5)));

        let hit = cache.get_stream(&key);
        assert!(hit.is_some());
        let cached = hit.unwrap();
        assert_eq!(cached.frames, frames);
        assert_eq!(cached.usage, Some(Usage::reported(5, 5)));
        assert_eq!(cache.stats().stream_hits, 1);
        assert_eq!(cache.stats().cached_streams, 1);
    }

    #[test]
    fn different_prompt_miss() {
        let cache = PromptCache::new(1024 * 1024, Duration::from_secs(60));
        let k1 = compute_simple_cache_key(
            "m1",
            None,
            &serde_json::json!([{"role":"user","content":"A"}]),
        );
        let k2 = compute_simple_cache_key(
            "m1",
            None,
            &serde_json::json!([{"role":"user","content":"B"}]),
        );

        cache.put(k1.clone(), test_body("answer A"), None);

        assert!(cache.get(&k1).is_some());
        assert!(cache.get(&k2).is_none());
        assert_eq!(cache.stats().hits, 1);
        assert_eq!(cache.stats().misses, 1);
    }

    #[tokio::test]
    async fn ttl_expiration() {
        let cache = PromptCache::new(1024 * 1024, Duration::from_millis(40));
        let key = "ttl_key".to_string();
        cache.put(key.clone(), test_body("expiring soon"), None);

        // Immediate check hits
        assert!(cache.get(&key).is_some());
        assert_eq!(cache.stats().hits, 1);

        // Wait past TTL
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Check after expiration misses and evicts
        assert!(cache.get(&key).is_none());
        assert_eq!(cache.stats().misses, 1);
        assert_eq!(cache.stats().evictions, 1);
        assert_eq!(cache.stats().cached_prompts, 0);
    }

    #[test]
    fn size_limit_eviction() {
        // Small cache budget of 400 bytes (json_cap = 300 bytes, each item is ~220 bytes)
        let cache = PromptCache::new(400, Duration::from_secs(60));

        let k1 = "item_one".to_string();
        let k2 = "item_two".to_string();

        cache.put(k1.clone(), test_body("content 1"), None);
        assert!(cache.get(&k1).is_some());

        // Insert k2 which will exceed budget (220 * 2 = 440 > 300) and evict LRU (k1)
        cache.put(k2.clone(), test_body("content 2"), None);

        // k1 was evicted, k2 is present
        assert!(cache.get(&k1).is_none());
        assert!(cache.get(&k2).is_some());
        assert!(cache.stats().evictions >= 1);
    }

    #[test]
    fn purge_clears_all() {
        let cache = PromptCache::new(1024 * 1024, Duration::from_secs(60));
        cache.put("k1".into(), test_body("v1"), None);
        cache.put_stream("k2".into(), vec![b"frame".to_vec()], None);
        assert_eq!(cache.stats().cached_prompts, 1);
        assert_eq!(cache.stats().cached_streams, 1);

        cache.purge();
        assert_eq!(cache.stats().cached_prompts, 0);
        assert_eq!(cache.stats().cached_streams, 0);
        assert_eq!(cache.stats().memory_bytes, 0);
        assert!(cache.get("k1").is_none());
        assert!(cache.get_stream("k2").is_none());
    }
}
