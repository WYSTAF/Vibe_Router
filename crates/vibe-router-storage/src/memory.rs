//! An in-memory backend: no disk, nothing survives a restart.
//!
//! The default when `storage.sqlite_path` is empty, and the fixture every
//! store test uses so it can assert against a known, deterministic starting
//! state.

use std::collections::VecDeque;

use parking_lot::Mutex;

use crate::{
    ComboStore, ProviderStore, RuntimeCombo, RuntimeProvider, StorageResult, StoredUsage, Totals,
    UsageRecord, UsageStore,
};

/// Ring buffer of recent rows: bounded by construction, so a long-running
/// test cannot grow it without limit.
const MAX_ROWS: usize = 2048;

#[derive(Default)]
pub struct MemoryStore {
    rows: Mutex<VecDeque<StoredUsage>>,
    combos: Mutex<Vec<RuntimeCombo>>,
    providers: Mutex<Vec<RuntimeProvider>>,
    next_id: std::sync::atomic::AtomicI64,
}

impl MemoryStore {
    fn next_row(&self, record: UsageRecord) -> StoredUsage {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let ts_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        StoredUsage { id, ts_ms, record }
    }
}

impl UsageStore for MemoryStore {
    fn append(&self, record: UsageRecord) {
        let row = self.next_row(record);
        let mut g = self.rows.lock();
        g.push_front(row);
        while g.len() > MAX_ROWS {
            g.pop_back();
        }
    }

    fn recent(&self, limit: u32) -> StorageResult<Vec<StoredUsage>> {
        Ok(self
            .rows
            .lock()
            .iter()
            .take(limit as usize)
            .cloned()
            .collect())
    }

    fn totals(&self) -> StorageResult<Totals> {
        let g = self.rows.lock();
        let mut t = Totals {
            requests: g.len() as u64,
            ..Totals::default()
        };
        for row in g.iter() {
            let r = &row.record;
            if (200..300).contains(&r.status) {
                t.succeeded += 1;
            }
            if r.status >= 400 {
                t.failed += 1;
            }
            t.prompt_tokens += r.prompt_tokens as u64;
            t.completion_tokens += r.completion_tokens as u64;
            if r.usage_estimated {
                t.estimated_usage_rows += 1;
            }
        }
        Ok(t)
    }
}

impl ComboStore for MemoryStore {
    fn list_combos(&self) -> StorageResult<Vec<RuntimeCombo>> {
        Ok(self.combos.lock().clone())
    }

    fn upsert_combo(&self, combo: &RuntimeCombo) -> StorageResult<()> {
        let mut g = self.combos.lock();
        match g.iter_mut().find(|c| c.name == combo.name) {
            Some(existing) => *existing = combo.clone(),
            None => g.push(combo.clone()),
        }
        Ok(())
    }

    fn delete_combo(&self, name: &str) -> StorageResult<bool> {
        let mut g = self.combos.lock();
        let before = g.len();
        g.retain(|c| c.name != name);
        Ok(g.len() < before)
    }
}

impl ProviderStore for MemoryStore {
    fn list_providers(&self) -> StorageResult<Vec<RuntimeProvider>> {
        Ok(self.providers.lock().clone())
    }

    fn upsert_provider(&self, provider: &RuntimeProvider) -> StorageResult<()> {
        let mut g = self.providers.lock();
        match g.iter_mut().find(|p| p.id == provider.id) {
            Some(existing) => *existing = provider.clone(),
            None => g.push(provider.clone()),
        }
        Ok(())
    }

    fn delete_provider(&self, id: &str) -> StorageResult<bool> {
        let mut g = self.providers.lock();
        let before = g.len();
        g.retain(|p| p.id != id);
        Ok(g.len() < before)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn record(model: &str) -> UsageRecord {
        UsageRecord {
            requested_model: model.into(),
            served_model: "p/m1".into(),
            provider: "p".into(),
            combo: None,
            status: 200,
            streamed: false,
            prompt_tokens: 3,
            completion_tokens: 4,
            usage_estimated: false,
            latency: Duration::from_millis(10),
            failovers: 0,
            ..UsageRecord::default()
        }
    }

    #[test]
    fn usage_round_trips_newest_first() {
        let s = MemoryStore::default();
        s.append(record("a"));
        s.append(record("b"));
        let rows = s.recent(10).unwrap();
        assert_eq!(rows[0].record.requested_model, "b");
        assert_eq!(s.totals().unwrap().requests, 2);
    }

    #[test]
    fn estimated_flag_is_preserved() {
        let s = MemoryStore::default();
        let mut r = record("a");
        r.usage_estimated = true;
        s.append(r);
        assert!(s.recent(10).unwrap()[0].record.usage_estimated);
        assert_eq!(s.totals().unwrap().estimated_usage_rows, 1);
    }

    #[test]
    fn ring_buffer_stays_bounded() {
        let s = MemoryStore::default();
        for i in 0..(MAX_ROWS as u32 + 500) {
            s.append(record(&format!("m{i}")));
        }
        assert_eq!(s.rows.lock().len(), MAX_ROWS);
        // The newest row is still first.
        assert_eq!(s.recent(1).unwrap()[0].record.requested_model, "m2547");
    }

    #[test]
    fn combos_upsert_and_delete() {
        let s = MemoryStore::default();
        let c = RuntimeCombo {
            name: "cheap".into(),
            description: None,
            models: vec!["a/x".into()],
        };
        s.upsert_combo(&c).unwrap();
        assert_eq!(s.list_combos().unwrap().len(), 1);
        let updated = RuntimeCombo {
            models: vec!["a/x".into(), "b/y".into()],
            ..c.clone()
        };
        s.upsert_combo(&updated).unwrap();
        let listed = s.list_combos().unwrap();
        assert_eq!(listed.len(), 1, "upsert must replace, not append");
        assert_eq!(listed[0].models.len(), 2);
        assert!(s.delete_combo("cheap").unwrap());
        assert!(
            !s.delete_combo("cheap").unwrap(),
            "second delete finds nothing"
        );
        assert!(s.list_combos().unwrap().is_empty());
    }
}
