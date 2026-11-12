//! The single SQLite persistence owner.
//!
//! One connection, one schema, one background writer. This struct implements
//! *both* the synchronous request-path traits ([`UsageStore`], [`ComboStore`],
//! [`ProviderStore`]) and the async [`StorageBackend`] trait, so the whole
//! binary shares one owner of `providers`, `combos`, `aliases`, `proxies`,
//! `rates`, and `usage`. There is exactly one usage write per request: the
//! request path calls [`StorageBackend::record_usage`], which prices the row
//! against the persisted rate table and enqueues it once.
//!
//! Disk-backed stores run in WAL mode with a 5s busy timeout. RAM-first
//! portable mode opens `:memory:`, hydrates once from disk, and snapshots back
//! atomically (temp file + rename) on a timer and at shutdown — zero flash wear
//! during traffic.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use crate::backend::{BackendError, BackendResult, StorageBackend};
use crate::types::{
    Combo, CostBreakdown, CostLine, Provider, ProxyRecord, Rates, UsageMetricsSummary, UsageRecord,
};
use crate::{
    ComboStore, ProviderStore, RuntimeCombo, RuntimeProvider, StorageError, StorageResult,
    StoredUsage, Totals, UsageStore,
};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS providers (
    id        TEXT PRIMARY KEY,
    payload   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS combos (
    name      TEXT PRIMARY KEY,
    payload   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS aliases (
    alias     TEXT PRIMARY KEY,
    target    TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS proxies (
    id        TEXT PRIMARY KEY,
    payload   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rates (
    model             TEXT PRIMARY KEY,
    prompt_micros     INTEGER NOT NULL DEFAULT 0,
    completion_micros INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS usage (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                 INTEGER NOT NULL,
    requested_model    TEXT    NOT NULL,
    served_model       TEXT    NOT NULL,
    provider           TEXT    NOT NULL,
    combo              TEXT,
    status             INTEGER NOT NULL,
    streamed           INTEGER NOT NULL,
    prompt_tokens      INTEGER NOT NULL,
    completion_tokens  INTEGER NOT NULL,
    usage_estimated    INTEGER NOT NULL,
    latency_ms         INTEGER NOT NULL,
    failovers          INTEGER NOT NULL,
    cost_micros        INTEGER NOT NULL DEFAULT 0,
    role               TEXT,
    cached             INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_usage_ts       ON usage (ts DESC);
CREATE INDEX IF NOT EXISTS idx_usage_provider ON usage (provider);
CREATE INDEX IF NOT EXISTS idx_usage_model    ON usage (served_model);
CREATE INDEX IF NOT EXISTS idx_usage_role     ON usage (role);
"#;

/// Bounded writer queue. The sync `append` path uses `try_send` so a response
/// never blocks on disk; the async path applies backpressure via `send`.
const WRITER_CAPACITY: usize = 4096;

enum Message {
    Usage(Box<UsageRecord>),
    /// Reply once every write queued before this message is committed.
    Flush(oneshot::Sender<()>),
}

pub struct SqliteStore {
    conn: Arc<Mutex<rusqlite::Connection>>,
    tx: mpsc::Sender<Message>,
    /// Rows with no matching rate: cost is genuinely unknown.
    unpriced: Arc<AtomicU64>,
    /// Set on any mutation; cleared by a successful disk snapshot. Drives the
    /// portable-mode periodic flush so a clean store never touches the disk.
    dirty: Arc<AtomicBool>,
    path: String,
}

impl SqliteStore {
    /// Open (creating if needed) a disk-backed store, or `:memory:`.
    pub fn open_sync(path: impl AsRef<Path>) -> BackendResult<Self> {
        let path = path.as_ref();
        let path_str = path.to_string_lossy().into_owned();
        let is_memory = path_str == ":memory:";
        if !is_memory {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| BackendError::Io(format!("{}: {e}", parent.display())))?;
                }
            }
        }
        let conn = rusqlite::Connection::open(path)
            .map_err(|e| BackendError::Io(format!("{}: {e}", path.display())))?;
        // A router shares its DB with CLI readers; wait rather than fail on a
        // brief lock contention.
        conn.busy_timeout(Duration::from_millis(5000))
            .map_err(|e| BackendError::Query(e.to_string()))?;
        if !is_memory {
            // WAL: the writer commits without blocking readers. NORMAL sync is
            // the standard WAL pairing; usage rows are analytics, not a ledger.
            conn.pragma_update(None, "journal_mode", "WAL")
                .map_err(|e| BackendError::Query(e.to_string()))?;
            conn.pragma_update(None, "synchronous", "NORMAL")
                .map_err(|e| BackendError::Query(e.to_string()))?;
        }
        conn.execute_batch(SCHEMA)
            .map_err(|e| BackendError::Query(e.to_string()))?;
        // Non-destructive migration for databases created before `cached`.
        let _ = conn.execute(
            "ALTER TABLE usage ADD COLUMN cached INTEGER NOT NULL DEFAULT 0",
            [],
        );

        let conn = Arc::new(Mutex::new(conn));
        let (tx, rx) = mpsc::channel::<Message>(WRITER_CAPACITY);
        spawn_writer(conn.clone(), rx);

        Ok(Self {
            conn,
            tx,
            unpriced: Arc::new(AtomicU64::new(0)),
            dirty: Arc::new(AtomicBool::new(false)),
            path: path_str,
        })
    }

    /// Open asynchronously, attaching to the current runtime.
    pub async fn open(path: impl AsRef<Path>) -> BackendResult<Self> {
        Self::open_sync(path)
    }

    /// An in-memory store: same behaviour, nothing on disk.
    pub async fn open_in_memory() -> BackendResult<Self> {
        Self::open_sync(":memory:")
    }

    /// RAM-first portable mode: hydrate existing state from `disk_path` into
    /// `:memory:` once. All subsequent traffic stays in RAM (zero flash wear).
    pub fn open_portable(disk_path: impl AsRef<Path>) -> BackendResult<Self> {
        let disk_path = disk_path.as_ref();
        let mem = Self::open_sync(":memory:")?;
        if disk_path.exists() {
            let disk_conn = rusqlite::Connection::open(disk_path)
                .map_err(|e| BackendError::Io(format!("{}: {e}", disk_path.display())))?;
            let mut mem_conn = mem.conn.lock();
            let backup = rusqlite::backup::Backup::new(&disk_conn, &mut mem_conn)
                .map_err(|e| BackendError::Query(format!("hydration backup init: {e}")))?;
            backup
                .run_to_completion(100, Duration::from_millis(1), None)
                .map_err(|e| BackendError::Query(format!("hydration failed: {e}")))?;
        }
        Ok(mem)
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// True when a mutation has occurred since the last successful snapshot.
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }

    fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    /// Wait until every record queued so far is committed. Tests and graceful
    /// shutdown only; the request path never blocks on it.
    pub async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Message::Flush(tx)).await.is_ok() {
            let _ = rx.await;
        }
    }

    /// Snapshot the in-memory database to disk via atomic write-to-temp +
    /// rename. Checkpoints and closes the temp handle before replacing the
    /// target so Windows never hits a sharing violation and Unix never orphans
    /// `-wal` data next to the replaced file.
    pub fn flush_to_disk(&self, disk_path: impl AsRef<Path>) -> BackendResult<()> {
        let disk_path = disk_path.as_ref();
        if let Some(parent) = disk_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| BackendError::Io(format!("{}: {e}", parent.display())))?;
            }
        }
        let tmp_path = disk_path.with_extension(format!("tmp.{}", std::process::id()));
        {
            let mut tmp_conn = rusqlite::Connection::open(&tmp_path)
                .map_err(|e| BackendError::Io(format!("{}: {e}", tmp_path.display())))?;
            {
                let mem_conn = self.conn.lock();
                let backup = rusqlite::backup::Backup::new(&mem_conn, &mut tmp_conn)
                    .map_err(|e| BackendError::Query(format!("flush backup init: {e}")))?;
                backup
                    .run_to_completion(100, Duration::from_millis(1), None)
                    .map_err(|e| BackendError::Query(format!("flush to disk failed: {e}")))?;
            }
            // Fold the temp WAL into the main file and release the handle
            // before the rename (the drop closes the connection).
            let _ = tmp_conn.pragma_update(None, "wal_checkpoint", "TRUNCATE");
        }
        // Remove any stale sidecar files belonging to the old target so the
        // renamed snapshot is the single source of truth.
        for ext in ["wal", "shm"] {
            let sidecar = disk_path.with_extension(format!("db-{ext}"));
            let _ = std::fs::remove_file(sidecar);
        }
        std::fs::rename(&tmp_path, disk_path)
            .map_err(|e| BackendError::Io(format!("atomic rename failed: {e}")))?;
        self.dirty.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Per-million-token rates: embedded defaults overlaid with persisted edits.
    /// Rates are written synchronously under the connection lock, so this read
    /// needs no writer barrier — keeping it off the hot path.
    async fn rates(&self) -> BackendResult<HashMap<String, Rates>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT model, prompt_micros, completion_micros FROM rates")
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)? as u64,
                    r.get::<_, i64>(2)? as u64,
                ))
            })
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let mut out: HashMap<String, Rates> = Rates::default_frontier_rates().into_iter().collect();
        for row in rows {
            let (model, p, c) = row.map_err(|e| BackendError::Query(e.to_string()))?;
            out.insert(
                model,
                Rates {
                    prompt_micros: p,
                    completion_micros: c,
                },
            );
        }
        Ok(out)
    }

    /// Compute the cost of a record from the rate table, marking it unpriced
    /// when no rate exists. An unknown price is never reported as zero.
    fn price(record: &UsageRecord, rates: &HashMap<String, Rates>, unpriced: &AtomicU64) -> u64 {
        if record.cached {
            return 0;
        }
        let key = if record.provider.is_empty() {
            record.served_model.clone()
        } else {
            format!("{}/{}", record.provider, record.served_model)
        };
        let rate = rates.get(&key).or_else(|| {
            record
                .served_model
                .rsplit_once('/')
                .and_then(|(_, m)| rates.get(m))
        });
        match rate {
            Some(r) => r.cost_micros(record.prompt_tokens, record.completion_tokens),
            None => {
                unpriced.fetch_add(1, Ordering::Relaxed);
                0
            }
        }
    }

    async fn usage_metrics(&self) -> BackendResult<UsageMetricsSummary> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT COUNT(*),
                        COALESCE(SUM(status BETWEEN 200 AND 299), 0),
                        COALESCE(SUM(status >= 400), 0),
                        COALESCE(SUM(prompt_tokens), 0),
                        COALESCE(SUM(completion_tokens), 0),
                        COALESCE(SUM(cost_micros), 0),
                        COALESCE(SUM(usage_estimated), 0),
                        COALESCE(SUM(streamed), 0),
                        COALESCE(SUM(failovers), 0)
                 FROM usage",
            )
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let mut summary = stmt
            .query_row([], |r| {
                Ok(UsageMetricsSummary {
                    requests: r.get::<_, i64>(0)? as u64,
                    succeeded: r.get::<_, i64>(1)? as u64,
                    failed: r.get::<_, i64>(2)? as u64,
                    prompt_tokens: r.get::<_, i64>(3)? as u64,
                    completion_tokens: r.get::<_, i64>(4)? as u64,
                    total_cost_micros: r.get::<_, i64>(5)? as u64,
                    estimated_rows: r.get::<_, i64>(6)? as u64,
                    streamed_requests: r.get::<_, i64>(7)? as u64,
                    failovers: r.get::<_, i64>(8)? as u64,
                    avg_latency_ms: 0,
                    p50_latency_ms: 0,
                    p95_latency_ms: 0,
                    p99_latency_ms: 0,
                })
            })
            .map_err(|e| BackendError::Query(e.to_string()))?;

        let mut latencies: Vec<u64> = {
            let mut stmt = conn
                .prepare("SELECT latency_ms FROM usage")
                .map_err(|e| BackendError::Query(e.to_string()))?;
            let rows = stmt
                .query_map([], |r| r.get::<_, i64>(0).map(|v| v as u64))
                .map_err(|e| BackendError::Query(e.to_string()))?;
            let mut v = Vec::new();
            for row in rows {
                v.push(row.map_err(|e| BackendError::Query(e.to_string()))?);
            }
            v
        };
        if !latencies.is_empty() {
            latencies.sort_unstable();
            summary.avg_latency_ms = latencies.iter().sum::<u64>() / latencies.len() as u64;
            summary.p50_latency_ms = percentile(&latencies, 0.50);
            summary.p95_latency_ms = percentile(&latencies, 0.95);
            summary.p99_latency_ms = percentile(&latencies, 0.99);
        }
        Ok(summary)
    }

    async fn cost_breakdown(&self) -> BackendResult<CostBreakdown> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT provider, served_model, COALESCE(combo, ''), COALESCE(role, ''), status,
                        prompt_tokens, completion_tokens, cost_micros, usage_estimated
                 FROM usage",
            )
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, i64>(5)? as u64,
                    r.get::<_, i64>(6)? as u64,
                    r.get::<_, i64>(7)? as u64,
                    r.get::<_, i64>(8)?,
                ))
            })
            .map_err(|e| BackendError::Query(e.to_string()))?;

        let mut by_provider: BTreeMap<String, CostLine> = BTreeMap::new();
        let mut by_model: BTreeMap<String, CostLine> = BTreeMap::new();
        let mut by_combo: BTreeMap<String, CostLine> = BTreeMap::new();
        let mut by_role: BTreeMap<String, CostLine> = BTreeMap::new();
        let mut total = 0u64;

        for row in rows {
            let (provider, model, combo, role, status, prompt, completion, cost, estimated) =
                row.map_err(|e| BackendError::Query(e.to_string()))?;
            let counted = (200..300).contains(&status);
            let priced = cost > 0 || (prompt == 0 && completion == 0);
            accumulate(
                &mut by_provider,
                &provider,
                counted,
                prompt,
                completion,
                cost,
                estimated,
            );
            accumulate(
                &mut by_model,
                &model,
                counted,
                prompt,
                completion,
                cost,
                estimated,
            );
            if !combo.is_empty() {
                accumulate(
                    &mut by_combo,
                    &combo,
                    counted,
                    prompt,
                    completion,
                    cost,
                    estimated,
                );
            }
            if !role.is_empty() {
                accumulate(
                    &mut by_role,
                    &role,
                    counted,
                    prompt,
                    completion,
                    cost,
                    estimated,
                );
            }
            if priced {
                total += cost;
            }
        }

        let unpriced = self.unpriced.load(Ordering::Relaxed);
        Ok(CostBreakdown {
            total_micros: total,
            by_provider,
            by_model,
            by_combo,
            by_role,
            unpriced_rows: unpriced,
        })
    }
}

/// Spawn the single background writer on the current runtime, or on a dedicated
/// thread when opened outside one (CLI inspection commands).
fn spawn_writer(conn: Arc<Mutex<rusqlite::Connection>>, mut rx: mpsc::Receiver<Message>) {
    let run = async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Message::Usage(record) => {
                    if let Err(e) = write_usage(&conn, &record) {
                        tracing::warn!("usage write failed: {e}");
                    }
                }
                Message::Flush(reply) => {
                    let _ = reply.send(());
                }
            }
        }
    };
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(run);
    } else {
        std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!("failed to create runtime for sqlite writer: {e}");
                    return;
                }
            };
            rt.block_on(run);
        });
    }
}

// ---------------------------------------------------------------- sync traits

impl UsageStore for SqliteStore {
    fn append(&self, record: UsageRecord) {
        // Non-blocking: a full queue drops the row rather than stalling a
        // response. The request path uses `record_usage` (priced, async); this
        // exists for callers that already priced a row.
        self.mark_dirty();
        if self.tx.try_send(Message::Usage(Box::new(record))).is_err() {
            tracing::warn!("usage queue full; dropping row");
        }
    }

    fn recent(&self, limit: u32) -> StorageResult<Vec<StoredUsage>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, ts, requested_model, served_model, provider, combo,
                        status, streamed, prompt_tokens, completion_tokens,
                        usage_estimated, latency_ms, failovers, cost_micros, role, cached
                 FROM usage ORDER BY id DESC LIMIT ?1",
            )
            .map_err(sqlite_err)?;
        let rows = stmt.query_map([limit], row_to_stored).map_err(sqlite_err)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(sqlite_err)
    }

    fn totals(&self) -> StorageResult<Totals> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare_cached(
                "SELECT COUNT(*),
                        COALESCE(SUM(status BETWEEN 200 AND 299), 0),
                        COALESCE(SUM(status >= 400), 0),
                        COALESCE(SUM(prompt_tokens), 0),
                        COALESCE(SUM(completion_tokens), 0),
                        COALESCE(SUM(usage_estimated), 0)
                 FROM usage",
            )
            .map_err(sqlite_err)?;
        stmt.query_row([], |r| {
            Ok(Totals {
                requests: r.get::<_, i64>(0)? as u64,
                succeeded: r.get::<_, i64>(1)? as u64,
                failed: r.get::<_, i64>(2)? as u64,
                prompt_tokens: r.get::<_, i64>(3)? as u64,
                completion_tokens: r.get::<_, i64>(4)? as u64,
                estimated_usage_rows: r.get::<_, i64>(5)? as u64,
            })
        })
        .map_err(sqlite_err)
    }
}

impl ComboStore for SqliteStore {
    fn list_combos(&self) -> StorageResult<Vec<RuntimeCombo>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare_cached("SELECT payload FROM combos ORDER BY name")
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(sqlite_err)?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(sqlite_err)?;
            let combo: Combo = serde_json::from_str(&payload)
                .map_err(|e| StorageError::Sqlite(format!("corrupt combo: {e}")))?;
            out.push(combo.into());
        }
        Ok(out)
    }

    fn upsert_combo(&self, combo: &RuntimeCombo) -> StorageResult<()> {
        let mut full: Combo = combo.clone().into();
        let now = now_secs();
        if full.created_at.is_none() {
            full.created_at = Some(now);
        }
        full.updated_at = Some(now);
        let payload = serde_json::to_string(&full)
            .map_err(|e| StorageError::Sqlite(format!("cannot encode combo: {e}")))?;
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO combos (name, payload) VALUES (?1, ?2)
             ON CONFLICT(name) DO UPDATE SET payload = ?2",
            rusqlite::params![full.name, payload],
        )
        .map_err(sqlite_err)?;
        drop(conn);
        self.mark_dirty();
        Ok(())
    }

    fn delete_combo(&self, name: &str) -> StorageResult<bool> {
        let conn = self.conn.lock();
        let changed = conn
            .execute("DELETE FROM combos WHERE name = ?1", [name])
            .map_err(sqlite_err)?;
        drop(conn);
        self.mark_dirty();
        Ok(changed > 0)
    }
}

impl ProviderStore for SqliteStore {
    fn list_providers(&self) -> StorageResult<Vec<RuntimeProvider>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare_cached("SELECT payload FROM providers ORDER BY id")
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(sqlite_err)?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(sqlite_err)?;
            let provider: Provider = serde_json::from_str(&payload)
                .map_err(|e| StorageError::Sqlite(format!("corrupt provider: {e}")))?;
            out.push(provider.into());
        }
        Ok(out)
    }

    fn upsert_provider(&self, provider: &RuntimeProvider) -> StorageResult<()> {
        let full: Provider = provider.clone().into();
        let payload = serde_json::to_string(&full)
            .map_err(|e| StorageError::Sqlite(format!("cannot encode provider: {e}")))?;
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO providers (id, payload) VALUES (?1, ?2)
             ON CONFLICT(id) DO UPDATE SET payload = ?2",
            rusqlite::params![full.id, payload],
        )
        .map_err(sqlite_err)?;
        drop(conn);
        self.mark_dirty();
        Ok(())
    }

    fn delete_provider(&self, id: &str) -> StorageResult<bool> {
        let conn = self.conn.lock();
        let changed = conn
            .execute("DELETE FROM providers WHERE id = ?1", [id])
            .map_err(sqlite_err)?;
        drop(conn);
        self.mark_dirty();
        Ok(changed > 0)
    }
}

// ---------------------------------------------------------------- async trait

#[async_trait]
impl StorageBackend for SqliteStore {
    async fn get_providers(&self) -> BackendResult<Vec<Provider>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT payload FROM providers ORDER BY id")
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| BackendError::Query(e.to_string()))?;
            let provider: Provider = serde_json::from_str(&payload)
                .map_err(|e| BackendError::Invalid(format!("provider row: {e}")))?;
            out.push(provider);
        }
        Ok(out)
    }

    async fn save_provider(&self, provider: &Provider) -> BackendResult<()> {
        let payload = serde_json::to_string(provider)
            .map_err(|e| BackendError::Invalid(format!("encode provider: {e}")))?;
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO providers (id, payload) VALUES (?1, ?2)
             ON CONFLICT(id) DO UPDATE SET payload = ?2",
            rusqlite::params![provider.id, payload],
        )
        .map_err(|e| BackendError::Query(e.to_string()))?;
        drop(conn);
        self.mark_dirty();
        Ok(())
    }

    async fn delete_provider(&self, id: &str) -> BackendResult<()> {
        let conn = self.conn.lock();
        let changed = conn
            .execute("DELETE FROM providers WHERE id = ?1", [id])
            .map_err(|e| BackendError::Query(e.to_string()))?;
        drop(conn);
        self.mark_dirty();
        if changed == 0 {
            return Err(BackendError::NotFound {
                entity: "provider",
                id: id.to_string(),
            });
        }
        Ok(())
    }

    async fn get_combos(&self) -> BackendResult<Vec<Combo>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT payload FROM combos ORDER BY name")
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| BackendError::Query(e.to_string()))?;
            let combo: Combo = serde_json::from_str(&payload)
                .map_err(|e| BackendError::Invalid(format!("combo row: {e}")))?;
            out.push(combo);
        }
        Ok(out)
    }

    async fn save_combo(&self, combo: &Combo) -> BackendResult<()> {
        let mut combo = combo.clone();
        let now = now_secs();
        if combo.created_at.is_none() {
            combo.created_at = Some(now);
        }
        combo.updated_at = Some(now);
        let payload = serde_json::to_string(&combo)
            .map_err(|e| BackendError::Invalid(format!("encode combo: {e}")))?;
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO combos (name, payload) VALUES (?1, ?2)
             ON CONFLICT(name) DO UPDATE SET payload = ?2",
            rusqlite::params![combo.name, payload],
        )
        .map_err(|e| BackendError::Query(e.to_string()))?;
        drop(conn);
        self.mark_dirty();
        Ok(())
    }

    async fn delete_combo(&self, id: &str) -> BackendResult<()> {
        let conn = self.conn.lock();
        let changed = conn
            .execute("DELETE FROM combos WHERE name = ?1", [id])
            .map_err(|e| BackendError::Query(e.to_string()))?;
        drop(conn);
        self.mark_dirty();
        if changed == 0 {
            return Err(BackendError::NotFound {
                entity: "combo",
                id: id.to_string(),
            });
        }
        Ok(())
    }

    async fn get_aliases(&self) -> BackendResult<HashMap<String, String>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT alias, target FROM aliases")
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let mut out = HashMap::new();
        for row in rows {
            let (alias, target) = row.map_err(|e| BackendError::Query(e.to_string()))?;
            out.insert(alias, target);
        }
        Ok(out)
    }

    async fn save_alias(&self, alias: &str, target: &str) -> BackendResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO aliases (alias, target) VALUES (?1, ?2)
             ON CONFLICT(alias) DO UPDATE SET target = ?2",
            rusqlite::params![alias, target],
        )
        .map_err(|e| BackendError::Query(e.to_string()))?;
        drop(conn);
        self.mark_dirty();
        Ok(())
    }

    async fn delete_alias(&self, alias: &str) -> BackendResult<bool> {
        let conn = self.conn.lock();
        let changed = conn
            .execute("DELETE FROM aliases WHERE alias = ?1", [alias])
            .map_err(|e| BackendError::Query(e.to_string()))?;
        drop(conn);
        self.mark_dirty();
        Ok(changed > 0)
    }

    async fn get_proxies(&self) -> BackendResult<Vec<ProxyRecord>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare("SELECT payload FROM proxies ORDER BY id")
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| BackendError::Query(e.to_string()))?;
        let mut out = Vec::new();
        for row in rows {
            let payload = row.map_err(|e| BackendError::Query(e.to_string()))?;
            let proxy: ProxyRecord = serde_json::from_str(&payload)
                .map_err(|e| BackendError::Invalid(format!("proxy row: {e}")))?;
            out.push(proxy);
        }
        Ok(out)
    }

    async fn save_proxy(&self, proxy: &ProxyRecord) -> BackendResult<()> {
        let payload = serde_json::to_string(proxy)
            .map_err(|e| BackendError::Invalid(format!("encode proxy: {e}")))?;
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO proxies (id, payload) VALUES (?1, ?2)
             ON CONFLICT(id) DO UPDATE SET payload = ?2",
            rusqlite::params![proxy.id, payload],
        )
        .map_err(|e| BackendError::Query(e.to_string()))?;
        drop(conn);
        self.mark_dirty();
        Ok(())
    }

    async fn delete_proxy(&self, id: &str) -> BackendResult<bool> {
        let conn = self.conn.lock();
        let changed = conn
            .execute("DELETE FROM proxies WHERE id = ?1", [id])
            .map_err(|e| BackendError::Query(e.to_string()))?;
        drop(conn);
        self.mark_dirty();
        Ok(changed > 0)
    }

    async fn record_usage(&self, stat: &UsageRecord) -> BackendResult<u64> {
        // Price before enqueueing so the stored cost matches the rate table at
        // the moment of the request, not whenever the writer gets to it.
        let rates = self.rates().await?;
        let cost = Self::price(stat, &rates, &self.unpriced);
        let mut record = stat.clone();
        record.cost_micros = cost;
        self.mark_dirty();
        self.tx
            .send(Message::Usage(Box::new(record)))
            .await
            .map_err(|_| BackendError::Unavailable("usage writer stopped".into()))?;
        Ok(cost)
    }

    async fn get_usage_metrics(&self) -> BackendResult<UsageMetricsSummary> {
        self.flush().await;
        self.usage_metrics().await
    }

    async fn get_cost_breakdown(&self) -> BackendResult<CostBreakdown> {
        self.flush().await;
        self.cost_breakdown().await
    }

    async fn set_rate(&self, model: &str, rates: Rates) -> BackendResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO rates (model, prompt_micros, completion_micros) VALUES (?1, ?2, ?3)
             ON CONFLICT(model) DO UPDATE SET
                prompt_micros = ?2, completion_micros = ?3",
            rusqlite::params![
                model,
                rates.prompt_micros as i64,
                rates.completion_micros as i64
            ],
        )
        .map_err(|e| BackendError::Query(e.to_string()))?;
        drop(conn);
        self.mark_dirty();
        Ok(())
    }

    async fn sync_to_disk(&self, path: &str) -> BackendResult<()> {
        self.flush().await;
        self.flush_to_disk(path)
    }
}

// ---------------------------------------------------------------- helpers

fn accumulate(
    map: &mut BTreeMap<String, CostLine>,
    key: &str,
    counted: bool,
    prompt: u64,
    completion: u64,
    cost: u64,
    estimated: i64,
) {
    let line = map.entry(key.to_string()).or_insert_with(|| CostLine {
        key: key.to_string(),
        ..Default::default()
    });
    if counted {
        line.requests += 1;
    }
    line.prompt_tokens += prompt;
    line.completion_tokens += completion;
    line.cost_micros += cost;
    if estimated != 0 {
        line.estimated_rows += 1;
    }
}

fn write_usage(conn: &Arc<Mutex<rusqlite::Connection>>, record: &UsageRecord) -> BackendResult<()> {
    let c = conn.lock();
    c.execute(
        "INSERT INTO usage (ts, requested_model, served_model, provider, combo, status,
                            streamed, prompt_tokens, completion_tokens, usage_estimated,
                            latency_ms, failovers, cost_micros, role, cached)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
        rusqlite::params![
            record.ts,
            record.requested_model,
            record.served_model,
            record.provider,
            record.combo,
            record.status as i64,
            record.streamed as i64,
            record.prompt_tokens as i64,
            record.completion_tokens as i64,
            record.usage_estimated as i64,
            record.latency.as_millis() as i64,
            record.failovers as i64,
            record.cost_micros as i64,
            record.role,
            record.cached as i64,
        ],
    )
    .map_err(|e| BackendError::Query(e.to_string()))?;
    Ok(())
}

fn row_to_stored(r: &rusqlite::Row<'_>) -> rusqlite::Result<StoredUsage> {
    let ts_secs: i64 = r.get(1)?;
    let record = UsageRecord {
        requested_model: r.get(2)?,
        served_model: r.get(3)?,
        provider: r.get(4)?,
        combo: r.get(5)?,
        status: r.get::<_, i64>(6)? as u16,
        streamed: r.get::<_, i64>(7)? != 0,
        prompt_tokens: r.get::<_, i64>(8)? as u32,
        completion_tokens: r.get::<_, i64>(9)? as u32,
        usage_estimated: r.get::<_, i64>(10)? != 0,
        latency: Duration::from_millis(r.get::<_, i64>(11)? as u64),
        failovers: r.get::<_, i64>(12)? as u32,
        cost_micros: r.get::<_, i64>(13)? as u64,
        ts: ts_secs,
        role: r.get(14).ok(),
        cached: r.get::<_, i64>(15).unwrap_or(0) != 0,
    };
    Ok(StoredUsage {
        id: r.get(0)?,
        ts_ms: ts_secs.saturating_mul(1000),
        record,
    })
}

/// Nearest-rank percentile over a sorted slice.
fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn sqlite_err(e: rusqlite::Error) -> StorageError {
    StorageError::Sqlite(e.to_string())
}

pub(crate) fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ModelRecord;

    fn temp_db(name: &str) -> (String, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "vibe-router-storage-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        (dir.join("usage.db").to_string_lossy().into_owned(), dir)
    }

    fn record(model: &str) -> UsageRecord {
        UsageRecord {
            requested_model: model.into(),
            served_model: "p/m1".into(),
            provider: "p".into(),
            combo: Some("chain".into()),
            status: 200,
            streamed: false,
            prompt_tokens: 10,
            completion_tokens: 5,
            usage_estimated: false,
            latency: Duration::from_millis(42),
            failovers: 0,
            ..UsageRecord::default()
        }
    }

    #[tokio::test]
    async fn round_trip_preserves_every_field() {
        let (path, dir) = temp_db("roundtrip");
        let store = SqliteStore::open(&path).await.unwrap();
        store.append(record("chain"));
        store.flush().await;
        let rows = store.recent(10).unwrap();
        assert_eq!(rows.len(), 1);
        let r = &rows[0].record;
        assert_eq!(r.requested_model, "chain");
        assert_eq!(r.served_model, "p/m1");
        assert_eq!(r.provider, "p");
        assert_eq!(r.combo.as_deref(), Some("chain"));
        assert_eq!(r.status, 200);
        assert!(!r.streamed);
        assert_eq!((r.prompt_tokens, r.completion_tokens), (10, 5));
        assert!(!r.usage_estimated);
        assert_eq!(r.latency, Duration::from_millis(42));
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn single_usage_write_per_record_usage() {
        let (path, dir) = temp_db("single-write");
        let store = SqliteStore::open(&path).await.unwrap();
        store.record_usage(&record("chain")).await.unwrap();
        store.flush().await;
        assert_eq!(store.recent(10).unwrap().len(), 1);
        assert_eq!(store.totals().unwrap().requests, 1);
        assert_eq!(store.get_usage_metrics().await.unwrap().requests, 1);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn estimated_flag_survives_the_database() {
        let (path, dir) = temp_db("estimated");
        let store = SqliteStore::open(&path).await.unwrap();
        let mut est = record("m");
        est.usage_estimated = true;
        store.append(est);
        store.append(record("m"));
        store.flush().await;
        let rows = store.recent(10).unwrap();
        let estimated = rows.iter().filter(|x| x.record.usage_estimated).count();
        let reported = rows.iter().filter(|x| !x.record.usage_estimated).count();
        assert_eq!((estimated, reported), (1, 1));
        assert_eq!(store.totals().unwrap().estimated_usage_rows, 1);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn totals_aggregate_correctly() {
        let (path, dir) = temp_db("totals");
        let store = SqliteStore::open(&path).await.unwrap();
        for i in 0..3 {
            let mut r = record("m");
            r.status = if i == 2 { 500 } else { 200 };
            r.prompt_tokens = 100;
            r.completion_tokens = 10;
            store.append(r);
        }
        store.flush().await;
        let t = store.totals().unwrap();
        assert_eq!(t.requests, 3);
        assert_eq!(t.succeeded, 2);
        assert_eq!(t.failed, 1);
        assert_eq!(t.prompt_tokens, 300);
        assert_eq!(t.completion_tokens, 30);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn recent_orders_newest_first_and_respects_limit() {
        let (path, dir) = temp_db("order");
        let store = SqliteStore::open(&path).await.unwrap();
        for i in 0..5 {
            store.append(record(&format!("m{i}")));
        }
        store.flush().await;
        let rows = store.recent(2).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].record.requested_model, "m4");
        assert_eq!(rows[1].record.requested_model, "m3");
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn reopen_keeps_history() {
        let (path, dir) = temp_db("reopen");
        {
            let store = SqliteStore::open(&path).await.unwrap();
            store.append(record("m"));
            store.flush().await;
        }
        let store = SqliteStore::open(&path).await.unwrap();
        assert_eq!(store.recent(10).unwrap().len(), 1);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn runtime_combos_round_trip_through_sqlite() {
        let (path, dir) = temp_db("combos");
        let store = SqliteStore::open(&path).await.unwrap();
        store
            .upsert_combo(&RuntimeCombo {
                name: "cheap".into(),
                description: Some("subscription then free".into()),
                models: vec!["a/x".into(), "b/y".into()],
            })
            .unwrap();
        let listed = store.list_combos().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].models, vec!["a/x", "b/y"]);
        assert_eq!(
            listed[0].description.as_deref(),
            Some("subscription then free")
        );

        store
            .upsert_combo(&RuntimeCombo {
                name: "cheap".into(),
                description: None,
                models: vec!["c/z".into()],
            })
            .unwrap();
        let listed = store.list_combos().unwrap();
        assert_eq!(listed.len(), 1, "upsert replaces, never appends");
        assert_eq!(listed[0].models, vec!["c/z"]);
        assert_eq!(listed[0].description, None);

        store.flush().await;
        drop(store);
        let reopened = SqliteStore::open(&path).await.unwrap();
        assert_eq!(reopened.list_combos().unwrap()[0].models, vec!["c/z"]);
        assert!(ComboStore::delete_combo(&reopened, "cheap").unwrap());
        assert!(!ComboStore::delete_combo(&reopened, "cheap").unwrap());
        assert!(reopened.list_combos().unwrap().is_empty());
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn runtime_providers_round_trip_through_sqlite() {
        let (path, dir) = temp_db("providers");
        let store = SqliteStore::open(&path).await.unwrap();
        let provider = RuntimeProvider {
            id: "custom-llm".into(),
            display_name: Some("Custom LLM".into()),
            base_url: "http://localhost:8080/v1".into(),
            format: Some("openai".into()),
            auth_scheme: Some("Bearer".into()),
            auth_secret: Some("env:CUSTOM_KEY".into()),
            api_keys: vec!["sk-key-1".into(), "sk-key-2".into()],
            key_strategy: Some("least-loaded".into()),
            headers: vec![("x-api".into(), "1".into())],
            models: vec![crate::RuntimeModel {
                id: "model-1".into(),
                name: Some("Model 1".into()),
                context: Some(32768),
                max_output: Some(4096),
            }],
            passthrough: true,
        };
        store.upsert_provider(&provider).unwrap();
        let listed = store.list_providers().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, "custom-llm");
        assert_eq!(listed[0].models.len(), 1);
        assert_eq!(listed[0].models[0].id, "model-1");
        assert_eq!(listed[0].api_keys, vec!["sk-key-1", "sk-key-2"]);
        assert_eq!(listed[0].key_strategy.as_deref(), Some("least-loaded"));

        store.flush().await;
        drop(store);
        let reopened = SqliteStore::open(&path).await.unwrap();
        assert_eq!(reopened.list_providers().unwrap().len(), 1);
        assert!(ProviderStore::delete_provider(&reopened, "custom-llm").unwrap());
        assert!(!ProviderStore::delete_provider(&reopened, "custom-llm").unwrap());
        assert!(reopened.list_providers().unwrap().is_empty());
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn unified_schema_serves_both_trait_surfaces() {
        // The sync ComboStore and async StorageBackend must see the same rows:
        // one owner, one schema.
        let store = SqliteStore::open_in_memory().await.unwrap();
        store
            .upsert_combo(&RuntimeCombo {
                name: "shared".into(),
                description: None,
                models: vec!["a/x".into()],
            })
            .unwrap();
        let async_view = store.get_combos().await.unwrap();
        assert_eq!(async_view.len(), 1);
        assert_eq!(async_view[0].name, "shared");

        store
            .save_provider(&Provider {
                models: vec![ModelRecord::new("m")],
                ..Provider::new("a", "https://a.test/v1")
            })
            .await
            .unwrap();
        assert_eq!(store.list_providers().unwrap().len(), 1);
    }
}
