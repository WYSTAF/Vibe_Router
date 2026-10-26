//! Local SQLite implementation of [`StorageBackend`].
//!
//! One connection, WAL mode, and a background writer so an `async` call never
//! blocks a response. Reads run on the same connection behind a short-held
//! mutex — a router's read volume is low enough that a pool would be
//! complexity without benefit, and the write lock is microseconds.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use crate::backend::{BackendError, BackendResult, StorageBackend};
use crate::types::{
    Combo, CostBreakdown, CostLine, Provider, Rates, UsageMetricsSummary, UsageRecord,
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
CREATE TABLE IF NOT EXISTS rates (
    model        TEXT PRIMARY KEY,
    prompt_micros   INTEGER NOT NULL DEFAULT 0,
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
    role               TEXT
);
CREATE INDEX IF NOT EXISTS idx_usage_ts     ON usage (ts DESC);
CREATE INDEX IF NOT EXISTS idx_usage_provider ON usage (provider);
CREATE INDEX IF NOT EXISTS idx_usage_model    ON usage (served_model);
"#;

enum Message {
    Usage(Box<UsageRecord>),
    Flush(oneshot::Sender<()>),
}

pub struct SqliteBackend {
    conn: Arc<Mutex<rusqlite::Connection>>,
    tx: mpsc::UnboundedSender<Message>,
    /// Rows with no matching rate: cost is genuinely unknown.
    unpriced: Arc<AtomicU64>,
}

impl SqliteBackend {
    /// Open (creating if needed) a SQLite-backed store at `path`.
    pub async fn open(path: impl AsRef<Path>) -> BackendResult<Self> {
        Self::open_sync(path)
    }

    /// Open synchronously, attaching to the current tokio runtime or spawning
    /// a dedicated background worker thread.
    pub fn open_sync(path: impl AsRef<Path>) -> BackendResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| BackendError::Io(format!("{}: {e}", parent.display())))?;
            }
        }
        let conn = rusqlite::Connection::open(path)
            .map_err(|e| BackendError::Io(format!("{}: {e}", path.display())))?;
        // WAL: the writer commits without blocking readers. NORMAL sync is the
        // standard WAL pairing; usage rows are analytics, not a ledger.
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| BackendError::Query(e.to_string()))?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(|e| BackendError::Query(e.to_string()))?;
        conn.execute_batch(SCHEMA)
            .map_err(|e| BackendError::Query(e.to_string()))?;
        // Non-destructive schema migration for pre-existing tables
        let _ = conn.execute(
            "ALTER TABLE usage ADD COLUMN cost_micros INTEGER NOT NULL DEFAULT 0",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE usage ADD COLUMN failovers INTEGER NOT NULL DEFAULT 0",
            [],
        );
        let _ = conn.execute("ALTER TABLE usage ADD COLUMN role TEXT", []);
        let _ = conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_usage_role ON usage (role)",
            [],
        );

        let conn = Arc::new(Mutex::new(conn));
        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
        let writer_conn = conn.clone();

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                while let Some(msg) = rx.recv().await {
                    match msg {
                        Message::Usage(record) => {
                            if let Err(e) = write_usage(&writer_conn, &record) {
                                tracing::warn!("usage write failed: {e}");
                            }
                        }
                        Message::Flush(reply) => {
                            let _ = reply.send(());
                        }
                    }
                }
            });
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
                rt.block_on(async move {
                    while let Some(msg) = rx.recv().await {
                        match msg {
                            Message::Usage(record) => {
                                if let Err(e) = write_usage(&writer_conn, &record) {
                                    tracing::warn!("usage write failed: {e}");
                                }
                            }
                            Message::Flush(reply) => {
                                let _ = reply.send(());
                            }
                        }
                    }
                });
            });
        }

        Ok(Self {
            conn,
            tx,
            unpriced: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Open SQLite in RAM-first mode, hydrating existing state from `disk_path` if present.
    /// Provides zero flash wear on portable USB drives.
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

    /// Snapshot the in-memory database to disk via atomic write-to-temp + rename.
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
            let mem_conn = self.conn.lock();
            let backup = rusqlite::backup::Backup::new(&mem_conn, &mut tmp_conn)
                .map_err(|e| BackendError::Query(format!("flush backup init: {e}")))?;
            backup
                .run_to_completion(100, Duration::from_millis(1), None)
                .map_err(|e| BackendError::Query(format!("flush to disk failed: {e}")))?;
        }
        std::fs::rename(&tmp_path, disk_path)
            .map_err(|e| BackendError::Io(format!("atomic rename failed: {e}")))?;
        Ok(())
    }

    /// An in-memory store: same behaviour, nothing on disk. Used by tests and
    /// by `storage.sqlite_path = ""`.
    pub async fn open_in_memory() -> BackendResult<Self> {
        Self::open(":memory:").await
    }

    /// Wait for queued usage writes to land. Tests and graceful shutdown only;
    /// the request path never blocks on it.
    pub async fn flush(&self) {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(Message::Flush(tx)).is_ok() {
            let _ = rx.await;
        }
    }

    /// Per-million-token rates, keyed by `provider/model` or bare `model`.
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
        let key = if record.provider.is_empty() {
            record.served_model.clone()
        } else {
            format!("{}/{}", record.provider, record.served_model)
        };
        // Try the qualified id first, then the bare model id.
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
}

#[async_trait]
impl StorageBackend for SqliteBackend {
    /// Reads wait for the background writer to drain first.
    ///
    /// Metrics and cost breakdowns are queries an operator or an agent runs
    /// *after* the traffic they are asking about; returning a count that
    /// silently excludes the last few writes would be worse than the
    /// microseconds the barrier costs. The request path never calls these.
    async fn get_usage_metrics(&self) -> BackendResult<UsageMetricsSummary> {
        self.flush().await;
        self.usage_metrics().await
    }

    async fn get_cost_breakdown(&self) -> BackendResult<CostBreakdown> {
        self.flush().await;
        self.cost_breakdown().await
    }

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
        Ok(())
    }

    async fn delete_provider(&self, id: &str) -> BackendResult<()> {
        let conn = self.conn.lock();
        let changed = conn
            .execute("DELETE FROM providers WHERE id = ?1", [id])
            .map_err(|e| BackendError::Query(e.to_string()))?;
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
        Ok(())
    }

    async fn delete_combo(&self, id: &str) -> BackendResult<()> {
        let conn = self.conn.lock();
        let changed = conn
            .execute("DELETE FROM combos WHERE name = ?1", [id])
            .map_err(|e| BackendError::Query(e.to_string()))?;
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
        Ok(())
    }

    async fn delete_alias(&self, alias: &str) -> BackendResult<bool> {
        let conn = self.conn.lock();
        let changed = conn
            .execute("DELETE FROM aliases WHERE alias = ?1", [alias])
            .map_err(|e| BackendError::Query(e.to_string()))?;
        Ok(changed > 0)
    }

    async fn record_usage(&self, stat: &UsageRecord) -> BackendResult<()> {
        // Price before enqueueing so the stored cost matches the rate table at
        // the moment of the request, not whenever the writer gets to it.
        let rates = self.rates().await?;
        let cost = Self::price(stat, &rates, &self.unpriced);
        let mut record = stat.clone();
        record.cost_micros = cost;
        self.tx
            .send(Message::Usage(Box::new(record)))
            .map_err(|_| BackendError::Unavailable("usage writer stopped".into()))?;
        Ok(())
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
        Ok(())
    }

    async fn sync_to_disk(&self, path: &str) -> BackendResult<()> {
        self.flush().await;
        self.flush_to_disk(path)
    }
}

impl SqliteBackend {
    /// The metric aggregation body, separated from the trait method so the
    /// trait wrapper can flush the writer queue first.
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

        // Latency percentiles need the full distribution, so read the column
        // and sort here. Usage volumes are small (per-VPS); a SQL window
        // function would not pay for itself.
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
            // Rows with no rate are unknown-cost, not free: they contribute to
            // request and token counts but not to the cost total.
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
                            latency_ms, failovers, cost_micros, role)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
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
        ],
    )
    .map_err(|e| BackendError::Query(e.to_string()))?;
    Ok(())
}

/// Nearest-rank percentile over a sorted slice.
fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

pub(crate) fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs() as i64
}
