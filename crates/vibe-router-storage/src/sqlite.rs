//! SQLite-backed usage store.
//!
//! `append` hands the record to a writer task over a channel and returns
//! immediately — disk latency never reaches a response. The writer owns the
//! connection (SQLite wants a single writer), and the connection sits behind
//! a mutex shared with readers; each statement is a single row, so the lock
//! is held for microseconds and UI reads queue behind nothing meaningful.
//!
//! WAL mode keeps the *disk* work cheap and lets the writer commit without
//! blocking readers.
//!
//! The `usage_estimated` column is the point of this whole module: an
//! estimate never becomes indistinguishable from a provider report.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::{
    ComboStore, ProviderStore, RuntimeCombo, RuntimeProvider, StorageError, StorageResult,
    StoredUsage, Totals, UsageRecord, UsageStore,
};

pub struct SqliteStore {
    tx: mpsc::UnboundedSender<Message>,
    conn: Arc<parking_lot::Mutex<rusqlite::Connection>>,
    path: String,
}

enum Message {
    Write(UsageRecord),
    /// Reply once every write queued before this message is committed. The
    /// writer task processes in order, so the reply is a true barrier.
    Flush(oneshot::Sender<()>),
}

const SCHEMA: &str = r#"
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
CREATE INDEX IF NOT EXISTS idx_usage_ts ON usage (ts DESC);

CREATE TABLE IF NOT EXISTS combos (
    name        TEXT PRIMARY KEY,
    description TEXT,
    models      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS providers (
    id           TEXT PRIMARY KEY,
    display_name TEXT,
    base_url     TEXT NOT NULL,
    config_json  TEXT NOT NULL
);
"#;

impl SqliteStore {
    pub fn open(path: &str) -> StorageResult<Self> {
        let conn = rusqlite::Connection::open(path).map_err(sqlite_err)?;
        // NORMAL is the standard WAL pairing: a power loss costs at most the
        // last checkpoint. For usage rows that is an accepted trade — the
        // alternative (FULL) fsyncs per insert and shows up in tail latency.
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(sqlite_err)?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(sqlite_err)?;
        conn.execute_batch(SCHEMA).map_err(sqlite_err)?;
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

        let conn = Arc::new(parking_lot::Mutex::new(conn));
        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

        let writer_conn = conn.clone();
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    Message::Write(record) => {
                        if let Err(e) = write_row(&writer_conn, &record) {
                            tracing::warn!("usage write failed: {e}");
                        }
                    }
                    Message::Flush(reply) => {
                        // Nothing to do but acknowledge: every prior Write was
                        // handled synchronously above.
                        let _ = reply.send(());
                    }
                }
            }
        });

        Ok(Self {
            tx,
            conn,
            path: path.to_string(),
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// Wait until every record appended so far is readable. Used by tests and
    /// by graceful shutdown; request handlers never call it.
    pub async fn flush(&self) {
        let (done_tx, done_rx) = oneshot::channel();
        if self.tx.send(Message::Flush(done_tx)).is_err() {
            return; // writer already gone; nothing to wait for
        }
        let _ = done_rx.await;
    }
}

impl UsageStore for SqliteStore {
    fn append(&self, record: UsageRecord) {
        // A closed writer means shutdown is racing us; the request already
        // succeeded, so dropping the row beats failing the caller.
        let _ = self.tx.send(Message::Write(record));
    }

    fn recent(&self, limit: u32) -> StorageResult<Vec<StoredUsage>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, ts, requested_model, served_model, provider, combo,
                        status, streamed, prompt_tokens, completion_tokens,
                        usage_estimated, latency_ms, failovers, cost_micros, role
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
            .prepare_cached("SELECT name, description, models FROM combos ORDER BY name")
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map([], |r| {
                let models_json: String = r.get(2)?;
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    models_json,
                ))
            })
            .map_err(sqlite_err)?;
        rows.map(|row| {
            let (name, description, models_json) = row.map_err(sqlite_err)?;
            let models: Vec<String> = serde_json::from_str(&models_json)
                .map_err(|e| StorageError::Sqlite(format!("corrupt combo `{name}`: {e}")))?;
            Ok(RuntimeCombo {
                name,
                description,
                models,
            })
        })
        .collect()
    }

    fn upsert_combo(&self, combo: &RuntimeCombo) -> StorageResult<()> {
        let conn = self.conn.lock();
        conn.execute(
            "INSERT INTO combos (name, description, models) VALUES (?1, ?2, ?3)
             ON CONFLICT(name) DO UPDATE SET description = ?2, models = ?3",
            rusqlite::params![
                combo.name,
                combo.description,
                serde_json::to_string(&combo.models)
                    .map_err(|e| { StorageError::Sqlite(format!("cannot encode combo: {e}")) })?
            ],
        )
        .map_err(sqlite_err)?;
        Ok(())
    }

    fn delete_combo(&self, name: &str) -> StorageResult<bool> {
        let conn = self.conn.lock();
        let changed = conn
            .execute("DELETE FROM combos WHERE name = ?1", [name])
            .map_err(sqlite_err)?;
        Ok(changed > 0)
    }
}

impl ProviderStore for SqliteStore {
    fn list_providers(&self) -> StorageResult<Vec<RuntimeProvider>> {
        let conn = self.conn.lock();
        let mut stmt = conn
            .prepare_cached("SELECT config_json FROM providers ORDER BY id")
            .map_err(sqlite_err)?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(sqlite_err)?;
        rows.map(|row| {
            let config_json = row.map_err(sqlite_err)?;
            serde_json::from_str(&config_json)
                .map_err(|e| StorageError::Sqlite(format!("corrupt provider: {e}")))
        })
        .collect()
    }

    fn upsert_provider(&self, provider: &RuntimeProvider) -> StorageResult<()> {
        let conn = self.conn.lock();
        let json = serde_json::to_string(provider)
            .map_err(|e| StorageError::Sqlite(format!("cannot encode provider: {e}")))?;
        conn.execute(
            "INSERT INTO providers (id, display_name, base_url, config_json) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(id) DO UPDATE SET display_name = ?2, base_url = ?3, config_json = ?4",
            rusqlite::params![provider.id, provider.display_name, provider.base_url, json],
        )
        .map_err(sqlite_err)?;
        Ok(())
    }

    fn delete_provider(&self, id: &str) -> StorageResult<bool> {
        let conn = self.conn.lock();
        let changed = conn
            .execute("DELETE FROM providers WHERE id = ?1", [id])
            .map_err(sqlite_err)?;
        Ok(changed > 0)
    }
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
    };
    Ok(StoredUsage {
        id: r.get(0)?,
        ts_ms: ts_secs.saturating_mul(1000),
        record,
    })
}

fn write_row(
    conn: &Arc<parking_lot::Mutex<rusqlite::Connection>>,
    record: &UsageRecord,
) -> StorageResult<()> {
    let c = conn.lock();
    c.execute(
        "INSERT INTO usage (ts, requested_model, served_model, provider, combo,
                            status, streamed, prompt_tokens, completion_tokens,
                            usage_estimated, latency_ms, failovers, cost_micros, role)
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
    .map_err(sqlite_err)?;
    Ok(())
}

fn sqlite_err(e: rusqlite::Error) -> StorageError {
    StorageError::Sqlite(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let store = SqliteStore::open(&path).unwrap();
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
    async fn estimated_flag_survives_the_database() {
        let (path, dir) = temp_db("estimated");
        let store = SqliteStore::open(&path).unwrap();
        let mut est = record("m");
        est.usage_estimated = true;
        store.append(est);
        store.append(record("m"));
        store.flush().await;
        let rows = store.recent(10).unwrap();
        let estimated = rows.iter().filter(|x| x.record.usage_estimated).count();
        let reported = rows.iter().filter(|x| !x.record.usage_estimated).count();
        assert_eq!(
            (estimated, reported),
            (1, 1),
            "an estimate must never look like a report"
        );
        assert_eq!(store.totals().unwrap().estimated_usage_rows, 1);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn totals_aggregate_correctly() {
        let (path, dir) = temp_db("totals");
        let store = SqliteStore::open(&path).unwrap();
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
        let store = SqliteStore::open(&path).unwrap();
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
            let store = SqliteStore::open(&path).unwrap();
            store.append(record("m"));
            store.flush().await;
        }
        let store = SqliteStore::open(&path).unwrap();
        assert_eq!(store.recent(10).unwrap().len(), 1);
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn open_empty_path_selects_memory_backend() {
        let s = crate::open("").unwrap();
        s.append(record("m"));
        // Memory backend: rows live in RAM until restart.
        assert_eq!(s.totals().unwrap().requests, 1);
    }

    #[tokio::test]
    async fn runtime_combos_round_trip_through_sqlite() {
        let (path, dir) = temp_db("combos");
        let store = SqliteStore::open(&path).unwrap();
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

        // Replace by name, then delete; reopen to prove persistence.
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
        let reopened = SqliteStore::open(&path).unwrap();
        assert_eq!(reopened.list_combos().unwrap()[0].models, vec!["c/z"]);
        assert!(reopened.delete_combo("cheap").unwrap());
        assert!(!reopened.delete_combo("cheap").unwrap());
        assert!(reopened.list_combos().unwrap().is_empty());
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn runtime_providers_round_trip_through_sqlite() {
        let (path, dir) = temp_db("providers");
        let store = SqliteStore::open(&path).unwrap();
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
        // The multi-key pool survives the round trip.
        assert_eq!(listed[0].api_keys, vec!["sk-key-1", "sk-key-2"]);
        assert_eq!(listed[0].key_strategy.as_deref(), Some("least-loaded"));

        store.flush().await;
        drop(store);
        let reopened = SqliteStore::open(&path).unwrap();
        assert_eq!(reopened.list_providers().unwrap().len(), 1);
        assert!(reopened.delete_provider("custom-llm").unwrap());
        assert!(reopened.list_providers().unwrap().is_empty());
        std::fs::remove_dir_all(dir).ok();
    }
}
