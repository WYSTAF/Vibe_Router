//! Tests for the async [`StorageBackend`] contract.
//!
//! These exercise the *trait*, not SQLite's implementation quirks, so the same
//! suite will pass unchanged against a PostgreSQL backend: they only use the
//! trait methods. That is the point of the abstraction.

use std::collections::HashMap;

use vibe_router_storage::{
    BackendError, Combo, ModelRecord, Provider, Rates, StorageBackend, UsageRecord,
};

/// Run the shared contract against any backend. Only trait methods are used,
/// so the same suite must pass against a future PostgreSQL backend unchanged.
async fn contract_suite<B: StorageBackend>(b: &B) {
    // -- providers ---------------------------------------------------------
    let mut p = Provider::new("alpha", "https://alpha.test/v1/chat/completions");
    p.api_keys = vec!["k1".into(), "k2".into()];
    p.key_strategy = Some("least-loaded".into());
    p.models = vec![ModelRecord::new("m1")];
    b.save_provider(&p).await.unwrap();

    let got = b.get_providers().await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].id, "alpha");
    assert_eq!(got[0].api_keys, vec!["k1", "k2"]);
    assert_eq!(got[0].key_strategy.as_deref(), Some("least-loaded"));
    assert_eq!(got[0].models[0].id, "m1");
    assert!(got[0].enabled, "providers are enabled by default");

    // Saving the same id replaces rather than duplicates.
    let mut p2 = p.clone();
    p2.base_url = "https://alpha2.test/v1".into();
    b.save_provider(&p2).await.unwrap();
    let got = b.get_providers().await.unwrap();
    assert_eq!(got.len(), 1, "upsert must replace, not append");
    assert_eq!(got[0].base_url, "https://alpha2.test/v1");

    b.delete_provider("alpha").await.unwrap();
    assert!(b.get_providers().await.unwrap().is_empty());
    // Deleting a missing row is an error, not a silent success.
    let err = b.delete_provider("ghost").await.unwrap_err();
    assert!(matches!(
        err,
        BackendError::NotFound {
            entity: "provider",
            ..
        }
    ));

    // -- combos ------------------------------------------------------------
    let c = Combo::new("chain", vec!["alpha/m1".into(), "beta/m2".into()]);
    b.save_combo(&c).await.unwrap();
    let combos = b.get_combos().await.unwrap();
    assert_eq!(combos.len(), 1);
    assert_eq!(combos[0].name, "chain");
    assert_eq!(combos[0].models.len(), 2);
    assert!(combos[0].created_at.is_some(), "created_at is stamped");
    assert!(combos[0].updated_at.is_some(), "updated_at is stamped");

    b.delete_combo("chain").await.unwrap();
    assert!(b.get_combos().await.unwrap().is_empty());
    assert!(matches!(
        b.delete_combo("chain").await.unwrap_err(),
        BackendError::NotFound {
            entity: "combo",
            ..
        }
    ));

    // -- aliases -----------------------------------------------------------
    assert!(b.get_aliases().await.unwrap().is_empty());
    b.save_alias("fast", "alpha/m1").await.unwrap();
    b.save_alias("smart", "alpha/m1").await.unwrap();
    let aliases: HashMap<String, String> = b.get_aliases().await.unwrap();
    assert_eq!(aliases.len(), 2);
    assert_eq!(aliases.get("fast").unwrap(), "alpha/m1");

    // Re-saving an alias updates the target in place.
    b.save_alias("fast", "alpha/m2").await.unwrap();
    let aliases = b.get_aliases().await.unwrap();
    assert_eq!(aliases.get("fast").unwrap(), "alpha/m2");
    assert_eq!(aliases.len(), 2, "re-saving must not duplicate");

    // Deleting an alias
    assert!(b.delete_alias("smart").await.unwrap());
    assert!(!b.delete_alias("smart").await.unwrap(), "already deleted");
    let aliases = b.get_aliases().await.unwrap();
    assert_eq!(aliases.len(), 1);
    assert!(!aliases.contains_key("smart"));

    // -- usage + cost ------------------------------------------------------
    let mut metrics = b.get_usage_metrics().await.unwrap();
    assert_eq!(metrics.requests, 0);
    assert_eq!(metrics.total_cost_micros, 0);

    // One priced request: 1M prompt at $1/M, 1M completion at $2/M => $3.
    b.set_rate(
        "alpha/m1",
        Rates {
            prompt_micros: 1_000_000,
            completion_micros: 2_000_000,
        },
    )
    .await
    .unwrap();
    b.record_usage(&UsageRecord {
        requested_model: "alpha/m1".into(),
        served_model: "m1".into(),
        provider: "alpha".into(),
        prompt_tokens: 1_000_000,
        completion_tokens: 1_000_000,
        latency: std::time::Duration::from_millis(120),
        ..Default::default()
    })
    .await
    .unwrap();

    // An unpriced request: counted, but honestly unknown-cost.
    b.record_usage(&UsageRecord {
        requested_model: "ghost/m".into(),
        served_model: "m".into(),
        provider: "ghost".into(),
        prompt_tokens: 10,
        completion_tokens: 20,
        ..Default::default()
    })
    .await
    .unwrap();

    metrics = b.get_usage_metrics().await.unwrap();
    assert_eq!(metrics.requests, 2);
    assert_eq!(metrics.succeeded, 2);
    assert_eq!(metrics.prompt_tokens, 1_000_010);
    assert_eq!(metrics.completion_tokens, 1_000_020);
    assert_eq!(metrics.total_cost_micros, 3_000_000, "priced request only");
    assert_eq!(metrics.avg_latency_ms, 60);

    let breakdown = b.get_cost_breakdown().await.unwrap();
    assert_eq!(breakdown.total_micros, 3_000_000);
    assert_eq!(breakdown.by_provider["alpha"].cost_micros, 3_000_000);
    assert_eq!(breakdown.by_provider["ghost"].cost_micros, 0);
    assert!(
        breakdown.unpriced_rows >= 1,
        "an unpriced model must be reported unpriced, never free"
    );

    // Percentiles over a small distribution.
    for i in 0..10u64 {
        b.record_usage(&UsageRecord {
            requested_model: "p".into(),
            served_model: "m".into(),
            provider: "p".into(),
            latency: std::time::Duration::from_millis(i * 10),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    let m = b.get_usage_metrics().await.unwrap();
    assert_eq!(m.requests, 12);
    // Percentiles must be monotonic and bounded by the slowest row. The
    // earlier rows (60ms, 120ms) are part of the distribution, so the p99
    // ceiling is the slowest sample, not the 0..90ms batch alone.
    assert!(m.p50_latency_ms <= m.p95_latency_ms);
    assert!(m.p95_latency_ms <= m.p99_latency_ms);
    assert!(m.p99_latency_ms <= 120, "p99 cannot exceed the max sample");
    assert!(m.avg_latency_ms <= 120);
}

/// Test-only helper: the pricing hook lives on the concrete SQLite backend.
#[tokio::test]
async fn sqlite_satisfies_the_backend_contract() {
    let b = vibe_router_storage::SqliteAsyncBackend::open_in_memory()
        .await
        .unwrap();
    contract_suite(&b).await;
}

#[tokio::test]
async fn sqlite_backend_persists_across_reopen() {
    let dir = std::env::temp_dir().join(format!("vibe-router-backend-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("usage.db");

    {
        let b = vibe_router_storage::SqliteAsyncBackend::open(&path)
            .await
            .unwrap();
        let mut p = Provider::new("persisted", "https://p.test/v1");
        p.api_keys = vec!["a".into(), "b".into()];
        b.save_provider(&p).await.unwrap();
        b.save_combo(&Combo::new("c", vec!["persisted/m".into()]))
            .await
            .unwrap();
        b.save_alias("p", "persisted/m").await.unwrap();
        b.flush().await;
    }

    // Reopen: everything a restart must preserve is still there.
    let b = vibe_router_storage::SqliteAsyncBackend::open(&path)
        .await
        .unwrap();
    assert_eq!(b.get_providers().await.unwrap().len(), 1);
    assert_eq!(b.get_providers().await.unwrap()[0].api_keys.len(), 2);
    assert_eq!(b.get_combos().await.unwrap().len(), 1);
    assert_eq!(
        b.get_aliases().await.unwrap().get("p").unwrap(),
        "persisted/m"
    );

    std::fs::remove_dir_all(dir).ok();
}

#[tokio::test]
async fn corrupt_row_is_an_error_not_a_silent_empty_result() {
    // A row that is not valid JSON must surface as `Invalid`, not vanish.
    let dir = std::env::temp_dir().join(format!("vibe-router-corrupt-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("usage.db");
    {
        let b = vibe_router_storage::SqliteAsyncBackend::open(&path)
            .await
            .unwrap();
        b.flush().await;
    }
    // Corrupt a payload directly.
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            "INSERT INTO providers (id, payload) VALUES ('bad', 'not-json')",
            [],
        )
        .unwrap();
    }
    let b = vibe_router_storage::SqliteAsyncBackend::open(&path)
        .await
        .unwrap();
    let err = b.get_providers().await.unwrap_err();
    assert!(
        matches!(err, BackendError::Invalid(_)),
        "expected Invalid, got {err:?}"
    );
    std::fs::remove_dir_all(dir).ok();
}
