//! The HTTP server: OpenAI-compatible `/v1/*`, the dashboard's `/api/*`,
//! and the embedded UI — one port, one process.
//!
//! `serve` binds, installs the graceful-shutdown listener, and returns the
//! bound socket address before the router starts accepting, so the CLI (and
//! integration tests) can print — or connect to — a real endpoint without
//! sleeping against a race.
//!
//! Depends on: everything below it, and nothing above it.

#![forbid(unsafe_code)]

pub mod assignment;
pub mod error;
pub mod routes;
pub mod sse;
pub mod state;

use std::sync::Arc;

use state::AppState;

pub use state::{AppRouter, Auth};

/// Bind and serve until the process is asked to stop.
///
/// The returned `SocketAddr` is live before the first accept: callers can
/// point clients at it immediately. Shutdown is Ctrl-C / SIGTERM; in-flight
/// requests finish first (axum drains open connections on graceful shutdown).
pub async fn serve(
    state: Arc<AppState>,
    bind: &str,
) -> Result<std::net::SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let app = routes::router(state.clone());
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let addr = listener.local_addr()?;

    let shutdown = async {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler");
            tokio::select! {
                _ = ctrl_c => tracing::info!("ctrl-c received, shutting down"),
                _ = term.recv() => tracing::info!("SIGTERM received, shutting down"),
            }
        }
        #[cfg(not(unix))]
        {
            tokio::select! {
                _ = ctrl_c => tracing::info!("ctrl-c received, shutting down"),
            }
        }
    };

    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown);
    tokio::spawn(async move {
        if let Err(e) = server.await {
            tracing::error!("server error: {e}");
        }
    });
    Ok(addr)
}
