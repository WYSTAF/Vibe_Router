//! File watching and hot reload.
//!
//! The config file is watched for changes and the new version is re-applied
//! without dropping in-flight requests: callers read through a cheap clone of
//! an `Arc<AppConfig>`, so a swap is atomic from the request path's point of
//! view. A failed reload is logged and the last good config keeps serving.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::{AppConfig, ConfigError, ConfigResult};

/// A reloadable handle to configuration.
#[derive(Clone)]
pub struct ConfigSource {
    inner: Arc<RwLock<Arc<AppConfig>>>,
    path: Option<PathBuf>,
}

impl ConfigSource {
    /// Load from a TOML file and keep watching it.
    pub fn load(path: &str) -> ConfigResult<Self> {
        let cfg = AppConfig::load(path)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(cfg))),
            path: Some(PathBuf::from(path)),
        })
    }

    /// Wrap an already-loaded config; changes are not watched.
    pub fn from_config(cfg: AppConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Arc::new(cfg))),
            path: None,
        }
    }

    /// Read the current config. Cheap: clones only an `Arc`.
    pub fn get(&self) -> Arc<AppConfig> {
        self.inner.read().clone()
    }

    /// Where the config came from, if a file.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Reload from disk, replacing the live config if the new file is valid.
    /// Returns the parsed result; on failure the previous config is untouched.
    pub fn reload(&self) -> ConfigResult<Arc<AppConfig>> {
        let path = self
            .path
            .as_ref()
            .ok_or_else(|| ConfigError::invalid("config has no file source"))?;
        let cfg = AppConfig::load(path.to_str().unwrap_or(""))?;
        let next = Arc::new(cfg);
        *self.inner.write() = next.clone();
        Ok(next)
    }
}

/// Spawn a background task that reloads the config when the file changes.
///
/// No-op when the config was not loaded from a file. Errors are logged and
/// swallowed on purpose: a bad edit must not take a running server down.
#[cfg(feature = "watch")]
pub fn watch(source: ConfigSource, mut rx: tokio::sync::mpsc::Receiver<()>) {
    let path = source.path().map(|p| p.display().to_string());
    tokio::spawn(async move {
        while rx.recv().await.is_some() {
            match source.reload() {
                Ok(_cfg) => tracing::info!("config reloaded from {path:?}"),
                Err(e) => tracing::warn!("config reload failed (keeping previous): {e}"),
            }
        }
    });
}
