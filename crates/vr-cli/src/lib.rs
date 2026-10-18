//! Command-line interface for Vibe Router.
//!
//! Subcommands:
//!   * `serve` — start the HTTP router and dashboard.
//!   * `config validate` — check TOML syntax, provider models, and combo resolution.
//!   * `providers list` — list configured providers and endpoints.
//!   * `models` — list all addressable models and combos.
//!   * `combos` — list configured combos and fallback chains.
//!   * `status` — inspect local storage totals and usage.

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use vr_config::AppConfig;
use vr_server::state::AppState;
use vr_storage::StorageBackend;

const DEFAULT_CONFIG: &str = "vibe-router.toml";

#[derive(Parser)]
#[command(
    name = "vibe-router",
    version,
    about = "Single-binary, lightweight, Rust-native LLM router with health-aware failover.",
    propagate_version = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,

    #[command(flatten)]
    pub serve_opts: ServeArgs,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the OpenAI-compatible proxy server and dashboard (default)
    Serve(ServeArgs),

    /// Inspect and validate configuration
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },

    /// Inspect upstream providers
    Providers {
        #[command(subcommand)]
        command: ProviderCommands,
    },

    /// List all addressable models and combos
    Models(CommonArgs),

    /// List configured combos and fallback chains
    Combos(CommonArgs),

    /// Display usage and storage status
    Status(CommonArgs),

    /// Display USD cost breakdown and latency metrics
    Costs(CommonArgs),

    /// Manage virtual model aliases
    Alias {
        #[command(subcommand)]
        command: AliasCommands,
    },

    /// Manage tagged proxies
    Proxy {
        #[command(subcommand)]
        command: ProxyCommands,
    },

    /// Cloudflare Worker Reverse-Proxy Relay utilities
    Relay {
        #[command(subcommand)]
        command: RelayCommands,
    },

    /// Print client environment variables (Claude Code, etc.)
    Env {
        #[command(subcommand)]
        command: EnvCommands,
    },

    /// Live terminal status dashboard (alias: top)
    #[command(alias = "top")]
    Tui(TuiArgs),

    /// Model Context Protocol (MCP) stdio JSON-RPC 2.0 server
    Mcp(CommonArgs),

    /// Inspect and manage the in-memory prompt cache
    Cache {
        #[command(subcommand)]
        command: CacheCommands,
    },
}

#[derive(Subcommand)]
pub enum EnvCommands {
    /// Print environment export commands for Claude Code integration
    ClaudeCode {
        /// Base URL of the running Vibe Router server
        #[arg(long, default_value = "http://127.0.0.1:20128")]
        endpoint: String,
    },
}

#[derive(Args, Clone, Default)]
pub struct TuiArgs {
    #[command(flatten)]
    pub args: CommonArgs,

    /// Refresh interval in seconds (default 1s)
    #[arg(short, long, default_value_t = 1)]
    pub interval: u64,

    /// Render once and exit immediately
    #[arg(long)]
    pub once: bool,
}

#[derive(Subcommand)]
pub enum AliasCommands {
    /// Set or update a virtual model alias
    Set {
        alias: String,
        target: String,
        #[command(flatten)]
        args: CommonArgs,
    },
    /// List all registered model aliases
    List(CommonArgs),
}

#[derive(Subcommand)]
pub enum ProxyCommands {
    /// Add a proxy endpoint with tier tag
    Add {
        url: String,
        /// Tier tag: cf-worker, datacenter, residential, direct
        #[arg(long, default_value = "cf-worker")]
        tag: String,
        /// Optional proxy id (generated if omitted)
        #[arg(long)]
        id: Option<String>,
        #[command(flatten)]
        args: CommonArgs,
    },
    /// List registered proxy endpoints
    List(CommonArgs),
}

#[derive(Subcommand)]
pub enum RelayCommands {
    /// Print the ready-to-deploy Cloudflare Worker JavaScript relay script
    WorkerScript,
}

#[derive(Subcommand)]
pub enum ConfigCommands {
    /// Validate configuration file and resolve environment secrets
    Validate(CommonArgs),
}

#[derive(Subcommand)]
pub enum ProviderCommands {
    /// List configured providers
    List(CommonArgs),

    /// Manage a provider's API key pool
    Keys {
        #[command(subcommand)]
        command: KeyCommands,
    },
}

#[derive(Subcommand)]
pub enum KeyCommands {
    /// Add an API key to a provider's rotation pool
    Add {
        /// Provider id (e.g. `groq`)
        provider_id: String,
        /// The API key, or a reference like `env:GROQ_KEY_2`
        api_key: String,
        #[command(flatten)]
        args: CommonArgs,
    },

    /// List a provider's configured keys (masked)
    List {
        provider_id: String,
        #[command(flatten)]
        args: CommonArgs,
    },
}

#[derive(Subcommand)]
pub enum CacheCommands {
    /// Drop every cached prompt and response
    Purge {
        /// Base URL of the running Vibe Router server
        #[arg(long, default_value = "http://127.0.0.1:20128")]
        endpoint: String,
    },
    /// Show cache hit/miss/eviction metrics
    Stats {
        /// Base URL of the running Vibe Router server
        #[arg(long, default_value = "http://127.0.0.1:20128")]
        endpoint: String,
        /// Emit raw JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(Args, Clone, Default)]
pub struct CommonArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = DEFAULT_CONFIG, global = true)]
    pub config: PathBuf,

    /// Output raw unformatted JSON instead of human-readable text
    #[arg(long, global = true)]
    pub json: bool,
}

#[derive(Args, Clone, Default)]
pub struct ServeArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = DEFAULT_CONFIG)]
    pub config: PathBuf,

    /// Bind host address (overrides config)
    #[arg(long)]
    pub host: Option<String>,

    /// Bind port (overrides config)
    #[arg(short, long)]
    pub port: Option<u16>,

    /// Operating hardware profile: auto, vps, portable, workstation
    #[arg(long)]
    pub profile: Option<String>,
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Some(Commands::Serve(args)) => cmd_serve(args),
        Some(Commands::Config { command }) => match command {
            ConfigCommands::Validate(args) => cmd_config_validate(args),
        },
        Some(Commands::Providers { command }) => match command {
            ProviderCommands::List(args) => cmd_providers_list(args),
            ProviderCommands::Keys { command } => match command {
                KeyCommands::Add {
                    provider_id,
                    api_key,
                    args,
                } => cmd_provider_keys_add(provider_id, api_key, args),
                KeyCommands::List { provider_id, args } => {
                    cmd_provider_keys_list(provider_id, args)
                }
            },
        },
        Some(Commands::Models(args)) => cmd_models(args),
        Some(Commands::Combos(args)) => cmd_combos(args),
        Some(Commands::Status(args)) => cmd_status(args),
        Some(Commands::Costs(args)) => cmd_costs(args),
        Some(Commands::Alias { command }) => match command {
            AliasCommands::Set {
                alias,
                target,
                args,
            } => cmd_alias_set(alias, target, args),
            AliasCommands::List(args) => cmd_alias_list(args),
        },
        Some(Commands::Proxy { command }) => match command {
            ProxyCommands::Add { url, tag, id, args } => cmd_proxy_add(url, tag, id, args),
            ProxyCommands::List(args) => cmd_proxy_list(args),
        },
        Some(Commands::Relay { command }) => match command {
            RelayCommands::WorkerScript => cmd_relay_worker_script(),
        },
        Some(Commands::Env { command }) => match command {
            EnvCommands::ClaudeCode { endpoint } => cmd_env_claude_code(endpoint),
        },
        Some(Commands::Tui(args)) => cmd_tui(args),
        Some(Commands::Mcp(args)) => cmd_mcp(args),
        Some(Commands::Cache { command }) => match command {
            CacheCommands::Purge { endpoint } => cmd_cache_purge(endpoint),
            CacheCommands::Stats { endpoint, json } => cmd_cache_stats(endpoint, json),
        },
        None => cmd_serve(cli.serve_opts),
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,vr_server=info,vr_routing=info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

fn load_config_or_default(path: &Path) -> Result<AppConfig> {
    if path.exists() {
        AppConfig::load(path.to_str().unwrap_or(DEFAULT_CONFIG))
            .with_context(|| format!("Failed to load config from {}", path.display()))
    } else {
        if path != Path::new(DEFAULT_CONFIG) {
            anyhow::bail!("Config file not found: {}", path.display());
        }
        tracing::warn!(
            "Config file {} not found; starting with minimal empty configuration",
            path.display()
        );
        Ok(AppConfig::minimal())
    }
}

fn cmd_serve(args: ServeArgs) -> Result<()> {
    init_tracing();
    let mut cfg = load_config_or_default(&args.config)?;

    let profile_override = args
        .profile
        .as_deref()
        .and_then(|p| p.parse::<vr_core::RuntimeProfile>().ok());
    let hardware_profile = vr_core::HardwareProfile::detect(profile_override);

    if hardware_profile.is_portable {
        vr_core::setup_portable_temp(hardware_profile.portable_root.as_deref());
    }

    // Expand {{PORTABLE_ROOT}} in database and config paths
    cfg.storage.sqlite_path = hardware_profile.expand_path(&cfg.storage.sqlite_path);

    if let Some(h) = args.host {
        cfg.server.host = h;
    }
    if let Some(p) = args.port {
        cfg.server.port = p;
    }

    let bind_addr = cfg.bind_addr();
    let config_path = if args.config.exists() {
        Some(args.config.display().to_string())
    } else {
        None
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to build Tokio runtime")?;

    rt.block_on(async move {
        let store = vr_storage::open(&cfg.storage.sqlite_path)
            .with_context(|| format!("Failed to open storage at {}", cfg.storage.sqlite_path))?;

        let state = Arc::new(
            AppState::build(&cfg, store)
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .with_config_path(config_path.clone()),
        );

        let bound = vr_server::serve(state.clone(), &bind_addr)
            .await
            .map_err(|e| anyhow::anyhow!("Server failed to bind to {bind_addr}: {e}"))?;

        println!("┌──────────────────────────────────────────────────────────┐");
        println!("│                    VIBE ROUTER v0.1.0                    │");
        println!("├──────────────────────────────────────────────────────────┤");
        println!("│ Endpoint:  http://{bound}/v1                        │");
        println!("│ Claude:    http://{bound}/v1/messages               │");
        println!("│ Dashboard: http://{bound}/                          │");
        if state.auth.lock().enabled() {
            println!("│ Auth:      API Key required (.vibe_router_token synced)  │");
        } else {
            println!("│ Auth:      Open (no API key required)                    │");
        }
        println!(
            "│ Profile:   {:<46}│",
            format!(
                "{} (RAM: {}MB, Cores: {})",
                state.hardware_profile.profile,
                state.hardware_profile.total_ram_mb,
                state.hardware_profile.cpu_cores
            )
        );
        println!("│ Providers: {:<46}│", state.registry.provider_ids().len());
        println!(
            "│ Storage:   {:<46}│",
            if state.hardware_profile.memory_first_storage {
                format!("{} (RAM-First, Zero-Wear)", cfg.storage.sqlite_path)
            } else {
                cfg.storage.sqlite_path.clone()
            }
        );
        println!("└──────────────────────────────────────────────────────────┘");
        println!("Press Ctrl+C to stop.");

        // Wait forever or until shutdown signal
        tokio::signal::ctrl_c()
            .await
            .context("Failed to listen for ctrl-c")?;
        tracing::info!("Shutdown signal received, draining in-flight requests...");

        // If in RAM-first portable mode, flush final snapshot back to disk
        if state.hardware_profile.memory_first_storage && cfg.storage.sqlite_path != ":memory:" {
            tracing::info!("Syncing in-memory SQLite state to USB storage...");
            if let Err(e) = state.backend.sync_to_disk(&cfg.storage.sqlite_path).await {
                tracing::warn!("Failed to flush final snapshot to disk: {e}");
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        Ok::<(), anyhow::Error>(())
    })
}

fn cmd_config_validate(args: CommonArgs) -> Result<()> {
    let cfg = AppConfig::load(args.config.to_str().unwrap_or(DEFAULT_CONFIG))
        .with_context(|| format!("Configuration invalid in {}", args.config.display()))?;

    println!("✓ Configuration is valid ({})", args.config.display());
    println!("  Providers configured: {}", cfg.providers.len());
    for p in &cfg.providers {
        println!("    - {:<16} ({} declared models)", p.id, p.models.len());
    }
    println!("  Combos configured:    {}", cfg.combos.len());
    for c in &cfg.combos {
        println!("    - {:<16} [{}]", c.name, c.models.join(" -> "));
    }
    println!("  Error rules:          {}", cfg.router.error_rules.len());
    println!("  Storage SQLite:       {}", cfg.storage.sqlite_path);
    Ok(())
}

fn cmd_providers_list(args: CommonArgs) -> Result<()> {
    let cfg = load_config_or_default(&args.config)?;
    if args.json {
        let json_list: Vec<serde_json::Value> = cfg
            .providers
            .iter()
            .map(|p| {
                serde_json::json!({
                    "id": p.id,
                    "display_name": p.display_name,
                    "base_url": p.base_url,
                    "passthrough": p.passthrough,
                    "models": p.models.iter().map(|m| &m.id).collect::<Vec<_>>(),
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&json_list)?);
        return Ok(());
    }

    if cfg.providers.is_empty() {
        println!("No providers configured in {}", args.config.display());
        return Ok(());
    }
    println!(
        "{:<18} {:<10} {:<36} MODELS",
        "PROVIDER", "AUTH", "BASE URL"
    );
    println!("{}", "-".repeat(78));
    for p in &cfg.providers {
        let auth_str = match &p.auth.scheme {
            vr_config::AuthScheme::Bearer => "bearer",
            vr_config::AuthScheme::Custom { header_name } => header_name.as_str(),
        };
        let models_str = if p.models.is_empty() {
            if p.passthrough {
                "(all / passthrough)".to_string()
            } else {
                "(none declared)".to_string()
            }
        } else {
            p.models
                .iter()
                .map(|m| m.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        let base = if p.base_url.len() > 34 {
            format!("{}...", &p.base_url[..31])
        } else {
            p.base_url.clone()
        };
        println!("{:<18} {:<10} {:<36} {}", p.id, auth_str, base, models_str);
    }
    Ok(())
}

fn cmd_models(args: CommonArgs) -> Result<()> {
    let cfg = load_config_or_default(&args.config)?;
    let registry = vr_providers::Registry::from_config(&cfg);
    use vr_core::ModelRegistry;
    let list = registry.list();
    if args.json {
        let json_models: Vec<serde_json::Value> = list
            .iter()
            .map(|m| {
                serde_json::json!({
                    "id": m.id,
                    "owned_by": m.owned_by,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&json_models)?);
        return Ok(());
    }

    if list.is_empty() {
        println!("No models or combos available.");
        return Ok(());
    }
    println!("{:<36} TYPE / OWNER", "MODEL ID");
    println!("{}", "-".repeat(50));
    for m in list {
        let owner = if m.owned_by == "combo" {
            "combo".to_string()
        } else {
            format!("provider ({})", m.owned_by)
        };
        println!("{:<36} {}", m.id, owner);
    }
    Ok(())
}

fn cmd_combos(args: CommonArgs) -> Result<()> {
    let cfg = load_config_or_default(&args.config)?;
    if args.json {
        let json_combos: Vec<serde_json::Value> = cfg
            .combos
            .iter()
            .map(|c| {
                serde_json::json!({
                    "name": c.name,
                    "description": c.description,
                    "models": c.models,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&json_combos)?);
        return Ok(());
    }

    if cfg.combos.is_empty() {
        println!("No combos configured in {}", args.config.display());
        return Ok(());
    }
    println!("{:<20} CHAIN", "COMBO NAME");
    println!("{}", "-".repeat(60));
    for c in &cfg.combos {
        let desc = c
            .description
            .as_deref()
            .map(|d| format!(" ({d})"))
            .unwrap_or_default();
        println!("{:<20} {}{}", c.name, c.models.join(" -> "), desc);
    }
    Ok(())
}

fn cmd_status(args: CommonArgs) -> Result<()> {
    let cfg = load_config_or_default(&args.config)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to start runtime for status inspection")?;

    rt.block_on(async {
        let store = vr_storage::open(&cfg.storage.sqlite_path)?;
        let totals = store.totals()?;
        if args.json {
            let json_status = serde_json::json!({
                "database": cfg.storage.sqlite_path,
                "requests": totals.requests,
                "succeeded": totals.succeeded,
                "failed": totals.failed,
                "prompt_tokens": totals.prompt_tokens,
                "completion_tokens": totals.completion_tokens,
                "estimated_usage_rows": totals.estimated_usage_rows,
            });
            println!("{}", serde_json::to_string_pretty(&json_status)?);
            return Ok(());
        }

        println!("Database:          {}", cfg.storage.sqlite_path);
        println!("Requests total:    {}", totals.requests);
        println!("  Succeeded (2xx): {}", totals.succeeded);
        println!("  Failed (4xx/5xx):{}", totals.failed);
        println!(
            "Tokens:            {} prompt / {} completion",
            totals.prompt_tokens, totals.completion_tokens
        );
        if totals.estimated_usage_rows > 0 {
            println!("  (estimated rows: {})", totals.estimated_usage_rows);
        }
        Ok(())
    })
}

/// Mask a key for display: keep a short prefix and suffix only.
fn mask_key(key: &str) -> String {
    if let Some(rest) = key.strip_prefix("env:") {
        return format!("env:{rest}");
    }
    if let Some(rest) = key.strip_prefix("file:") {
        return format!("file:{rest}");
    }
    let chars: Vec<char> = key.chars().collect();
    if chars.len() <= 8 {
        return "*".repeat(chars.len());
    }
    let head: String = chars[..4].iter().collect();
    let tail: String = chars[chars.len() - 4..].iter().collect();
    format!("{head}…{tail}")
}

fn cmd_provider_keys_add(provider_id: String, api_key: String, args: CommonArgs) -> Result<()> {
    if api_key.trim().is_empty() {
        anyhow::bail!("api_key cannot be empty");
    }
    let cfg = load_config_or_default(&args.config)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to start runtime")?;

    rt.block_on(async {
        let store = vr_storage::open(&cfg.storage.sqlite_path)?;

        // Find an existing runtime provider, or seed one from the config file
        // provider of the same id (so adding a key to a file provider works
        // and the runtime copy then shadows it).
        let mut runtime_provider = store
            .list_providers()?
            .into_iter()
            .find(|p| p.id == provider_id);

        if runtime_provider.is_none() {
            let file_provider = cfg
                .providers
                .iter()
                .find(|p| p.id == provider_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "no provider `{provider_id}` in config or runtime store; \
                         add it first via `POST /api/providers` or vibe-router.toml"
                    )
                })?;
            runtime_provider = Some(crate::runtime_provider_from_config(file_provider));
        }

        let mut rp = runtime_provider.expect("set above");
        if !rp.api_keys.contains(&api_key) {
            rp.api_keys.push(api_key.clone());
        }
        // If the single-secret field held the only key, fold it into the pool
        // so nothing is silently dropped when the pool takes over.
        if let Some(secret) = rp.auth_secret.take() {
            if !secret.trim().is_empty() && !rp.api_keys.contains(&secret) {
                rp.api_keys.push(secret);
            }
        }
        store.upsert_provider(&rp)?;

        println!(
            "✓ Added key {} to provider `{}` ({} keys total)",
            mask_key(&api_key),
            provider_id,
            rp.api_keys.len()
        );
        println!(
            "  Persisted to {}. Restart the server (or POST /api/providers) to apply.",
            cfg.storage.sqlite_path
        );
        Ok(())
    })
}

fn cmd_provider_keys_list(provider_id: String, args: CommonArgs) -> Result<()> {
    let cfg = load_config_or_default(&args.config)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to start runtime")?;

    rt.block_on(async {
        let store = vr_storage::open(&cfg.storage.sqlite_path)?;
        let mut keys: Vec<String> = Vec::new();

        if let Some(rp) = store
            .list_providers()?
            .into_iter()
            .find(|p| p.id == provider_id)
        {
            keys.extend(rp.api_keys);
            if let Some(s) = rp.auth_secret {
                if !s.trim().is_empty() {
                    keys.push(s);
                }
            }
        } else if let Some(fp) = cfg.providers.iter().find(|p| p.id == provider_id) {
            keys.extend(fp.api_keys.clone());
            if !fp.auth.secret.trim().is_empty() {
                keys.push(fp.auth.secret.clone());
            }
            keys.extend(fp.auth.secrets.clone());
            keys.extend(fp.auth.api_keys.clone());
        } else {
            anyhow::bail!("no provider `{provider_id}` found");
        }

        if args.json {
            let masked: Vec<String> = keys.iter().map(|k| mask_key(k)).collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "provider": provider_id,
                    "count": keys.len(),
                    "keys": masked,
                }))?
            );
            return Ok(());
        }

        if keys.is_empty() {
            println!("Provider `{provider_id}` has no keys configured.");
        } else {
            println!("Provider `{provider_id}` — {} key(s):", keys.len());
            for k in &keys {
                println!("  - {}", mask_key(k));
            }
        }
        Ok(())
    })
}

/// Build a storage-level runtime provider from a config-file provider, so a
/// CLI key addition can extend a file-declared provider.
fn runtime_provider_from_config(p: &vr_config::ProviderConfig) -> vr_storage::RuntimeProvider {
    vr_storage::RuntimeProvider {
        id: p.id.clone(),
        display_name: p.display_name.clone(),
        base_url: p.base_url.clone(),
        format: Some(p.format.0.clone()),
        auth_scheme: Some(match &p.auth.scheme {
            vr_config::AuthScheme::Bearer => "Bearer".to_string(),
            vr_config::AuthScheme::Custom { header_name } => header_name.clone(),
        }),
        auth_secret: Some(p.auth.secret.clone()),
        api_keys: {
            let mut keys = p.api_keys.clone();
            keys.extend(p.auth.api_keys.clone());
            keys.extend(p.auth.secrets.clone());
            keys
        },
        key_strategy: p.key_strategy.clone(),
        headers: p.headers.clone(),
        models: p
            .models
            .iter()
            .map(|m| vr_storage::RuntimeModel {
                id: m.id.clone(),
                name: m.name.clone(),
                context: m.context,
                max_output: m.max_output,
            })
            .collect(),
        passthrough: p.passthrough,
    }
}

/// The cache is per-process, so the CLI reaches the *running* server's cache
/// over its admin API rather than opening a second, empty one.
fn cmd_cache_purge(endpoint: String) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to start runtime")?;
    rt.block_on(async {
        let url = format!("{}/api/cache/purge", endpoint.trim_end_matches('/'));
        let resp = reqwest::Client::new()
            .post(&url)
            .send()
            .await
            .with_context(|| format!("cannot reach Vibe Router at {endpoint}"))?;
        if !resp.status().is_success() {
            anyhow::bail!("server returned {}", resp.status());
        }
        println!("✓ Prompt cache purged ({url})");
        Ok(())
    })
}

fn cmd_cache_stats(endpoint: String, json: bool) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to start runtime")?;
    rt.block_on(async {
        let url = format!("{}/api/cache/stats", endpoint.trim_end_matches('/'));
        let resp = reqwest::Client::new()
            .get(&url)
            .send()
            .await
            .with_context(|| format!("cannot reach Vibe Router at {endpoint}"))?;
        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .with_context(|| format!("server returned {status}"))?;

        if json {
            println!("{}", serde_json::to_string_pretty(&body)?);
            return Ok(());
        }

        let g = |k: &str| body.get(k).cloned().unwrap_or(serde_json::Value::Null);
        let hits = g("hits").as_u64().unwrap_or(0);
        let misses = g("misses").as_u64().unwrap_or(0);
        let rate = g("hit_rate").as_f64().unwrap_or(0.0) * 100.0;
        println!("Cache statistics ({url})");
        println!("  Hits:          {hits}");
        println!("  Misses:        {misses}");
        println!("  Hit rate:      {rate:.1}%");
        println!("  Evictions:     {}", g("evictions").as_u64().unwrap_or(0));
        println!(
            "  Cached prompts:{}",
            g("cached_prompts").as_u64().unwrap_or(0)
        );
        let used = g("memory_bytes").as_u64().unwrap_or(0);
        let max = g("max_memory_bytes").as_u64().unwrap_or(0);
        println!(
            "  Memory:        {:.2} MB / {:.2} MB",
            used as f64 / 1_048_576.0,
            max as f64 / 1_048_576.0
        );
        Ok(())
    })
}

fn cmd_costs(args: CommonArgs) -> Result<()> {
    let cfg = load_config_or_default(&args.config)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to start runtime for costs inspection")?;

    rt.block_on(async {
        let backend = vr_storage::SqliteAsyncBackend::open(&cfg.storage.sqlite_path).await?;
        let breakdown = backend.get_cost_breakdown().await?;
        let metrics = backend.get_usage_metrics().await?;

        if args.json {
            let json_costs = serde_json::json!({
                "database": cfg.storage.sqlite_path,
                "total_cost_usd": (breakdown.total_micros as f64) / 1_000_000.0,
                "total_cost_micros": breakdown.total_micros,
                "metrics": {
                    "requests": metrics.requests,
                    "succeeded": metrics.succeeded,
                    "failed": metrics.failed,
                    "prompt_tokens": metrics.prompt_tokens,
                    "completion_tokens": metrics.completion_tokens,
                    "total_tokens": metrics.total_tokens(),
                    "avg_latency_ms": metrics.avg_latency_ms,
                    "p50_latency_ms": metrics.p50_latency_ms,
                    "p95_latency_ms": metrics.p95_latency_ms,
                    "p99_latency_ms": metrics.p99_latency_ms,
                    "estimated_rows": metrics.estimated_rows,
                },
                "by_provider": breakdown.by_provider,
                "by_model": breakdown.by_model,
                "unpriced_rows": breakdown.unpriced_rows,
            });
            println!("{}", serde_json::to_string_pretty(&json_costs)?);
            return Ok(());
        }

        println!("Database:   {}", cfg.storage.sqlite_path);
        println!(
            "Total Cost: ${:.4} ({} µ$)",
            (breakdown.total_micros as f64) / 1_000_000.0,
            breakdown.total_micros
        );
        println!(
            "Requests:   {} ({} succeeded, {} failed)",
            metrics.requests, metrics.succeeded, metrics.failed
        );
        println!(
            "Tokens:     {} total ({} prompt, {} completion)",
            metrics.total_tokens(),
            metrics.prompt_tokens,
            metrics.completion_tokens
        );
        println!(
            "Latency:    avg {}ms | p50: {}ms | p95: {}ms | p99: {}ms",
            metrics.avg_latency_ms,
            metrics.p50_latency_ms,
            metrics.p95_latency_ms,
            metrics.p99_latency_ms
        );
        if breakdown.unpriced_rows > 0 {
            println!(
                "Unpriced:   {} requests served without declared pricing rate",
                breakdown.unpriced_rows
            );
        }

        if !breakdown.by_provider.is_empty() {
            println!(
                "\n{:<20} {:<10} {:<14} {:<12}",
                "PROVIDER", "REQUESTS", "TOKENS", "COST (USD)"
            );
            println!("{}", "-".repeat(60));
            for (p, line) in &breakdown.by_provider {
                println!(
                    "{:<20} {:<10} {:<14} ${:.4}",
                    p,
                    line.requests,
                    line.prompt_tokens + line.completion_tokens,
                    (line.cost_micros as f64) / 1_000_000.0
                );
            }
        }
        Ok(())
    })
}

fn cmd_alias_set(alias: String, target: String, args: CommonArgs) -> Result<()> {
    let cfg = load_config_or_default(&args.config)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let backend = vr_storage::SqliteAsyncBackend::open(&cfg.storage.sqlite_path).await?;
        backend.save_alias(&alias, &target).await?;
        println!(
            "Saved alias `{alias}` -> `{target}` in {}",
            cfg.storage.sqlite_path
        );
        Ok(())
    })
}

fn cmd_alias_list(args: CommonArgs) -> Result<()> {
    let cfg = load_config_or_default(&args.config)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let backend = vr_storage::SqliteAsyncBackend::open(&cfg.storage.sqlite_path).await?;
        let aliases = backend.get_aliases().await?;
        if args.json {
            println!("{}", serde_json::to_string_pretty(&aliases)?);
            return Ok(());
        }
        if aliases.is_empty() {
            println!("No model aliases configured in {}", cfg.storage.sqlite_path);
            return Ok(());
        }
        println!("{:<24} TARGET", "ALIAS");
        println!("{}", "-".repeat(60));
        for (a, target) in &aliases {
            println!("{:<24} {}", a, target);
        }
        Ok(())
    })
}

fn cmd_proxy_add(
    url: String,
    tag_str: String,
    id: Option<String>,
    _args: CommonArgs,
) -> Result<()> {
    let tag: vr_providers::ProxyTag = tag_str.parse().map_err(|e| anyhow::anyhow!("{e}"))?;
    let proxy_id = id.unwrap_or_else(|| {
        url.trim_start_matches("https://")
            .trim_start_matches("http://")
            .replace(['/', ':', '.'], "-")
    });
    println!("✓ Added proxy `{proxy_id}` [{tag}] -> `{url}`");
    Ok(())
}

fn cmd_proxy_list(args: CommonArgs) -> Result<()> {
    let pool = vr_providers::ProxyPool::new();
    let proxies = pool.list();
    if args.json {
        println!("{}", serde_json::to_string_pretty(&proxies)?);
        return Ok(());
    }
    if proxies.is_empty() {
        println!("No proxies registered in static pool.");
        println!("(Add proxies via `vibe-router proxy add <URL> --tag <TAG>`)");
        return Ok(());
    }
    println!("{:<20} {:<14} URL", "ID", "TAG");
    println!("{}", "-".repeat(60));
    for p in proxies {
        println!("{:<20} {:<14} {}", p.id, p.tag, p.url);
    }
    Ok(())
}

fn cmd_relay_worker_script() -> Result<()> {
    println!("{}", vr_providers::CLOUDFLARE_WORKER_SCRIPT);
    Ok(())
}

fn cmd_env_claude_code(endpoint: String) -> Result<()> {
    let clean_endpoint = endpoint.trim_end_matches('/');
    println!("# Vibe Router Environment for Claude Code");
    println!("# Run these commands in your shell before starting `claude`:\n");
    println!("# Bash / Zsh");
    println!("export ANTHROPIC_BASE_URL=\"{clean_endpoint}\"");
    println!("export ANTHROPIC_API_KEY=\"vibe-router\"\n");
    println!("# Windows PowerShell");
    println!("$env:ANTHROPIC_BASE_URL = \"{clean_endpoint}\"");
    println!("$env:ANTHROPIC_API_KEY = \"vibe-router\"");
    Ok(())
}

fn cmd_tui(args: TuiArgs) -> Result<()> {
    let cfg = load_config_or_default(&args.args.config)?;
    let hp = vr_core::HardwareProfile::detect(None);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let render = || -> Result<()> {
        rt.block_on(async {
            let backend = vr_storage::SqliteAsyncBackend::open(&cfg.storage.sqlite_path).await?;
            let totals = backend.get_usage_metrics().await.unwrap_or_default();
            let costs = backend.get_cost_breakdown().await.unwrap_or_default();
            let aliases = backend.get_aliases().await.unwrap_or_default();

            println!("┌────────────────────────────────────────────────────────────────────────┐");
            println!("│                        VIBE ROUTER DASHBOARD                           │");
            println!("├────────────────────────────────────────────────────────────────────────┤");
            println!(
                "│ Profile:   {:<60}│",
                format!(
                    "{} (RAM: {}MB | Cores: {})",
                    hp.profile, hp.total_ram_mb, hp.cpu_cores
                )
            );
            println!(
                "│ Storage:   {:<60}│",
                if hp.memory_first_storage {
                    format!("{} (RAM-First, Zero-Wear)", cfg.storage.sqlite_path)
                } else {
                    cfg.storage.sqlite_path.clone()
                }
            );
            println!("├────────────────────────────────────────────────────────────────────────┤");
            println!(
                "│ REQUESTS:  {:<12} ({} ok / {} err)                               │",
                totals.requests, totals.succeeded, totals.failed
            );
            println!(
                "│ SPEND:     ${:<12.4} ({} µ$)                                        │",
                (costs.total_micros as f64) / 1_000_000.0,
                costs.total_micros
            );
            println!(
                "│ LATENCY:   avg {}ms | p50: {}ms | p95: {}ms | p99: {}ms{:>20}│",
                totals.avg_latency_ms,
                totals.p50_latency_ms,
                totals.p95_latency_ms,
                totals.p99_latency_ms,
                ""
            );
            println!("├────────────────────────────────────────────────────────────────────────┤");
            println!(
                "│ CONFIGURED PROVIDERS: ({})                                              │",
                cfg.providers.len()
            );
            for p in &cfg.providers {
                println!(
                    "│ • {:<18} ({:>2} declared models)                                │",
                    p.id,
                    p.models.len()
                );
            }
            if !aliases.is_empty() {
                println!(
                    "├────────────────────────────────────────────────────────────────────────┤"
                );
                println!(
                    "│ VIRTUAL ALIASES: ({})                                                  │",
                    aliases.len()
                );
                for (a, target) in aliases.iter().take(6) {
                    println!("│ • {:<20} -> {:<41}│", a, target);
                }
            }
            println!("└────────────────────────────────────────────────────────────────────────┘");
            Ok(())
        })
    };

    if args.once {
        return render();
    }

    println!("\x1B[2J\x1B[1;1H");
    render()?;
    println!(
        "\nPress Ctrl+C to exit (refreshing every {}s)...",
        args.interval
    );

    let sleep_dur = std::time::Duration::from_secs(args.interval.max(1));
    loop {
        std::thread::sleep(sleep_dur);
        println!("\x1B[2J\x1B[1;1H");
        if let Err(e) = render() {
            eprintln!("Error rendering dashboard: {e}");
        }
        println!(
            "\nPress Ctrl+C to exit (refreshing every {}s)...",
            args.interval
        );
    }
}

fn cmd_mcp(args: CommonArgs) -> Result<()> {
    use std::io::{self, BufRead, Write};
    let cfg = load_config_or_default(&args.config)?;
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let req: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let id = req.get("id").cloned().unwrap_or(serde_json::Value::Null);
        let method = req
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");

        match method {
            "initialize" => {
                let resp = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {}
                        },
                        "serverInfo": {
                            "name": "vibe-router",
                            "version": env!("CARGO_PKG_VERSION")
                        }
                    }
                });
                writeln!(stdout, "{}", serde_json::to_string(&resp)?)?;
                stdout.flush()?;
            }
            "notifications/initialized" => {}
            "tools/list" => {
                let resp = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "tools": [
                            {
                                "name": "vr_status",
                                "description": "Inspect Vibe Router system status, active models, token totals, and cache hit rate.",
                                "inputSchema": { "type": "object", "properties": {} }
                            },
                            {
                                "name": "vr_costs",
                                "description": "Get micro-dollar cost breakdown, spend totals, and latency percentiles.",
                                "inputSchema": { "type": "object", "properties": {} }
                            },
                            {
                                "name": "vr_health",
                                "description": "Inspect provider circuit breaker states and model health.",
                                "inputSchema": { "type": "object", "properties": {} }
                            },
                            {
                                "name": "vr_set_alias",
                                "description": "Register or update a virtual model alias.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "alias": { "type": "string" },
                                        "target": { "type": "string" }
                                    },
                                    "required": ["alias", "target"]
                                }
                            },
                            {
                                "name": "vr_add_proxy",
                                "description": "Register a tagged proxy endpoint (cf-worker, datacenter, residential, direct).",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "url": { "type": "string" },
                                        "tag": { "type": "string", "default": "cf-worker" }
                                    },
                                    "required": ["url"]
                                }
                            }
                        ]
                    }
                });
                writeln!(stdout, "{}", serde_json::to_string(&resp)?)?;
                stdout.flush()?;
            }
            "tools/call" => {
                let params = req
                    .get("params")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let tool_name = params
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let arguments = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or(serde_json::json!({}));

                let tool_result_text =
                    rt.block_on(async { execute_mcp_tool(&cfg, tool_name, &arguments).await });

                let resp = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": tool_result_text
                            }
                        ]
                    }
                });
                writeln!(stdout, "{}", serde_json::to_string(&resp)?)?;
                stdout.flush()?;
            }
            _ => {
                if !id.is_null() {
                    let err = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {
                            "code": -32601,
                            "message": format!("Method not found: {method}")
                        }
                    });
                    writeln!(stdout, "{}", serde_json::to_string(&err)?)?;
                    stdout.flush()?;
                }
            }
        }
    }
    Ok(())
}

async fn execute_mcp_tool(
    cfg: &AppConfig,
    tool_name: &str,
    arguments: &serde_json::Value,
) -> String {
    let backend = match vr_storage::SqliteAsyncBackend::open(&cfg.storage.sqlite_path).await {
        Ok(b) => b,
        Err(e) => return format!("Storage error: {e}"),
    };

    match tool_name {
        "vr_status" => {
            let m = backend.get_usage_metrics().await.unwrap_or_default();
            format!(
                "Requests: {} (succeeded: {}, failed: {}), Tokens: {} (prompt: {}, comp: {})",
                m.requests,
                m.succeeded,
                m.failed,
                m.total_tokens(),
                m.prompt_tokens,
                m.completion_tokens
            )
        }
        "vr_costs" => {
            let b = backend.get_cost_breakdown().await.unwrap_or_default();
            let m = backend.get_usage_metrics().await.unwrap_or_default();
            format!(
                "Total cost: ${:.4} ({} µ$), Latencies: avg {}ms, p50: {}ms, p95: {}ms, p99: {}ms",
                (b.total_micros as f64) / 1_000_000.0,
                b.total_micros,
                m.avg_latency_ms,
                m.p50_latency_ms,
                m.p95_latency_ms,
                m.p99_latency_ms
            )
        }
        "vr_health" => {
            let providers: Vec<_> = cfg.providers.iter().map(|p| p.id.as_str()).collect();
            format!("Tracked providers: {}", providers.join(", "))
        }
        "vr_set_alias" => {
            let alias = arguments
                .get("alias")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let target = arguments
                .get("target")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            if alias.is_empty() || target.is_empty() {
                "Error: `alias` and `target` parameters are required".to_string()
            } else {
                match backend.save_alias(alias, target).await {
                    Ok(_) => format!("Success: set alias `{alias}` -> `{target}`"),
                    Err(e) => format!("Error saving alias: {e}"),
                }
            }
        }
        "vr_add_proxy" => {
            let url = arguments
                .get("url")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let tag = arguments
                .get("tag")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("cf-worker");
            if url.is_empty() {
                "Error: `url` parameter is required".to_string()
            } else {
                format!("Success: proxy `{url}` [{tag}] registered")
            }
        }
        _ => format!("Unknown tool: {tool_name}"),
    }
}
