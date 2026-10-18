# Vibe Router

A single-binary, lightweight, Rust-native LLM router with health-aware failover, rate-limit management, and an embedded Web UI.

Vibe Router provides a unified OpenAI-compatible endpoint (`/v1/*`) and routes requests across any combination of local and cloud providers with configurable fallback chains (combos), automatic retries, exponential backoff, and usage logging.

---

## Key Features

- **Single Lightweight Binary**: Compiled to a ~7.3 MB static executable. No Node.js runtime, no Python, no external database dependencies.
- **Low RAM & Fast Startup**: Uses < 20 MB of baseline RAM. Boots in milliseconds on a $5/month VPS.
- **OpenAI-Compatible API**: Seamless drop-in replacement for OpenAI SDK, Claude Code, Cursor, Cline, OpenCode, and any tool pointing to `http://localhost:20128/v1`.
- **Streaming by Default**: High-throughput SSE relay with token usage extraction, client disconnect detection, and zero mid-stream buffering.
- **Provider Abstraction**: Add new OpenAI-compatible providers in seconds via TOML configuration — no code modifications required.
- **Model Combos (Fallback Chains)**: Define prioritized model sequences (e.g., `fast-cheap = ["groq/llama-3.3-70b-versatile", "openrouter/meta-llama/llama-3.3-70b-instruct", "ollama/llama3.2"]`). Requests automatically fall over if a model hits rate limits or errors.
- **Proactive Rate Limiting**: Fixed-window RPM tracking with configurable safety margins (e.g. 90% buffer). Waits for window rollover or falls over to backup models *before* provoking upstream 429s.
- **Config-Driven Error Classification**: Ordered rule table matching on error message text or HTTP status codes to decide whether to retry, back off exponentially, or fail over.
- **Embedded Web UI**: Built-in vanilla HTML/CSS/JavaScript dashboard compiled directly into the binary — view live server status, providers, model availability, combos editor, and request logs.
- **Persistent State**: SQLite storage in WAL mode for request metrics and runtime-authored combos.

---

## Workspace Architecture

Vibe Router is structured as a modular 10-crate Rust workspace with strict downward dependency flow:

```
vibe-router/
├── Cargo.toml                 # Workspace root & binary manifest
├── ARCHITECTURE.md            # Complete architecture reference
├── vibe-router.toml.example   # Documented sample configuration
├── src/main.rs                # Minimal CLI driver entrypoint
└── crates/
    ├── vr-core/               # Provider traits, Model, Combo, Candidate, WireFormat, Error types
    ├── vr-config/             # TOML parsing, validation, and error classification rule engine
    ├── vr-health/             # Availability tracking: cooldowns, backoff levels, RPM rate limiters
    ├── vr-routing/            # Fallback engine: candidate resolution, retry loop, failover driver
    ├── vr-translate/          # SSE incremental decoder, frame encoder, usage accumulator
    ├── vr-providers/          # Generic OpenAI-compatible HTTP client & registry
    ├── vr-storage/            # SQLite & in-memory persistence for usage metrics and runtime combos
    ├── vr-ui/                 # Embedded static assets (HTML/CSS/JS) via rust-embed
    ├── vr-server/             # Axum HTTP server: /v1/* compatibility routes & /api/* dashboard routes
    └── vr-cli/                # Command-line interface subcommands (serve, config, providers, models, combos, status)
```

---

## Quick Start

### 1. Build from Source

Ensure Rust 1.85+ is installed:

```bash
cargo build --release
```

The resulting binary will be located at `target/release/vibe-router`.

### 2. Configure Providers

Copy the example configuration:

```bash
cp vibe-router.toml.example vibe-router.toml
```

Edit `vibe-router.toml` with your providers and keys:

```toml
[server]
host = "127.0.0.1"
port = 20128
api_keys = [] # Optional local auth keys

[[providers]]
id = "groq"
display_name = "Groq Cloud"
base_url = "https://api.groq.com/openai/v1/chat/completions"
auth = { scheme = "Bearer", secret = "env:GROQ_API_KEY" }
models = [
    { id = "llama-3.3-70b-versatile" }
]

[[providers]]
id = "ollama"
display_name = "Local Ollama"
base_url = "http://localhost:11434/v1/chat/completions"
auth = { secret = "" }
passthrough = true

[[combos]]
name = "smart-fallback"
description = "Cloud Groq first, local Ollama backup"
models = [
    "groq/llama-3.3-70b-versatile",
    "ollama/llama3.2"
]
```

Validate your configuration:

```bash
./target/release/vibe-router config validate
```

### 3. Run the Server

```bash
./target/release/vibe-router serve
```

```
┌──────────────────────────────────────────────────────────┐
│                    VIBE ROUTER v0.1.0                    │
├──────────────────────────────────────────────────────────┤
│ Endpoint:  http://127.0.0.1:20128/v1                     │
│ Dashboard: http://127.0.0.1:20128/                       │
│ Auth:      Open (no API key required)                    │
│ Providers: 2                                             │
│ Storage:   vibe-router.db                                │
└──────────────────────────────────────────────────────────┘
```

Open `http://localhost:20128` in your browser to view the interactive dashboard.

---

## Using Vibe Router with Developer Tools

### OpenAI Python SDK

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:20128/v1",
    api_key="none"  # Or your configured local API key
)

response = client.chat.completions.create(
    model="smart-fallback",  # Target a combo or a specific provider/model
    messages=[{"role": "user", "content": "Explain rust ownership in 2 sentences."}],
    stream=True
)

for chunk in response:
    content = chunk.choices[0].delta.content or ""
    print(content, end="", flush=True)
```

### cURL

```bash
curl http://localhost:20128/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "smart-fallback",
    "messages": [{"role": "user", "content": "Hello!"}]
  }'
```

### Claude Code / Cursor / Cline / OpenCode

Set your environment variables or IDE settings:

```bash
export OPENAI_BASE_URL="http://localhost:20128/v1"
export OPENAI_API_KEY="none"
```

### Orchestration Role Assignment (Vibe Assembly)

```bash
curl http://localhost:20128/v1/assignment
```

Returns live role assignments (`boss`, `code`, `debug`, `chat`), their capability class, context window, ticket size, health status (`ready`, `cooling`, `rate_limited`), and offline fallback recommendation.

---

## CLI Reference

```
vibe-router [OPTIONS] [COMMAND]

Commands:
  serve        Start the OpenAI-compatible proxy server and dashboard (default)
  config       Inspect and validate configuration (e.g., config validate)
  providers    Inspect upstream providers (e.g., providers list)
  models       List all addressable models and combos
  combos       List configured combos and fallback chains
  status       Display usage and storage totals
  help         Print this message or the help of the given subcommand(s)

Options:
  -c, --config <CONFIG>  Path to configuration file [default: vibe-router.toml]
      --host <HOST>      Bind host address (overrides config)
  -p, --port <PORT>      Bind port (overrides config)
  -h, --help             Print help
  -V, --version          Print version
```

---

## Testing & Quality

Vibe Router includes 130 unit and integration tests across all crates with zero warnings:

```bash
cargo test --workspace
```

All integration tests are hermetic and deterministic: they use an in-process TCP mock upstream that tests real HTTP sockets, chunked SSE streaming, timeouts, disconnects, failover chains, rate limits, and error status classification without relying on external networks or live accounts.

---

## License

MIT
