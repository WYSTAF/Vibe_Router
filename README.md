# Vibe Router

A single-binary, lightweight, production-grade Rust LLM routing gateway and dashboard with health-aware failover, rate-limit management, multi-key rotation, Claude Code compatibility, and portable zero-wear execution.

Vibe Router provides unified OpenAI (`/v1/chat/completions`) and Anthropic (`/v1/messages`) compatibility and routes requests across any combination of local and cloud providers with data-driven fallback chains (combos), automatic retries, exponential backoff, micro-dollar cost tracking, and an embedded Web UI.

---

## Key Features

- **Single Lightweight Binary**: Compiled to an ~8.8 MB standalone executable. No Node.js runtime, no Python, no external database dependencies.
- **Hardware-Aware Adaptive Runtime**:
  - **Disk-First VPS Mode**: Minimizes RSS footprint (< 30 MB) and restricts cache to 4 MB for small $5/mo VPS instances.
  - **RAM-First Portable Mode**: Hydrates SQLite into host memory (`:memory:`) with periodic crash-safe atomic flush back to disk, providing zero flash wear on USB drives.
  - **Drive-Letter Independence**: Normalizes paths with `{{PORTABLE_ROOT}}` so changing USB drive letters on Windows never breaks the configuration.
- **Universal Client Compatibility**:
  - **OpenAI Compatible**: Direct drop-in for OpenAI SDK, Cursor, Cline, OpenCode, and local agents at `http://localhost:20128/v1`.
  - **Claude Code (`/v1/messages`)**: Native Anthropic protocol adapter supporting both JSON completions and real-time SSE streaming. Run `vibe-router env claude-code` to connect Claude CLI in seconds.
- **Native `Vibe-Link` Synergy Protocol (with Vibe Assembly)**:
  - Telemetry response headers: `X-Vibe-Router-Cost-Micros`, `X-Vibe-Router-Latency-Ms`, `X-Vibe-Router-Cache`, and `X-Vibe-Router-Provider`.
  - Inbound role tracking (`X-Vibe-Role: boss | code | debug | judge | chat | hermes`) with per-role token & spend accounting.
  - **Judge Retry Bypass (`X-Vibe-Retry: 1`)**: Bypasses the cache and rotates to the next healthy candidate in the chain so code rejected by evaluation judges is retried on an alternative model.
  - **Zero-401 Local Handshake**: Synchronizes `.vibe_router_token` for seamless local agent authentication.
- **Multi-Key Pool Rotation & 429 Auto-Cooldown**:
  - Supports `RoundRobin`, `LeastLoaded`, and `Weighted` selection strategies across multiple API keys per provider.
  - Upstream 429 rate-limit responses trigger automatic exponential backoff cooldowns and instantly retry the next available healthy key transparently.
- **High-Capacity In-Memory Prompt & Stream Cache**:
  - 256 MB LRU cache hashing `(model_id, temperature, top_p, tools, messages_json)` via SHA-256 with sub-millisecond cache hits.
  - Opt-in stream caching cleanly buffers and replays completed SSE streams.
- **Virtual Model Aliasing**:
  - Map arbitrary model names (e.g., `gpt-4o`, `claude-3-7-sonnet`) to underlying combos or provider targets via REST, CLI, or Web UI.
- **Circuit Breakers & Health Probes**:
  - Per-provider circuit breaker automatically trips to `OPEN` after repeated 5xx errors or timeouts, transitions to `HALF_OPEN` for probing, and resets to `CLOSED` on success.
- **Tagged Proxy Pools & Cloudflare Worker Relay**:
  - Support proxy tier tags (`cf-worker`, `datacenter`, `residential`, `direct`).
  - Deterministic 64-bit FNV-1a sticky IP affinity ensures an API key consistently uses the same proxy to avoid anti-fraud bans.
  - Built-in ready-to-deploy Cloudflare Worker reverse-proxy script (`vibe-router relay worker-script`).
- **Interactive Terminal UI & MCP Server**:
  - `vibe-router tui` (or `top`): Live terminal dashboard displaying RAM profile, spend, latencies, and circuit states.
  - `vibe-router mcp`: Built-in stdio Model Context Protocol (MCP) server exposing tools to external AI assistants (`vibe_router_status`, `vibe_router_costs`, `vibe_router_health`, `vibe_router_set_alias`, `vibe_router_add_proxy`).
- **Embedded Web Dashboard**:
  - Vanilla HTML/CSS/JavaScript interface embedded via `rust-embed` with industrial dark/yellow branding (`#F5B800`), live token counters (distinguishing reported vs. estimated), USD cost & p50/p95/p99 latency metrics, combo editor, and API playground.

---

## Workspace Architecture

Vibe Router is structured as a modular 11-crate Rust workspace with strictly downward, acyclic dependency flow:

```
vibe-router/
├── Cargo.toml                      # Workspace manifest
├── ARCHITECTURE.md                 # Architecture reference
├── vibe-router.toml.example        # Documented sample configuration
├── src/main.rs                     # Minimal CLI driver entrypoint
└── crates/
    ├── vibe-router-core/           # Core traits, Model, Combo, Candidate, WireFormat, HardwareProfile
    ├── vibe-router-config/         # TOML parsing, validation, and error classification rule engine
    ├── vibe-router-health/         # Availability tracking: cooldowns, backoff levels, RPM limits, circuit breakers
    ├── vibe-router-routing/        # Fallback engine: candidate resolution, retry loop, failover driver
    ├── vibe-router-translate/      # Wire format translation (OpenAI, Anthropic /v1/messages, SSE decoders)
    ├── vibe-router-providers/      # HTTP transport, multi-key rotation pools, sticky proxies, CF relay
    ├── vibe-router-storage/        # Async StorageBackend trait, SQLite WAL, in-memory portable hydration
    ├── vibe-router-cache/          # In-memory bounded LRU prompt & SSE stream cache with SHA-256 keys
    ├── vibe-router-ui/             # Embedded static Web UI assets (HTML/CSS/JS) via rust-embed
    ├── vibe-router-server/         # Axum server: /v1/*, /api/*, embedded dashboard, and Vibe-Link handlers
    └── vibe-router-cli/            # Command-line interface, TUI, and stdio MCP server
```

---

## Quick Start

### 1. Build from Source

Ensure Rust 1.85+ is installed:

```bash
cargo build --release
```

The resulting binary will be at `target/release/vibe-router` (or `target/release/vibe-router.exe` on Windows).

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
│ Claude:    http://127.0.0.1:20128/v1/messages            │
│ Dashboard: http://127.0.0.1:20128/                       │
│ Auth:      Open (no API key required)                    │
│ Profile:   workstation (RAM: 8192MB, Cores: 4)           │
│ Providers: 2                                             │
│ Storage:   vibe-router.db                                │
└──────────────────────────────────────────────────────────┘
```

Open `http://localhost:20128` in your browser to access the dashboard.

---

## Integration Guides

### Claude Code

Run Vibe Router and print the shell environment:

```bash
./target/release/vibe-router env claude-code
```

Then export the variables in your shell before launching `claude`:

```bash
export ANTHROPIC_BASE_URL="http://127.0.0.1:20128"
export ANTHROPIC_API_KEY="vibe-router"
claude
```

### OpenAI SDK

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:20128/v1",
    api_key="none"
)

response = client.chat.completions.create(
    model="smart-fallback",  # Target a combo, alias, or provider/model
    messages=[{"role": "user", "content": "Explain Rust borrowing in one sentence."}],
    stream=True
)

for chunk in response:
    content = chunk.choices[0].delta.content or ""
    print(content, end="", flush=True)
```

### Model Context Protocol (MCP)

To connect Vibe Router as an MCP tool server for Claude Desktop, Cursor, or Hermes:

```json
{
  "mcpServers": {
    "vibe-router": {
      "command": "/path/to/vibe-router",
      "args": ["mcp", "--config", "/path/to/vibe-router.toml"]
    }
  }
}
```

---

## CLI Commands

```
Commands:
  serve      Start the OpenAI/Anthropic proxy server and dashboard (default)
  config     Inspect and validate configuration (e.g., config validate)
  providers  Inspect upstream providers and manage key pools
  models     List all addressable models, combos, and aliases
  combos     List configured combos and fallback chains
  status     Display usage and storage status
  costs      Display USD cost breakdown and latency metrics (p50/p95/p99)
  alias      Manage virtual model aliases (set, list)
  proxy      Manage tagged proxies (add, list)
  relay      Cloudflare Worker reverse-proxy relay utilities (worker-script)
  env        Print client environment variables (claude-code)
  tui        Live terminal status dashboard (alias: top)
  mcp        Model Context Protocol (MCP) stdio JSON-RPC 2.0 server
  cache      Inspect and purge in-memory prompt cache (stats, purge)
```

---

## Testing & Quality

Vibe Router maintains 100% passing tests across all crates with zero warnings:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

All integration tests are hermetic and deterministic using an in-process TCP mock upstream that tests real HTTP sockets, chunked SSE streaming, timeouts, disconnects, failover chains, rate limits, and error status classification without relying on external networks or live accounts.

---

## License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
