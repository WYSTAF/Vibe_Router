# Vibe Router — Architecture

A single-binary, Rust-native LLM router: OpenAI-compatible API in, pluggable
providers out, with health- and rate-limit-aware failover.

**Status:** design only. Nothing is implemented yet.

---

## Part 1 — What 9Router is (reference analysis)

9Router is a Next.js application: a full web dashboard *and* an OpenAI-compatible
gateway in one process. Requests enter at `/v1/*` Next routes, are handled by a
shared core (`src/sse` + the `open-sse` package), translated across wire formats,
executed against an upstream, and streamed back.

Its essential runtime model, stripped of the web framework:

```
client ──/v1/*──▶ request parse
                   ├─ resolve model string (alias? combo? provider/model?)
                   ├─ select credential (account pool)
                   ├─ detect source wire format
                   ├─ translate request  (source → canonical → target)
                   ├─ execute upstream  (executor per provider family)
                   ├─ on 401/403: refresh token, retry
                   ├─ on fallback-eligible error: cooldown account, next candidate
                   └─ translate response / SSE stream back to client format
```

The concepts worth carrying into a Rust rewrite, and how 9Router implements each:

| Concept | 9Router's mechanism | Worth keeping |
|---|---|---|
| Provider abstraction | ~130 hand-written JS registry files, one per provider, each a static config object; a code-generated `index.js` imports them all | **The idea, not the mechanism.** Data-driven registry is the right shape; 130 static source files is not. |
| Model handling | `provider/models` table + per-model metadata (capabilities, context/output limits, strip lists, upstream id) | Yes, as data. |
| Failover | `handleComboChat`: ordered candidate list, try each, move on on any fallback-eligible error | Yes — simple and correct. |
| Retry | per-status retry table (`DEFAULT_RETRY_CONFIG`); separate from failover | Yes, keep retry and failover as distinct layers. |
| Error classification | an ordered **rule table** (`ERROR_RULES`): text rules first, then status rules, each yielding `shouldFallback` + `cooldownMs`; exponential backoff for rate limits | **The single best idea in the codebase.** Classification as config, not code. |
| Rate-limit handling | per-account and per-model cooldown timestamps (`rateLimitedUntil`, `modelLock_*` flat fields) + exponential backoff capped at 15 levels | Yes; a proper in-memory state object instead of flat DB fields. |
| Health checks | mostly implicit — inferred from live error state. Explicit probing is partial (`/api/health`, tunnel healthchecks) | Start implicit (error-driven), add explicit probing later. |
| OpenAI-compatible API | `/v1/chat/completions`, `/v1/messages`, `/v1/responses`, `/v1/models` | Yes. |
| Streaming/SSE | stream translation pipeline with per-format chunk converters, stall/first-token timeouts | Yes. |
| Routing strategies | `fallback`, `round-robin` (with sticky limit), `fusion` (panel + judge) | `fallback` in v0.1; others later. |
| Capability auto-switch | inspect the current user turn for required modalities (vision/pdf/audio/video) and reorder the candidate list | Later; it depends on a capability catalog. |
| Config | JSON state in `db.json` (providers, connections, aliases, combos, keys, settings) | Yes, but TOML for static config and a small store for state. |
| Web UI | full Next.js dashboard (React, flow diagrams, i18n, SAML/OIDC SSO) | **No.** Replace with a tiny embedded static UI. |

### The three-tier fallback idea

9Router's product framing is a cost ladder: **subscription → cheap → free**.
A "combo" is a named ordered list of `provider/model` strings; requesting the
combo name walks the list until one succeeds. This is the whole value
proposition, and it is a *data structure*, not an algorithm. Vibe Router keeps
exactly that.

### What to deliberately leave behind

- Shipping a Node.js/React server runtime to get a gateway.
- One source file per provider.
- Provider-scoped concerns leaking into the routing core (9Router's core imports
  provider-specific executors and model resolvers directly).
- The sprawling surface: MITM proxy, tunnels, skills marketplace, cloud sync,
  SSO, media providers, 130 providers. None of it belongs in v0.1.

---

## Part 2 — Vibe Router architecture

### Design principles

1. **One binary, one process.** No Node runtime, no separate UI server, no
   container requirement. Static assets for the UI are compiled into the binary.
2. **The routing core knows nothing about specific providers.** It operates on a
   `Provider` trait and on data. Adding a provider must not touch routing code.
3. **Behaviour is configuration.** Error classification, retry policy, cooldown
   parameters, and combo membership are all data, editable at runtime.
4. **Async, bounded, low-memory.** One Tokio runtime, bounded channels, streaming
   by default so memory does not scale with response length.
5. **Degradable.** Every optional subsystem (health probes, catalog sync, UI)
   can fail or be disabled without affecting request serving.

### Layered view

```
                     ┌────────────────────────────────────────────┐
   CLI client ──▶    │  vr-cli   (serve / config / models / ...)  │
                     └────────────────────────────────────────────┘
                     ┌────────────────────────────────────────────┐
   HTTP client ──▶   │  vr-server                                │
   (OpenAI API)      │   /v1/chat/completions  /v1/models         │
   Browser    ──▶    │   /v1/messages (later)   embedded UI       │
                     └───────────────┬────────────────────────────┘
                                     │
                     ┌───────────────▼────────────────────────────┐
                     │  vr-routing                               │
                     │   resolve → select → execute → react      │
                     │   (alias, combo, failover, retry,          │
                     │    cooldown, rate-limit window)            │
                     └───────┬───────────────┬────────────────────┘
                             │               │  state
              ┌──────────────▼────────┐  ┌───▼─────────────────────┐
              │  vr-providers         │  │  vr-health              │
              │   Provider trait      │  │   availability +        │
              │   built-in transports │  │   rate-limit state      │
              └──────────────┬────────┘  └─────────────────────────┘
                             │
              ┌──────────────▼────────────────────────────────────┐
              │  vr-translate   (wire formats, canonical IR)      │
              └──────────────┬────────────────────────────────────┘
                             │
              ┌──────────────▼────────┐  ┌──────────────────────────┐
              │  vr-core             │  │  vr-config / vr-storage  │
              │  Provider trait,     │  │  TOML + env, hot reload, │
              │  Model, Combo,       │  │  usage + state store     │
              │  error types         │  └──────────────────────────┘
              └──────────────────────┘
```

Dependency direction is strictly downward. **`vr-providers` never imports
`vr-routing`**, and `vr-routing` never imports a concrete provider. That
inversion is what makes "add a provider without touching the core" structural
rather than a matter of discipline.

### Request flow (v0.1)

```
POST /v1/chat/completions  { model: "fast-cheap", stream: true }
  │
  1. auth            verify API key (constant-time) against configured keys
  2. resolve         "fast-cheap" → combo? alias? → ordered Vec<Candidate>
  3. select          ask vr-health for candidates not in cooldown;
                     apply rate-limit window; pick first available
  4. translate       source format → provider target format (vr-translate)
  5. execute         provider transport (HTTP, streaming body)
     ├─ success      stream SSE back, translate chunks, record usage
     ├─ retryable    (per retry policy) retry same candidate
     ├─ fallback-eligible  record failure in vr-health, goto 3
     └─ hard error   return OpenAI-shaped error to client
  6. finish          emit usage, release rate-limit token
```

The candidate list is materialised **before** execution. Failover is then a loop
over an already-decided list, which keeps the hot path free of policy lookups.

### Failover, retry, and rate limits — separated

Three distinct layers, because conflating them is what makes 9Router's cooldown
behaviour hard to reason about:

| Layer | Question it answers | Owns |
|---|---|---|
| **Retry** | "Should I try *this* candidate again?" | attempt count, backoff, per-status rules |
| **Failover** | "Should I try the *next* candidate?" | ordered list, error classification |
| **Health** | "Should this candidate even be considered?" | cooldown expiry, rate-limit window, probe results |

Error classification is an ordered rule table in config (adopting 9Router's best
idea, implemented originally):

```toml
[[error_rule]]
match = { text = "rate limit" }     # substring, case-insensitive
fallback = true
backoff = true                      # exponential

[[error_rule]]
match = { status = 503 }
fallback = true
cooldown_ms = 5000

[[error_rule]]
match = { status = 400 }
fallback = false                    # client's fault; don't punish the provider
```

Evaluated top to bottom, first match wins, text rules before status rules.
Unclassified 4xx (except 401/403/429) does **not** fall back — a request-scoped
error says nothing about the provider.

### Provider abstraction

```rust
// lives in vr-core so neither side has to depend on the other
pub trait Provider: Send + Sync {
    fn id(&self) -> &str;
    fn kind(&self) -> ProviderKind;              // chat | embedding | ...
    fn models(&self) -> &[ModelSpec];            // static or discovered
    fn transport(&self) -> &Transport;           // base url, auth, format, quirks
    fn discover_models(&self) -> Option<DiscoverySource>;   // v0.2+
}
```

Built-in providers are instances built from **config records**, not code:

```toml
[[provider]]
id = "openrouter"
base_url = "https://openrouter.ai/api/v1/chat/completions"
format = "openai"
auth = { header = "Authorization", scheme = "bearer", from = "env:OPENROUTER_KEY" }
[[provider.models]]
id = "anthropic/claude-sonnet-4"
context = 200_000
```

A provider that needs real custom behaviour (a non-standard protocol, a special
auth dance) becomes a small crate that implements `Provider` and registers
itself. That is the escape hatch — used rarely, since ~90% of the long tail is
"OpenAI-shaped HTTP with a different base URL and header."

### Web UI

A single static HTML/CSS/JS bundle embedded with `rust-embed`, served from the
same port. v0.1 shows: endpoint + key, provider status, model list, combo
editor, request log. No build step, no framework, no Node. The admin API it
calls is a small versioned JSON surface under `/api/`.

### Configuration

- `vibe-router.toml` — providers, models, combos, error rules, auth, ports.
  Watched for changes; hot reload without dropping in-flight requests.
- Environment variables for secrets and overrides (never write keys into TOML).
- A small state store (SQLite via `rusqlite`, or JSON) for runtime-only data:
  usage rows, cooldown state (if persistence is wanted), generated API keys.

---

## Part 3 — Project structure

```
vibe-router/
├── Cargo.toml                 # workspace
├── README.md
├── ARCHITECTURE.md            # this document
├── config/
│   └── vibe-router.toml.example
├── crates/
│   ├── vr-core/               # Provider trait, Model, Combo, Candidate,
│   │                          #   error/status types, format enum.
│   │                          #   Depends on: nothing.
│   ├── vr-config/             # TOML + env parsing, validation, hot reload.
│   │                          #   Depends on: vr-core.
│   ├── vr-providers/          # Provider trait impls for the built-in
│   │                          #   OpenAI-compatible transport; registry that
│   │                          #   builds providers from config records.
│   │                          #   Depends on: vr-core, vr-translate.
│   ├── vr-translate/          # wire formats + canonical IR; request and
│   │                          #   response/SSE converters. No provider knowledge.
│   │                          #   Depends on: vr-core.
│   ├── vr-health/             # availability state: cooldowns, rate-limit
│   │                          #   windows, backoff levels, (later) probe results.
│   │                          #   Depends on: vr-core.
│   ├── vr-routing/            # resolution (alias/combo), selection policy,
│   │                          #   retry loop, failover driver. Talks to
│   │                          #   providers only through the trait.
│   │                          #   Depends on: vr-core, vr-health.
│   ├── vr-storage/            # usage logging + state persistence.
│   │                          #   Depends on: vr-core.
│   ├── vr-server/             # HTTP server: /v1/* compatibility routes,
│   │                          #   /api/* admin routes, SSE, embedded UI.
│   │                          #   Depends on: vr-routing, vr-providers,
│   │                          #   vr-translate, vr-config, vr-storage, vr-ui.
│   ├── vr-ui/                 # static assets (embedded at build time).
│   └── vr-cli/                # arg parsing, subcommands, logging setup.
│                              #   Depends on: vr-server (+ others as needed).
└── src/main.rs                # thin entrypoint: parse args, hand to vr-cli.
```

`main.rs` stays trivial so the binary is a library with a CLI skin — which also
makes integration testing easy (drive the whole stack in-process).

**Why a workspace, given "minimal dependencies"?** Compile time and binary size
are what a single binary buys; a workspace does not cost either (one binary is
still produced). What it buys is the dependency boundary above, enforced by the
compiler. That boundary *is* the "providers don't touch routing core"
requirement. If later compile time matters, the crates collapse into modules of
one crate without API changes, because module boundaries already match.

---

## Part 4 — Core modules and responsibilities

| Module | Responsibility | Not responsible for |
|---|---|---|
| `vr-core` | The vocabulary: `Provider` trait, `Model`, `ModelSpec`, `Combo`, `Candidate`, `WireFormat`, `RouterError`. Zero logic. | Any I/O, any policy. |
| `vr-config` | Load/validate TOML + env, expose typed config, watch file, emit reload events. | Interpreting config (routing reads it, config doesn't route). |
| `vr-providers` | Turn config records into live `Provider` values; the OpenAI-compatible HTTP transport; connection pooling; timeouts; proxy env. | Deciding which provider to use. |
| `vr-translate` | Convert request/response between wire formats via a canonical internal representation; SSE chunk conversion; `[DONE]` handling. | Talking to the network. |
| `vr-health` | Track per-`(provider, model)` availability: cooldown until, backoff level, rate-limit window and token count; answer "is this candidate usable now?". | Executing requests. |
| `vr-routing` | The decision path: resolve a model string to candidates, order them, apply health filter, drive retry + failover, record outcomes back into health. | HTTP, formats. |
| `vr-storage` | Append usage rows and request logs; rotate; expose aggregates. | Anything on the request path except a final write. |
| `vr-server` | HTTP surface: compat routes, admin API, SSE plumbing, auth middleware, embedded static UI, graceful shutdown. | Policy decisions (delegates to routing). |
| `vr-ui` | Static bundle. | Nothing server-side. |
| `vr-cli` | Subcommands: `serve`, `config (edit/validate)`, `providers (list/test)`, `models`, `combos`, `status`. | Business logic. |

### The contract that keeps it clean

`vr-routing` consumes exactly two interfaces:

```rust
trait ProviderRegistry: Send + Sync {
    fn resolve(&self, model: &str) -> Resolution;   // Candidates or error
    fn get(&self, id: &str) -> Option<Arc<dyn Provider>>;
}

trait HealthView: Send + Sync {
    fn usable(&self, c: &Candidate) -> Usability;   // Ready | Cooldown(until) | RateLimited(until)
    fn record(&self, c: &Candidate, outcome: Outcome);
}
```

`vr-server` wires a concrete `vr-providers` registry and a concrete `vr-health`
store into `vr-routing` at startup. Everything else in the binary is a detail of
those two implementations.

---

## Part 5 — MVP boundary

### v0.1 — included

**API surface**
- `POST /v1/chat/completions` (streaming + non-streaming)
- `GET /v1/models`
- OpenAI-shaped error bodies and status codes
- Single API key auth (`Authorization: Bearer`), constant-time compare

**Routing**
- Model string resolution: literal `provider/model`, alias, combo name
- Combos as ordered candidate lists with `fallback` strategy
- Config-driven error classification rule table
- Retry (per-status attempts + delay), distinct from failover
- Per-`(provider, model)` cooldown with exponential backoff, capped
- Simple rate-limit window: fixed window per model, configurable RPM/TPM

**Providers**
- Any OpenAI-compatible endpoint via config (base URL, auth header/scheme, models)
- One example OAuth-free provider set in the example config

**Streaming**
- SSE passthrough and translation for OpenAI-format chunks
- Client disconnect detection; stall timeout
- Non-streaming aggregation when `stream: false`

**Operational**
- TOML config with env overrides and hot reload
- Structured logging (tracing), request log to file
- Usage logging (prompt/completion tokens, model, latency, status)
- Embedded static UI: provider status, model list, combo editor, log viewer
- CLI: `serve`, `config validate`, `providers list`, `models`, `status`
- Graceful shutdown; single static binary

### Explicitly postponed (and why)

| Feature | Why later |
|---|---|
| Anthropic `/v1/messages`, Gemini, Responses-API formats | Translation surface is large; v0.1 is OpenAI-format in and out. Keeps `vr-translate` honest without exploding it. |
| `round-robin` / `sticky` / `fusion` strategies | `fallback` covers the value proposition. Strategies are a `SelectionPolicy` swap, easy once the interface exists. |
| Capability auto-switch (vision/pdf reordering) | Needs a capability catalog first. |
| Model catalog sync (e.g. models.dev) | See Part 6 — designed for now, built later. |
| Provider auto-discovery | Same. |
| Automatic combo generation | Same. |
| OAuth / device-code provider flows | Highest complexity, lowest value for a single-user VPS router. API keys first. |
| Quota tracking & cost estimation | Needs usage accounting to mature first; usage *logging* is in v0.1. |
| Explicit health probes | v0.1 availability is error-driven (implicit). Probe slot is reserved. |
| Multi-account pools per provider | Single credential per provider in v0.1; the account dimension is additive. |
| Dashboard auth (password/SSO/SAML) | v0.1 trusts loopback + API key; UI auth is a deployment concern. |
| Cloud sync, tunnels, MITM, skills, media providers | Out of scope for the project's purpose. |

The v0.1 boundary is chosen so that a single user can point one client at one
endpoint and get working failover across cheap OpenAI-compatible providers,
with a usable UI — nothing more.

---

## Part 6 — Extensibility for discovery, sync, and auto-combos

These three future features are the main reason for the trait/data split. Each
has a reserved extension point that v0.1 defines but does not implement.

### 1. Provider discovery

**Reserved:** `Provider::discover_models() -> Option<DiscoverySource>`

```rust
enum DiscoverySource {
    Static,                          // models declared in config (v0.1)
    OpenAiModelsEndpoint { url },    // GET /v1/models on the provider itself
    RemoteCatalog { url, mapping },  // a shared catalog (models.dev-style)
}
```

Because models are data attached to a provider, "discovering models" is just
*another source of that data*. Adding it touches `vr-providers` and a new
`vr-discovery` crate; `vr-routing`, `vr-health`, and `vr-server` are untouched.
A provider is free to declare a discovery source that v0.1 ignores — the field
is already on the trait, so nothing breaks later.

### 2. Automatic provider synchronization

**Reserved:** a background task type in `vr-server` that owns exactly one job —
periodically refresh a `DiscoverySource` and hand the result to the registry.

The registry is built as a read-heavy, copy-on-write map behind an `Arc<RwLock>`
(or a generation-indexed snapshot). A swap of the model list is therefore atomic
from the request path's point of view, and in-flight requests keep their old
snapshot. That is the whole trick: sync is *safe* because reads are snapshot
reads, not because sync is careful.

Config stays the source of truth for everything sync does not know — auth,
endpoints, cost tier. Sync only fills in model metadata.

### 3. Automatic model combos

**Reserved:** `vr-combo-gen` (future crate) implementing:

```rust
trait ComboGenerator: Send + Sync {
    fn build(&self, catalog: &ModelCatalog, policy: &GenPolicy) -> Vec<ComboSpec>;
}
```

An auto-combo generator is a pure function over a model catalog: given
"subscription → cheap → free" tiers and capability requirements, emit ordered
candidate lists. It needs nothing from the routing core — it only writes `Combo`
records into the same store the UI edits. A user-generated combo and an
auto-generated one are then indistinguishable on the request path, and "edit the
auto combo" just forks it into a manual one.

### Extension points at a glance

| Future feature | Extension point | What changes in the core |
|---|---|---|
| Provider discovery | `DiscoverySource` enum + `discover_models()` on the trait | Nothing |
| Catalog sync | Background task + snapshot reads in the registry | Nothing |
| Auto combos | `ComboGenerator` writing `ComboSpec` | Nothing |
| New routing strategy | `SelectionPolicy` impl | `vr-routing` gains a policy enum |
| Explicit health probe | `HealthView` implementation gains a probe writer | Nothing (health is already a trait) |
| New wire format | converter pair in `vr-translate` | Nothing |
| Custom provider behaviour | small crate impl'ing `Provider` | Nothing |

Every row in the last column reads "Nothing" — that is the test this
architecture is designed to pass, and it is the property 9Router does not have.

---

## Open decisions (need your call before implementation)

1. **HTTP stack.** `axum`/`hyper` (ecosystem, slightly heavier) vs a thinner
   `hyper`-only or `actix-web` choice. All satisfy the single-binary goal.
2. **Storage.** SQLite (`rusqlite`) vs a JSON file for state/usage. SQLite scales
   better and is still one compiled-in dependency.
3. **Async runtime scope.** Full Tokio vs a minimal single-threaded executor.
   A small VPS with modest concurrency may not need the multi-thread scheduler.
4. **UI technology.** Vanilla JS vs a precompiled framework output (Lit/Preact).
   Affects binary size and maintenance, not architecture.
5. **Target providers for v0.1's example config** — which 3–5 OpenAI-compatible
   endpoints to ship as worked examples.
