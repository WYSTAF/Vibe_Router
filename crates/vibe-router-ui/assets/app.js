// Vibe Router Web Dashboard — Vanilla JS, zero dependencies.
// Talks directly to /v1/* and /api/* endpoints.

const $ = (id) => document.getElementById(id);

// ---------------------------------------------------------------- THEME

const THEME_KEY = "vibe_router_theme";

function initTheme() {
  const saved = localStorage.getItem(THEME_KEY) || localStorage.getItem("vr_theme");
  const preferred = window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
  const theme = saved || preferred;
  applyTheme(theme);

  $("theme-toggle").addEventListener("click", () => {
    const cur = document.documentElement.getAttribute("data-theme") || "dark";
    const next = cur === "dark" ? "light" : "dark";
    applyTheme(next);
    localStorage.setItem(THEME_KEY, next);
  });
}

function applyTheme(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  const icon = $("theme-toggle").querySelector(".theme-icon");
  if (icon) {
    icon.textContent = theme === "dark" ? "☼" : "☾";
  }
}

// ---------------------------------------------------------------- NAVIGATION

document.querySelectorAll(".tab").forEach((btn) => {
  btn.addEventListener("click", () => showPage(btn.dataset.page));
});

document.querySelectorAll(".tab-link").forEach((link) => {
  link.addEventListener("click", (e) => {
    e.preventDefault();
    showPage(link.dataset.page);
  });
});

function showPage(name) {
  document.querySelectorAll(".tab").forEach((b) =>
    b.classList.toggle("active", b.dataset.page === name)
  );
  document.querySelectorAll(".page").forEach((p) => {
    p.hidden = p.id !== "page-" + name;
  });
  if (name === "assignment") loadAssignment();
  if (name === "providers") loadProviders();
  if (name === "combos") loadCombos();
  if (name === "aliases") loadAliases();
  if (name === "proxies") loadProxies();
  if (name === "costs") loadCosts();
  if (name === "usage") loadUsage();
}

// ---------------------------------------------------------------- API

async function api(path, opts = {}) {
  const resp = await fetch(path, opts);
  if (!resp.ok) {
    let detail = "";
    try {
      detail = (await resp.json()).error?.message ?? "";
    } catch {
      detail = resp.statusText;
    }
    throw new Error(detail || resp.status);
  }
  return resp.status === 204 ? null : resp.json();
}

function fmtTokens(n) {
  if (n == null) return "0";
  return n >= 1_000_000
    ? (n / 1_000_000).toFixed(1) + "M"
    : n >= 1_000
      ? (n / 1_000).toFixed(1) + "k"
      : String(n);
}

function fmtUsd(micros) {
  if (!micros) return "$0.0000";
  return "$" + (micros / 1_000_000).toFixed(4);
}

function fmtTime(tsMs) {
  const d = new Date(tsMs);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

// ---------------------------------------------------------------- STATUS & TOKENS

async function loadStatus() {
  try {
    const [statusData, totals, costsData, healthData] = await Promise.all([
      api("/api/status"),
      api("/api/usage/totals"),
      api("/api/costs").catch(() => null),
      api("/api/health").catch(() => null),
    ]);

    $("endpoint").textContent = statusData.endpoint;
    $("stat-requests").textContent = fmtTokens(totals.requests);
    $("stat-ok").textContent = fmtTokens(totals.succeeded);
    $("stat-fail").textContent = fmtTokens(totals.failed);
    $("stat-auth").textContent = statusData.auth_required ? "API Key Required" : "Open";

    // Reported vs Estimated breakdown
    const prompt = totals.prompt_tokens || 0;
    const comp = totals.completion_tokens || 0;
    const totalReported = prompt + comp;
    const estRows = totals.estimated_usage_rows || 0;

    $("stat-tokens-prompt").textContent = fmtTokens(prompt);
    $("stat-tokens-completion").textContent = fmtTokens(comp);
    $("stat-tokens-reported-total").textContent = `${fmtTokens(totalReported)} total reported`;
    $("stat-estimated-rows").textContent = `${estRows} requests`;

    const ratio = totals.requests > 0 ? Math.round((estRows / totals.requests) * 100) : 0;
    const ratioBadge = $("token-ratio-badge");
    ratioBadge.textContent = `${ratio}% estimated`;
    ratioBadge.className = `badge ${ratio > 25 ? "warn" : ratio === 0 ? "ok" : ""}`;

    // USD Cost & Latency metrics (p50/p95/p99)
    if (costsData && costsData.metrics) {
      const m = costsData.metrics;
      const totalMicros = costsData.breakdown?.total_micros || 0;
      $("stat-total-spend").textContent = fmtUsd(totalMicros);
      $("stat-avg-latency").textContent = `${m.avg_latency_ms || 0} ms`;
      $("stat-percentiles").textContent = `${m.p50_latency_ms || 0} / ${m.p95_latency_ms || 0} / ${m.p99_latency_ms || 0} ms`;
    }

    // Circuit Breaker health summary
    if (healthData && healthData.circuits) {
      const openCount = healthData.circuits.filter((c) => c.state === "open").length;
      const circuitBadge = $("circuit-summary-badge");
      if (openCount > 0) {
        circuitBadge.textContent = `${openCount} Circuit(s) OPEN`;
        circuitBadge.className = "badge err";
      } else {
        circuitBadge.textContent = "Circuits Healthy";
        circuitBadge.className = "badge ok";
      }
    }

    // Cache metrics (Prompt & Stream cache)
    const cache = statusData.cache || {};
    const cacheHits = cache.hits || 0;
    const cacheMisses = cache.misses || 0;
    const cacheRate = ((cache.hit_rate || 0) * 100).toFixed(1);
    $("stat-cache-rate").textContent = `${cacheRate}%`;
    const cachedTotal = (cache.cached_prompts || 0) + (cache.cached_streams || 0);
    $("stat-cache-count").textContent = fmtTokens(cachedTotal);
    $("stat-cache-mem").textContent = `${(
      (cache.memory_bytes || 0) / 1_048_576
    ).toFixed(1)} MB`;
    const cacheBadge = $("cache-badge");
    cacheBadge.textContent = `${cacheHits} hits / ${cacheMisses} misses`;
    cacheBadge.className = `badge ${
      cacheHits > 0 && cacheHits > cacheMisses ? "ok" : ""
    }`;

    loadAssignmentPreview();
  } catch (e) {
    console.error("Failed to load status:", e);
  }
}

// ---------------------------------------------------------------- ROLES / ASSIGNMENT

function renderRoleCard(roleKey, roleData) {
  const status = roleData.status || "ready";
  const statusBadge =
    status === "ready"
      ? '<span class="badge ok">ready</span>'
      : status === "cooling"
        ? '<span class="badge cooling">cooling</span>'
        : status === "rate_limited"
          ? '<span class="badge rate_limited">rate limited</span>'
          : `<span class="badge">${status}</span>`;

  const isVr = roleData.model.startsWith("VIBE_ROUTER_") || roleData.model.startsWith("VR_");
  const modelHtml = isVr
    ? `<span class="badge badge-vibe-router-combo">${roleData.model}</span>`
    : `<span class="role-model">${roleData.model}</span>`;

  const ctxHtml = roleData.context_window
    ? `<span class="role-class-badge">${fmtTokens(roleData.context_window)} ctx</span>`
    : "";

  const ticketHtml = roleData.recommended_ticket_size
    ? `<span class="role-class-badge">Ticket ${roleData.recommended_ticket_size}</span>`
    : "";

  return `
    <div class="role-card role-${roleKey}">
      <div class="role-header">
        <span class="role-title">${roleKey}</span>
        ${statusBadge}
      </div>
      <div>${modelHtml}</div>
      <div class="role-meta">
        <span class="role-class-badge">${roleData.class || "generic"}</span>
        ${ctxHtml}
        ${ticketHtml}
      </div>
    </div>
  `;
}

async function loadAssignment() {
  try {
    const data = await api("/v1/assignment");
    $("assignment-refreshed").textContent = `Synced: ${data.refreshed_at}`;
    const offlineBadge = $("assignment-offline-badge");
    if (data.offline_fallback_recommended) {
      offlineBadge.textContent = "Offline Fallback Recommended";
      offlineBadge.className = "badge err";
    } else {
      offlineBadge.textContent = "Online Ready";
      offlineBadge.className = "badge ok";
    }

    const grid = $("roles-full-grid");
    grid.innerHTML = "";
    for (const [key, role] of Object.entries(data.roles)) {
      grid.innerHTML += renderRoleCard(key, role);
    }
  } catch (e) {
    $("roles-full-grid").innerHTML = `<p class="muted">Could not load assignments: ${e.message}</p>`;
  }
}

async function loadAssignmentPreview() {
  try {
    const data = await api("/v1/assignment");
    const previewGrid = $("roles-preview-grid");
    previewGrid.innerHTML = "";
    for (const [key, role] of Object.entries(data.roles)) {
      previewGrid.innerHTML += renderRoleCard(key, role);
    }
  } catch {
    // Silent on background preview error
  }
}

// ---------------------------------------------------------------- PROVIDERS

async function loadProviders() {
  try {
    const [providers, healthData] = await Promise.all([
      api("/api/providers"),
      api("/api/health").catch(() => ({ circuits: [] })),
    ]);

    $("providers-count").textContent = `${providers.length} providers`;
    const rows = $("provider-rows");
    rows.innerHTML = "";
    $("provider-note").hidden = providers.length > 0;

    const circuitMap = {};
    for (const c of healthData.circuits || []) {
      circuitMap[c.provider] = c;
    }

    for (const p of providers) {
      const tr = document.createElement("tr");
      const modelsHtml = p.models.length > 0
        ? p.models.map((m) => {
            const h = m.health || "ready";
            const badgeCls = h === "cooling" ? "cooling" : h === "rate_limited" ? "rate_limited" : "ok";
            return `<span class="badge ${badgeCls}" title="status: ${h}">${m.id}</span>`;
          }).join(" ")
        : '<span class="muted font-xs">(passthrough all)</span>';

      const originBadge = p.from_file
        ? '<span class="badge">file</span>'
        : '<span class="badge ok">runtime</span>';

      const circuit = circuitMap[p.id];
      const circuitState = circuit ? circuit.state : "closed";
      const circuitBadge = circuitState === "open"
        ? '<span class="badge err" title="Circuit OPEN (5xx tripped)">circuit open</span>'
        : circuitState === "half-open"
          ? '<span class="badge warn" title="Circuit HALF-OPEN (probing)">circuit half-open</span>'
          : '<span class="badge ok" title="Circuit CLOSED (healthy)">circuit closed</span>';

      const keyCount = p.key_count || 0;
      const healthyKeys = p.healthy_keys ?? keyCount;
      const keyBadge =
        keyCount === 0
          ? '<span class="badge">no key</span>'
          : `<span class="badge ${healthyKeys === 0 ? "cooling" : "ok"}" title="healthy / total keys">${healthyKeys}/${keyCount} keys</span>`;

      tr.innerHTML = `
        <td class="mono font-bold">${p.id}</td>
        <td class="mono muted font-xs">${p.base_url}</td>
        <td>${modelsHtml}</td>
        <td>${originBadge} ${keyBadge}</td>
        <td>${circuitBadge}</td>
      `;
      rows.appendChild(tr);
    }
  } catch (e) {
    console.error("Failed to load providers:", e);
  }
}

// ---------------------------------------------------------------- COMBOS

async function loadCombos() {
  try {
    const [combos, models] = await Promise.all([
      api("/api/combos"),
      api("/v1/models"),
    ]);

    $("combos-count").textContent = `${combos.length} combos`;
    const list = $("combo-list");
    list.innerHTML = "";

    const dl = $("model-list");
    dl.innerHTML = (models.data || [])
      .map((m) => `<option value="${m.id}">`)
      .join("");

    for (const c of combos) {
      const div = document.createElement("div");
      const isVr = c.name.startsWith("VIBE_ROUTER_") || c.name.startsWith("VR_");
      div.className = `combo ${isVr ? "is-vibe-router-combo is-vr-combo" : ""}`;

      const origin = c.from_file
        ? '<span class="badge" title="Declared in vibe-router.toml">file</span>'
        : '<span class="badge ok" title="Authored in runtime">runtime</span>';

      const vrBadge = isVr
        ? '<span class="badge badge-vibe-router-combo" title="Generated auto-combo">AUTO</span>'
        : "";

      div.innerHTML = `
        <span class="name">${c.name}</span>
        ${vrBadge}
        ${origin}
        <span class="chain">${c.models.join(" → ")}</span>
        <span class="actions">
          <button class="btn-test-combo" data-name="${c.name}">Select Test</button>
          ${!c.from_file ? `<button class="btn-del danger" data-name="${c.name}">Delete</button>` : ""}
        </span>
      `;

      div.querySelector(".btn-test-combo")?.addEventListener("click", () => {
        $("test-model").value = c.name;
        showPage("combos");
        $("test-prompt").focus();
      });

      if (!c.from_file) {
        div.querySelector(".btn-del")?.addEventListener("click", async () => {
          if (!confirm(`Delete runtime combo "${c.name}"?`)) return;
          await api(`/api/combos/${encodeURIComponent(c.name)}`, { method: "DELETE" });
          loadCombos();
        });
      }

      list.appendChild(div);
    }
  } catch (e) {
    console.error("Failed to load combos:", e);
  }
}

$("combo-save")?.addEventListener("click", async () => {
  const msg = $("combo-message");
  msg.textContent = "";
  const name = $("combo-name").value.trim();
  const models = $("combo-models")
    .value.split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  if (!name || models.length === 0) {
    msg.textContent = "Name and at least one model required.";
    return;
  }
  try {
    await api("/api/combos", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name, description: $("combo-desc").value, models }),
    });
    $("combo-name").value = "";
    $("combo-desc").value = "";
    $("combo-models").value = "";
    msg.textContent = "Saved!";
    setTimeout(() => { msg.textContent = ""; }, 2000);
    loadCombos();
  } catch (e) {
    msg.textContent = "Save failed: " + e.message;
  }
});

// ---------------------------------------------------------------- VIRTUAL ALIASES

async function loadAliases() {
  try {
    const data = await api("/api/aliases");
    const aliases = data.aliases || {};
    const keys = Object.keys(aliases);
    $("aliases-count").textContent = `${keys.length} aliases`;
    const tbody = $("alias-rows");
    tbody.innerHTML = "";
    $("alias-note").hidden = keys.length > 0;

    for (const key of keys) {
      const target = aliases[key];
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td class="mono font-bold">${key}</td>
        <td class="mono muted font-xs">${target}</td>
        <td class="right">
          <button class="btn-del danger font-xs" data-alias="${key}">Delete</button>
        </td>
      `;

      tr.querySelector(".btn-del")?.addEventListener("click", async () => {
        if (!confirm(`Delete alias "${key}"?`)) return;
        await api(`/api/aliases/${encodeURIComponent(key)}`, { method: "DELETE" });
        loadAliases();
      });

      tbody.appendChild(tr);
    }
  } catch (e) {
    console.error("Failed to load aliases:", e);
  }
}

$("alias-save")?.addEventListener("click", async () => {
  const msg = $("alias-message");
  msg.textContent = "";
  const alias = $("alias-name").value.trim();
  const target = $("alias-target").value.trim();

  if (!alias || !target) {
    msg.textContent = "Alias and target required.";
    return;
  }
  try {
    await api("/api/aliases", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ alias, target }),
    });
    $("alias-name").value = "";
    $("alias-target").value = "";
    msg.textContent = "Alias saved!";
    setTimeout(() => { msg.textContent = ""; }, 2000);
    loadAliases();
  } catch (e) {
    msg.textContent = "Save failed: " + e.message;
  }
});

// ---------------------------------------------------------------- PROXIES & RELAY

async function loadProxies() {
  try {
    const data = await api("/api/proxies");
    const proxies = data.proxies || [];
    $("proxies-count").textContent = `${proxies.length} proxies`;
    const tbody = $("proxy-rows");
    tbody.innerHTML = "";
    $("proxy-note").hidden = proxies.length > 0;

    for (const p of proxies) {
      const tr = document.createElement("tr");
      const tagBadge = `<span class="badge ${p.tag === 'cf-worker' ? 'badge-vibe-router-combo' : 'ok'}">${p.tag}</span>`;
      const statusBadge = p.enabled
        ? '<span class="badge ok">active</span>'
        : '<span class="badge cooling">disabled</span>';

      tr.innerHTML = `
        <td class="mono font-bold">${p.id}</td>
        <td>${tagBadge}</td>
        <td class="mono muted font-xs">${p.url}</td>
        <td>${statusBadge}</td>
        <td class="right">
          <button class="btn-del danger font-xs" data-id="${p.id}">Delete</button>
        </td>
      `;

      tr.querySelector(".btn-del")?.addEventListener("click", async () => {
        if (!confirm(`Remove proxy "${p.id}"?`)) return;
        await api(`/api/proxies/${encodeURIComponent(p.id)}`, { method: "DELETE" });
        loadProxies();
      });

      tbody.appendChild(tr);
    }
  } catch (e) {
    console.error("Failed to load proxies:", e);
  }
}

$("proxy-save")?.addEventListener("click", async () => {
  const msg = $("proxy-message");
  msg.textContent = "";
  const url = $("proxy-url").value.trim();
  const tag = $("proxy-tag").value;
  const customId = $("proxy-id").value.trim();

  if (!url) {
    msg.textContent = "Proxy URL required.";
    return;
  }
  const id = customId || url.replace(/^https?:\/\//, "").replace(/[^a-zA-Z0-9]/g, "-").slice(0, 24);

  try {
    await api("/api/proxies", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ id, url, tag, enabled: true, weight: 1 }),
    });
    $("proxy-url").value = "";
    $("proxy-id").value = "";
    msg.textContent = "Proxy added!";
    setTimeout(() => { msg.textContent = ""; }, 2000);
    loadProxies();
  } catch (e) {
    msg.textContent = "Add failed: " + e.message;
  }
});

$("btn-copy-worker-script")?.addEventListener("click", async () => {
  const btn = $("btn-copy-worker-script");
  try {
    const resp = await fetch("/api/relay/worker.js");
    const script = await resp.text();
    await navigator.clipboard.writeText(script);
    const orig = btn.textContent;
    btn.textContent = "✓ Copied to Clipboard!";
    setTimeout(() => { btn.textContent = orig; }, 2500);
  } catch (e) {
    alert("Could not copy script directly. Open /api/relay/worker.js in your browser.");
  }
});

// ---------------------------------------------------------------- COSTS & LATENCY

async function loadCosts() {
  try {
    const data = await api("/api/costs");
    const m = data.metrics || {};
    const b = data.breakdown || {};

    $("costs-total-spend").textContent = fmtUsd(b.total_micros || 0);
    $("costs-avg-lat").textContent = `${m.avg_latency_ms || 0} ms`;
    $("costs-p50").textContent = `${m.p50_latency_ms || 0} ms`;
    $("costs-p95-p99").textContent = `${m.p95_latency_ms || 0} / ${m.p99_latency_ms || 0} ms`;

    // Providers spend table
    const pTbody = $("costs-provider-rows");
    pTbody.innerHTML = "";
    for (const [provider, line] of Object.entries(b.by_provider || {})) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td class="mono font-bold">${provider}</td>
        <td>${fmtTokens(line.requests)}</td>
        <td>${fmtTokens(line.prompt_tokens)}</td>
        <td>${fmtTokens(line.completion_tokens)}</td>
        <td class="right mono font-bold ok">${fmtUsd(line.cost_micros)}</td>
      `;
      pTbody.appendChild(tr);
    }

    // Models spend table
    const mTbody = $("costs-model-rows");
    mTbody.innerHTML = "";
    for (const [model, line] of Object.entries(b.by_model || {})) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td class="mono font-bold">${model}</td>
        <td>${fmtTokens(line.requests)}</td>
        <td>${fmtTokens(line.prompt_tokens)}</td>
        <td>${fmtTokens(line.completion_tokens)}</td>
        <td class="right mono font-bold ok">${fmtUsd(line.cost_micros)}</td>
      `;
      mTbody.appendChild(tr);
    }
  } catch (e) {
    console.error("Failed to load costs:", e);
  }
}

$("btn-refresh-costs")?.addEventListener("click", loadCosts);

// ---------------------------------------------------------------- PLAYGROUND TESTER

$("btn-run-test")?.addEventListener("click", async () => {
  const model = $("test-model").value.trim();
  const prompt = $("test-prompt").value.trim();
  const outBox = $("test-output-box");
  const statusEl = $("test-status");

  if (!model) {
    statusEl.textContent = "Specify a target model, combo, or alias";
    return;
  }

  statusEl.textContent = "Connecting...";
  outBox.hidden = false;
  outBox.textContent = "";

  const t0 = performance.now();
  try {
    const resp = await fetch("/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model,
        messages: [{ role: "user", content: prompt }],
        stream: true,
      }),
    });

    if (!resp.ok) {
      let errText = resp.statusText;
      try {
        const errJson = await resp.json();
        errText = errJson.error?.message || errText;
      } catch {}
      outBox.textContent = `[HTTP ${resp.status}] ${errText}`;
      statusEl.textContent = `Failed in ${(performance.now() - t0).toFixed(0)}ms`;
      return;
    }

    statusEl.textContent = "Streaming response...";
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let text = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunk = decoder.decode(value, { stream: true });
      for (const line of chunk.split("\n")) {
        const trimmed = line.trim();
        if (trimmed.startsWith("data: ")) {
          const dataStr = trimmed.slice(6);
          if (dataStr === "[DONE]") continue;
          try {
            const parsed = JSON.parse(dataStr);
            const delta = parsed.choices?.[0]?.delta?.content || "";
            text += delta;
            outBox.textContent = text;
          } catch {}
        }
      }
    }
    statusEl.textContent = `Completed in ${(performance.now() - t0).toFixed(0)}ms`;
    loadStatus();
  } catch (e) {
    outBox.textContent = `Error: ${e.message}`;
    statusEl.textContent = "Execution failed";
  }
});

// ---------------------------------------------------------------- USAGE LOG

async function loadUsage() {
  try {
    const rows = await api("/api/usage/recent?limit=50");
    const tbody = $("usage-rows");
    tbody.innerHTML = "";
    $("usage-note").hidden = rows.length > 0;

    for (const r of rows) {
      const tr = document.createElement("tr");
      const isEst = r.usage_estimated;
      const prefix = isEst ? "~" : "";
      const promptTok = r.prompt_tokens ? `${prefix}${fmtTokens(r.prompt_tokens)}` : "0";
      const compTok = r.completion_tokens ? `${prefix}${fmtTokens(r.completion_tokens)}` : "0";
      const estBadge = isEst
        ? ' <span class="estimated" title="Estimated, not reported by provider">~est</span>'
        : "";

      tr.innerHTML = `
        <td class="mono muted font-xs">${fmtTime(r.ts_ms)}</td>
        <td class="mono font-bold">${r.requested_model}</td>
        <td class="mono muted font-xs">${r.provider || "—"} / ${r.served_model || "—"}</td>
        <td>
          <span class="badge ${r.status < 300 ? "ok" : "err"}">${r.status}</span>
        </td>
        <td class="mono right">${promptTok} / ${compTok}${estBadge}</td>
        <td class="mono right muted font-xs">${r.latency_ms} ms</td>
      `;
      tbody.appendChild(tr);
    }
  } catch (e) {
    console.error("Failed to load usage:", e);
  }
}

// ---------------------------------------------------------------- INIT

initTheme();
loadStatus();
$("btn-refresh-status")?.addEventListener("click", loadStatus);
setInterval(loadStatus, 5000);
