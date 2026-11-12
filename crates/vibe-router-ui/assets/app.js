// Vibe Router Web Dashboard — Vanilla JS, zero dependencies.
// Talks directly to /v1/* and /api/* endpoints.
//
// Security: every dynamic value is inserted with textContent / createElement
// (never innerHTML interpolation), and every request carries the session admin
// key as `Authorization: Bearer <key>`. The key lives in sessionStorage only.

const $ = (id) => document.getElementById(id);

// ---------------------------------------------------------------- SAFE DOM

function el(tag, cls, text) {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text != null) n.textContent = String(text);
  return n;
}

function badge(text, cls, title) {
  const b = el("span", "badge" + (cls ? " " + cls : ""), text);
  if (title) b.title = title;
  return b;
}

function td(cls, content) {
  const c = el("td", cls);
  if (content == null) return c;
  if (typeof content === "string" || typeof content === "number") {
    c.textContent = String(content);
  } else if (Array.isArray(content)) {
    for (const node of content) if (node) c.appendChild(node);
  } else {
    c.appendChild(content);
  }
  return c;
}

// ---------------------------------------------------------------- ADMIN KEY

const KEY_STORAGE = "vibe_router_admin_key";
let adminKey = sessionStorage.getItem(KEY_STORAGE) || "";

function authHeaders() {
  // X-Vibe-Admin marks a deliberate admin call so bodyless DELETEs pass the
  // server's CSRF check (a cross-site simple request cannot set this header).
  const h = { "X-Vibe-Admin": "1" };
  if (adminKey) h.Authorization = "Bearer " + adminKey;
  return h;
}

function showKeyBar(locked) {
  const bar = $("api-key-bar");
  const status = $("api-key-status");
  if (locked) {
    bar.hidden = false;
    status.hidden = false;
    status.textContent = "Locked";
    status.className = "badge err";
  } else if (adminKey) {
    bar.hidden = true;
    status.hidden = false;
    status.textContent = "Unlocked";
    status.className = "badge ok";
  } else {
    bar.hidden = true;
    status.hidden = true;
  }
}

function initKeyUI() {
  showKeyBar(false);
  $("api-key-save")?.addEventListener("click", () => {
    adminKey = $("api-key-input").value.trim();
    if (adminKey) sessionStorage.setItem(KEY_STORAGE, adminKey);
    else sessionStorage.removeItem(KEY_STORAGE);
    $("api-key-input").value = "";
    showKeyBar(false);
    loadStatus();
  });
  $("api-key-input")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") $("api-key-save").click();
  });
}

// ---------------------------------------------------------------- THEME

const THEME_KEY = "vibe_router_theme";

function initTheme() {
  const saved = localStorage.getItem(THEME_KEY) || localStorage.getItem("vr_theme");
  const preferred = window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
  applyTheme(saved || preferred);

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
  if (icon) icon.textContent = theme === "dark" ? "☼" : "☾";
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
  const headers = { ...(opts.headers || {}), ...authHeaders() };
  const resp = await fetch(path, { ...opts, headers });
  if (resp.status === 401) {
    showKeyBar(true);
    throw new Error("Authentication required — enter the admin API key.");
  }
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

// Raw fetch that still carries the admin key (playground, relay script).
function authFetch(path, opts = {}) {
  const headers = { ...(opts.headers || {}), ...authHeaders() };
  return fetch(path, { ...opts, headers });
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

    if (costsData && costsData.metrics) {
      const m = costsData.metrics;
      const totalMicros = costsData.breakdown?.total_micros || 0;
      $("stat-total-spend").textContent = fmtUsd(totalMicros);
      $("stat-avg-latency").textContent = `${m.avg_latency_ms || 0} ms`;
      $("stat-percentiles").textContent = `${m.p50_latency_ms || 0} / ${m.p95_latency_ms || 0} / ${m.p99_latency_ms || 0} ms`;
    }

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

    const cache = statusData.cache || {};
    const cacheHits = cache.hits || 0;
    const cacheMisses = cache.misses || 0;
    const cacheRate = ((cache.hit_rate || 0) * 100).toFixed(1);
    $("stat-cache-rate").textContent = `${cacheRate}%`;
    const cachedTotal = (cache.cached_prompts || 0) + (cache.cached_streams || 0);
    $("stat-cache-count").textContent = fmtTokens(cachedTotal);
    $("stat-cache-mem").textContent = `${((cache.memory_bytes || 0) / 1_048_576).toFixed(1)} MB`;
    const cacheBadge = $("cache-badge");
    cacheBadge.textContent = `${cacheHits} hits / ${cacheMisses} misses`;
    cacheBadge.className = `badge ${cacheHits > 0 && cacheHits > cacheMisses ? "ok" : ""}`;

    loadAssignmentPreview();
  } catch (e) {
    console.error("Failed to load status:", e);
  }
}

// ---------------------------------------------------------------- ROLES / ASSIGNMENT

function renderRoleCard(roleKey, roleData) {
  const card = el("div", `role-card role-${roleKey}`);

  const header = el("div", "role-header");
  header.appendChild(el("span", "role-title", roleKey));
  const status = roleData.status || "ready";
  const statusCls =
    status === "ready" ? "ok" : status === "cooling" ? "cooling" : status === "rate_limited" ? "rate_limited" : "";
  header.appendChild(badge(status === "rate_limited" ? "rate limited" : status, statusCls));
  card.appendChild(header);

  const modelWrap = el("div");
  const model = String(roleData.model || "");
  const isVr = model.startsWith("VIBE_ROUTER_") || model.startsWith("VR_");
  modelWrap.appendChild(el("span", isVr ? "badge badge-vibe-router-combo" : "role-model", model));
  card.appendChild(modelWrap);

  const meta = el("div", "role-meta");
  meta.appendChild(badge(roleData.class || "generic", "role-class-badge"));
  if (roleData.context_window) {
    meta.appendChild(badge(`${fmtTokens(roleData.context_window)} ctx`, "role-class-badge"));
  }
  if (roleData.recommended_ticket_size) {
    meta.appendChild(badge(`Ticket ${roleData.recommended_ticket_size}`, "role-class-badge"));
  }
  card.appendChild(meta);

  return card;
}

async function loadAssignment() {
  const grid = $("roles-full-grid");
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
    grid.replaceChildren();
    for (const [key, role] of Object.entries(data.roles)) {
      grid.appendChild(renderRoleCard(key, role));
    }
  } catch (e) {
    grid.replaceChildren(el("p", "muted", `Could not load assignments: ${e.message}`));
  }
}

async function loadAssignmentPreview() {
  try {
    const data = await api("/v1/assignment");
    const previewGrid = $("roles-preview-grid");
    previewGrid.replaceChildren();
    for (const [key, role] of Object.entries(data.roles)) {
      previewGrid.appendChild(renderRoleCard(key, role));
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
    rows.replaceChildren();
    $("provider-note").hidden = providers.length > 0;

    const circuitMap = {};
    for (const c of healthData.circuits || []) circuitMap[c.provider] = c;

    for (const p of providers) {
      const tr = el("tr");

      const modelsCell = td("");
      if (p.models.length > 0) {
        for (const m of p.models) {
          const h = m.health || "ready";
          const badgeCls = h === "cooling" ? "cooling" : h === "rate_limited" ? "rate_limited" : "ok";
          modelsCell.appendChild(badge(m.id, badgeCls, `status: ${h}`));
          modelsCell.appendChild(document.createTextNode(" "));
        }
      } else {
        modelsCell.appendChild(el("span", "muted font-xs", "(passthrough all)"));
      }

      const originCell = td("");
      originCell.appendChild(p.from_file ? badge("file", "") : badge("runtime", "ok"));
      originCell.appendChild(document.createTextNode(" "));
      const keyCount = p.key_count || 0;
      const healthyKeys = p.healthy_keys ?? keyCount;
      originCell.appendChild(
        keyCount === 0
          ? badge("no key", "")
          : badge(`${healthyKeys}/${keyCount} keys`, healthyKeys === 0 ? "cooling" : "ok", "healthy / total keys")
      );

      const circuit = circuitMap[p.id];
      const circuitState = circuit ? circuit.state : "closed";
      const circuitBadge =
        circuitState === "open"
          ? badge("circuit open", "err", "Circuit OPEN (5xx tripped)")
          : circuitState === "half-open"
            ? badge("circuit half-open", "warn", "Circuit HALF-OPEN (probing)")
            : badge("circuit closed", "ok", "Circuit CLOSED (healthy)");

      tr.appendChild(td("mono font-bold", p.id));
      tr.appendChild(td("mono muted font-xs", p.base_url));
      tr.appendChild(modelsCell);
      tr.appendChild(originCell);
      tr.appendChild(td("", circuitBadge));
      rows.appendChild(tr);
    }
  } catch (e) {
    console.error("Failed to load providers:", e);
  }
}

// ---------------------------------------------------------------- COMBOS

async function loadCombos() {
  try {
    const [combos, models] = await Promise.all([api("/api/combos"), api("/v1/models")]);

    $("combos-count").textContent = `${combos.length} combos`;
    const list = $("combo-list");
    list.replaceChildren();

    const dl = $("model-list");
    dl.replaceChildren();
    for (const m of models.data || []) {
      const opt = el("option");
      opt.value = m.id;
      dl.appendChild(opt);
    }

    for (const c of combos) {
      const isVr = c.name.startsWith("VIBE_ROUTER_") || c.name.startsWith("VR_");
      const div = el("div", `combo ${isVr ? "is-vibe-router-combo is-vr-combo" : ""}`);

      div.appendChild(el("span", "name", c.name));
      if (isVr) div.appendChild(badge("AUTO", "badge-vibe-router-combo", "Generated auto-combo"));
      div.appendChild(
        c.from_file
          ? badge("file", "", "Declared in vibe-router.toml")
          : badge("runtime", "ok", "Authored in runtime")
      );
      div.appendChild(el("span", "chain", c.models.join(" → ")));

      const actions = el("span", "actions");
      const testBtn = el("button", "btn-test-combo", "Select Test");
      testBtn.dataset.name = c.name;
      testBtn.addEventListener("click", () => {
        $("test-model").value = c.name;
        showPage("combos");
        $("test-prompt").focus();
      });
      actions.appendChild(testBtn);

      if (!c.from_file) {
        const delBtn = el("button", "btn-del danger", "Delete");
        delBtn.dataset.name = c.name;
        delBtn.addEventListener("click", async () => {
          if (!confirm(`Delete runtime combo "${c.name}"?`)) return;
          await api(`/api/combos/${encodeURIComponent(c.name)}`, { method: "DELETE" });
          loadCombos();
        });
        actions.appendChild(delBtn);
      }

      div.appendChild(actions);
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
  const models = $("combo-models").value.split(",").map((s) => s.trim()).filter(Boolean);

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
    tbody.replaceChildren();
    $("alias-note").hidden = keys.length > 0;

    for (const key of keys) {
      const tr = el("tr");
      tr.appendChild(td("mono font-bold", key));
      tr.appendChild(td("mono muted font-xs", aliases[key]));

      const actionCell = td("right");
      const delBtn = el("button", "btn-del danger font-xs", "Delete");
      delBtn.dataset.alias = key;
      delBtn.addEventListener("click", async () => {
        if (!confirm(`Delete alias "${key}"?`)) return;
        await api(`/api/aliases/${encodeURIComponent(key)}`, { method: "DELETE" });
        loadAliases();
      });
      actionCell.appendChild(delBtn);
      tr.appendChild(actionCell);
      tbody.appendChild(tr);
    }
  } catch (e) {
    console.error("Failed to load aliases:", e);
  }
});

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
    tbody.replaceChildren();
    $("proxy-note").hidden = proxies.length > 0;

    for (const p of proxies) {
      const tr = el("tr");
      tr.appendChild(td("mono font-bold", p.id));
      tr.appendChild(td("", badge(p.tag, p.tag === "cf-worker" ? "badge-vibe-router-combo" : "ok")));
      tr.appendChild(td("mono muted font-xs", p.url));
      tr.appendChild(td("", p.enabled ? badge("active", "ok") : badge("disabled", "cooling")));

      const actionCell = td("right");
      const delBtn = el("button", "btn-del danger font-xs", "Delete");
      delBtn.dataset.id = p.id;
      delBtn.addEventListener("click", async () => {
        if (!confirm(`Remove proxy "${p.id}"?`)) return;
        await api(`/api/proxies/${encodeURIComponent(p.id)}`, { method: "DELETE" });
        loadProxies();
      });
      actionCell.appendChild(delBtn);
      tr.appendChild(actionCell);
      tbody.appendChild(tr);
    }
  } catch (e) {
    console.error("Failed to load proxies:", e);
  }
});

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
    const resp = await authFetch("/api/relay/worker.js");
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

    const fillSpend = (tbodyId, entries) => {
      const tbody = $(tbodyId);
      tbody.replaceChildren();
      for (const [name, line] of Object.entries(entries || {})) {
        const tr = el("tr");
        tr.appendChild(td("mono font-bold", name));
        tr.appendChild(td("", fmtTokens(line.requests)));
        tr.appendChild(td("", fmtTokens(line.prompt_tokens)));
        tr.appendChild(td("", fmtTokens(line.completion_tokens)));
        tr.appendChild(td("right mono font-bold ok", fmtUsd(line.cost_micros)));
        tbody.appendChild(tr);
      }
    };
    fillSpend("costs-provider-rows", b.by_provider);
    fillSpend("costs-model-rows", b.by_model);
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
    const resp = await authFetch("/v1/chat/completions", {
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
    tbody.replaceChildren();
    $("usage-note").hidden = rows.length > 0;

    for (const r of rows) {
      const isEst = r.usage_estimated;
      const prefix = isEst ? "~" : "";
      const promptTok = r.prompt_tokens ? `${prefix}${fmtTokens(r.prompt_tokens)}` : "0";
      const compTok = r.completion_tokens ? `${prefix}${fmtTokens(r.completion_tokens)}` : "0";

      const tokenCell = td("mono right", `${promptTok} / ${compTok}`);
      if (isEst) {
        tokenCell.appendChild(document.createTextNode(" "));
        const est = el("span", "estimated", "~est");
        est.title = "Estimated, not reported by provider";
        tokenCell.appendChild(est);
      }

      const tr = el("tr");
      tr.appendChild(td("mono muted font-xs", fmtTime(r.ts_ms)));
      tr.appendChild(td("mono font-bold", r.requested_model));
      tr.appendChild(td("mono muted font-xs", `${r.provider || "—"} / ${r.served_model || "—"}`));
      tr.appendChild(td("", badge(r.status, r.status < 300 ? "ok" : "err")));
      tr.appendChild(tokenCell);
      tr.appendChild(td("mono right muted font-xs", `${r.latency_ms} ms`));
      tbody.appendChild(tr);
    }
  } catch (e) {
    console.error("Failed to load usage:", e);
  }
}

// ---------------------------------------------------------------- INIT

initTheme();
initKeyUI();
loadStatus();
$("btn-refresh-status")?.addEventListener("click", loadStatus);
setInterval(loadStatus, 5000);