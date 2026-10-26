//! HTTP routes: the OpenAI-compatible `/v1/*` surface, the `/api/*` admin
//! surface the dashboard consumes, and the embedded UI.
//!
//! Handlers stay thin: parse, call into the wired stack, shape the response.
//! Every policy decision — failover, cooldowns, rate limits — happens in the
//! layers below; nothing here re-implements any of it.

use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use vibe_router_core::{Candidate, ModelRegistry, RouterError, UpstreamResponse};

use crate::assignment::get_assignment;
use crate::sse;
use crate::state::AppState;

/// Extract the bearer token from an Authorization header value.
fn bearer(auth: &str) -> Option<&str> {
    auth.strip_prefix("Bearer ").map(str::trim)
}

// ---------------------------------------------------------------- /v1

/// `POST /v1/chat/completions` — the entire product, one endpoint.
pub async fn chat_completions(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();
    let requested_model = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    // Auth first: fail before doing any work. Supports Bearer and X-API-Key.
    if let Some(err) = check_auth(&state, &headers) {
        return err;
    }
    if requested_model.is_empty() {
        return error_response(&RouterError::BadRequest(
            "`model` is required and must be a string".into(),
        ));
    }

    let router = state.router();

    // Vibe-Link context headers
    let role = headers
        .get("x-vibe-role")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let is_retry = headers
        .get("x-vibe-retry")
        .and_then(|v| v.to_str().ok())
        .map(|s| s == "1" || s == "true")
        .unwrap_or(false)
        || headers
            .get(header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.contains("no-cache") || s.contains("no-store"))
            .unwrap_or(false);

    // Transparent virtual model alias resolution:
    // e.g. "gpt-4o" resolves to "openai/gpt-4o-mini" or a combo chain.
    let effective_model = state.registry.resolve_alias(&requested_model);
    let mut routed_body = body.clone();
    if effective_model != requested_model {
        routed_body["model"] = Value::String(effective_model.clone());
    }
    if is_retry {
        routed_body["_vibe_retry"] = Value::Bool(true);
    }

    // Cache lookup: keyed by (model, temperature, top_p, tools, messages).
    // Bypassed if X-Vibe-Retry: 1 or Cache-Control: no-cache.
    let streaming = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
    let temperature = body.get("temperature").and_then(Value::as_f64);
    let top_p = body.get("top_p").and_then(Value::as_f64);
    let tools = body.get("tools");
    let messages = body.get("messages").cloned().unwrap_or(Value::Null);

    let cache_key = if !streaming && state.cache_enabled && !is_retry {
        Some(vibe_router_cache::compute_cache_key(
            &requested_model,
            temperature,
            top_p,
            tools,
            &messages,
        ))
    } else {
        None
    };

    let stream_cache_key =
        if streaming && state.cache_enabled && state.stream_cache_enabled && !is_retry {
            Some(vibe_router_cache::compute_cache_key(
                &requested_model,
                temperature,
                top_p,
                tools,
                &messages,
            ))
        } else {
            None
        };

    if let Some(key) = &cache_key {
        if let Some(hit) = state.cache.get(key) {
            tracing::info!(model = %requested_model, "cache HIT");
            let mut resp = (StatusCode::OK, Json(hit.body)).into_response();
            attach_telemetry_headers(&mut resp, 0, 0, true, "cache");
            return resp;
        }
    }

    if let Some(key) = &stream_cache_key {
        if let Some(hit) = state.cache.get_stream(key) {
            tracing::info!(model = %requested_model, "stream cache HIT");
            let stream = futures_util::stream::iter(
                hit.frames
                    .into_iter()
                    .map(Ok::<_, std::convert::Infallible>),
            );
            let mut resp = Response::new(Body::from_stream(stream));
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/event-stream"),
            );
            resp.headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            attach_telemetry_headers(&mut resp, 0, 0, true, "cache");
            return resp;
        }
    }

    let routed = router.route(routed_body).await;

    match routed {
        Ok(routed) => {
            let candidate = routed.candidate;
            let provider = routed.provider;
            let mut resp_body = match &routed.response {
                UpstreamResponse::Json { body, .. } => body.clone(),
                UpstreamResponse::Stream { .. } => Value::Null, // handled below
                UpstreamResponse::Transport(e) => {
                    // The router never returns Ok with a transport failure;
                    // reaching here is an internal invariant break.
                    tracing::error!("router returned Ok(Transport): {e:?}");
                    return error_response(&RouterError::Internal(
                        "upstream transport failure escaped the router".into(),
                    ));
                }
            };
            if !resp_body.is_null() {
                // Non-streaming: echo the client's model string back and
                // record usage with the reported/estimated flag intact.
                if resp_body.is_object() {
                    resp_body["model"] = Value::String(requested_model.clone());
                }
                let usage = routed.usage.unwrap_or_default();
                // Store in cache before returning (post model-echo, so a hit
                // replays exactly what the first caller saw).
                if let Some(key) = &cache_key {
                    state.cache.put(key.clone(), resp_body.clone(), Some(usage));
                }
                let rec = record_for(
                    &requested_model,
                    &candidate,
                    &provider,
                    StatusCode::OK.as_u16(),
                    false,
                    &usage,
                    started,
                    routed.fell_back_past as u32,
                    role,
                );
                state.store.append(rec.clone());
                let b = state.backend.clone();
                let rec_clone = rec.clone();
                tokio::spawn(async move {
                    let _ = b.record_usage(&rec_clone).await;
                });
                state
                    .health
                    .record_provider_latency(&provider, started.elapsed().as_millis() as u64);

                tracing::info!(
                    model = %requested_model,
                    served_by = %candidate.model,
                    fallbacks = routed.fell_back_past,
                    "non-streaming completion served"
                );
                let latency_ms = started.elapsed().as_millis() as u64;
                let mut resp = (StatusCode::OK, Json(resp_body)).into_response();
                attach_telemetry_headers(&mut resp, rec.cost_micros, latency_ms, false, &provider);
                return resp;
            }

            // Streaming: pump upstream chunks into the client connection.
            let combo = candidate.combo.clone();
            let served_model = candidate.model.clone();
            let (body, outcome_rx) = stream_body(routed.response, candidate, &requested_model);
            let provider_echo = provider.clone();
            let state2 = state.clone();
            let started2 = started;
            let failovers = routed.fell_back_past as u32;
            let request_model = requested_model.clone();
            let s_cache_key = stream_cache_key.clone();
            let role_clone = role.clone();
            tokio::spawn(async move {
                let outcome = outcome_rx.await.unwrap_or(sse::StreamOutcome {
                    usage: vibe_router_core::Usage::default(),
                    completed: false,
                    events_sent: 0,
                    recorded_frames: Vec::new(),
                });
                if let Some(key) = s_cache_key {
                    if outcome.completed && !outcome.recorded_frames.is_empty() {
                        state2
                            .cache
                            .put_stream(key, outcome.recorded_frames, Some(outcome.usage));
                    }
                }
                let mut candidate_rec = Candidate::direct(&served_model);
                candidate_rec.combo = combo;
                let rec = record_for(
                    &request_model,
                    &candidate_rec,
                    &provider_echo,
                    if outcome.completed { 200 } else { 502 },
                    true,
                    &outcome.usage,
                    started2,
                    failovers,
                    role_clone,
                );
                state2.store.append(rec.clone());
                let b = state2.backend.clone();
                let _ = b.record_usage(&rec).await;
                state2
                    .health
                    .record_provider_latency(&provider_echo, started2.elapsed().as_millis() as u64);
            });
            let latency_ms = started.elapsed().as_millis() as u64;
            let mut resp = Response::new(body);
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/event-stream"),
            );
            resp.headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            attach_telemetry_headers(&mut resp, 0, latency_ms, false, &provider);
            resp
        }
        Err(err) => {
            let status = err.status();
            state.store.append(record_for(
                &requested_model,
                &Candidate::direct(&requested_model),
                "",
                status,
                false,
                &vibe_router_core::Usage::default(),
                started,
                0,
                role,
            ));
            error_response(&err)
        }
    }
}

/// `POST /v1/messages` — Anthropic Claude Code compatible messages endpoint.
pub async fn anthropic_messages(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let started = Instant::now();

    if let Some(err) = check_auth(&state, &headers) {
        return err;
    }

    let requested_model = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if requested_model.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "message": "missing `model` field"
                }
            })),
        )
            .into_response();
    }

    let openai_body = match vibe_router_translate::anthropic_to_openai_request(&body) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "type": "error",
                    "error": {
                        "type": "invalid_request_error",
                        "message": e
                    }
                })),
            )
                .into_response();
        }
    };

    let router = state.router();
    let effective_model = state.registry.resolve_alias(&requested_model);
    let mut routed_body = openai_body.clone();
    if effective_model != requested_model {
        routed_body["model"] = Value::String(effective_model.clone());
    }

    let streaming = body.get("stream").and_then(Value::as_bool).unwrap_or(false);
    let is_retry = headers
        .get("x-vibe-retry")
        .and_then(|v| v.to_str().ok())
        .map(|s| s == "1" || s == "true")
        .unwrap_or(false)
        || headers
            .get(header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.contains("no-cache") || s.contains("no-store"))
            .unwrap_or(false);

    if is_retry {
        routed_body["_vibe_retry"] = Value::Bool(true);
    }

    let role = headers
        .get("x-vibe-role")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let cache_key = if !streaming && state.cache_enabled && !is_retry {
        Some(vibe_router_cache::compute_cache_key(
            &requested_model,
            body.get("temperature").and_then(Value::as_f64),
            body.get("top_p").and_then(Value::as_f64),
            body.get("tools"),
            &routed_body.get("messages").cloned().unwrap_or(Value::Null),
        ))
    } else {
        None
    };

    if let Some(key) = &cache_key {
        if let Some(hit) = state.cache.get(key) {
            tracing::info!(model = %requested_model, "Anthropic cache HIT");
            let anthropic_resp =
                vibe_router_translate::openai_to_anthropic_response(&hit.body, &requested_model);
            let mut resp = (StatusCode::OK, Json(anthropic_resp)).into_response();
            attach_telemetry_headers(&mut resp, 0, 0, true, "cache");
            return resp;
        }
    }

    let routed = router.route(routed_body).await;

    match routed {
        Ok(routed) => {
            let candidate = routed.candidate;
            let provider = routed.provider;

            match routed.response {
                UpstreamResponse::Json {
                    body: openai_resp_body,
                    ..
                } => {
                    let mut final_openai = openai_resp_body;
                    if final_openai.is_object() {
                        final_openai["model"] = Value::String(requested_model.clone());
                    }
                    let usage = routed.usage.unwrap_or_default();
                    if let Some(key) = &cache_key {
                        state
                            .cache
                            .put(key.clone(), final_openai.clone(), Some(usage));
                    }
                    let rec = record_for(
                        &requested_model,
                        &candidate,
                        &provider,
                        StatusCode::OK.as_u16(),
                        false,
                        &usage,
                        started,
                        routed.fell_back_past as u32,
                        role,
                    );
                    state.store.append(rec.clone());
                    let b = state.backend.clone();
                    let rec_clone = rec.clone();
                    tokio::spawn(async move {
                        let _ = b.record_usage(&rec_clone).await;
                    });
                    state
                        .health
                        .record_provider_latency(&provider, started.elapsed().as_millis() as u64);

                    let anthropic_resp = vibe_router_translate::openai_to_anthropic_response(
                        &final_openai,
                        &requested_model,
                    );
                    let latency_ms = started.elapsed().as_millis() as u64;
                    let mut resp = (StatusCode::OK, Json(anthropic_resp)).into_response();
                    attach_telemetry_headers(
                        &mut resp,
                        rec.cost_micros,
                        latency_ms,
                        false,
                        &provider,
                    );
                    resp
                }
                UpstreamResponse::Stream { mut events, .. } => {
                    let combo = candidate.combo.clone();
                    let served_model = candidate.model.clone();
                    let provider_echo = provider.clone();
                    let state2 = state.clone();
                    let started2 = started;
                    let failovers = routed.fell_back_past as u32;
                    let request_model = requested_model.clone();
                    let role_clone = role.clone();

                    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(64);
                    let mut stream_state =
                        vibe_router_translate::AnthropicStreamState::new(&requested_model);

                    tokio::spawn(async move {
                        let mut acc = vibe_router_translate::UsageAccumulator::new();
                        while let Some(event) = events.recv().await {
                            match event {
                                vibe_router_core::StreamEvent::Chunk(chunk_data) => {
                                    acc.observe(&chunk_data);
                                    let anthropic_events =
                                        vibe_router_translate::openai_chunk_to_anthropic_events(
                                            &chunk_data,
                                            &mut stream_state,
                                        );
                                    for ev in anthropic_events {
                                        if tx.send(ev.into_bytes()).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                                vibe_router_core::StreamEvent::Done => {
                                    let done_evs =
                                        vibe_router_translate::openai_chunk_to_anthropic_events(
                                            "[DONE]",
                                            &mut stream_state,
                                        );
                                    for ev in done_evs {
                                        let _ = tx.send(ev.into_bytes()).await;
                                    }
                                    break;
                                }
                                vibe_router_core::StreamEvent::Error(err) => {
                                    tracing::warn!("anthropic stream error: {err}");
                                    break;
                                }
                            }
                        }
                        let final_usage = acc.finish();
                        let mut cand_rec = Candidate::direct(&served_model);
                        cand_rec.combo = combo;
                        let rec = record_for(
                            &request_model,
                            &cand_rec,
                            &provider_echo,
                            200,
                            true,
                            &final_usage,
                            started2,
                            failovers,
                            role_clone,
                        );
                        state2.store.append(rec.clone());
                        let b = state2.backend.clone();
                        let _ = b.record_usage(&rec).await;
                        state2.health.record_provider_latency(
                            &provider_echo,
                            started2.elapsed().as_millis() as u64,
                        );
                    });

                    let stream = tokio_stream_from_receiver(rx);
                    let latency_ms = started.elapsed().as_millis() as u64;
                    let mut resp = Response::new(Body::from_stream(stream));
                    resp.headers_mut().insert(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("text/event-stream; charset=utf-8"),
                    );
                    resp.headers_mut()
                        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
                    attach_telemetry_headers(&mut resp, 0, latency_ms, false, &provider);
                    resp
                }
                UpstreamResponse::Transport(e) => error_response(&RouterError::Internal(format!(
                    "upstream transport error: {e:?}"
                ))),
            }
        }
        Err(err) => (
            StatusCode::from_u16(err.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(json!({
                "type": "error",
                "error": {
                    "type": "api_error",
                    "message": err.message()
                }
            })),
        )
            .into_response(),
    }
}

pub(crate) fn check_auth(
    state: &Arc<AppState>,
    headers: &axum::http::HeaderMap,
) -> Option<Response> {
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(bearer)
        .or_else(|| {
            headers
                .get("x-api-key")
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
        });
    if state.auth.lock().verify(presented) {
        None
    } else {
        Some(error_response(&RouterError::Unauthorized(
            "Missing or invalid API key".into(),
        )))
    }
}

#[allow(clippy::too_many_arguments)]
fn record_for(
    requested: &str,
    candidate: &Candidate,
    provider: &str,
    status: u16,
    streamed: bool,
    usage: &vibe_router_core::Usage,
    started: Instant,
    failovers: u32,
    role: Option<String>,
) -> vibe_router_storage::UsageRecord {
    let rates = vibe_router_storage::Rates::default_frontier_rates();
    let model_bare = candidate
        .model
        .split_once('/')
        .map(|(_, m)| m)
        .unwrap_or(&candidate.model);
    let cost = rates
        .get(model_bare)
        .or_else(|| rates.get(&candidate.model))
        .or_else(|| rates.get(requested))
        .map(|r| r.cost_micros(usage.prompt_tokens, usage.completion_tokens))
        .unwrap_or(0);

    vibe_router_storage::UsageRecord {
        requested_model: requested.to_string(),
        served_model: candidate.model.clone(),
        provider: provider.to_string(),
        combo: candidate.combo.clone(),
        status,
        streamed,
        prompt_tokens: usage.prompt_tokens,
        completion_tokens: usage.completion_tokens,
        usage_estimated: usage.estimated,
        latency: started.elapsed(),
        failovers,
        cost_micros: cost,
        role,
        ..vibe_router_storage::UsageRecord::default()
    }
}

fn attach_telemetry_headers(
    resp: &mut Response,
    cost_micros: u64,
    latency_ms: u64,
    cache_hit: bool,
    provider: &str,
) {
    let h = resp.headers_mut();
    if let Ok(cv) = HeaderValue::from_str(&cost_micros.to_string()) {
        h.insert("x-vibe-router-cost-micros", cv.clone());
        h.insert("x-vr-cost-micros", cv);
    }
    if let Ok(lv) = HeaderValue::from_str(&latency_ms.to_string()) {
        h.insert("x-vibe-router-latency-ms", lv.clone());
        h.insert("x-vr-latency-ms", lv);
    }
    let cache_val = if cache_hit {
        HeaderValue::from_static("HIT")
    } else {
        HeaderValue::from_static("MISS")
    };
    h.insert("x-vibe-router-cache", cache_val.clone());
    h.insert("x-vr-cache", cache_val);

    if !provider.is_empty() {
        if let Ok(pv) = HeaderValue::from_str(provider) {
            h.insert("x-vibe-router-provider", pv.clone());
            h.insert("x-vr-provider", pv);
        }
    }
}

fn error_response(err: &RouterError) -> Response {
    crate::error::into_response(err)
}

/// Bridge the channel-based SSE relay into an axum response body.
///
/// The relay task pushes encoded frames into a bounded channel;
/// `Body::from_stream` drains it into the client connection. When the client
/// disconnects, the body is dropped, the channel closes, and the relay's send
/// fails — stopping the relay, which drops the upstream receiver and tears
/// down the provider connection. The relay's outcome (usage, completed) comes
/// back on a oneshot so the usage record can be written after the last byte.
fn stream_body(
    response: UpstreamResponse,
    candidate: Candidate,
    requested_model: &str,
) -> (Body, tokio::sync::oneshot::Receiver<sse::StreamOutcome>) {
    const CHANNEL_CAPACITY: usize = 64;
    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(CHANNEL_CAPACITY);
    let (outcome_tx, outcome_rx) = tokio::sync::oneshot::channel();
    let model = requested_model.to_string();
    tokio::spawn(async move {
        let outcome = sse::relay(&candidate, response, &model, tx).await;
        let _ = outcome_tx.send(outcome);
    });
    let stream = tokio_stream_from_receiver(rx);
    (Body::from_stream(stream), outcome_rx)
}

/// Adapt an mpsc receiver into the `Stream` axum wants: frames arrive as plain
/// bytes and are wrapped in `Ok` here — the relay never produces a body error,
/// it simply stops on failure and the stream ends.
fn tokio_stream_from_receiver(
    mut rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
) -> impl futures_util::stream::Stream<Item = Result<Vec<u8>, std::convert::Infallible>> {
    futures_util::stream::poll_fn(move |cx| rx.poll_recv(cx).map(|opt| opt.map(Ok)))
}

/// `GET /v1/models` — every addressable id: concrete provider/model entries
/// plus combo names, exactly what resolution accepts.
pub async fn models(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(err) = check_auth(&state, &headers) {
        return err;
    }
    let list = router_registry(&state).list();
    let data: Vec<Value> = list
        .into_iter()
        .map(|m| json!({"id": m.id, "object": "model", "owned_by": m.owned_by}))
        .collect();
    Json(json!({"object": "list", "data": data})).into_response()
}

fn router_registry(state: &Arc<AppState>) -> Arc<vibe_router_providers::Registry> {
    state.registry.clone()
}

// ---------------------------------------------------------------- /api (dashboard)

/// `GET /api/status` — return full payload of active providers, combos, and health metrics.
pub async fn api_status(State(state): State<Arc<AppState>>) -> Response {
    let health = state.health.snapshot();
    let cfg_auth = state.auth.lock().enabled();
    let registry = state.registry.clone();
    let totals = state.store.totals().unwrap_or_default();

    let mut providers = Vec::new();
    for id in registry.provider_ids() {
        if let Some(p) = registry.provider(&id) {
            providers.push(json!({
                "id": p.id,
                "display_name": p.display_name,
                "base_url": p.transport.base_url,
                "models": p.models.iter().map(|m| &m.id).collect::<Vec<_>>(),
                "passthrough": p.passthrough,
                "from_file": registry.is_file_provider(&p.id),
            }));
        }
    }

    let mut combos = Vec::new();
    for info in registry.list() {
        if info.owned_by == "combo" {
            if let Some(c) = registry.combo(&info.id) {
                combos.push(json!({
                    "name": c.name,
                    "description": c.description,
                    "models": c.models,
                    "from_file": registry.is_file_combo(&c.name),
                }));
            }
        }
    }

    let now = std::time::Instant::now();
    let health_metrics: Vec<Value> = health
        .iter()
        .map(|h| {
            json!({
                "model": h.model,
                "cooling": h.is_cooling(),
                "cooling_until_ms": h.cooling_until.map(|t| {
                    t.checked_duration_since(now).map(|d| d.as_millis()).unwrap_or(0)
                }),
                "backoff_level": h.backoff_level,
                "rpm_used": h.rpm_used,
                "rpm_limit": h.rpm_limit,
                "last_error_status": h.last_error.as_ref().map(|e| e.status),
            })
        })
        .collect();

    Json(json!({
        "endpoint": format!("http://{}/v1", state.bind_addr),
        "auth_required": cfg_auth,
        "models_tracked": health.len(),
        "providers": providers,
        "combos": combos,
        "health": health_metrics,
        "totals": {
            "requests": totals.requests,
            "succeeded": totals.succeeded,
            "failed": totals.failed,
            "prompt_tokens": totals.prompt_tokens,
            "completion_tokens": totals.completion_tokens,
            "estimated_usage_rows": totals.estimated_usage_rows,
        },
        "cache": state.cache.stats(),
    }))
    .into_response()
}

/// `GET /api/cache/stats` — cache metrics for dashboards and agents.
pub async fn api_cache_stats(State(state): State<Arc<AppState>>) -> Response {
    Json(state.cache.stats()).into_response()
}

/// `POST /api/cache/purge` — drop every cached prompt/response.
pub async fn api_cache_purge(State(state): State<Arc<AppState>>) -> Response {
    state.cache.purge();
    tracing::info!("prompt cache purged via API");
    Json(json!({"status": "purged"})).into_response()
}

/// `GET /api/providers` — providers with per-model health.
pub async fn api_providers(State(state): State<Arc<AppState>>) -> Response {
    let registry = state.registry.clone();
    let health = state.health.snapshot();
    let mut out = Vec::new();
    for id in registry.provider_ids() {
        let Some(p) = registry.provider(&id) else {
            continue;
        };
        let models: Vec<Value> = p
            .models
            .iter()
            .map(|m| {
                let qualified = format!("{}/{}", p.id, m.id);
                let h = health.iter().find(|x| x.model == qualified);
                let health_word = match h {
                    Some(x) if x.is_cooling() => "cooling",
                    Some(_) => "healthy",
                    None => "unknown",
                };
                json!({"id": m.id, "health": health_word})
            })
            .collect();
        out.push(json!({
            "id": p.id,
            "display_name": p.display_name,
            "base_url": p.transport.base_url,
            "from_file": registry.is_file_provider(&p.id),
            "passthrough": p.passthrough,
            "key_count": p.key_pool.total_keys(),
            "healthy_keys": p.key_pool.healthy_keys(),
            "models": models,
        }));
    }
    Json(Value::Array(out)).into_response()
}

#[derive(serde::Deserialize)]
pub struct ProviderInput {
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    pub base_url: String,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub auth_scheme: Option<String>,
    #[serde(default)]
    pub auth_secret: Option<String>,
    /// Multiple API keys for pool rotation.
    #[serde(default)]
    pub api_keys: Vec<String>,
    /// "round-robin" (default) or "least-loaded".
    #[serde(default)]
    pub key_strategy: Option<String>,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub models: Vec<RuntimeModelInput>,
    #[serde(default)]
    pub passthrough: bool,
}

#[derive(serde::Deserialize)]
pub struct RuntimeModelInput {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub context: Option<u32>,
    #[serde(default)]
    pub max_output: Option<u32>,
}

/// `POST /api/providers` — add or update a runtime provider.
pub async fn api_provider_create(
    State(state): State<Arc<AppState>>,
    Json(input): Json<ProviderInput>,
) -> Response {
    let id = input.id.trim().to_string();
    if id.is_empty() {
        return bad_request("provider id is required");
    }
    if id.contains('/') {
        return bad_request("provider id cannot contain `/`");
    }
    let base_url = input.base_url.trim().to_string();
    if base_url.is_empty()
        || (!base_url.starts_with("http://") && !base_url.starts_with("https://"))
    {
        return bad_request("base_url must start with http:// or https://");
    }

    let rp = vibe_router_storage::RuntimeProvider {
        id: id.clone(),
        display_name: input.display_name.clone(),
        base_url,
        format: input.format.clone(),
        auth_scheme: input.auth_scheme.clone(),
        auth_secret: input.auth_secret.clone(),
        api_keys: input.api_keys.clone(),
        key_strategy: input.key_strategy.clone(),
        headers: input.headers.clone(),
        models: input
            .models
            .into_iter()
            .map(|m| vibe_router_storage::RuntimeModel {
                id: m.id,
                name: m.name,
                context: m.context,
                max_output: m.max_output,
            })
            .collect(),
        passthrough: input.passthrough,
    };

    let p_cfg = crate::state::provider_from_runtime(&rp);
    let config_provider = vibe_router_providers::build_provider(&p_cfg);
    state.registry.set_runtime_provider(config_provider);
    if let Err(e) = state.store.upsert_provider(&rp) {
        tracing::warn!("provider registered in memory but failed to persist: {e}");
    }
    tracing::info!(provider = %id, "runtime provider saved");
    (
        StatusCode::CREATED,
        Json(json!({"id": id, "status": "saved"})),
    )
        .into_response()
}

/// `DELETE /api/providers/:id` — remove a runtime provider.
pub async fn api_provider_delete(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    if state.registry.is_file_provider(&id) && !state.registry.runtime_provider_ids().contains(&id)
    {
        return bad_request(&format!(
            "`{id}` is declared in the config file; remove it from vibe-router.toml"
        ));
    }
    state.registry.remove_runtime_provider(&id);
    let _ = state.store.delete_provider(&id);
    // Routing changed: a cached answer for a now-removed model must not
    // survive.
    state.cache.purge();
    StatusCode::NO_CONTENT.into_response()
}

/// `GET /api/combos` — file and runtime combos with provenance.
pub async fn api_combos(State(state): State<Arc<AppState>>) -> Response {
    let registry = state.registry.clone();
    let mut out = Vec::new();
    // Merged view (what resolution sees), tagged with origin.
    for info in registry.list() {
        if info.owned_by != "combo" {
            continue;
        }
        let Some(combo) = registry.combo(&info.id) else {
            continue;
        };
        out.push(json!({
            "name": combo.name,
            "description": combo.description,
            "models": combo.models,
            "from_file": registry.is_file_combo(&combo.name),
        }));
    }
    Json(Value::Array(out)).into_response()
}

#[derive(serde::Deserialize)]
pub struct ComboInput {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub models: Vec<String>,
}

/// `POST /api/combos` — create or replace a runtime combo. Model ids are
/// validated against the registry so a typo fails here, not at request time.
pub async fn api_combo_create(
    State(state): State<Arc<AppState>>,
    Json(input): Json<ComboInput>,
) -> Response {
    let name = input.name.trim().to_string();
    if name.is_empty() {
        return bad_request("combo name is required");
    }
    if name.contains('/') {
        return bad_request("combo names cannot contain `/` (reserved for provider/model ids)");
    }
    if input.models.is_empty() {
        return bad_request("a combo needs at least one model");
    }
    let registry = state.registry.clone();
    let known = registry.known_models();
    for m in &input.models {
        // provider/model or a passthrough provider's arbitrary model
        match registry.resolve(m) {
            Ok(_) => {}
            Err(_) => {
                if !known.contains(m) {
                    return bad_request(&format!("unknown model `{m}`"));
                }
            }
        }
    }
    let combo = vibe_router_core::Combo {
        name: name.clone(),
        description: input.description.clone(),
        models: input.models.clone(),
        strategy: Default::default(),
    };
    registry.set_runtime_combo(combo.clone());
    // Persist so the combo survives a restart.
    if let Err(e) = state
        .store
        .upsert_combo(&vibe_router_storage::RuntimeCombo {
            name: input.name.clone(),
            description: input.description.clone(),
            models: input.models.clone(),
        })
    {
        tracing::warn!("combo persisted in memory but not to storage: {e}");
    }
    tracing::info!(combo = %name, "runtime combo saved");
    (StatusCode::CREATED, Json(json!({"name": name}))).into_response()
}

/// `DELETE /api/combos/:name` — remove a runtime combo. File combos are not
/// deletable here: they live in the TOML, and deleting from disk-config via
/// an API would fight the file watcher.
pub async fn api_combo_delete(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    if state.registry.is_file_combo(&name) && !state.registry.runtime_combo_names().contains(&name)
    {
        return bad_request(
            "`{name}` is declared in the config file; remove it from vibe-router.toml",
        );
    }
    state.registry.remove_runtime_combo(&name);
    // Routing changed: drop cached answers so a deleted combo stops serving.
    state.cache.purge();
    match state.store.delete_combo(&name) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": {"message": e.to_string()}})),
        )
            .into_response(),
    }
}

fn bad_request(msg: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": {"message": msg}})),
    )
        .into_response()
}

/// `GET /api/usage/recent?limit=N`
pub async fn api_usage_recent(
    State(state): State<Arc<AppState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let limit = params
        .get("limit")
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(50)
        .min(500);
    match state.store.recent(limit) {
        Ok(rows) => {
            let data: Vec<Value> = rows
                .into_iter()
                .map(|r| {
                    json!({
                        "id": r.id,
                        "ts_ms": r.ts_ms,
                        "requested_model": r.record.requested_model,
                        "served_model": r.record.served_model,
                        "provider": r.record.provider,
                        "combo": r.record.combo,
                        "status": r.record.status,
                        "streamed": r.record.streamed,
                        "prompt_tokens": r.record.prompt_tokens,
                        "completion_tokens": r.record.completion_tokens,
                        // The flag travels with the numbers: the UI renders
                        // estimates with a ~, and this field is why it can.
                        "usage_estimated": r.record.usage_estimated,
                        "latency_ms": r.record.latency.as_millis() as u64,
                        "failovers": r.record.failovers,
                    })
                })
                .collect();
            Json(Value::Array(data)).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": {"message": e.to_string()}})),
        )
            .into_response(),
    }
}

/// `GET /api/usage/totals`
pub async fn api_usage_totals(State(state): State<Arc<AppState>>) -> Response {
    match state.store.totals() {
        Ok(t) => Json(json!({
            "requests": t.requests,
            "succeeded": t.succeeded,
            "failed": t.failed,
            "prompt_tokens": t.prompt_tokens,
            "completion_tokens": t.completion_tokens,
            "estimated_usage_rows": t.estimated_usage_rows,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": {"message": e.to_string()}})),
        )
            .into_response(),
    }
}

/// `GET /api/costs` — cost breakdown and usage metrics (including p50/p95/p99 latency).
pub async fn api_costs(State(state): State<Arc<AppState>>) -> Response {
    let breakdown = state.backend.get_cost_breakdown().await.unwrap_or_default();
    let metrics = state.backend.get_usage_metrics().await.unwrap_or_default();
    Json(json!({
        "breakdown": breakdown,
        "metrics": metrics,
    }))
    .into_response()
}

/// `GET /api/health` — provider health, latencies, and circuit breaker states.
pub async fn api_health(State(state): State<Arc<AppState>>) -> Response {
    let circuits = state.health.circuit_snapshots();
    let models = state.health.snapshot();
    let models_json: Vec<Value> = models
        .into_iter()
        .map(|m| {
            json!({
                "model": m.model,
                "cooling": m.is_cooling(),
                "backoff_level": m.backoff_level,
                "rpm_used": m.rpm_used,
                "rpm_limit": m.rpm_limit,
                "last_error": m.last_error.map(|e| json!({"status": e.status})),
            })
        })
        .collect();
    Json(json!({
        "circuits": circuits,
        "models": models_json,
    }))
    .into_response()
}

/// `GET /api/aliases` — list all model aliases.
pub async fn api_aliases_list(State(state): State<Arc<AppState>>) -> Response {
    let aliases = state.registry.aliases();
    Json(json!({ "aliases": aliases })).into_response()
}

#[derive(serde::Deserialize)]
pub struct AliasInput {
    pub alias: String,
    pub target: String,
}

/// `POST /api/aliases` — register or update a virtual model alias.
pub async fn api_alias_create(
    State(state): State<Arc<AppState>>,
    Json(input): Json<AliasInput>,
) -> Response {
    let alias = input.alias.trim().to_string();
    let target = input.target.trim().to_string();
    if alias.is_empty() || target.is_empty() {
        return bad_request("alias and target cannot be empty");
    }
    state.registry.set_alias(alias.clone(), target.clone());
    if let Err(e) = state.backend.save_alias(&alias, &target).await {
        tracing::warn!("failed to persist alias: {e}");
    }
    tracing::info!(alias = %alias, target = %target, "alias registered");
    (
        StatusCode::CREATED,
        Json(json!({"alias": alias, "target": target})),
    )
        .into_response()
}

/// `DELETE /api/aliases/:alias` — remove a virtual model alias.
pub async fn api_alias_delete(
    State(state): State<Arc<AppState>>,
    Path(alias): Path<String>,
) -> Response {
    state.registry.remove_alias(&alias);
    let _ = state.backend.delete_alias(&alias).await;
    StatusCode::NO_CONTENT.into_response()
}

/// `GET /api/proxies` — list all registered tagged proxies.
pub async fn api_proxies_list(State(state): State<Arc<AppState>>) -> Response {
    let list = state.registry.proxies().list();
    Json(json!({ "proxies": list })).into_response()
}

#[derive(serde::Deserialize)]
pub struct ProxyInput {
    pub id: String,
    pub url: String,
    pub tag: String,
    #[serde(default = "default_proxy_enabled")]
    pub enabled: bool,
    #[serde(default = "default_proxy_weight")]
    pub weight: u32,
}

fn default_proxy_enabled() -> bool {
    true
}

fn default_proxy_weight() -> u32 {
    1
}

/// `POST /api/proxies` — register a tagged proxy.
pub async fn api_proxy_create(
    State(state): State<Arc<AppState>>,
    Json(input): Json<ProxyInput>,
) -> Response {
    let tag: vibe_router_providers::ProxyTag = match input.tag.parse() {
        Ok(t) => t,
        Err(e) => return bad_request(&e),
    };
    let proxy = vibe_router_providers::ProxyEntry {
        id: input.id.trim().to_string(),
        url: input.url.trim().to_string(),
        tag,
        enabled: input.enabled,
        weight: input.weight.max(1),
    };
    state.registry.proxies().add(proxy.clone());
    (StatusCode::CREATED, Json(json!({"proxy": proxy}))).into_response()
}

/// `DELETE /api/proxies/:id` — remove a tagged proxy.
pub async fn api_proxy_delete(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    state.registry.proxies().remove(&id);
    StatusCode::NO_CONTENT.into_response()
}

/// `GET /api/relay/worker.js` — Cloudflare Worker reverse-proxy script.
pub async fn api_relay_worker_script() -> Response {
    let mut resp = Response::new(Body::from(vibe_router_providers::CLOUDFLARE_WORKER_SCRIPT));
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/javascript; charset=utf-8"),
    );
    resp
}

// ---------------------------------------------------------------- UI

/// `GET /` — the embedded dashboard.
pub async fn dashboard() -> Response {
    match vibe_router_ui::index() {
        Some(asset) => serve_asset(asset.data, asset.mime),
        None => (StatusCode::NOT_FOUND, "dashboard missing").into_response(),
    }
}

/// `GET /*path` — embedded static assets.
pub async fn asset(Path(path): Path<String>) -> Response {
    match vibe_router_ui::get(&path) {
        Some(asset) => serve_asset(asset.data, asset.mime),
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

fn serve_asset(data: impl AsRef<[u8]>, mime: &'static str) -> Response {
    let bytes = data.as_ref().to_vec();
    let mut resp = Response::new(Body::from(bytes));
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(mime));
    resp
}

// ---------------------------------------------------------------- router

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/messages", post(anthropic_messages))
        .route("/v1/models", get(models))
        .route("/v1/assignment", get(get_assignment))
        .route("/api/assignment", get(get_assignment))
        .route("/api/status", get(api_status))
        .route(
            "/api/providers",
            get(api_providers).post(api_provider_create),
        )
        .route("/api/providers/:id", delete(api_provider_delete))
        .route("/api/combos", get(api_combos).post(api_combo_create))
        .route("/api/combos/:name", delete(api_combo_delete))
        .route("/api/aliases", get(api_aliases_list).post(api_alias_create))
        .route("/api/aliases/:alias", delete(api_alias_delete))
        .route("/api/proxies", get(api_proxies_list).post(api_proxy_create))
        .route("/api/proxies/:id", delete(api_proxy_delete))
        .route("/api/relay/worker.js", get(api_relay_worker_script))
        .route("/api/costs", get(api_costs))
        .route("/api/health", get(api_health))
        .route("/api/usage/recent", get(api_usage_recent))
        .route("/api/usage/totals", get(api_usage_totals))
        .route("/api/cache/stats", get(api_cache_stats))
        .route("/api/cache/purge", post(api_cache_purge))
        .route("/", get(dashboard))
        .route("/*path", get(asset))
        .with_state(state)
}
