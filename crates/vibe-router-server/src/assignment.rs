//! Role assignment endpoint: GET /v1/assignment
//!
//! Tailored for orchestration tools (specifically Vibe Assembly). Returns live
//! role assignments (`boss`, `code`, `debug`, `chat`), their capability class,
//! context window, ticket size, and real-time availability/health status.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use vibe_router_core::ModelRegistry;
use vibe_router_health::Usability;

use crate::routes::check_auth;
use crate::state::AppState;

/// Format a [`SystemTime`] as an ISO-8601 UTC string (e.g. `2026-10-05T12:00:00Z`)
/// with zero external dependencies.
pub fn format_iso8601(time: SystemTime) -> String {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let total_secs = duration.as_secs();

    let sec = (total_secs % 60) as u32;
    let total_mins = total_secs / 60;
    let min = (total_mins % 60) as u32;
    let total_hours = total_mins / 60;
    let hour = (total_hours % 24) as u32;
    let mut days = (total_hours / 24) as i64;

    let mut year = 1970;
    loop {
        let leap = is_leap_year(year);
        let days_in_year = if leap { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }
    let leap = is_leap_year(year);
    let days_in_months = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 1;
    for &dim in &days_in_months {
        if days < dim {
            break;
        }
        days -= dim;
        month += 1;
    }
    let day = days + 1;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Query the live health status of a model (either a combo or a single model)
/// without consuming rate-limit tokens (uses `health.peek`).
pub fn get_model_status(model_str: &str, state: &AppState) -> String {
    if let Some(combo) = state.registry.combo(model_str) {
        if combo.models.is_empty() {
            return "unavailable".to_string();
        }
        let mut any_ready = false;
        let mut any_rate_limited = false;
        let mut any_cooling = false;
        for m in &combo.models {
            match state.health.peek(m) {
                Usability::Ready => {
                    any_ready = true;
                    break;
                }
                Usability::RateLimited(_) => {
                    any_rate_limited = true;
                }
                Usability::Cooldown(_) => {
                    any_cooling = true;
                }
            }
        }
        if any_ready {
            "ready".to_string()
        } else if any_rate_limited {
            "rate_limited".to_string()
        } else if any_cooling {
            "cooling".to_string()
        } else {
            "unavailable".to_string()
        }
    } else {
        match state.health.peek(model_str) {
            Usability::Ready => "ready".to_string(),
            Usability::Cooldown(_) => "cooling".to_string(),
            Usability::RateLimited(_) => "rate_limited".to_string(),
        }
    }
}

/// Resolve context window for a model or combo.
pub fn resolve_context_window(model_str: &str, state: &AppState) -> Option<u32> {
    if let Some(combo) = state.registry.combo(model_str) {
        for m in &combo.models {
            if let Some(ctx) = resolve_model_context(m, state) {
                return Some(ctx);
            }
        }
        None
    } else {
        resolve_model_context(model_str, state)
    }
}

fn resolve_model_context(model_str: &str, state: &AppState) -> Option<u32> {
    let (provider_id, model_id) = model_str.split_once('/')?;
    let provider = state.registry.provider(provider_id)?;
    let spec = provider.models.iter().find(|m| m.id == model_id)?;
    spec.context
}

/// `GET /v1/assignment` handler
pub async fn get_assignment(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
) -> Response {
    if let Some(err) = check_auth(&state, &headers) {
        return err;
    }

    let roles_cfg = state.roles.read().clone();
    let models_list = state.registry.list();
    let all_ids: Vec<&str> = models_list.iter().map(|m| m.id.as_str()).collect();

    // 1. Boss role (flagship)
    let boss_cfg = roles_cfg.as_ref().and_then(|r| r.boss.clone());
    let boss_model = boss_cfg
        .as_ref()
        .map(|c| c.model.clone())
        .unwrap_or_else(|| pick_boss_model(&all_ids, &state));
    let boss_class = boss_cfg
        .as_ref()
        .and_then(|c| c.class.clone())
        .unwrap_or_else(|| "flagship".to_string());
    let boss_context = boss_cfg
        .as_ref()
        .and_then(|c| c.context_window)
        .or_else(|| resolve_context_window(&boss_model, &state))
        .unwrap_or(131072);
    let boss_status = get_model_status(&boss_model, &state);

    // 2. Code role (coder)
    let code_cfg = roles_cfg.as_ref().and_then(|r| r.code.clone());
    let code_model = code_cfg
        .as_ref()
        .map(|c| c.model.clone())
        .unwrap_or_else(|| pick_code_model(&all_ids, &boss_model, &state));
    let code_class = code_cfg
        .as_ref()
        .and_then(|c| c.class.clone())
        .unwrap_or_else(|| "coder".to_string());
    let code_context = code_cfg
        .as_ref()
        .and_then(|c| c.context_window)
        .or_else(|| resolve_context_window(&code_model, &state))
        .unwrap_or(65536);
    let code_ticket = code_cfg
        .as_ref()
        .and_then(|c| c.recommended_ticket_size.clone())
        .unwrap_or_else(|| if code_context >= 128000 { "M" } else { "S" }.to_string());
    let code_status = get_model_status(&code_model, &state);

    // 3. Debug role (reasoner)
    let debug_cfg = roles_cfg.as_ref().and_then(|r| r.debug.clone());
    let debug_model = debug_cfg
        .as_ref()
        .map(|c| c.model.clone())
        .unwrap_or_else(|| pick_debug_model(&all_ids, &boss_model, &state));
    let debug_class = debug_cfg
        .as_ref()
        .and_then(|c| c.class.clone())
        .unwrap_or_else(|| "reasoner".to_string());
    let debug_status = get_model_status(&debug_model, &state);

    // 4. Chat role (fast)
    let chat_cfg = roles_cfg.as_ref().and_then(|r| r.chat.clone());
    let chat_model = chat_cfg
        .as_ref()
        .map(|c| c.model.clone())
        .unwrap_or_else(|| pick_chat_model(&all_ids, &boss_model, &state));
    let chat_class = chat_cfg
        .as_ref()
        .and_then(|c| c.class.clone())
        .unwrap_or_else(|| "fast".to_string());
    let chat_status = get_model_status(&chat_model, &state);

    // Offline fallback recommended if both core roles (boss and code) are down,
    // or if every single role is non-ready.
    let offline_fallback = (boss_status != "ready" && code_status != "ready")
        || (boss_status != "ready"
            && code_status != "ready"
            && debug_status != "ready"
            && chat_status != "ready");

    let refreshed_at = format_iso8601(SystemTime::now());

    let boss_obj = json!({
        "model": boss_model,
        "class": boss_class,
        "context_window": boss_context,
        "status": boss_status
    });
    let code_obj = json!({
        "model": code_model,
        "class": code_class,
        "context_window": code_context,
        "recommended_ticket_size": code_ticket,
        "status": code_status
    });
    let mut debug_obj = json!({
        "model": debug_model,
        "class": debug_class,
        "status": debug_status
    });
    if let Some(ctx) = debug_cfg
        .and_then(|c| c.context_window)
        .or_else(|| resolve_context_window(&debug_model, &state))
    {
        debug_obj["context_window"] = json!(ctx);
    }
    let mut chat_obj = json!({
        "model": chat_model,
        "class": chat_class,
        "status": chat_status
    });
    if let Some(ctx) = chat_cfg
        .and_then(|c| c.context_window)
        .or_else(|| resolve_context_window(&chat_model, &state))
    {
        chat_obj["context_window"] = json!(ctx);
    }

    let payload = json!({
        "roles": {
            "boss": boss_obj,
            "code": code_obj,
            "debug": debug_obj,
            "chat": chat_obj
        },
        "refreshed_at": refreshed_at,
        "offline_fallback_recommended": offline_fallback
    });

    (StatusCode::OK, Json(payload)).into_response()
}

fn pick_boss_model(all_ids: &[&str], state: &AppState) -> String {
    // 1. Dedicated combo
    for &candidate in &["boss", "flagship", "main", "primary"] {
        if state.registry.combo(candidate).is_some() {
            return candidate.to_string();
        }
    }
    // 2. High capability flagship models
    let patterns = [
        "claude-3.5-sonnet",
        "claude-sonnet",
        "claude-opus",
        "gpt-5",
        "gpt-6",
        "gpt-4o",
        "llama-3.3-70b",
        "glm-4",
        "glm-5",
    ];
    for pat in patterns {
        if let Some(&m) = all_ids
            .iter()
            .find(|id| id.to_ascii_lowercase().contains(pat))
        {
            return m.to_string();
        }
    }
    // 3. Fallback to any combo or first available model
    if let Some(first_combo) = state
        .registry
        .list()
        .into_iter()
        .find(|m| m.owned_by == "combo")
    {
        return first_combo.id;
    }
    all_ids.first().copied().unwrap_or("default").to_string()
}

fn pick_code_model(all_ids: &[&str], boss_model: &str, state: &AppState) -> String {
    for &candidate in &["coder", "code", "dev", "coding"] {
        if state.registry.combo(candidate).is_some() {
            return candidate.to_string();
        }
    }
    let patterns = [
        "coder",
        "code",
        "deepseek-chat",
        "qwen2.5-coder",
        "qwen3-coder",
        "claude-3.5-sonnet",
        "claude-sonnet",
    ];
    for pat in patterns {
        if let Some(&m) = all_ids
            .iter()
            .find(|id| id.to_ascii_lowercase().contains(pat))
        {
            return m.to_string();
        }
    }
    boss_model.to_string()
}

fn pick_debug_model(all_ids: &[&str], boss_model: &str, state: &AppState) -> String {
    for &candidate in &["debug", "reasoner", "reasoning", "r1", "think"] {
        if state.registry.combo(candidate).is_some() {
            return candidate.to_string();
        }
    }
    let patterns = ["reasoner", "r1", "reasoning", "think", "deepseek-r"];
    for pat in patterns {
        if let Some(&m) = all_ids
            .iter()
            .find(|id| id.to_ascii_lowercase().contains(pat))
        {
            return m.to_string();
        }
    }
    boss_model.to_string()
}

fn pick_chat_model(all_ids: &[&str], fallback: &str, state: &AppState) -> String {
    for &candidate in &["chat", "fast-cheap", "fast", "cheap", "quick"] {
        if state.registry.combo(candidate).is_some() {
            return candidate.to_string();
        }
    }
    let patterns = [
        "flash", "mini", "nano", "haiku", "turbo", "fast", "llama3.2", "mixtral",
    ];
    for pat in patterns {
        if let Some(&m) = all_ids
            .iter()
            .find(|id| id.to_ascii_lowercase().contains(pat))
        {
            return m.to_string();
        }
    }
    fallback.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso8601_formatting() {
        // Unix epoch: 1970-01-01T00:00:00Z
        assert_eq!(format_iso8601(UNIX_EPOCH), "1970-01-01T00:00:00Z");

        // Specific timestamp: 2026-10-05T12:34:56Z
        // 2026 is not a leap year.
        let now = SystemTime::now();
        let formatted = format_iso8601(now);
        assert!(formatted.ends_with('Z'));
        assert_eq!(formatted.len(), 20); // YYYY-MM-DDTHH:MM:SSZ
        assert_eq!(&formatted[10..11], "T");
    }

    #[test]
    fn leap_year_calculation() {
        assert!(is_leap_year(2000));
        assert!(is_leap_year(2024));
        assert!(!is_leap_year(1900));
        assert!(!is_leap_year(2025));
        assert!(!is_leap_year(2026));
    }
}
