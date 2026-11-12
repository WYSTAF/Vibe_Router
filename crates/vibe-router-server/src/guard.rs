//! Security boundary for the admin surface and runtime-authored upstreams.
//!
//! * [`admin_auth`] — authentication + CSRF middleware mounted over every
//!   `/api/*` route. The dashboard sends the session key as `Authorization:
//!   Bearer <key>`; state-mutating requests must also carry a JSON content type
//!   or the `X-Vibe-Admin: 1` header so a cross-site simple request cannot
//!   purge the cache or edit providers.
//! * [`validate_upstream_url`] — SSRF guard for runtime-authored provider and
//!   proxy URLs. Loopback, link-local (cloud metadata), and RFC1918 private
//!   targets are rejected unless `server.allow_local_upstreams = true`.
//! * [`is_forbidden_runtime_secret`] — runtime providers may not smuggle
//!   `env:` / `file:` secret references that would exfiltrate host secrets to
//!   an attacker-controlled base URL. Those references are trusted only in the
//!   on-disk config file loaded at startup.

use std::net::IpAddr;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{header, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::state::AppState;

/// Strict Content-Security-Policy for the embedded dashboard. Scripts load only
/// from self (no inline scripts exist); inline `style` attributes are permitted.
pub const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; \
base-uri 'self'; frame-ancestors 'none'";

/// Extract a bearer token case-insensitively (`Bearer` / `bearer` / `BEARER`).
pub fn bearer_token(auth: &str) -> Option<&str> {
    let mut parts = auth.splitn(2, ' ');
    let scheme = parts.next()?.trim();
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    Some(parts.next()?.trim())
}

/// Authentication + CSRF middleware for `/api/*`.
pub async fn admin_auth(State(state): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    let headers = req.headers();
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(bearer_token)
        .or_else(|| {
            headers
                .get("x-api-key")
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
        });

    if !state.auth.lock().verify(presented) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "error": {
                    "message": "Missing or invalid API key",
                    "type": "authentication_error",
                    "code": "invalid_api_key",
                }
            })),
        )
            .into_response();
    }

    // CSRF: a state-mutating request must be a deliberate JSON call (or carry
    // the custom admin header). A cross-site form/simple request cannot set
    // either, so it is rejected before any mutation runs.
    let method = req.method();
    if matches!(
        method,
        &Method::POST | &Method::PUT | &Method::PATCH | &Method::DELETE
    ) {
        let json_ct = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|c| c.to_ascii_lowercase().starts_with("application/json"))
            .unwrap_or(false);
        let admin_header = headers
            .get("x-vibe-admin")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim() == "1")
            .unwrap_or(false);
        if !json_ct && !admin_header {
            return (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                Json(json!({
                    "error": {
                        "message": "state-mutating /api requests require Content-Type: application/json or X-Vibe-Admin: 1",
                        "type": "invalid_request_error",
                    }
                })),
            )
                .into_response();
        }
    }

    next.run(req).await
}

/// True when a runtime secret reference would resolve host environment/files.
pub fn is_forbidden_runtime_secret(s: &str) -> bool {
    let t = s.trim();
    t.starts_with("env:") || t.starts_with("file:")
}

/// Validate a runtime-authored upstream/proxy URL against the SSRF policy.
pub fn validate_upstream_url(raw: &str, allow_local: bool) -> Result<(), String> {
    let u = url::Url::parse(raw).map_err(|e| format!("invalid URL: {e}"))?;
    match u.scheme() {
        "http" | "https" => {}
        s => {
            return Err(format!(
                "unsupported scheme `{s}`; only http:// and https:// are allowed"
            ))
        }
    }
    if allow_local {
        return Ok(());
    }
    let host = u.host_str().ok_or_else(|| "URL has no host".to_string())?;
    if host_is_blocked(host) {
        return Err(format!(
            "host `{host}` is a loopback, link-local, or private address; \
             set server.allow_local_upstreams = true to permit trusted local upstreams"
        ));
    }
    Ok(())
}

fn host_is_blocked(host: &str) -> bool {
    let h = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    if h == "localhost" || h.ends_with(".localhost") {
        return true;
    }
    if let Ok(ip) = h.parse::<IpAddr>() {
        return match ip {
            IpAddr::V4(v4) => {
                v4.is_loopback()
                    || v4.is_link_local()
                    || v4.is_private()
                    || v4.is_unspecified()
                    || v4.is_broadcast()
            }
            IpAddr::V6(v6) => {
                let seg = v6.segments();
                v6.is_loopback()
                    || v6.is_unspecified()
                    || (seg[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
                    || (seg[0] & 0xfe00) == 0xfc00 // fc00::/7 unique-local
            }
        };
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_is_case_insensitive() {
        assert_eq!(bearer_token("Bearer abc"), Some("abc"));
        assert_eq!(bearer_token("bearer abc"), Some("abc"));
        assert_eq!(bearer_token("BEARER  abc "), Some("abc"));
        assert_eq!(bearer_token("Basic abc"), None);
    }

    #[test]
    fn ssrf_blocks_local_and_private_targets() {
        for bad in [
            "http://127.0.0.1:11434/v1",
            "http://localhost/v1",
            "http://169.254.169.254/latest/meta-data",
            "http://10.0.0.5/v1",
            "http://192.168.1.10/v1",
            "http://172.16.0.1/v1",
            "http://[::1]/v1",
            "http://[fe80::1]/v1",
        ] {
            assert!(
                validate_upstream_url(bad, false).is_err(),
                "{bad} must be blocked"
            );
            assert!(
                validate_upstream_url(bad, true).is_ok(),
                "{bad} allowed with flag"
            );
        }
    }

    #[test]
    fn ssrf_allows_public_targets_and_rejects_bad_schemes() {
        assert!(validate_upstream_url("https://api.openai.com/v1", false).is_ok());
        assert!(validate_upstream_url("ftp://example.com/x", false).is_err());
        assert!(validate_upstream_url("not a url", false).is_err());
    }

    #[test]
    fn runtime_secret_refs_are_forbidden() {
        assert!(is_forbidden_runtime_secret("env:SECRET"));
        assert!(is_forbidden_runtime_secret("file:/etc/passwd"));
        assert!(!is_forbidden_runtime_secret("sk-literal-key"));
    }
}
