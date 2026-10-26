//! Router errors to OpenAI-shaped HTTP responses.
//!
//! One function, one place: no handler ever invents a status code or an
//! error body shape. Every response carries the OpenAI envelope
//! (`error.message` / `type` / `code`) so clients that switch to Vibe Router
//! from another gateway see the format they already parse.
//!
//! The one addition is `Retry-After` when we genuinely know a cooldown
//! expiry — an honest, numeric hint beats a generic 503.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use vibe_router_core::RouterError;

/// Convert a router error into the client response.
pub fn into_response(err: &RouterError) -> Response {
    let status = StatusCode::from_u16(err.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut resp = (status, Json(err.to_json())).into_response();
    if let Some(secs) = err.retry_after_secs() {
        if let Ok(v) = axum::http::HeaderValue::from_str(&secs.to_string()) {
            resp.headers_mut()
                .insert(axum::http::header::RETRY_AFTER, v);
        }
    }
    resp
}

/// Extension trait so handlers can write `?`-style expressions.
pub trait RouterErrorExt {
    fn into_http(self) -> Response;
}

impl RouterErrorExt for RouterError {
    fn into_http(self) -> Response {
        into_response(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_for_every_error_variant() {
        let cases = [
            (RouterError::BadRequest("x".into()), 400),
            (RouterError::Unauthorized("x".into()), 401),
            (RouterError::Forbidden("x".into()), 403),
            (
                RouterError::ModelNotFound {
                    requested: "m".into(),
                    detail: "d".into(),
                },
                404,
            ),
            (
                RouterError::RateLimited {
                    requested: "m".into(),
                    retry_after_secs: None,
                    detail: "d".into(),
                },
                429,
            ),
            (
                RouterError::BadGateway {
                    provider: "p".into(),
                    detail: "d".into(),
                },
                502,
            ),
            (
                RouterError::AllUnavailable {
                    requested: "m".into(),
                    detail: "d".into(),
                },
                503,
            ),
            (
                RouterError::Timeout {
                    requested: "m".into(),
                    detail: "d".into(),
                },
                504,
            ),
            (RouterError::Internal("x".into()), 500),
        ];
        for (err, want) in cases {
            assert_eq!(err.status(), want, "status for {err:?}");
            let resp = into_response(&err);
            assert_eq!(resp.status().as_u16(), want, "http status for {err:?}");
        }
    }

    #[test]
    fn envelope_shape_is_openai() {
        let err = RouterError::ModelNotFound {
            requested: "ghost/m".into(),
            detail: "no such provider".into(),
        };
        let v = err.to_json();
        assert_eq!(v["error"]["type"], "invalid_request_error");
        assert_eq!(v["error"]["code"], "model_not_found");
        let msg = v["error"]["message"].as_str().unwrap();
        assert!(msg.contains("ghost/m"), "{msg}");
    }

    #[test]
    fn rate_limit_sets_retry_after() {
        let err = RouterError::RateLimited {
            requested: "m".into(),
            retry_after_secs: Some(7),
            detail: "d".into(),
        };
        let resp = into_response(&err);
        assert_eq!(resp.status().as_u16(), 429);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::RETRY_AFTER)
                .map(|v| v.to_str().unwrap()),
            Some("7")
        );
    }

    #[test]
    fn no_retry_after_when_unknown() {
        let err = RouterError::RateLimited {
            requested: "m".into(),
            retry_after_secs: None,
            detail: "d".into(),
        };
        let resp = into_response(&err);
        assert!(resp
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .is_none());
    }

    #[test]
    fn upstream_passthrough_keeps_provider_status() {
        let err = RouterError::Upstream {
            status: 400,
            message: "context length exceeded".into(),
        };
        assert_eq!(into_response(&err).status().as_u16(), 400);
        assert_eq!(err.to_json()["error"]["message"], "context length exceeded");
    }
}
