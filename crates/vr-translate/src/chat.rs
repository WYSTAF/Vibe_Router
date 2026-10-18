//! Request/response translation, and the translator registry shape.
//!
//! v0.1 speaks OpenAI format in and out, so both directions are the identity
//! function. The point of this module is the seam: a `Translator` is a pair of
//! converters selected by (client format, provider format), and adding
//! Anthropic or Gemini later means registering a pair here — the router,
//! server and health layers never learn the names of formats.

use serde_json::Value;
use vr_core::WireFormat;

/// Translate a client request body toward the provider.
///
/// Identity while both sides are OpenAI. Also the place where
/// `stream_options` is ensured for streaming requests so usage arrives on
/// the final chunk — providers differ on whether an absent `include_usage`
/// means usage on the last chunk or nowhere, and asking explicitly is the
/// only portable behaviour.
pub fn translate_request(
    from: WireFormat,
    to: WireFormat,
    mut body: Value,
    streaming: bool,
) -> Value {
    assert_eq!(
        (from, to),
        (WireFormat::OpenAi, WireFormat::OpenAi),
        "v0.1 translates only openai→openai"
    );
    if streaming && body.get("stream_options").is_none() {
        body["stream_options"] = serde_json::json!({ "include_usage": true });
    }
    body
}

/// A request+response converter pair for one (source, target) format edge.
///
/// Only the request direction has a concrete function in v0.1; responses are
/// byte-forwarded because the client and provider formats match. When a
/// second format lands, `translate_response` joins this struct and the SSE
/// path starts running chunks through it instead of relaying frames.
#[derive(Clone, Copy)]
pub struct Translator {
    pub from: WireFormat,
    pub to: WireFormat,
    pub request: fn(WireFormat, WireFormat, Value, bool) -> Value,
}

/// The identity translator registered for openai→openai.
pub const OPENAI_IDENTITY: Translator = Translator {
    from: WireFormat::OpenAi,
    to: WireFormat::OpenAi,
    request: translate_request,
};

/// Pick the translator for a format edge. v0.1: only identity exists.
pub fn translator_for(from: WireFormat, to: WireFormat) -> Option<Translator> {
    matches!((from, to), (WireFormat::OpenAi, WireFormat::OpenAi)).then_some(OPENAI_IDENTITY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_preserves_body() {
        let body =
            serde_json::json!({"model": "gpt", "messages": [{"role": "user", "content": "x"}]});
        let out = translate_request(WireFormat::OpenAi, WireFormat::OpenAi, body.clone(), false);
        assert_eq!(out, body);
    }

    #[test]
    fn streaming_gets_usage_options_added() {
        let body = serde_json::json!({"model": "gpt", "stream": true});
        let out = translate_request(WireFormat::OpenAi, WireFormat::OpenAi, body, true);
        assert_eq!(out["stream_options"]["include_usage"], Value::Bool(true));
    }

    #[test]
    fn explicit_stream_options_are_kept() {
        let body = serde_json::json!({"model": "gpt", "stream_options": {"include_usage": false}});
        let out = translate_request(WireFormat::OpenAi, WireFormat::OpenAi, body, true);
        assert_eq!(out["stream_options"]["include_usage"], Value::Bool(false));
    }

    #[test]
    fn only_openai_edge_resolves() {
        assert!(translator_for(WireFormat::OpenAi, WireFormat::OpenAi).is_some());
    }
}
