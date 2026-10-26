//! Wire-format translation through a canonical representation.
//!
//! v0.1 speaks OpenAI format in and out, so request translation is the
//! identity — but the *shape* is here on purpose: a [`Translator`] pairs a
//! request converter with a response converter, and adding Anthropic or
//! Gemini format support later means adding a converter pair, not touching
//! the server or the router.
//!
//! The genuinely load-bearing piece is the SSE machinery: an incremental
//! decoder (bytes → events) and a frame writer (events → SSE bytes) with
//! usage capture, both pure functions with no I/O so they test in isolation.
//!
//! Depends on: vibe-router-core.

#![forbid(unsafe_code)]

pub mod anthropic;
mod chat;
mod sse;

pub use anthropic::{
    anthropic_to_openai_request, openai_chunk_to_anthropic_events, openai_to_anthropic_response,
    AnthropicStreamState,
};
pub use chat::{translate_request, translator_for, Translator, OPENAI_IDENTITY};
pub use sse::{encode_frame, SseDecoder, SseEvent, UsageAccumulator, DONE_TOKEN};
