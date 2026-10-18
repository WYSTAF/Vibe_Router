//! Anthropic `/v1/messages` protocol translation.
//!
//! Enables direct integration with Claude Code (`claude` CLI) by transparently
//! translating Anthropic request payloads and SSE streaming events to and from
//! the OpenAI-compatible representation.

use serde_json::{json, Value};

/// State tracker for Anthropic SSE stream generation.
#[derive(Debug, Default)]
pub struct AnthropicStreamState {
    pub message_id: String,
    pub model: String,
    pub started: bool,
    pub block_started: bool,
    pub output_tokens: u32,
    pub finished: bool,
}

impl AnthropicStreamState {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            message_id: format!(
                "msg_{:016x}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ),
            model: model.into(),
            started: false,
            block_started: false,
            output_tokens: 0,
            finished: false,
        }
    }
}

/// Convert an Anthropic `/v1/messages` request body to OpenAI chat completions format.
pub fn anthropic_to_openai_request(anthropic: &Value) -> Result<Value, String> {
    let model = anthropic
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing `model` in Anthropic request".to_string())?;

    let mut openai_messages = Vec::new();

    // 1. Process system prompt (can be string or array of text blocks)
    if let Some(system) = anthropic.get("system") {
        let system_text = extract_text_content(system);
        if !system_text.is_empty() {
            openai_messages.push(json!({
                "role": "system",
                "content": system_text,
            }));
        }
    }

    // 2. Process conversation messages
    let messages = anthropic
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| "missing or invalid `messages` in Anthropic request".to_string())?;

    for msg in messages {
        let role = msg.get("role").and_then(Value::as_str).unwrap_or("user");
        let content = msg.get("content").unwrap_or(&Value::Null);
        let content_text = extract_text_content(content);

        openai_messages.push(json!({
            "role": role,
            "content": content_text,
        }));
    }

    let mut openai_req = json!({
        "model": model,
        "messages": openai_messages,
    });

    if let Some(stream) = anthropic.get("stream").and_then(Value::as_bool) {
        openai_req["stream"] = json!(stream);
    }
    if let Some(temp) = anthropic.get("temperature").and_then(Value::as_f64) {
        openai_req["temperature"] = json!(temp);
    }
    if let Some(top_p) = anthropic.get("top_p").and_then(Value::as_f64) {
        openai_req["top_p"] = json!(top_p);
    }
    if let Some(max_tokens) = anthropic.get("max_tokens").and_then(Value::as_u64) {
        openai_req["max_tokens"] = json!(max_tokens);
    }

    // 3. Process tools if present
    if let Some(tools) = anthropic.get("tools").and_then(Value::as_array) {
        let mut openai_tools = Vec::new();
        for t in tools {
            let name = t.get("name").and_then(Value::as_str).unwrap_or_default();
            let desc = t
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let schema = t.get("input_schema").cloned().unwrap_or(json!({}));
            openai_tools.push(json!({
                "type": "function",
                "function": {
                    "name": name,
                    "description": desc,
                    "parameters": schema,
                }
            }));
        }
        if !openai_tools.is_empty() {
            openai_req["tools"] = json!(openai_tools);
        }
    }

    Ok(openai_req)
}

/// Convert an OpenAI chat completions response into Anthropic `/v1/messages` format.
pub fn openai_to_anthropic_response(openai: &Value, model_name: &str) -> Value {
    let id = openai
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("chatcmpl-default");

    let text = openai
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(Value::as_str)
        .unwrap_or_default();

    let finish_reason = openai
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("finish_reason"))
        .and_then(Value::as_str)
        .unwrap_or("stop");

    let stop_reason = match finish_reason {
        "stop" => "end_turn",
        "length" => "max_tokens",
        "tool_calls" => "tool_use",
        _ => "end_turn",
    };

    let prompt_tokens = openai
        .get("usage")
        .and_then(|u| u.get("prompt_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let completion_tokens = openai
        .get("usage")
        .and_then(|u| u.get("completion_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);

    json!({
        "id": format!("msg_{id}"),
        "type": "message",
        "role": "assistant",
        "content": [
            {
                "type": "text",
                "text": text,
            }
        ],
        "model": model_name,
        "stop_reason": stop_reason,
        "stop_sequence": null,
        "usage": {
            "input_tokens": prompt_tokens,
            "output_tokens": completion_tokens,
        }
    })
}

/// Convert an incoming OpenAI SSE chunk or DONE token to Anthropic SSE events.
pub fn openai_chunk_to_anthropic_events(
    chunk_str: &str,
    state: &mut AnthropicStreamState,
) -> Vec<String> {
    let mut out = Vec::new();

    // Check for end of stream
    if chunk_str.trim() == "[DONE]" || state.finished {
        if !state.finished {
            state.finished = true;
            if state.block_started {
                out.push(format!(
                    "event: content_block_stop\ndata: {}\n\n",
                    json!({ "type": "content_block_stop", "index": 0 })
                ));
            }
            out.push(format!(
                "event: message_delta\ndata: {}\n\n",
                json!({
                    "type": "message_delta",
                    "delta": { "stop_reason": "end_turn", "stop_sequence": null },
                    "usage": { "output_tokens": state.output_tokens }
                })
            ));
            out.push("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".to_string());
        }
        return out;
    }

    let parsed: Value = match serde_json::from_str(chunk_str) {
        Ok(v) => v,
        Err(_) => return out,
    };

    // Emit message_start and content_block_start once at beginning
    if !state.started {
        state.started = true;
        out.push(format!(
            "event: message_start\ndata: {}\n\n",
            json!({
                "type": "message_start",
                "message": {
                    "id": state.message_id,
                    "type": "message",
                    "role": "assistant",
                    "content": [],
                    "model": state.model,
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": { "input_tokens": 0, "output_tokens": 0 }
                }
            })
        ));

        state.block_started = true;
        out.push(format!(
            "event: content_block_start\ndata: {}\n\n",
            json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": { "type": "text", "text": "" }
            })
        ));
    }

    // Extract text delta
    let delta_text = parsed
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("delta"))
        .and_then(|d| d.get("content"))
        .and_then(Value::as_str)
        .unwrap_or_default();

    if !delta_text.is_empty() {
        state.output_tokens += 1;
        out.push(format!(
            "event: content_block_delta\ndata: {}\n\n",
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": { "type": "text_delta", "text": delta_text }
            })
        ));
    }

    // Check if finish_reason signaled in chunk
    let finish_reason = parsed
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("finish_reason"))
        .and_then(Value::as_str);

    if finish_reason.is_some() && !state.finished {
        state.finished = true;
        if state.block_started {
            out.push(format!(
                "event: content_block_stop\ndata: {}\n\n",
                json!({ "type": "content_block_stop", "index": 0 })
            ));
        }
        out.push(format!(
            "event: message_delta\ndata: {}\n\n",
            json!({
                "type": "message_delta",
                "delta": { "stop_reason": "end_turn", "stop_sequence": null },
                "usage": { "output_tokens": state.output_tokens }
            })
        ));
        out.push("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n".to_string());
    }

    out
}

fn extract_text_content(val: &Value) -> String {
    match val {
        Value::String(s) => s.clone(),
        Value::Array(arr) => {
            let mut parts = Vec::new();
            for item in arr {
                if let Some(t) = item.get("text").and_then(Value::as_str) {
                    parts.push(t);
                } else if let Some(s) = item.as_str() {
                    parts.push(s);
                }
            }
            parts.join("\n")
        }
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anthropic_to_openai_request_translation() {
        let anthropic_req = json!({
            "model": "claude-3-7-sonnet",
            "system": "You are a code reviewer.",
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "Review this Rust function"}]
                }
            ],
            "max_tokens": 512,
            "temperature": 0.2,
            "stream": false
        });

        let openai_req = anthropic_to_openai_request(&anthropic_req).unwrap();
        assert_eq!(openai_req["model"], "claude-3-7-sonnet");
        assert_eq!(openai_req["temperature"], 0.2);
        assert_eq!(openai_req["max_tokens"], 512);

        let msgs = openai_req["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["role"], "system");
        assert_eq!(msgs[0]["content"], "You are a code reviewer.");
        assert_eq!(msgs[1]["role"], "user");
        assert_eq!(msgs[1]["content"], "Review this Rust function");
    }

    #[test]
    fn test_openai_to_anthropic_response_translation() {
        let openai_resp = json!({
            "id": "chatcmpl-test-99",
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "Rust code looks idiomatic."
                    },
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": 15,
                "completion_tokens": 8
            }
        });

        let anthropic_resp = openai_to_anthropic_response(&openai_resp, "claude-3-7-sonnet");
        assert_eq!(anthropic_resp["type"], "message");
        assert_eq!(anthropic_resp["role"], "assistant");
        assert_eq!(anthropic_resp["model"], "claude-3-7-sonnet");
        assert_eq!(anthropic_resp["stop_reason"], "end_turn");
        assert_eq!(anthropic_resp["usage"]["input_tokens"], 15);
        assert_eq!(anthropic_resp["usage"]["output_tokens"], 8);
        assert_eq!(
            anthropic_resp["content"][0]["text"],
            "Rust code looks idiomatic."
        );
    }

    #[test]
    fn test_streaming_anthropic_event_generation() {
        let mut state = AnthropicStreamState::new("claude-3-7-sonnet");
        let chunk1 = json!({
            "choices": [{"delta": {"content": "Hello"}}]
        })
        .to_string();

        let evs1 = openai_chunk_to_anthropic_events(&chunk1, &mut state);
        assert!(evs1.iter().any(|e| e.contains("message_start")));
        assert!(evs1.iter().any(|e| e.contains("content_block_start")));
        assert!(evs1.iter().any(|e| e.contains("Hello")));

        let chunk2 = json!({
            "choices": [{"delta": {"content": " world"}}]
        })
        .to_string();
        let evs2 = openai_chunk_to_anthropic_events(&chunk2, &mut state);
        assert!(evs2.iter().any(|e| e.contains(" world")));
        assert!(!evs2.iter().any(|e| e.contains("message_start")));

        let done_evs = openai_chunk_to_anthropic_events("[DONE]", &mut state);
        assert!(done_evs.iter().any(|e| e.contains("message_delta")));
        assert!(done_evs.iter().any(|e| e.contains("message_stop")));
    }
}
