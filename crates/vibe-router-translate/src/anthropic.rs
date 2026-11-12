//! Anthropic `/v1/messages` protocol translation.
//!
//! Full bidirectional bridge to the OpenAI chat-completions representation:
//! text, vision (image blocks), tool definitions, assistant `tool_use`, user
//! `tool_result`, and streaming tool-call deltas. This is what lets Claude
//! Code (which leans heavily on tool use) run against OpenAI-shaped upstreams.

use std::collections::HashMap;

use serde_json::{json, Value};

/// State tracker for Anthropic SSE stream generation.
///
/// Anthropic streams a sequence of content blocks (text and `tool_use`); OpenAI
/// streams a single choice with `delta.content` and `delta.tool_calls`. This
/// state maps OpenAI tool-call indices onto Anthropic block indices and keeps
/// exactly one block open at a time.
#[derive(Debug)]
pub struct AnthropicStreamState {
    pub message_id: String,
    pub model: String,
    pub started: bool,
    pub finished: bool,
    /// Approximate output tokens when the provider reports none.
    pub output_tokens: u32,
    /// Provider-reported output tokens, if a usage chunk arrives.
    pub reported_output_tokens: Option<u32>,
    /// OpenAI tool_call index -> Anthropic content block index.
    tool_blocks: HashMap<u64, usize>,
    next_block_index: usize,
    current_open_block: Option<usize>,
    text_block_index: Option<usize>,
    pub stop_reason: String,
    saw_tool_calls: bool,
}

impl Default for AnthropicStreamState {
    fn default() -> Self {
        Self::new("unknown")
    }
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
            finished: false,
            output_tokens: 0,
            reported_output_tokens: None,
            tool_blocks: HashMap::new(),
            next_block_index: 0,
            current_open_block: None,
            text_block_index: None,
            stop_reason: "end_turn".to_string(),
            saw_tool_calls: false,
        }
    }

    fn final_output_tokens(&self) -> u32 {
        self.reported_output_tokens.unwrap_or(self.output_tokens)
    }
}

// ---------------------------------------------------------------- request

/// Convert an Anthropic `/v1/messages` request body to OpenAI chat completions.
pub fn anthropic_to_openai_request(anthropic: &Value) -> Result<Value, String> {
    let model = anthropic
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing `model` in Anthropic request".to_string())?;

    let mut openai_messages = Vec::new();

    if let Some(system) = anthropic.get("system") {
        let system_text = extract_text_content(system);
        if !system_text.is_empty() {
            openai_messages.push(json!({"role": "system", "content": system_text}));
        }
    }

    let messages = anthropic
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| "missing or invalid `messages` in Anthropic request".to_string())?;

    for msg in messages {
        let role = msg.get("role").and_then(Value::as_str).unwrap_or("user");
        let content = msg.get("content").unwrap_or(&Value::Null);
        translate_message(role, content, &mut openai_messages);
    }

    let mut openai_req = json!({"model": model, "messages": openai_messages});

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
    if let Some(stop) = anthropic.get("stop_sequences").and_then(Value::as_array) {
        if !stop.is_empty() {
            openai_req["stop"] = json!(stop);
        }
    }

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
                "function": {"name": name, "description": desc, "parameters": schema}
            }));
        }
        if !openai_tools.is_empty() {
            openai_req["tools"] = json!(openai_tools);
        }
    }

    if let Some(choice) = anthropic.get("tool_choice") {
        if let Some(mapped) = translate_tool_choice(choice) {
            openai_req["tool_choice"] = mapped;
        }
    }

    Ok(openai_req)
}

/// Translate one Anthropic message (string or block array) into OpenAI messages.
fn translate_message(role: &str, content: &Value, out: &mut Vec<Value>) {
    // Plain string content.
    if let Some(s) = content.as_str() {
        out.push(json!({"role": role, "content": s}));
        return;
    }

    let Some(blocks) = content.as_array() else {
        out.push(json!({"role": role, "content": extract_text_content(content)}));
        return;
    };

    if role == "assistant" {
        let mut text_parts = Vec::new();
        let mut tool_calls = Vec::new();
        for block in blocks {
            match block.get("type").and_then(Value::as_str).unwrap_or("") {
                "text" => {
                    if let Some(t) = block.get("text").and_then(Value::as_str) {
                        text_parts.push(t.to_string());
                    }
                }
                "tool_use" => {
                    let id = block.get("id").and_then(Value::as_str).unwrap_or_default();
                    let name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let input = block.get("input").cloned().unwrap_or(json!({}));
                    tool_calls.push(json!({
                        "id": id,
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": serde_json::to_string(&input).unwrap_or_else(|_| "{}".into()),
                        }
                    }));
                }
                _ => {}
            }
        }
        let text = text_parts.join("\n");
        let mut msg = json!({"role": "assistant"});
        msg["content"] = if text.is_empty() {
            Value::Null
        } else {
            json!(text)
        };
        if !tool_calls.is_empty() {
            msg["tool_calls"] = json!(tool_calls);
        }
        out.push(msg);
        return;
    }

    // User (or other) role: text/image parts plus tool_result blocks.
    let mut user_parts: Vec<Value> = Vec::new();
    let mut has_non_text = false;
    for block in blocks {
        match block.get("type").and_then(Value::as_str).unwrap_or("") {
            "text" => {
                if let Some(t) = block.get("text").and_then(Value::as_str) {
                    user_parts.push(json!({"type": "text", "text": t}));
                }
            }
            "image" => {
                if let Some(part) = translate_image_block(block) {
                    user_parts.push(part);
                    has_non_text = true;
                }
            }
            "tool_result" => {
                let tool_call_id = block
                    .get("tool_use_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let result_text =
                    extract_text_content(block.get("content").unwrap_or(&Value::Null));
                out.push(json!({
                    "role": "tool",
                    "tool_call_id": tool_call_id,
                    "content": result_text,
                }));
            }
            _ => {}
        }
    }
    if !user_parts.is_empty() {
        // Collapse to a plain string when it is text-only (max compatibility).
        if !has_non_text {
            let text = user_parts
                .iter()
                .filter_map(|p| p.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n");
            out.push(json!({"role": role, "content": text}));
        } else {
            out.push(json!({"role": role, "content": user_parts}));
        }
    }
}

/// Translate an Anthropic image block (base64 source or URL) to OpenAI image_url.
fn translate_image_block(block: &Value) -> Option<Value> {
    let source = block.get("source")?;
    let kind = source.get("type").and_then(Value::as_str).unwrap_or("");
    let url = match kind {
        "base64" => {
            let media = source
                .get("media_type")
                .and_then(Value::as_str)
                .unwrap_or("image/png");
            let data = source.get("data").and_then(Value::as_str)?;
            format!("data:{media};base64,{data}")
        }
        "url" => source.get("url").and_then(Value::as_str)?.to_string(),
        _ => return None,
    };
    Some(json!({"type": "image_url", "image_url": {"url": url}}))
}

fn translate_tool_choice(choice: &Value) -> Option<Value> {
    if let Some(s) = choice.as_str() {
        return match s {
            "auto" => Some(json!("auto")),
            "any" | "required" => Some(json!("required")),
            "none" => Some(json!("none")),
            _ => None,
        };
    }
    let kind = choice.get("type").and_then(Value::as_str)?;
    match kind {
        "auto" => Some(json!("auto")),
        "any" => Some(json!("required")),
        "none" => Some(json!("none")),
        "tool" => {
            let name = choice.get("name").and_then(Value::as_str)?;
            Some(json!({"type": "function", "function": {"name": name}}))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------- response

/// Convert an OpenAI chat completions response into Anthropic `/v1/messages`.
pub fn openai_to_anthropic_response(openai: &Value, model_name: &str) -> Value {
    let id = openai
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("chatcmpl-default");
    let message = openai
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("message"))
        .cloned()
        .unwrap_or(json!({}));

    let text = message
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let finish_reason = openai
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("finish_reason"))
        .and_then(Value::as_str)
        .unwrap_or("stop");

    let mut content_blocks = Vec::new();
    if !text.is_empty() {
        content_blocks.push(json!({"type": "text", "text": text}));
    }

    let mut has_tool_use = false;
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        for tc in tool_calls {
            let tc_id = tc.get("id").and_then(Value::as_str).unwrap_or_default();
            let func = tc.get("function").cloned().unwrap_or(json!({}));
            let name = func.get("name").and_then(Value::as_str).unwrap_or_default();
            let args_str = func
                .get("arguments")
                .and_then(Value::as_str)
                .unwrap_or("{}");
            let input: Value = serde_json::from_str(args_str).unwrap_or(json!({}));
            content_blocks
                .push(json!({"type": "tool_use", "id": tc_id, "name": name, "input": input}));
            has_tool_use = true;
        }
    }

    if content_blocks.is_empty() {
        content_blocks.push(json!({"type": "text", "text": ""}));
    }

    let stop_reason = if has_tool_use || finish_reason == "tool_calls" {
        "tool_use"
    } else {
        match finish_reason {
            "length" => "max_tokens",
            "content_filter" => "end_turn",
            _ => "end_turn",
        }
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
        "content": content_blocks,
        "model": model_name,
        "stop_reason": stop_reason,
        "stop_sequence": null,
        "usage": {"input_tokens": prompt_tokens, "output_tokens": completion_tokens}
    })
}

// ---------------------------------------------------------------- streaming

fn frame(event: &str, data: Value) -> String {
    format!("event: {event}\ndata: {data}\n\n")
}

/// Convert an incoming OpenAI SSE chunk (or `[DONE]`) into Anthropic SSE events.
pub fn openai_chunk_to_anthropic_events(
    chunk_str: &str,
    state: &mut AnthropicStreamState,
) -> Vec<String> {
    let mut out = Vec::new();

    if chunk_str.trim() == "[DONE]" || state.finished {
        if !state.finished {
            finish_stream(state, &mut out);
        }
        return out;
    }

    let parsed: Value = match serde_json::from_str(chunk_str) {
        Ok(v) => v,
        Err(_) => return out,
    };

    if !state.started {
        state.started = true;
        out.push(frame(
            "message_start",
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
                    "usage": {"input_tokens": 0, "output_tokens": 0}
                }
            }),
        ));
    }

    // Capture provider-reported usage when present (final usage chunk).
    if let Some(u) = parsed.get("usage") {
        if let Some(c) = u.get("completion_tokens").and_then(Value::as_u64) {
            state.reported_output_tokens = Some(c as u32);
        }
    }

    let choice = parsed.get("choices").and_then(|c| c.get(0));
    let delta = choice.and_then(|c| c.get("delta"));

    // Text delta -> text content block.
    if let Some(text) = delta.and_then(|d| d.get("content")).and_then(Value::as_str) {
        if !text.is_empty() {
            let idx = ensure_text_block(state, &mut out);
            state.output_tokens += 1;
            out.push(frame(
                "content_block_delta",
                json!({"type": "content_block_delta", "index": idx, "delta": {"type": "text_delta", "text": text}}),
            ));
        }
    }

    // Tool-call deltas -> tool_use content blocks with input_json_delta.
    if let Some(tool_calls) = delta
        .and_then(|d| d.get("tool_calls"))
        .and_then(Value::as_array)
    {
        for tc in tool_calls {
            let tc_index = tc.get("index").and_then(Value::as_u64).unwrap_or(0);
            let func = tc.get("function");
            let name = func.and_then(|f| f.get("name")).and_then(Value::as_str);
            let id = tc.get("id").and_then(Value::as_str);

            let block_idx = if let Some(&idx) = state.tool_blocks.get(&tc_index) {
                idx
            } else {
                // First sighting of this tool call: open a new tool_use block.
                close_open_block(state, &mut out);
                let idx = state.next_block_index;
                state.next_block_index += 1;
                state.tool_blocks.insert(tc_index, idx);
                state.saw_tool_calls = true;
                out.push(frame(
                    "content_block_start",
                    json!({
                        "type": "content_block_start",
                        "index": idx,
                        "content_block": {
                            "type": "tool_use",
                            "id": id.unwrap_or_default(),
                            "name": name.unwrap_or_default(),
                            "input": {}
                        }
                    }),
                ));
                state.current_open_block = Some(idx);
                idx
            };

            if let Some(args) = func
                .and_then(|f| f.get("arguments"))
                .and_then(Value::as_str)
            {
                if !args.is_empty() {
                    state.output_tokens += 1;
                    out.push(frame(
                        "content_block_delta",
                        json!({"type": "content_block_delta", "index": block_idx, "delta": {"type": "input_json_delta", "partial_json": args}}),
                    ));
                }
            }
        }
    }

    // Finish reason signals the end of the message.
    if let Some(reason) = choice
        .and_then(|c| c.get("finish_reason"))
        .and_then(Value::as_str)
    {
        state.stop_reason = match reason {
            "tool_calls" => "tool_use".to_string(),
            "length" => "max_tokens".to_string(),
            _ => "end_turn".to_string(),
        };
        if !state.finished {
            finish_stream(state, &mut out);
        }
    }

    out
}

fn ensure_text_block(state: &mut AnthropicStreamState, out: &mut Vec<String>) -> usize {
    if let Some(idx) = state.text_block_index {
        return idx;
    }
    close_open_block(state, out);
    let idx = state.next_block_index;
    state.next_block_index += 1;
    state.text_block_index = Some(idx);
    out.push(frame(
        "content_block_start",
        json!({"type": "content_block_start", "index": idx, "content_block": {"type": "text", "text": ""}}),
    ));
    state.current_open_block = Some(idx);
    idx
}

fn close_open_block(state: &mut AnthropicStreamState, out: &mut Vec<String>) {
    if let Some(idx) = state.current_open_block.take() {
        out.push(frame(
            "content_block_stop",
            json!({"type": "content_block_stop", "index": idx}),
        ));
    }
}

fn finish_stream(state: &mut AnthropicStreamState, out: &mut Vec<String>) {
    state.finished = true;
    close_open_block(state, out);
    let stop_reason = if state.saw_tool_calls && state.stop_reason == "end_turn" {
        "tool_use"
    } else {
        state.stop_reason.as_str()
    };
    out.push(frame(
        "message_delta",
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": stop_reason, "stop_sequence": null},
            "usage": {"output_tokens": state.final_output_tokens()}
        }),
    ));
    out.push(frame("message_stop", json!({"type": "message_stop"})));
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
    fn request_translates_tools_and_tool_choice() {
        let req = json!({
            "model": "claude-3-7-sonnet",
            "system": "You are helpful.",
            "max_tokens": 512,
            "tools": [{"name": "get_weather", "description": "Weather", "input_schema": {"type": "object"}}],
            "tool_choice": {"type": "any"},
            "messages": [{"role": "user", "content": [{"type": "text", "text": "Hi"}]}]
        });
        let out = anthropic_to_openai_request(&req).unwrap();
        assert_eq!(out["tools"][0]["function"]["name"], "get_weather");
        assert_eq!(out["tool_choice"], "required");
        assert_eq!(out["messages"][0]["role"], "system");
        assert_eq!(out["max_tokens"], 512);
    }

    #[test]
    fn request_translates_tool_use_and_tool_result() {
        let req = json!({
            "model": "m",
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": "weather?"}]},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "Checking."},
                    {"type": "tool_use", "id": "tu_1", "name": "get_weather", "input": {"city": "Paris"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "tu_1", "content": [{"type": "text", "text": "sunny"}]}
                ]}
            ]
        });
        let out = anthropic_to_openai_request(&req).unwrap();
        let msgs = out["messages"].as_array().unwrap();
        let assistant = msgs.iter().find(|m| m["role"] == "assistant").unwrap();
        assert_eq!(assistant["tool_calls"][0]["id"], "tu_1");
        assert_eq!(
            assistant["tool_calls"][0]["function"]["name"],
            "get_weather"
        );
        let args: Value = serde_json::from_str(
            assistant["tool_calls"][0]["function"]["arguments"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(args["city"], "Paris");
        let tool_msg = msgs.iter().find(|m| m["role"] == "tool").unwrap();
        assert_eq!(tool_msg["tool_call_id"], "tu_1");
        assert_eq!(tool_msg["content"], "sunny");
    }

    #[test]
    fn request_translates_image_block() {
        let req = json!({
            "model": "m",
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "what is this?"},
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"}}
            ]}]
        });
        let out = anthropic_to_openai_request(&req).unwrap();
        let content = out["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content[1]["type"], "image_url");
        assert_eq!(content[1]["image_url"]["url"], "data:image/png;base64,AAAA");
    }

    #[test]
    fn response_translates_tool_calls_to_tool_use() {
        let resp = json!({
            "id": "c1",
            "choices": [{"message": {"role": "assistant", "content": null, "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{\"city\":\"Paris\"}"}}
            ]}, "finish_reason": "tool_calls"}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5}
        });
        let out = openai_to_anthropic_response(&resp, "m");
        assert_eq!(out["stop_reason"], "tool_use");
        assert_eq!(out["content"][0]["type"], "tool_use");
        assert_eq!(out["content"][0]["name"], "get_weather");
        assert_eq!(out["content"][0]["input"]["city"], "Paris");
    }

    #[test]
    fn streaming_text_then_tool_call() {
        let mut state = AnthropicStreamState::new("m");
        let c1 = json!({"choices": [{"delta": {"content": "Hello"}, "finish_reason": null}]})
            .to_string();
        let evs = openai_chunk_to_anthropic_events(&c1, &mut state);
        assert!(evs.iter().any(|e| e.contains("message_start")));
        assert!(evs
            .iter()
            .any(|e| e.contains("content_block_start") && e.contains("\"text\"")));
        assert!(evs.iter().any(|e| e.contains("Hello")));

        let c2 = json!({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call_1", "function": {"name": "get_weather", "arguments": "{\"city\":"}}]}, "finish_reason": null}]}).to_string();
        let evs2 = openai_chunk_to_anthropic_events(&c2, &mut state);
        assert!(evs2.iter().any(|e| e.contains("content_block_stop"))); // text block closed
        assert!(evs2
            .iter()
            .any(|e| e.contains("tool_use") && e.contains("get_weather")));

        let c3 = json!({"choices": [{"delta": {"tool_calls": [{"index": 0, "function": {"arguments": "\"Paris\"}"}}]}, "finish_reason": null}]}).to_string();
        let evs3 = openai_chunk_to_anthropic_events(&c3, &mut state);
        assert!(evs3
            .iter()
            .any(|e| e.contains("input_json_delta") && e.contains("Paris")));

        let c4 = json!({"choices": [{"delta": {}, "finish_reason": "tool_calls"}]}).to_string();
        let evs4 = openai_chunk_to_anthropic_events(&c4, &mut state);
        assert!(evs4
            .iter()
            .any(|e| e.contains("message_delta") && e.contains("tool_use")));
        assert!(evs4.iter().any(|e| e.contains("message_stop")));
    }

    #[test]
    fn streaming_done_synthesizes_stop() {
        let mut state = AnthropicStreamState::new("m");
        let c1 =
            json!({"choices": [{"delta": {"content": "hi"}, "finish_reason": null}]}).to_string();
        openai_chunk_to_anthropic_events(&c1, &mut state);
        let evs = openai_chunk_to_anthropic_events("[DONE]", &mut state);
        assert!(evs.iter().any(|e| e.contains("message_delta")));
        assert!(evs.iter().any(|e| e.contains("message_stop")));
    }
}
