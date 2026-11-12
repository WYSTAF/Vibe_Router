//! OpenAI -> Anthropic translation for *upstream* providers that speak the
//! Anthropic Messages protocol natively.
//!
//! The routing core always carries an OpenAI-shaped body. When a provider's
//! configured `format` is `anthropic`, the transport converts the request to
//! `/v1/messages` shape on the way out and converts the response (JSON or SSE)
//! back to OpenAI shape on the way in, so health/usage/classification all keep
//! working unchanged.

use std::collections::HashMap;

use serde_json::{json, Value};

/// Convert an OpenAI chat-completions request to an Anthropic Messages request.
pub fn openai_to_anthropic_request(openai: &Value) -> Value {
    let model = openai
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default();

    let mut system_parts: Vec<String> = Vec::new();
    let mut messages: Vec<Value> = Vec::new();

    if let Some(msgs) = openai.get("messages").and_then(Value::as_array) {
        for msg in msgs {
            let role = msg.get("role").and_then(Value::as_str).unwrap_or("user");
            match role {
                "system" => {
                    if let Some(t) = msg.get("content").and_then(Value::as_str) {
                        system_parts.push(t.to_string());
                    }
                }
                "tool" => {
                    let tool_call_id = msg
                        .get("tool_call_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let content = msg
                        .get("content")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    messages.push(json!({
                        "role": "user",
                        "content": [{"type": "tool_result", "tool_use_id": tool_call_id, "content": content}]
                    }));
                }
                "assistant" => {
                    let mut blocks: Vec<Value> = Vec::new();
                    if let Some(t) = msg.get("content").and_then(Value::as_str) {
                        if !t.is_empty() {
                            blocks.push(json!({"type": "text", "text": t}));
                        }
                    }
                    if let Some(tcs) = msg.get("tool_calls").and_then(Value::as_array) {
                        for tc in tcs {
                            let id = tc.get("id").and_then(Value::as_str).unwrap_or_default();
                            let func = tc.get("function").cloned().unwrap_or(json!({}));
                            let name = func.get("name").and_then(Value::as_str).unwrap_or_default();
                            let args = func
                                .get("arguments")
                                .and_then(Value::as_str)
                                .unwrap_or("{}");
                            let input: Value = serde_json::from_str(args).unwrap_or(json!({}));
                            blocks.push(
                                json!({"type": "tool_use", "id": id, "name": name, "input": input}),
                            );
                        }
                    }
                    if blocks.is_empty() {
                        blocks.push(json!({"type": "text", "text": ""}));
                    }
                    messages.push(json!({"role": "assistant", "content": blocks}));
                }
                _ => {
                    // user (or unknown): string or multimodal array.
                    let content = msg.get("content").cloned().unwrap_or(Value::Null);
                    let blocks = openai_user_content_to_anthropic(&content);
                    messages.push(json!({"role": "user", "content": blocks}));
                }
            }
        }
    }

    let mut req = json!({"model": model, "messages": messages});
    if !system_parts.is_empty() {
        req["system"] = json!(system_parts.join("\n"));
    }
    let max_tokens = openai
        .get("max_tokens")
        .and_then(Value::as_u64)
        .or_else(|| openai.get("max_completion_tokens").and_then(Value::as_u64))
        .unwrap_or(4096);
    req["max_tokens"] = json!(max_tokens);

    if let Some(t) = openai.get("temperature").and_then(Value::as_f64) {
        req["temperature"] = json!(t);
    }
    if let Some(p) = openai.get("top_p").and_then(Value::as_f64) {
        req["top_p"] = json!(p);
    }
    if let Some(s) = openai.get("stream").and_then(Value::as_bool) {
        req["stream"] = json!(s);
    }
    if let Some(stop) = openai.get("stop") {
        req["stop_sequences"] = stop.clone();
    }

    if let Some(tools) = openai.get("tools").and_then(Value::as_array) {
        let mut anth_tools = Vec::new();
        for t in tools {
            let func = t.get("function").cloned().unwrap_or(json!({}));
            let name = func.get("name").and_then(Value::as_str).unwrap_or_default();
            let desc = func
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let schema = func.get("parameters").cloned().unwrap_or(json!({}));
            anth_tools.push(json!({"name": name, "description": desc, "input_schema": schema}));
        }
        if !anth_tools.is_empty() {
            req["tools"] = json!(anth_tools);
        }
    }

    if let Some(choice) = openai.get("tool_choice") {
        if let Some(mapped) = openai_tool_choice_to_anthropic(choice) {
            req["tool_choice"] = mapped;
        }
    }

    req
}

fn openai_user_content_to_anthropic(content: &Value) -> Vec<Value> {
    if let Some(s) = content.as_str() {
        return vec![json!({"type": "text", "text": s})];
    }
    let mut blocks = Vec::new();
    if let Some(parts) = content.as_array() {
        for part in parts {
            match part.get("type").and_then(Value::as_str).unwrap_or("") {
                "text" => {
                    if let Some(t) = part.get("text").and_then(Value::as_str) {
                        blocks.push(json!({"type": "text", "text": t}));
                    }
                }
                "image_url" => {
                    let url = part
                        .get("image_url")
                        .and_then(|i| i.get("url"))
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if let Some(rest) = url.strip_prefix("data:") {
                        // data:<media>;base64,<data>
                        if let Some((meta, data)) = rest.split_once(";base64,") {
                            blocks.push(json!({"type": "image", "source": {"type": "base64", "media_type": meta, "data": data}}));
                        }
                    } else if !url.is_empty() {
                        blocks
                            .push(json!({"type": "image", "source": {"type": "url", "url": url}}));
                    }
                }
                _ => {}
            }
        }
    }
    if blocks.is_empty() {
        blocks.push(json!({"type": "text", "text": ""}));
    }
    blocks
}

fn openai_tool_choice_to_anthropic(choice: &Value) -> Option<Value> {
    if let Some(s) = choice.as_str() {
        return match s {
            "auto" => Some(json!({"type": "auto"})),
            "required" | "any" => Some(json!({"type": "any"})),
            "none" => Some(json!({"type": "none"})),
            _ => None,
        };
    }
    if choice.get("type").and_then(Value::as_str) == Some("function") {
        let name = choice
            .get("function")
            .and_then(|f| f.get("name"))
            .and_then(Value::as_str)?;
        return Some(json!({"type": "tool", "name": name}));
    }
    None
}

/// Convert an Anthropic Messages JSON response to OpenAI chat-completions shape.
pub fn anthropic_to_openai_response(anthropic: &Value, model: &str) -> Value {
    let id = anthropic
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("msg_default");
    let mut text_parts = Vec::new();
    let mut tool_calls = Vec::new();

    if let Some(blocks) = anthropic.get("content").and_then(Value::as_array) {
        for block in blocks {
            match block.get("type").and_then(Value::as_str).unwrap_or("") {
                "text" => {
                    if let Some(t) = block.get("text").and_then(Value::as_str) {
                        text_parts.push(t.to_string());
                    }
                }
                "tool_use" => {
                    let tc_id = block.get("id").and_then(Value::as_str).unwrap_or_default();
                    let name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let input = block.get("input").cloned().unwrap_or(json!({}));
                    tool_calls.push(json!({
                        "id": tc_id,
                        "type": "function",
                        "function": {"name": name, "arguments": serde_json::to_string(&input).unwrap_or_else(|_| "{}".into())}
                    }));
                }
                _ => {}
            }
        }
    }

    let stop_reason = anthropic
        .get("stop_reason")
        .and_then(Value::as_str)
        .unwrap_or("end_turn");
    let finish_reason = match stop_reason {
        "tool_use" => "tool_calls",
        "max_tokens" => "length",
        _ => "stop",
    };

    let text = text_parts.join("\n");
    let mut message = json!({"role": "assistant", "content": if text.is_empty() { Value::Null } else { json!(text) }});
    if !tool_calls.is_empty() {
        message["tool_calls"] = json!(tool_calls);
    }

    let input_tokens = anthropic
        .get("usage")
        .and_then(|u| u.get("input_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = anthropic
        .get("usage")
        .and_then(|u| u.get("output_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);

    json!({
        "id": id,
        "object": "chat.completion",
        "model": model,
        "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
        "usage": {"prompt_tokens": input_tokens, "completion_tokens": output_tokens, "total_tokens": input_tokens + output_tokens}
    })
}

/// State for converting an Anthropic SSE event stream into OpenAI chunks.
#[derive(Debug, Default)]
pub struct AnthropicToOpenAiStream {
    pub id: String,
    pub model: String,
    started: bool,
    finished: bool,
    /// Anthropic content block index -> OpenAI tool_call index.
    tool_indices: HashMap<u64, u64>,
    next_tool_index: u64,
    finish_reason: String,
    output_tokens: u64,
}

impl AnthropicToOpenAiStream {
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            id: format!(
                "chatcmpl-{:016x}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ),
            model: model.into(),
            finish_reason: "stop".to_string(),
            ..Default::default()
        }
    }

    fn chunk(&self, delta: Value, finish_reason: Option<&str>) -> String {
        json!({
            "id": self.id,
            "object": "chat.completion.chunk",
            "model": self.model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}]
        })
        .to_string()
    }

    /// Convert one Anthropic SSE `data:` payload into zero or more OpenAI chunk
    /// JSON strings. The terminal `[DONE]` token is emitted by the caller after
    /// `message_stop`.
    pub fn push(&mut self, data: &str) -> Vec<String> {
        let mut out = Vec::new();
        let ev: Value = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(_) => return out,
        };
        let kind = ev.get("type").and_then(Value::as_str).unwrap_or("");
        match kind {
            "message_start" => {
                if !self.started {
                    self.started = true;
                    if let Some(m) = ev.get("message") {
                        if let Some(id) = m.get("id").and_then(Value::as_str) {
                            self.id = id.to_string();
                        }
                    }
                    out.push(self.chunk(json!({"role": "assistant", "content": ""}), None));
                }
            }
            "content_block_start"
                if ev
                    .get("content_block")
                    .and_then(|b| b.get("type"))
                    .and_then(Value::as_str)
                    == Some("tool_use") =>
            {
                let block_idx = ev.get("index").and_then(Value::as_u64).unwrap_or(0);
                let block = ev.get("content_block").cloned().unwrap_or(json!({}));
                let tool_idx = self.next_tool_index;
                self.next_tool_index += 1;
                self.tool_indices.insert(block_idx, tool_idx);
                let id = block.get("id").and_then(Value::as_str).unwrap_or_default();
                let name = block
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                out.push(self.chunk(
                    json!({"tool_calls": [{"index": tool_idx, "id": id, "type": "function", "function": {"name": name, "arguments": ""}}]}),
                    None,
                ));
            }
            "content_block_delta" => {
                let block_idx = ev.get("index").and_then(Value::as_u64).unwrap_or(0);
                let delta = ev.get("delta").cloned().unwrap_or(json!({}));
                match delta.get("type").and_then(Value::as_str).unwrap_or("") {
                    "text_delta" => {
                        if let Some(t) = delta.get("text").and_then(Value::as_str) {
                            out.push(self.chunk(json!({"content": t}), None));
                        }
                    }
                    "input_json_delta" => {
                        if let Some(&tool_idx) = self.tool_indices.get(&block_idx) {
                            if let Some(pj) = delta.get("partial_json").and_then(Value::as_str) {
                                out.push(self.chunk(
                                    json!({"tool_calls": [{"index": tool_idx, "function": {"arguments": pj}}]}),
                                    None,
                                ));
                            }
                        }
                    }
                    _ => {}
                }
            }
            "message_delta" => {
                if let Some(sr) = ev
                    .get("delta")
                    .and_then(|d| d.get("stop_reason"))
                    .and_then(Value::as_str)
                {
                    self.finish_reason = match sr {
                        "tool_use" => "tool_calls".to_string(),
                        "max_tokens" => "length".to_string(),
                        _ => "stop".to_string(),
                    };
                }
                if let Some(ot) = ev
                    .get("usage")
                    .and_then(|u| u.get("output_tokens"))
                    .and_then(Value::as_u64)
                {
                    self.output_tokens = ot;
                }
            }
            "message_stop" if !self.finished => {
                self.finished = true;
                out.push(self.chunk(json!({}), Some(&self.finish_reason)));
                out.push(
                    json!({
                        "id": self.id,
                        "object": "chat.completion.chunk",
                        "model": self.model,
                        "choices": [],
                        "usage": {"prompt_tokens": 0, "completion_tokens": self.output_tokens, "total_tokens": self.output_tokens}
                    })
                    .to_string(),
                );
            }
            _ => {}
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_request_tools_and_messages() {
        let openai = json!({
            "model": "m",
            "max_tokens": 100,
            "messages": [
                {"role": "system", "content": "be brief"},
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": null, "tool_calls": [{"id": "c1", "type": "function", "function": {"name": "f", "arguments": "{\"x\":1}"}}]},
                {"role": "tool", "tool_call_id": "c1", "content": "ok"}
            ],
            "tools": [{"type": "function", "function": {"name": "f", "description": "d", "parameters": {"type": "object"}}}],
            "tool_choice": "required"
        });
        let anth = openai_to_anthropic_request(&openai);
        assert_eq!(anth["system"], "be brief");
        assert_eq!(anth["max_tokens"], 100);
        assert_eq!(anth["tool_choice"]["type"], "any");
        assert_eq!(anth["tools"][0]["name"], "f");
        let msgs = anth["messages"].as_array().unwrap();
        assert!(msgs.iter().any(|m| m["content"][0]["type"] == "tool_use"));
        assert!(msgs
            .iter()
            .any(|m| m["content"][0]["type"] == "tool_result"));
    }

    #[test]
    fn response_roundtrip_tool_use() {
        let anth = json!({
            "id": "msg_1",
            "content": [{"type": "tool_use", "id": "tu1", "name": "f", "input": {"x": 1}}],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 3, "output_tokens": 4}
        });
        let oa = anthropic_to_openai_response(&anth, "m");
        assert_eq!(oa["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(
            oa["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            "f"
        );
        assert_eq!(oa["usage"]["prompt_tokens"], 3);
        assert_eq!(oa["usage"]["completion_tokens"], 4);
    }

    #[test]
    fn stream_converts_text_and_tools() {
        let mut s = AnthropicToOpenAiStream::new("m");
        let start = json!({"type": "message_start", "message": {"id": "msg_1"}}).to_string();
        assert!(!s.push(&start).is_empty());
        let bs = json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}).to_string();
        s.push(&bs);
        let bd = json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "hi"}}).to_string();
        let chunks = s.push(&bd);
        assert!(chunks[0].contains("\"content\":\"hi\""));
        let tb = json!({"type": "content_block_start", "index": 1, "content_block": {"type": "tool_use", "id": "tu1", "name": "f", "input": {}}}).to_string();
        let tchunks = s.push(&tb);
        assert!(tchunks[0].contains("tool_calls"));
        let md = json!({"type": "message_delta", "delta": {"stop_reason": "tool_use"}, "usage": {"output_tokens": 9}}).to_string();
        s.push(&md);
        let stop = json!({"type": "message_stop"}).to_string();
        let final_chunks = s.push(&stop);
        assert!(final_chunks
            .iter()
            .any(|c| c.contains("tool_calls") && c.contains("finish_reason")));
        assert!(final_chunks
            .iter()
            .any(|c| c.contains("\"completion_tokens\":9")));
    }
}
