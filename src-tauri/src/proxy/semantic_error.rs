//! Best-effort detection of provider errors hidden behind an HTTP 2xx status.
//!
//! A number of OpenAI-compatible gateways return a normal-looking HTTP status and
//! put the actual failure in a JSON envelope instead.  This module deliberately
//! recognizes only high-confidence shapes so a normal model response is not treated
//! as a provider failure.

use super::content_encoding::{decompress_body_with_limit, get_content_encoding};
use super::hyper_client::{ProxyResponse, MAX_RESPONSE_BODY_BYTES};
use super::ProxyError;
use bytes::Bytes;
use futures::StreamExt;
use serde_json::Value;
use std::time::Duration;

const MAX_ERROR_MESSAGE_CHARS: usize = 512;

/// Return a concise error message when `body` is an obvious error envelope.
pub(crate) fn json_error_message(body: &[u8]) -> Option<String> {
    let value = serde_json::from_slice::<Value>(body).ok()?;
    value_error_message(&value)
}

/// Inspect one complete SSE event. `None` means it is not an error event.
pub(crate) fn sse_error_message(block: &str) -> Option<String> {
    let mut event_name = None;
    let mut data_lines = Vec::new();

    for line in block.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(value) = line.strip_prefix("event:") {
            event_name = Some(value.trim());
        } else if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.strip_prefix(' ').unwrap_or(value));
        }
    }

    let event_name = event_name.unwrap_or("");
    if event_name.eq_ignore_ascii_case("error")
        || event_name.eq_ignore_ascii_case("response.error")
        || event_name.eq_ignore_ascii_case("response.failed")
    {
        if data_lines.is_empty() {
            return Some("upstream emitted an error event".to_string());
        }

        let data = data_lines.join("\n");
        if let Some(message) = json_error_message(data.as_bytes()) {
            return Some(message);
        }
        let message = data.trim();
        if !message.is_empty() && message != "[DONE]" {
            return Some(truncate_message(message));
        }
        return Some("upstream emitted an error event".to_string());
    }

    if data_lines.is_empty() {
        return None;
    }

    let data = data_lines.join("\n");
    if data.trim() == "[DONE]" {
        return None;
    }
    json_error_message(data.as_bytes())
}

/// Validate a buffered or live 2xx response before recording provider success.
pub(crate) async fn validate_success_response(
    response: ProxyResponse,
) -> Result<ProxyResponse, ProxyError> {
    let status = response.status();
    let headers = response.headers().clone();
    let encoding = get_content_encoding(&headers);
    let raw = response.bytes_with_limit(MAX_RESPONSE_BODY_BYTES).await?;
    let decoded = match encoding {
        Some(encoding) => {
            match decompress_body_with_limit(&encoding, &raw, MAX_RESPONSE_BODY_BYTES) {
                Ok(Some(decompressed)) => decompressed,
                _ => raw.to_vec(),
            }
        }
        None => raw.to_vec(),
    };

    if let Some(message) = json_error_message(&decoded) {
        return Err(ProxyError::TransformError(format!(
            "Upstream returned a 2xx semantic error: {message}"
        )));
    }

    Ok(ProxyResponse::buffered(status, headers, raw))
}

/// Prime a generic SSE response until its first complete event, allowing a clear
/// initial error event to participate in provider failover.
pub(crate) async fn validate_stream_start(
    response: ProxyResponse,
    first_byte_timeout: Duration,
) -> Result<ProxyResponse, ProxyError> {
    const MAX_PRIME_BYTES: usize = 256 * 1024;

    // Preserve the existing "disabled" semantics: with no first-byte timeout,
    // do not wait for an SSE delimiter that a gateway may never send.
    if first_byte_timeout.is_zero() {
        return Ok(response);
    }

    let status = response.status();
    let headers = response.headers().clone();
    let mut stream = Box::pin(response.bytes_stream());
    let mut replay_chunks: Vec<Bytes> = Vec::new();
    let mut parse_buffer = String::new();
    let mut utf8_remainder = Vec::new();

    loop {
        let next = tokio::time::timeout(first_byte_timeout, stream.next())
            .await
            .map_err(|_| {
                ProxyError::Timeout(format!(
                    "流式响应首个语义事件超时: {}s",
                    first_byte_timeout.as_secs()
                ))
            })?;

        let Some(chunk) = next else {
            if let Some(message) = json_error_message(parse_buffer.trim().as_bytes()) {
                return Err(ProxyError::TransformError(format!(
                    "Upstream returned a 2xx semantic error: {message}"
                )));
            }
            if let Some(message) = sse_error_message(parse_buffer.trim()) {
                return Err(ProxyError::TransformError(format!(
                    "Upstream returned a 2xx semantic error: {message}"
                )));
            }
            let replay = futures::stream::iter(replay_chunks.into_iter().map(Ok));
            return Ok(ProxyResponse::streamed(status, headers, replay));
        };

        let chunk = chunk.map_err(|error| {
            ProxyError::ForwardFailed(format!(
                "Failed while validating semantic stream start: {error}"
            ))
        })?;
        crate::proxy::sse::append_utf8_safe(&mut parse_buffer, &mut utf8_remainder, &chunk);
        replay_chunks.push(chunk);

        // A gateway may return one complete JSON document even when the request
        // asked for streaming. Parse it before looking for SSE delimiters.
        let trimmed = parse_buffer.trim();
        if matches!(trimmed.as_bytes().first(), Some(b'{') | Some(b'['))
            && serde_json::from_str::<Value>(trimmed).is_ok()
        {
            if let Some(message) = json_error_message(trimmed.as_bytes()) {
                return Err(ProxyError::TransformError(format!(
                    "Upstream returned a 2xx semantic error: {message}"
                )));
            }
            let replay = futures::stream::iter(replay_chunks.into_iter().map(Ok)).chain(stream);
            return Ok(ProxyResponse::streamed(status, headers, replay));
        }

        while let Some(block) = crate::proxy::sse::take_sse_block(&mut parse_buffer) {
            if let Some(message) = sse_error_message(&block) {
                return Err(ProxyError::TransformError(format!(
                    "Upstream returned a 2xx semantic error: {message}"
                )));
            }

            // One complete non-error event is enough to commit the stream. The
            // Responses-specific priming remains stricter in its own path.
            if block.lines().any(|line| line.trim_start().starts_with("data:")) {
                let replay =
                    futures::stream::iter(replay_chunks.into_iter().map(Ok)).chain(stream);
                return Ok(ProxyResponse::streamed(status, headers, replay));
            }
        }

        let trimmed = parse_buffer.trim_start();
        if !trimmed.starts_with("event:") && !trimmed.starts_with("data:") {
            // Unknown non-SSE payloads retain the old first-chunk behavior. JSON-looking
            // partial documents stay buffered so a split error envelope can be detected.
            if !matches!(trimmed.as_bytes().first(), Some(b'{') | Some(b'[')) {
                let replay =
                    futures::stream::iter(replay_chunks.into_iter().map(Ok)).chain(stream);
                return Ok(ProxyResponse::streamed(status, headers, replay));
            }
        }

        if replay_chunks.iter().map(Bytes::len).sum::<usize>() >= MAX_PRIME_BYTES {
            log::warn!(
                "[Forwarder] semantic stream priming exceeded {MAX_PRIME_BYTES} bytes; committing buffered stream"
            );
            let replay = futures::stream::iter(replay_chunks.into_iter().map(Ok)).chain(stream);
            return Ok(ProxyResponse::streamed(status, headers, replay));
        }
    }
}

fn value_error_message(value: &Value) -> Option<String> {
    let object = value.as_object()?;

    if let Some(error) = object.get("error").filter(|value| is_meaningful_error(value)) {
        return Some(format_error_value(error, object));
    }

    if object.get("success").and_then(Value::as_bool) == Some(false)
        || object.get("ok").and_then(Value::as_bool) == Some(false)
    {
        return Some(format_object_message(object, "upstream reported failure"));
    }

    if let Some(status) = object.get("status").and_then(Value::as_str) {
        if matches!(
            status.to_ascii_lowercase().as_str(),
            "error" | "failed" | "failure" | "cancelled" | "canceled" | "overloaded"
        ) {
            return Some(format_object_message(object, status));
        }
    }

    if object
        .get("status")
        .and_then(Value::as_u64)
        .is_some_and(|status| status >= 400)
        && has_message_field(object)
    {
        return Some(format_object_message(object, "upstream HTTP-style failure"));
    }

    if object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| matches!(kind.to_ascii_lowercase().as_str(), "error" | "error_event"))
    {
        return Some(format_object_message(object, "upstream error"));
    }

    // Common relay shape: {"code": 1006, "msg": "..."}. A business code is
    // considered meaningful only when an accompanying message is present.
    for key in ["code", "error_code", "errorCode"] {
        let Some(code) = object.get(key) else {
            continue;
        };
        if is_success_code(code) {
            continue;
        }
        if has_message_field(object) {
            return Some(format_object_message(object, "upstream business error"));
        }
    }

    // A few relays wrap their response one level below `data` while keeping the
    // same error envelope. Limit this to that conventional wrapper to avoid walking
    // arbitrary model-generated JSON.
    if let Some(data) = object.get("data").and_then(Value::as_object) {
        if let Some(error) = data.get("error").filter(|value| is_meaningful_error(value)) {
            return Some(format_error_value(error, data));
        }
        if data.get("success").and_then(Value::as_bool) == Some(false)
            || data.get("ok").and_then(Value::as_bool) == Some(false)
        {
            return Some(format_object_message(data, "upstream reported failure"));
        }
        for key in ["code", "error_code", "errorCode"] {
            let Some(code) = data.get(key) else {
                continue;
            };
            if !is_success_code(code) && has_message_field(data) {
                return Some(format_object_message(data, "upstream business error"));
            }
        }
    }

    None
}

fn is_meaningful_error(value: &Value) -> bool {
    match value {
        Value::String(message) => !message.trim().is_empty(),
        Value::Object(object) => {
            field_label(object, &["type", "status"]).is_some()
                || has_message_field(object)
                || ["code", "error_code", "errorCode"].iter().any(|key| {
                    object
                        .get(*key)
                        .is_some_and(|code| !is_success_code(code))
                })
                || object
                    .get("param")
                    .and_then(Value::as_str)
                    .is_some_and(|param| !param.trim().is_empty())
        }
        Value::Bool(value) => *value,
        Value::Array(values) => !values.is_empty(),
        Value::Null | Value::Number(_) => false,
    }
}

fn format_error_value(error: &Value, parent: &serde_json::Map<String, Value>) -> String {
    match error {
        Value::String(message) => truncate_message(message),
        Value::Object(object) => {
            let error_type = field_label(object, &["type", "code", "status"]);
            let message = object
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| object.get("msg").and_then(Value::as_str))
                .or_else(|| object.get("detail").and_then(Value::as_str))
                .or_else(|| object.get("error_description").and_then(Value::as_str));
            match (error_type, message) {
                (Some(error_type), Some(message)) => {
                    format!("{}: {}", error_type, truncate_message(message))
                }
                (None, Some(message)) => truncate_message(message),
                (Some(error_type), None) => {
                    format!("{}: {}", error_type, truncate_message(&error.to_string()))
                }
                (None, None) => format_object_message(object, "upstream error"),
            }
        }
        Value::Bool(true) => format_object_message(parent, "upstream reported failure"),
        _ => format_object_message(parent, "upstream error"),
    }
}

fn format_object_message(object: &serde_json::Map<String, Value>, fallback: &str) -> String {
    let error_type = field_label(object, &["type", "code", "status"]);
    let message = object
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| object.get("msg").and_then(Value::as_str))
        .or_else(|| object.get("detail").and_then(Value::as_str))
        .or_else(|| object.get("error_description").and_then(Value::as_str));

    match (error_type, message) {
        (Some(error_type), Some(message)) => {
            format!("{}: {}", error_type, truncate_message(message))
        }
        (None, Some(message)) => truncate_message(message),
        (Some(error_type), None) => format!("{}: {}", error_type, fallback),
        (None, None) => fallback.to_string(),
    }
}

fn field_label(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| match object.get(*key) {
        Some(Value::String(value)) if !value.trim().is_empty() => Some(value.clone()),
        Some(Value::Number(value)) => Some(value.to_string()),
        _ => None,
    })
}

fn has_message_field(object: &serde_json::Map<String, Value>) -> bool {
    ["message", "msg", "detail", "error_description"]
        .iter()
        .any(|key| {
            object
                .get(*key)
                .and_then(Value::as_str)
                .is_some_and(|s| !s.trim().is_empty())
        })
}

fn is_success_code(code: &Value) -> bool {
    match code {
        Value::Number(number) => number.as_i64() == Some(0) || number.as_i64() == Some(200),
        Value::String(code) => matches!(
            code.trim().to_ascii_lowercase().as_str(),
            "" | "0" | "200" | "ok" | "success" | "succeed"
        ),
        _ => false,
    }
}

fn truncate_message(message: &str) -> String {
    let message = message.split_whitespace().collect::<Vec<_>>().join(" ");
    if message.chars().count() <= MAX_ERROR_MESSAGE_CHARS {
        return message;
    }
    let truncated: String = message.chars().take(MAX_ERROR_MESSAGE_CHARS).collect();
    format!("{truncated}...")
}

#[cfg(test)]
mod tests {
    use super::{json_error_message, sse_error_message};

    #[test]
    fn recognizes_common_error_envelopes() {
        assert_eq!(
            json_error_message(
                br#"{"error":{"code":"insufficient_quota","message":"balance exhausted"}}"#,
            )
            .as_deref(),
            Some("insufficient_quota: balance exhausted")
        );
        assert_eq!(
            json_error_message(br#"{"success":false,"message":"余额不足"}"#).as_deref(),
            Some("余额不足")
        );
        assert_eq!(
            json_error_message(br#"{"code":1006,"msg":"quota exceeded"}"#).as_deref(),
            Some("1006: quota exceeded")
        );
    }

    #[test]
    fn ignores_success_payloads_and_success_codes() {
        assert!(json_error_message(br#"{"error":null,"choices":[{}]}"#).is_none());
        assert!(json_error_message(br#"{"error":{},"choices":[{}]}"#).is_none());
        assert!(json_error_message(br#"{"error":"","choices":[{}]}"#).is_none());
        assert!(json_error_message(br#"{"status":"incomplete","output":[]}"#).is_none());
        assert_eq!(
            json_error_message(br#"{"status":503,"message":"temporarily unavailable"}"#)
                .as_deref(),
            Some("503: temporarily unavailable")
        );
        assert!(json_error_message(br#"{"code":200,"msg":"ok","data":{}}"#).is_none());
        assert!(json_error_message(br#"{"message":"normal metadata","data":{}}"#).is_none());
        assert_eq!(
            json_error_message(br#"{"data":{"code":1006,"msg":"balance exhausted"}}"#)
                .as_deref(),
            Some("1006: balance exhausted")
        );
    }

    #[test]
    fn recognizes_sse_error_events_and_data() {
        assert_eq!(
            sse_error_message(
                "event: error\ndata: {\"error\":{\"type\":\"overloaded\",\"message\":\"busy\"}}"
            )
            .as_deref(),
            Some("overloaded: busy")
        );
        assert_eq!(
            sse_error_message("data: {\"code\":429,\"msg\":\"quota exceeded\"}")
                .as_deref(),
            Some("429: quota exceeded")
        );
        assert!(sse_error_message("data: {\"choices\":[{\"delta\":{}}]}").is_none());
    }
}
