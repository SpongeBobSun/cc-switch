//! Semantic health detection for OpenAI Responses SSE streams.
//!
//! Upstream aggregators can silently route a Responses request to a *degraded*
//! channel that does not understand the Responses tool protocol. The model never
//! sees the tools, prints fake tool calls as plain text, and nothing executes —
//! while the HTTP status stays `200` and the SSE envelope stays valid.
//!
//! The only reliable early signal is an **identifier fingerprint**:
//!
//! | position                          | healthy                         | degraded          |
//! |-----------------------------------|---------------------------------|-------------------|
//! | `response.created` → `response.id`| `resp_` + 50 lowercase hex      | `resp_` + 32 hex  |
//! | `response.output_item.added` item | `rs_`/`msg_`/… + long session id| `…_` + 32 hex     |
//!
//! This module is split into two deliberately different tiers:
//!
//! * **Tier A** — available in the first few SSE events, *before any client byte
//!   is written*. Only Tier A may drive replay. It is implemented as a pure,
//!   side-effect-free state machine (`ResponseProbe`) plus a streaming adapter
//!   (`probe_sse`) used by tests and the forwarder.
//! * **Tier B** — only knowable once the stream has finished (usage collapse,
//!   tools requested but zero tool calls, the model claiming it has no tools).
//!   Tier B is **never** allowed to trigger a replay; it exists for circuit
//!   weighting and after-the-fact auditing only.
//!
//! Classification is whitelist-based on purpose: an id that is not an exact
//! 50-hex / 32-hex shape is `Unknown`, which means **pass through, never replay**
//! ("rather miss than kill a healthy response").

use serde_json::Value;
use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use futures::{stream, Stream, StreamExt};

use super::{
    hyper_client::ProxyResponse,
    sse::{append_utf8_safe, strip_sse_field, take_sse_block},
};

/// Hard cap on the Tier A buffering window. The window exists to collect a
/// second independent sample without adding measurable latency to healthy
/// requests. It is deliberately not configurable beyond this.
pub const MAX_TIER_A_WINDOW: Duration = Duration::from_millis(200);
/// Hard cap on the number of buffered Tier A events.
pub const MAX_TIER_A_EVENTS: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SemanticTier {
    A,
    B,
}

/// Verdict for a single identifier sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdVerdict {
    /// Matches the known-healthy whitelist.
    KnownGood,
    /// Matches the known-degraded 32-hex whitelist.
    Suspect,
    /// Anything else. Never triggers anything.
    Unknown,
}

/// Structured evidence for logs and the audit table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SemanticEvidence {
    ResponseIdKnownGood,
    ResponseId32Hex,
    ItemId32Hex,
    SessionScopedItemId,
    UnknownResponseId,
    UnknownItemId,
}

impl SemanticEvidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ResponseIdKnownGood => "response_id_known_good",
            Self::ResponseId32Hex => "response_id_32hex",
            Self::ItemId32Hex => "item_id_32hex",
            Self::SessionScopedItemId => "item_id_session_scoped",
            Self::UnknownResponseId => "response_id_unknown",
            Self::UnknownItemId => "item_id_unknown",
        }
    }

    /// `true` when the sample is itself positive evidence of degradation.
    pub fn is_suspect(self) -> bool {
        matches!(self, Self::ResponseId32Hex | Self::ItemId32Hex)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SemanticObservation {
    pub tier: SemanticTier,
    pub verdict: IdVerdict,
    pub evidence: SemanticEvidence,
}

/// Tier B signals are audit-only and never trigger replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TierBSignal {
    /// `input_tokens` fell sharply versus the previous request of the session,
    /// consistent with tool results never entering the prompt.
    UsageCollapse,
    /// The request declared `tools` but the stream produced no tool-call item.
    ToolsRequestedButNoToolCall,
    /// The assistant explicitly claims it has no tools / cannot see definitions.
    SelfReportedNoTools,
}

impl TierBSignal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UsageCollapse => "usage_collapse",
            Self::ToolsRequestedButNoToolCall => "tools_requested_but_no_tool_call",
            Self::SelfReportedNoTools => "self_reported_no_tools",
        }
    }
}

/// Everything the Tier B analyzer needs. All fields come from data that is only
/// complete once the stream has ended.
#[derive(Debug, Clone)]
pub struct TierBInput<'a> {
    pub request_had_tools: bool,
    pub previous_input_tokens: Option<u64>,
    pub input_tokens: Option<u64>,
    pub function_call_items: u64,
    pub custom_tool_call_items: u64,
    pub assistant_texts: &'a [String],
}

/// Analyze the post-stream Tier B signals. Pure and deterministic.
pub fn analyze_tier_b(input: &TierBInput<'_>) -> Vec<TierBSignal> {
    let mut signals = Vec::new();

    if let (Some(previous), Some(current)) = (input.previous_input_tokens, input.input_tokens) {
        // A healthy tool loop grows the prompt. Requiring both an absolute floor
        // and a >50% drop keeps ordinary compaction / short turns out of the way.
        if previous >= 10_000 && current.saturating_mul(2) < previous {
            signals.push(TierBSignal::UsageCollapse);
        }
    }

    if input.request_had_tools
        && input.function_call_items == 0
        && input.custom_tool_call_items == 0
    {
        signals.push(TierBSignal::ToolsRequestedButNoToolCall);
    }

    let needles = [
        "no tool",
        "not seeing any definitions",
        "no definitions in the prompt",
        "tools were not provided",
        "no tools were provided",
        "cannot call tools",
        "没有工具",
        "未提供工具",
        "无法调用工具",
    ];
    let self_reported = input.assistant_texts.iter().any(|text| {
        let lowered = text.to_ascii_lowercase();
        needles.iter().any(|needle| lowered.contains(needle))
    });
    if self_reported {
        signals.push(TierBSignal::SelfReportedNoTools);
    }

    signals
}

/// Decision after feeding one event into [`ResponseProbe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeDecision {
    /// Keep buffering; the window is still open.
    Continue,
    /// Commit and pass the buffered bytes through unchanged.
    Pass,
    /// Tier A degradation confirmed; replay is safe (no client byte written yet).
    Degraded,
}

/// Pure Tier A state machine. Feed parsed SSE events in order.
///
/// Replay is only confirmed when the **first** event carries a suspect
/// `response.created` id *and* at least one further independent suspect sample
/// arrives before the window closes at the first `response.output_item.added`,
/// [`MAX_TIER_A_EVENTS`] events, or [`MAX_TIER_A_WINDOW`].
#[derive(Debug)]
pub struct ResponseProbe {
    first_event_seen: bool,
    first_event_suspect: bool,
    samples: HashSet<SemanticEvidence>,
    events_seen: usize,
    finished: bool,
    degraded: bool,
    request_id: Option<String>,
    last_sample: Option<SemanticEvidence>,
}

impl Default for ResponseProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseProbe {
    pub fn new() -> Self {
        Self {
            first_event_seen: false,
            first_event_suspect: false,
            samples: HashSet::new(),
            events_seen: 0,
            finished: false,
            degraded: false,
            request_id: None,
            last_sample: None,
        }
    }

    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    pub fn decision(&self) -> ProbeDecision {
        if self.degraded {
            ProbeDecision::Degraded
        } else if self.finished {
            ProbeDecision::Pass
        } else {
            ProbeDecision::Continue
        }
    }

    /// Ordered, de-duplicated evidence for logging / the audit row.
    pub fn evidence(&self) -> Vec<SemanticEvidence> {
        let mut ordered = Vec::new();
        for candidate in [
            SemanticEvidence::ResponseId32Hex,
            SemanticEvidence::ItemId32Hex,
            SemanticEvidence::SessionScopedItemId,
            SemanticEvidence::UnknownResponseId,
            SemanticEvidence::UnknownItemId,
        ] {
            if self.samples.contains(&candidate) {
                ordered.push(candidate);
            }
        }
        ordered
    }

    pub fn suspect_sample_count(&self) -> usize {
        self.samples
            .iter()
            .filter(|sample| sample.is_suspect())
            .count()
    }

    /// Close the window because the time budget elapsed. Never degrades on its
    /// own: a timeout without two independent samples is a pass-through.
    pub fn close_window(&mut self) -> ProbeDecision {
        if self.degraded {
            return ProbeDecision::Degraded;
        }
        self.finished = true;
        ProbeDecision::Pass
    }

    /// Feed one parsed SSE event. `event_type` may be empty; the payload's own
    /// `type` field is used as a fallback.
    pub fn observe_event(&mut self, event_type: &str, payload: &Value) -> ProbeDecision {
        if self.finished {
            return self.decision();
        }
        let event = if event_type.is_empty() {
            payload.get("type").and_then(Value::as_str).unwrap_or("")
        } else {
            event_type
        };

        self.events_seen += 1;
        let observation = observe(event, payload);
        self.last_sample = Some(observation.evidence);

        if event == "response.created" && self.request_id.is_none() {
            self.request_id = payload
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(Value::as_str)
                .or_else(|| payload.get("id").and_then(Value::as_str))
                .map(ToString::to_string);
        }

        if !self.first_event_seen {
            self.first_event_seen = true;
            self.first_event_suspect =
                event == "response.created" && observation.verdict == IdVerdict::Suspect;
            if self.first_event_suspect {
                self.samples.insert(observation.evidence);
                return ProbeDecision::Continue;
            }
            // The first event is not the expected suspect `response.created`:
            // never replay, never look further.
            self.finished = true;
            return ProbeDecision::Pass;
        }

        if !self.first_event_suspect {
            self.finished = true;
            return ProbeDecision::Pass;
        }

        if observation.verdict == IdVerdict::Suspect {
            self.samples.insert(observation.evidence);
            if self.suspect_sample_count() >= 2 {
                self.degraded = true;
                self.finished = true;
                return ProbeDecision::Degraded;
            }
        }

        // `response.output_item.added` closes the collection window whether or
        // not it was a second suspect sample.
        if event == "response.output_item.added" || self.events_seen >= MAX_TIER_A_EVENTS {
            self.finished = true;
            return ProbeDecision::Pass;
        }

        ProbeDecision::Continue
    }
}

/// Classify one SSE event's identifier shape.
pub fn observe(event_type: &str, payload: &Value) -> SemanticObservation {
    match event_type {
        "response.created" => {
            let id = payload
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(Value::as_str)
                .or_else(|| payload.get("id").and_then(Value::as_str));
            classify_response_id(id)
        }
        "response.output_item.added" => {
            let id = payload
                .get("item")
                .and_then(|item| item.get("id"))
                .and_then(Value::as_str)
                .or_else(|| payload.get("item_id").and_then(Value::as_str));
            classify_item_id(id)
        }
        _ => SemanticObservation {
            tier: SemanticTier::A,
            verdict: IdVerdict::Unknown,
            evidence: SemanticEvidence::UnknownItemId,
        },
    }
}

/// `resp_` + exactly 50 lowercase hex is known-good; `resp_` + exactly 32
/// lowercase hex is known-degraded. Everything else is `Unknown`.
pub fn classify_response_id(id: Option<&str>) -> SemanticObservation {
    let (verdict, evidence) = match id.and_then(|value| value.strip_prefix("resp_")) {
        Some(hex) if hex.len() == 50 && is_lowercase_hex(hex) => {
            (IdVerdict::KnownGood, SemanticEvidence::ResponseIdKnownGood)
        }
        Some(hex) if hex.len() == 32 && is_lowercase_hex(hex) => {
            (IdVerdict::Suspect, SemanticEvidence::ResponseId32Hex)
        }
        _ => (IdVerdict::Unknown, SemanticEvidence::UnknownResponseId),
    };
    SemanticObservation {
        tier: SemanticTier::A,
        verdict,
        evidence,
    }
}

/// `(rs|msg|fc|ctc)_` + exactly 32 lowercase hex is known-degraded. The healthy
/// long session-scoped form and unknown prefixes stay `Unknown`.
pub fn classify_item_id(id: Option<&str>) -> SemanticObservation {
    let Some(id) = id else {
        return SemanticObservation {
            tier: SemanticTier::A,
            verdict: IdVerdict::Unknown,
            evidence: SemanticEvidence::UnknownItemId,
        };
    };
    let suffix = ["rs_", "msg_", "fc_", "ctc_"]
        .iter()
        .find_map(|prefix| id.strip_prefix(prefix));
    let (verdict, evidence) = match suffix {
        Some(hex) if hex.len() == 32 && is_lowercase_hex(hex) => {
            (IdVerdict::Suspect, SemanticEvidence::ItemId32Hex)
        }
        Some(_) => (IdVerdict::Unknown, SemanticEvidence::SessionScopedItemId),
        None => (IdVerdict::Unknown, SemanticEvidence::UnknownItemId),
    };
    SemanticObservation {
        tier: SemanticTier::A,
        verdict,
        evidence,
    }
}

fn is_lowercase_hex(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Result of the Tier A streaming probe.
pub enum ProbeOutcome {
    /// No replayable degradation (healthy, unknown, or window timeout).
    Pass {
        response: ProxyResponse,
        detected: Option<SemanticEvidence>,
        request_id: Option<String>,
    },
    /// Degradation confirmed before any client byte was written. The wrapped
    /// response carries the already-consumed prefix so the caller can either
    /// drop it (abort) or forward it (dry-run).
    Degraded {
        response: ProxyResponse,
        evidence: Vec<SemanticEvidence>,
        request_id: Option<String>,
    },
}

/// Buffer at most the Tier A window of an SSE response, then decide.
///
/// While buffering, not a single byte is exposed to the caller. On `Pass` the
/// exact original bytes are replayed ahead of the still-live upstream stream, so
/// healthy requests pay only the window cost and byte-for-byte identity is
/// preserved.
pub async fn probe_sse(response: ProxyResponse, window: Duration) -> ProbeOutcome {
    let status = response.status();
    let headers = response.headers().clone();
    let mut upstream: std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
        Box::pin(response.bytes_stream());

    let mut buffered = Vec::<Bytes>::new();
    let mut parse_buffer = String::new();
    let mut utf8_remainder = Vec::new();
    let mut probe = ResponseProbe::new();
    let deadline = tokio::time::Instant::now() + window.min(MAX_TIER_A_WINDOW);

    loop {
        let next = match tokio::time::timeout_at(deadline, upstream.next()).await {
            Ok(value) => value,
            Err(_) => return pass(status, headers, buffered, upstream, &probe),
        };

        let Some(chunk) = next else {
            return pass(status, headers, buffered, upstream, &probe);
        };

        let bytes = match chunk {
            Ok(bytes) => bytes,
            Err(error) => {
                // Surface the original error after replaying everything we
                // already consumed so the caller sees an unmodified stream.
                let tail = stream::iter(buffered.into_iter().map(Ok))
                    .chain(stream::once(async move { Err(error) }));
                return ProbeOutcome::Pass {
                    response: ProxyResponse::streamed(status, headers, tail),
                    detected: probe.last_sample,
                    request_id: probe.request_id().map(ToString::to_string),
                };
            }
        };

        append_utf8_safe(&mut parse_buffer, &mut utf8_remainder, &bytes);
        buffered.push(bytes);

        while let Some(block) = take_sse_block(&mut parse_buffer) {
            let Some((event_type, payload)) = parse_sse_data(&block) else {
                continue;
            };
            match probe.observe_event(&event_type, &payload) {
                ProbeDecision::Degraded => {
                    return ProbeOutcome::Degraded {
                        response: rebuild(status, headers, buffered, upstream),
                        evidence: probe.evidence(),
                        request_id: probe.request_id().map(ToString::to_string),
                    }
                }
                ProbeDecision::Pass => return pass(status, headers, buffered, upstream, &probe),
                ProbeDecision::Continue => {}
            }
        }
    }
}

/// Parse one SSE block into `(event_type, payload)`. Returns `None` for blocks
/// without a JSON `data` payload (comments, `[DONE]`, …).
pub fn parse_sse_data(block: &str) -> Option<(String, Value)> {
    let mut named_event = None;
    let mut data_lines = Vec::new();
    for line in block.lines() {
        if let Some(event) = strip_sse_field(line, "event") {
            named_event = Some(event.trim().to_string());
        } else if let Some(data) = strip_sse_field(line, "data") {
            data_lines.push(data);
        }
    }
    if data_lines.is_empty() {
        return None;
    }
    let joined = data_lines.join("\n");
    if joined.trim() == "[DONE]" {
        return None;
    }
    let payload = serde_json::from_str::<Value>(&joined).ok()?;
    let event_type = named_event
        .filter(|event| !event.is_empty())
        .or_else(|| {
            payload
                .get("type")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .unwrap_or_default();
    Some((event_type, payload))
}

fn pass(
    status: http::StatusCode,
    headers: http::HeaderMap,
    buffered: Vec<Bytes>,
    upstream: std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
    probe: &ResponseProbe,
) -> ProbeOutcome {
    ProbeOutcome::Pass {
        response: rebuild(status, headers, buffered, upstream),
        detected: probe.last_sample,
        request_id: probe.request_id().map(ToString::to_string),
    }
}

fn rebuild(
    status: http::StatusCode,
    headers: http::HeaderMap,
    buffered: Vec<Bytes>,
    upstream: std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
) -> ProxyResponse {
    let stream = stream::iter(buffered.into_iter().map(Ok)).chain(upstream);
    ProxyResponse::streamed(status, headers, stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const GOOD_RESP: &str = "resp_0b90cdd71061d98f016a9bad07e29487d1ac8b3aeda7d200a2";
    const GOOD_RESP_2: &str = "resp_0b1a742d14f12666016a9bad0ccae887d1b30a0ed37abd1bb7";
    const BAD_RESP: &str = "resp_d96da2e57b58406c921d0c90486646c3";
    const BAD_RESP_2: &str = "resp_26cfe358111348e2adc810fb4942c0a1";
    const GOOD_ITEM: &str = "rs_03d5a775fca33580016ab1432b1f7887d1b2caba9c3d9b7f83";
    const BAD_ITEM: &str = "rs_cea4809436f44b6b8fde96bbe033f447";
    const BAD_MSG: &str = "msg_feb7455d07dd483e85c564679a6db380";

    #[test]
    fn response_id_whitelist_covers_all_four_shapes() {
        assert_eq!(
            classify_response_id(Some(GOOD_RESP)).verdict,
            IdVerdict::KnownGood
        );
        assert_eq!(
            classify_response_id(Some(GOOD_RESP_2)).verdict,
            IdVerdict::KnownGood
        );
        assert_eq!(
            classify_response_id(Some(BAD_RESP)).verdict,
            IdVerdict::Suspect
        );
        assert_eq!(
            classify_response_id(Some(BAD_RESP_2)).verdict,
            IdVerdict::Suspect
        );
        assert_eq!(
            classify_response_id(Some("resp_bad")).verdict,
            IdVerdict::Unknown
        );
        assert_eq!(
            classify_response_id(Some("chatcmpl-abc")).verdict,
            IdVerdict::Unknown
        );
        // Uppercase hex and mixed-length ids are never "suspect by exclusion".
        assert_eq!(
            classify_response_id(Some("resp_D96DA2E57B58406C921D0C90486646C3")).verdict,
            IdVerdict::Unknown
        );
        assert_eq!(
            classify_response_id(Some("resp_d96da2e57b58406c921d0c90486646c")).verdict,
            IdVerdict::Unknown
        );
        assert_eq!(classify_response_id(None).verdict, IdVerdict::Unknown);
        assert_eq!(classify_response_id(Some("")).verdict, IdVerdict::Unknown);
        // Non-ASCII must not panic on byte slicing.
        assert_eq!(
            classify_response_id(Some("resp_中文测试")).verdict,
            IdVerdict::Unknown
        );
    }

    #[test]
    fn item_id_whitelist_covers_all_shapes() {
        assert_eq!(classify_item_id(Some(BAD_ITEM)).verdict, IdVerdict::Suspect);
        assert_eq!(classify_item_id(Some(BAD_MSG)).verdict, IdVerdict::Suspect);
        assert_eq!(
            classify_item_id(Some(GOOD_ITEM)).verdict,
            IdVerdict::Unknown
        );
        assert_eq!(
            classify_item_id(Some("rs_short")).verdict,
            IdVerdict::Unknown
        );
        assert_eq!(
            classify_item_id(Some("unknown_prefix_32")).verdict,
            IdVerdict::Unknown
        );
        assert_eq!(classify_item_id(Some("")).verdict, IdVerdict::Unknown);
        assert_eq!(classify_item_id(None).verdict, IdVerdict::Unknown);
        // fc_/ctc_ tool item ids follow the same rule.
        assert_eq!(
            classify_item_id(Some("fc_cea4809436f44b6b8fde96bbe033f447")).verdict,
            IdVerdict::Suspect
        );
        assert_eq!(
            classify_item_id(Some("ctc_cea4809436f44b6b8fde96bbe033f447")).verdict,
            IdVerdict::Suspect
        );
    }

    fn created(id: &str) -> Value {
        json!({"type":"response.created","response":{"id":id}})
    }

    fn item_added(id: &str) -> Value {
        json!({"type":"response.output_item.added","item":{"id":id}})
    }

    #[test]
    fn healthy_first_event_passes_immediately_without_replay() {
        let mut probe = ResponseProbe::new();
        assert_eq!(
            probe.observe_event("response.created", &created(GOOD_RESP)),
            ProbeDecision::Pass
        );
        assert_eq!(probe.decision(), ProbeDecision::Pass);
        assert!(probe.evidence().is_empty());
    }

    #[test]
    fn degraded_stream_needs_two_independent_tier_a_samples() {
        let mut probe = ResponseProbe::new();
        assert_eq!(
            probe.observe_event("response.created", &created(BAD_RESP)),
            ProbeDecision::Continue
        );
        assert_eq!(
            probe.observe_event("response.output_item.added", &item_added(BAD_ITEM)),
            ProbeDecision::Degraded
        );
        assert_eq!(probe.suspect_sample_count(), 2);
        assert!(probe
            .evidence()
            .contains(&SemanticEvidence::ResponseId32Hex));
        assert!(probe.evidence().contains(&SemanticEvidence::ItemId32Hex));
    }

    #[test]
    fn suspect_response_id_with_long_item_id_is_not_replayed() {
        let mut probe = ResponseProbe::new();
        assert_eq!(
            probe.observe_event("response.created", &created(BAD_RESP)),
            ProbeDecision::Continue
        );
        // Only one independent suspect sample before the window closes.
        assert_eq!(
            probe.observe_event("response.output_item.added", &item_added(GOOD_ITEM)),
            ProbeDecision::Pass
        );
        assert_eq!(probe.decision(), ProbeDecision::Pass);
    }

    #[test]
    fn unknown_first_event_never_replays() {
        let mut probe = ResponseProbe::new();
        assert_eq!(
            probe.observe_event("response.created", &created("resp_not_a_known_shape")),
            ProbeDecision::Pass
        );
    }

    #[test]
    fn event_cap_closes_the_window_without_degrading() {
        let mut probe = ResponseProbe::new();
        assert_eq!(
            probe.observe_event("response.created", &created(BAD_RESP)),
            ProbeDecision::Continue
        );
        for _ in 0..(MAX_TIER_A_EVENTS - 1) {
            let decision = probe.observe_event("response.in_progress", &json!({"type":"x"}));
            if decision == ProbeDecision::Pass {
                return;
            }
        }
        assert_eq!(probe.decision(), ProbeDecision::Pass);
    }

    #[test]
    fn timeout_close_never_degrades() {
        let mut probe = ResponseProbe::new();
        probe.observe_event("response.created", &created(BAD_RESP));
        assert_eq!(probe.close_window(), ProbeDecision::Pass);
    }

    #[test]
    fn tier_b_never_reports_tier_a_degradation() {
        let texts = vec!["I'm not seeing any definitions in the prompt".to_string()];
        let signals = analyze_tier_b(&TierBInput {
            request_had_tools: true,
            previous_input_tokens: Some(95_551),
            input_tokens: Some(12_855),
            function_call_items: 0,
            custom_tool_call_items: 0,
            assistant_texts: &texts,
        });
        assert!(signals.contains(&TierBSignal::UsageCollapse));
        assert!(signals.contains(&TierBSignal::ToolsRequestedButNoToolCall));
        assert!(signals.contains(&TierBSignal::SelfReportedNoTools));
    }

    #[test]
    fn tier_b_ignores_healthy_tool_loop() {
        let texts = vec!["Running the command".to_string()];
        let signals = analyze_tier_b(&TierBInput {
            request_had_tools: true,
            previous_input_tokens: Some(95_551),
            input_tokens: Some(103_116),
            function_call_items: 2,
            custom_tool_call_items: 0,
            assistant_texts: &texts,
        });
        assert!(signals.is_empty());
    }

    // ---- streaming probe scenarios (fake in-memory upstream) ----

    fn sse_response(chunks: Vec<String>) -> ProxyResponse {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("text/event-stream"),
        );
        let chunks: Vec<Bytes> = chunks.into_iter().map(Bytes::from).collect();
        ProxyResponse::streamed(
            http::StatusCode::OK,
            headers,
            futures::stream::iter(chunks.into_iter().map(Ok)),
        )
    }

    fn created_block(id: &str) -> String {
        format!("event: response.created\ndata: {{\"type\":\"response.created\",\"response\":{{\"id\":\"{id}\"}}}}\n\n")
    }

    fn item_block(id: &str) -> String {
        format!("event: response.output_item.added\ndata: {{\"type\":\"response.output_item.added\",\"item\":{{\"id\":\"{id}\"}}}}\n\n")
    }

    #[tokio::test]
    async fn probe_healthy_stream_passes_and_replays_every_byte() {
        let body = vec![
            created_block(GOOD_RESP),
            item_block(GOOD_ITEM),
            "event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n".to_string(),
        ];
        let outcome = probe_sse(sse_response(body), MAX_TIER_A_WINDOW).await;
        match outcome {
            ProbeOutcome::Pass {
                response,
                request_id,
                ..
            } => {
                assert_eq!(request_id.as_deref(), Some(GOOD_RESP));
                let bytes = response.bytes_with_limit(1024 * 1024).await.unwrap();
                let text = String::from_utf8(bytes.to_vec()).unwrap();
                assert!(text.contains(GOOD_RESP));
                assert!(text.contains(GOOD_ITEM));
                assert!(text.contains("response.completed"));
            }
            ProbeOutcome::Degraded { .. } => panic!("healthy stream must never degrade"),
        }
    }

    #[tokio::test]
    async fn probe_degraded_stream_aborts_before_client_bytes() {
        let body = vec![created_block(BAD_RESP), item_block(BAD_ITEM)];
        let outcome = probe_sse(sse_response(body), MAX_TIER_A_WINDOW).await;
        match outcome {
            ProbeOutcome::Degraded {
                evidence,
                request_id,
                ..
            } => {
                assert_eq!(request_id.as_deref(), Some(BAD_RESP));
                assert!(evidence.contains(&SemanticEvidence::ResponseId32Hex));
                assert!(evidence.contains(&SemanticEvidence::ItemId32Hex));
            }
            ProbeOutcome::Pass { .. } => panic!("degraded stream must be flagged"),
        }
    }

    #[tokio::test]
    async fn probe_healthy_stream_without_tool_call_is_never_replayed() {
        // Tier B would fire (no tool call) but Tier A is healthy: no replay.
        let body = vec![
            created_block(GOOD_RESP),
            item_block(GOOD_ITEM),
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n".to_string(),
        ];
        let outcome = probe_sse(sse_response(body), MAX_TIER_A_WINDOW).await;
        assert!(matches!(outcome, ProbeOutcome::Pass { .. }));
    }

    #[tokio::test]
    async fn probe_slow_upstream_stays_within_window_budget() {
        let response = ProxyResponse::streamed(
            http::StatusCode::OK,
            http::HeaderMap::new(),
            futures::stream::once(async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<Bytes, std::io::Error>(Bytes::from_static(
                    b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_0b90cdd71061d98f016a9bad07e29487d1ac8b3aeda7d200a2\"}}\n\n",
                ))
            }),
        );
        let started = std::time::Instant::now();
        let outcome = probe_sse(response, MAX_TIER_A_WINDOW).await;
        assert!(matches!(outcome, ProbeOutcome::Pass { .. }));
        // 50ms upstream delay + bounded window; comfortably under one second.
        assert!(started.elapsed() < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn probe_first_event_timeout_passes_through() {
        let response = ProxyResponse::streamed(
            http::StatusCode::OK,
            http::HeaderMap::new(),
            futures::stream::pending::<Result<Bytes, std::io::Error>>(),
        );
        let outcome = probe_sse(response, MAX_TIER_A_WINDOW).await;
        assert!(matches!(outcome, ProbeOutcome::Pass { .. }));
    }
}
