//! Independent safety rails around [`super::semantic_detector`].
//!
//! HTTP status stays `200` when a Responses channel degrades, so the generic
//! circuit breaker and failover counters (which key off transport errors) never
//! see it. This module keeps a **separate** failure counter and a separate
//! cooldown so badly behaved aggregator channels stop being hammered, without
//! ever poisoning the transport-level health of the provider.
//!
//! It also owns the replay policy: how many sends are allowed and when the
//! `prompt_cache_key` may be perturbed. Nothing here performs IO, so the whole
//! policy is unit-testable with an injected clock.

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::types::SemanticProbeConfig;

/// Rolling window over which semantic failures are counted.
pub const SEMANTIC_FAILURE_WINDOW: Duration = Duration::from_secs(300);

/// What to do after a Tier A degradation was detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticAction {
    /// Replay the exact same request (keep `prompt_cache_key` → keep upstream cache).
    RetrySame,
    /// Replay after perturbing channel affinity (`prompt_cache_key` suffix).
    RetryPerturbed,
    /// Replay budget exhausted; surface the degradation and let failover decide.
    GiveUp,
}

/// Decide the next move. `attempts_done` counts sends already performed for this
/// client request (1 = the original send).
pub fn plan_next_attempt(attempts_done: u32, max_attempts: u32) -> SemanticAction {
    let max_attempts = max_attempts.clamp(1, 3);
    if attempts_done >= max_attempts {
        return SemanticAction::GiveUp;
    }
    if attempts_done <= 1 {
        SemanticAction::RetrySame
    } else {
        SemanticAction::RetryPerturbed
    }
}

/// Append a random suffix to an existing `prompt_cache_key`.
///
/// Returns `(old, new)` on success, or `None` when the body has no string key to
/// perturb (we never invent one: adding a cache key changes upstream semantics).
pub fn perturb_prompt_cache_key(body: &mut Value, salt: u64) -> Option<(String, String)> {
    let object = body.as_object_mut()?;
    let current = object.get("prompt_cache_key")?.as_str()?.to_string();
    if current.is_empty() {
        return None;
    }
    let perturbed = format!("{current}-sem-{salt:016x}");
    object.insert(
        "prompt_cache_key".to_string(),
        Value::String(perturbed.clone()),
    );
    Some((current, perturbed))
}

/// Build the semantic circuit key. Precise to endpoint when one is known.
pub fn circuit_key(app_type: &str, provider_id: &str, endpoint: Option<&str>) -> String {
    match endpoint.filter(|endpoint| !endpoint.is_empty()) {
        Some(endpoint) => {
            let path = endpoint.split('?').next().unwrap_or(endpoint);
            format!("{app_type}:{provider_id}:{path}")
        }
        None => format!("{app_type}:{provider_id}"),
    }
}

/// Outcome of one degradation event, stored for auditing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticOutcome {
    /// Probe detected a degradation but replay was disabled (dry-run).
    DryRun,
    /// Replay was attempted and the next attempt was healthy.
    Recovered,
    /// Replay budget exhausted while still degraded.
    Exhausted,
    /// Replay was enabled but skipped because this target's semantic circuit is open.
    CircuitOpen,
}

impl SemanticOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DryRun => "dry_run",
            Self::Recovered => "recovered",
            Self::Exhausted => "exhausted",
            Self::CircuitOpen => "circuit_open",
        }
    }
}

/// Independent semantic circuit breaker. Keyed by `app_type:provider_id[:path]`.
#[derive(Debug, Default)]
pub struct SemanticGuard {
    inner: Mutex<GuardState>,
}

#[derive(Debug, Default)]
struct GuardState {
    /// key → timestamps of semantic failures inside the rolling window.
    failures: HashMap<String, Vec<Instant>>,
    /// key → instant the circuit closes again.
    open_until: HashMap<String, Instant>,
}

impl SemanticGuard {
    pub fn new() -> Self {
        Self::default()
    }

    /// `true` when the target may be attempted.
    pub fn allow(&self, key: &str, now: Instant) -> bool {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        match state.open_until.get(key).copied() {
            Some(until) if until > now => false,
            Some(_) => {
                state.open_until.remove(key);
                state.failures.remove(key);
                true
            }
            None => true,
        }
    }

    /// Record a semantic failure that survived replay. Returns `true` if this
    /// pushed the target into cooldown.
    pub fn record_failure(&self, key: &str, config: &SemanticProbeConfig, now: Instant) -> bool {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let threshold = config.circuit_failure_threshold.max(1) as usize;
        let trip = {
            let stamps = state.failures.entry(key.to_string()).or_default();
            stamps.retain(|stamp| now.duration_since(*stamp) < SEMANTIC_FAILURE_WINDOW);
            stamps.push(now);
            stamps.len() >= threshold
        };
        if trip {
            let cooldown = Duration::from_secs(config.circuit_timeout_seconds.max(1) as u64);
            state.open_until.insert(key.to_string(), now + cooldown);
            state.failures.remove(key);
            return true;
        }
        false
    }

    /// A healthy send clears the rolling window for this target.
    pub fn record_success(&self, key: &str) {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.failures.remove(key);
        state.open_until.remove(key);
    }

    #[cfg(test)]
    fn failure_count(&self, key: &str) -> usize {
        self.inner
            .lock()
            .unwrap()
            .failures
            .get(key)
            .map(Vec::len)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config() -> SemanticProbeConfig {
        SemanticProbeConfig {
            enabled: true,
            replay_enabled: true,
            window_ms: 200,
            max_attempts: 2,
            circuit_failure_threshold: 3,
            circuit_timeout_seconds: 60,
        }
    }

    #[test]
    fn replay_plan_matches_policy() {
        // Default budget: original + one cache-preserving replay.
        assert_eq!(plan_next_attempt(1, 2), SemanticAction::RetrySame);
        assert_eq!(plan_next_attempt(2, 2), SemanticAction::GiveUp);
        // With a third send allowed, the second replay breaks affinity.
        assert_eq!(plan_next_attempt(2, 3), SemanticAction::RetryPerturbed);
        assert_eq!(plan_next_attempt(3, 3), SemanticAction::GiveUp);
        // Never more than three sends even if misconfigured.
        assert_eq!(plan_next_attempt(1, 99), SemanticAction::RetrySame);
        assert_eq!(plan_next_attempt(3, 99), SemanticAction::GiveUp);
    }

    #[test]
    fn perturbation_only_touches_existing_cache_key() {
        let mut body = json!({"prompt_cache_key":"abc","model":"gpt-5.6-sol"});
        let (old, new) = perturb_prompt_cache_key(&mut body, 42).expect("key perturbed");
        assert_eq!(old, "abc");
        assert!(new.starts_with("abc-sem-"));
        assert_eq!(body["prompt_cache_key"], json!(new));

        let mut untouched = json!({"model":"gpt-5.6-sol"});
        assert!(perturb_prompt_cache_key(&mut untouched, 42).is_none());
        assert!(untouched.get("prompt_cache_key").is_none());
    }

    #[test]
    fn circuit_key_is_endpoint_scoped_and_strips_query() {
        assert_eq!(
            circuit_key("codex", "hejuapi", Some("/v1/responses?foo=1")),
            "codex:hejuapi:/v1/responses"
        );
        assert_eq!(circuit_key("codex", "hejuapi", None), "codex:hejuapi");
    }

    #[test]
    fn circuit_opens_after_threshold_then_cools_down() {
        let guard = SemanticGuard::new();
        let config = config();
        let key = "codex:hejuapi:/v1/responses";
        let start = Instant::now();

        assert!(guard.allow(key, start));
        assert!(!guard.record_failure(key, &config, start));
        assert!(!guard.record_failure(key, &config, start + Duration::from_secs(1)));
        assert!(guard.record_failure(key, &config, start + Duration::from_secs(2)));

        // Open during cooldown (tripped at +2s, cooldown 60s → closes at +62s).
        assert!(!guard.allow(key, start + Duration::from_secs(30)));
        assert!(!guard.allow(key, start + Duration::from_secs(61)));
        // Closed after cooldown.
        assert!(guard.allow(key, start + Duration::from_secs(63)));
        assert_eq!(guard.failure_count(key), 0);
    }

    #[test]
    fn failures_outside_window_do_not_trip_circuit() {
        let guard = SemanticGuard::new();
        let config = config();
        let key = "codex:hejuapi";
        let start = Instant::now();

        assert!(!guard.record_failure(key, &config, start));
        assert!(!guard.record_failure(key, &config, start + Duration::from_secs(120)));
        // Third failure is >300s after the first, so the window has rolled over.
        assert!(!guard.record_failure(key, &config, start + Duration::from_secs(400)));
        assert!(guard.allow(key, start + Duration::from_secs(401)));
    }

    #[test]
    fn success_clears_failures() {
        let guard = SemanticGuard::new();
        let config = config();
        let key = "codex:hejuapi";
        let now = Instant::now();
        guard.record_failure(key, &config, now);
        guard.record_failure(key, &config, now);
        guard.record_success(key);
        assert_eq!(guard.failure_count(key), 0);
        assert!(!guard.record_failure(key, &config, now));
    }
}
