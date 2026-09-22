//! Audit trail for Responses semantic degradation events.
//!
//! Kept in its own table (rather than folded into `proxy_request_logs`) so that
//! usage/cost accounting stays untouched and the events can be replayed by
//! burst from a single indexed table:
//!
//! ```sql
//! SELECT date(created_at, 'unixepoch'), count(*)
//! FROM semantic_degradation_events GROUP BY 1 ORDER BY 1;
//! ```

use crate::error::AppError;

use super::super::{lock_conn, Database};

/// One semantic degradation event (Tier A detection, or Tier B audit signal).
#[derive(Debug, Clone)]
pub struct SemanticDegradationEvent {
    pub request_id: Option<String>,
    pub app_type: String,
    pub provider_id: String,
    pub endpoint: Option<String>,
    /// `"A"` or `"B"`.
    pub tier: String,
    /// Short machine-readable reasons, e.g. `response_id_32hex`.
    pub evidence: Vec<String>,
    /// Number of upstream sends performed for the client request.
    pub attempts: u32,
    /// Whether a replay was actually attempted.
    pub replayed: bool,
    /// `dry_run` / `recovered` / `exhausted` / `circuit_open` / `tier_b`.
    pub outcome: String,
    /// `true` when replay was globally disabled (observe-only).
    pub dry_run: bool,
    pub detail: Option<String>,
}

impl Database {
    /// Persist one semantic degradation event. Never fails the proxy request:
    /// callers should treat an error as a logging warning only.
    pub fn insert_semantic_degradation_event(
        &self,
        event: &SemanticDegradationEvent,
    ) -> Result<(), AppError> {
        let conn = lock_conn!(self.conn);
        conn.execute(
            "INSERT INTO semantic_degradation_events (
                created_at, request_id, app_type, provider_id, endpoint,
                tier, evidence, attempts, replayed, outcome, dry_run, detail
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            rusqlite::params![
                chrono::Utc::now().timestamp(),
                event.request_id,
                event.app_type,
                event.provider_id,
                event.endpoint,
                event.tier,
                event.evidence.join(","),
                event.attempts as i64,
                if event.replayed { 1 } else { 0 },
                event.outcome,
                if event.dry_run { 1 } else { 0 },
                event.detail,
            ],
        )
        .map_err(|e| AppError::Database(format!("记录语义降级事件失败: {e}")))?;
        Ok(())
    }
}
