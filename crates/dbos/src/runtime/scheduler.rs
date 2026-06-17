//! Cron scheduling: fire a registered workflow at each cron tick. Ports Go `scheduler.go`'s static
//! `WithSchedule` path.
//!
//! Each tick runs the workflow with a deterministic id `"{name}-{rfc3339_tick}"`, so exactly one
//! run happens per (schedule, tick) across the whole fleet (idempotency dedups concurrent firings).

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::workflow::start_workflow;
use super::{DbosInner, WorkflowOptions};
use crate::serialize::{encode_input, Format};

/// The input a scheduled workflow receives: the cron tick it is running for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledWorkflowInput {
    pub scheduled_time: DateTime<Utc>,
}

/// Accept both 5-field (no seconds) and 6/7-field cron specs by defaulting seconds to 0.
fn normalize_cron(cron: &str) -> String {
    if cron.split_whitespace().count() == 5 {
        format!("0 {cron}")
    } else {
        cron.to_string()
    }
}

/// Run one scheduled workflow's cron loop until cancelled.
pub(crate) async fn run_scheduler(
    inner: Arc<DbosInner>,
    name: String,
    cron: String,
    token: CancellationToken,
) {
    let schedule = match cron::Schedule::from_str(&normalize_cron(&cron)) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(name = %name, cron = %cron, error = %e, "invalid cron schedule");
            return;
        }
    };

    // Schedule forward from the previous tick to avoid drift.
    let mut last = Utc::now();
    loop {
        let next = match schedule.after(&last).next() {
            Some(t) => t,
            None => return, // no further ticks
        };
        let wait = (next - Utc::now()).to_std().unwrap_or(Duration::ZERO);
        tokio::select! {
            _ = token.cancelled() => return,
            _ = tokio::time::sleep(wait) => {}
        }
        last = next;

        // Fire the workflow with a tick-deterministic id (fleet-wide exactly-once per tick).
        let id = format!("{name}-{}", next.to_rfc3339_opts(SecondsFormat::Secs, true));
        let input = ScheduledWorkflowInput {
            scheduled_time: next,
        };
        let encoded = match encode_input(&input, Format::Portable) {
            Ok(e) => e,
            Err(e) => {
                tracing::error!(name = %name, error = %e, "failed to encode scheduled input");
                continue;
            }
        };
        let opts = WorkflowOptions {
            workflow_id: Some(id),
            ..Default::default()
        };
        if let Err(e) = start_workflow::<()>(
            inner.clone(),
            &name,
            Some(encoded),
            opts,
            None,
            Format::Portable,
            false,
        )
        .await
        {
            tracing::error!(name = %name, error = %e, "scheduled workflow run failed");
        }
    }
}
