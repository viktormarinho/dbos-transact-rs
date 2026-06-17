//! Crash recovery: on launch, re-run this executor's `PENDING` workflows so they resume from their
//! last completed step. Ports Go `recoverPendingWorkflows`.

use std::sync::Arc;

use super::workflow::start_workflow;
use super::{DbosInner, WorkflowOptions};
use crate::db::status::list_pending_for_recovery;
use crate::error::Result;
use crate::serialize::Format;

/// Recover every `PENDING` workflow owned by one of `executor_ids` (at this app version): re-run it
/// with `is_recovery` so it replays completed steps and bumps `recovery_attempts`. Returns the ids
/// that were re-launched. Queued workflows are re-enqueued instead (handled once queues land).
pub(crate) async fn recover_pending_workflows(
    inner: Arc<DbosInner>,
    executor_ids: &[String],
) -> Result<Vec<String>> {
    let pending =
        list_pending_for_recovery(&inner.pool, &inner.schema, executor_ids, &inner.application_version)
            .await?;

    let mut recovered = Vec::new();
    for wf in pending {
        if wf.queue_name.is_some() {
            // Queued workflows are recovered by re-enqueueing them (durable queues milestone).
            tracing::debug!(workflow_id = %wf.id, "skipping recovery of queued workflow");
            continue;
        }
        if inner.registry.get(&wf.name).is_none() {
            tracing::error!(name = %wf.name, workflow_id = %wf.id, "workflow not registered; cannot recover");
            continue;
        }

        // Re-attach the original auth identity so recovered children inherit it.
        let roles = wf
            .authenticated_roles
            .as_deref()
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok());
        let opts = WorkflowOptions {
            workflow_id: Some(wf.id.clone()),
            authenticated_user: wf.authenticated_user.clone(),
            assumed_role: wf.assumed_role.clone(),
            authenticated_roles: roles,
            ..Default::default()
        };
        let fmt = Format::from_name(wf.serialization.as_deref());

        match start_workflow::<serde_json::Value>(
            inner.clone(),
            &wf.name,
            wf.inputs.clone(),
            opts,
            None,
            fmt,
            true,
        )
        .await
        {
            Ok(_handle) => recovered.push(wf.id),
            Err(e) => tracing::error!(workflow_id = %wf.id, error = %e, "recovery failed"),
        }
    }
    Ok(recovered)
}
