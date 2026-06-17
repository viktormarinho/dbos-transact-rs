//! [`Client`]: a lightweight connector for external applications to interact with a DBOS system
//! database without running a full runtime (no migrations, no background tasks). Ports the Go/
//! Python `Client`.

use std::time::Duration;

use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::db::management::{
    self, ForkWorkflowInput, ListWorkflowsFilter, StepInfo, WorkflowStatus, INTERNAL_QUEUE_NAME,
};
use crate::db::notifications::{insert_notification, NULL_TOPIC};
use crate::db::now_epoch_ms;
use crate::db::status::{insert_workflow_status, InsertWorkflowInput, WorkflowStatusType};
use crate::error::{DbosError, Result};
use crate::runtime::{EnqueueOptions, WorkflowHandle};
use crate::serialize::{encode_input, encode_value, Format};

/// A connection to a DBOS system database for use from outside a DBOS application — e.g. to enqueue
/// workflows, send messages, or inspect/manage workflows. It does **not** run migrations or any
/// background tasks.
pub struct Client {
    pool: PgPool,
    schema: String,
    poll_interval: Duration,
}

impl Client {
    /// Connect to the system database. Assumes the schema has already been created by a running
    /// DBOS application.
    pub async fn connect(database_url: &str, schema: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        Ok(Client {
            pool,
            schema: schema.to_string(),
            poll_interval: Duration::from_secs(1),
        })
    }

    fn handle<R>(&self, id: String) -> WorkflowHandle<R> {
        WorkflowHandle::polling(id, self.pool.clone(), self.schema.clone(), self.poll_interval)
    }

    /// Enqueue a workflow (by name) onto a queue. A DBOS server with that workflow registered will
    /// pick it up. Returns a polling handle.
    pub async fn enqueue<P: Serialize>(
        &self,
        queue_name: &str,
        workflow_name: &str,
        input: P,
        opts: EnqueueOptions,
    ) -> Result<WorkflowHandle<serde_json::Value>> {
        let encoded = encode_input(&input, Format::Portable)?;
        let workflow_id = opts
            .workflow_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let now = now_epoch_ms();
        let (status, delay_until) = match opts.delay {
            Some(d) => (
                WorkflowStatusType::Delayed,
                Some(now + d.as_millis() as i64),
            ),
            None => (WorkflowStatusType::Enqueued, None),
        };
        let row = InsertWorkflowInput {
            workflow_id: workflow_id.clone(),
            status: status.as_str().to_string(),
            name: workflow_name.to_string(),
            queue_name: Some(queue_name.to_string()),
            application_version: opts.application_version.clone(),
            created_at: now,
            updated_at: now,
            recovery_attempts: 0,
            inputs: Some(encoded),
            priority: opts.priority.unwrap_or(0),
            deduplication_id: opts.deduplication_id.clone(),
            queue_partition_key: opts.queue_partition_key.clone(),
            delay_until_epoch_ms: delay_until,
            owner_xid: uuid::Uuid::new_v4().to_string(),
            serialization: Some(Format::Portable.name().to_string()),
            increment: 0,
            ..Default::default()
        };
        let mut tx = self.pool.begin().await?;
        match insert_workflow_status(&mut tx, &self.schema, &row, 100).await {
            Ok(_) => {
                tx.commit().await?;
                Ok(self.handle(workflow_id))
            }
            Err(e) if e.is_db_unique_violation() => {
                let _ = tx.rollback().await;
                Err(DbosError::queue_deduplicated(
                    workflow_id,
                    queue_name,
                    opts.deduplication_id.unwrap_or_default(),
                ))
            }
            Err(e) => {
                let _ = tx.rollback().await;
                Err(e)
            }
        }
    }

    /// Send a message to a workflow's mailbox.
    pub async fn send<M: Serialize>(
        &self,
        destination_id: &str,
        message: M,
        topic: Option<&str>,
    ) -> Result<()> {
        let topic = topic.filter(|t| !t.is_empty()).unwrap_or(NULL_TOPIC);
        let encoded = encode_value(&message, Format::Portable)?;
        let mut conn = self.pool.acquire().await?;
        insert_notification(
            &mut conn,
            &self.schema,
            destination_id,
            topic,
            &encoded,
            Format::Portable.name(),
        )
        .await
    }

    /// Get a polling handle to an existing workflow by id.
    pub fn retrieve_workflow<R>(&self, workflow_id: &str) -> WorkflowHandle<R> {
        self.handle(workflow_id.to_string())
    }

    pub async fn list_workflows(&self, filter: ListWorkflowsFilter) -> Result<Vec<WorkflowStatus>> {
        management::list_workflows(&self.pool, &self.schema, filter).await
    }

    pub async fn list_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        management::get_workflow_steps(&self.pool, &self.schema, workflow_id).await
    }

    pub async fn cancel_workflow(&self, workflow_id: &str) -> Result<()> {
        let ids = [workflow_id.to_string()];
        let found = management::cancel_workflows(&self.pool, &self.schema, &ids).await?;
        if found.is_empty() {
            return Err(DbosError::non_existent_workflow(workflow_id));
        }
        Ok(())
    }

    pub async fn cancel_workflows(&self, workflow_ids: &[String]) -> Result<Vec<String>> {
        management::cancel_workflows(&self.pool, &self.schema, workflow_ids).await
    }

    pub async fn resume_workflow<R>(&self, workflow_id: &str) -> Result<WorkflowHandle<R>> {
        let ids = [workflow_id.to_string()];
        let found =
            management::resume_workflows(&self.pool, &self.schema, &ids, INTERNAL_QUEUE_NAME).await?;
        if found.is_empty() {
            return Err(DbosError::non_existent_workflow(workflow_id));
        }
        Ok(self.handle(workflow_id.to_string()))
    }

    pub async fn fork_workflow<R>(&self, input: ForkWorkflowInput) -> Result<WorkflowHandle<R>> {
        let new_id = management::fork_workflow(&self.pool, &self.schema, input).await?;
        Ok(self.handle(new_id))
    }

    pub async fn garbage_collect(
        &self,
        cutoff_epoch_ms: Option<i64>,
        rows_threshold: Option<i64>,
    ) -> Result<()> {
        management::garbage_collect(&self.pool, &self.schema, cutoff_epoch_ms, rows_threshold).await
    }

    /// Close the underlying connection pool.
    pub async fn shutdown(self) {
        self.pool.close().await;
    }
}
