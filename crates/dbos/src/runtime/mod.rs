//! The DBOS runtime: building, launching, and running durable workflows.

mod context;
mod registry;
mod step;
mod workflow;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde::{de::DeserializeOwned, Serialize};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::{process_config, Config};
use crate::db::{connect, run_migrations};
use crate::error::{DbosError, Result};
use crate::serialize::{decode_input, encode_input, encode_value, Format};
use crate::BoxFuture;

use registry::{ErasedWorkflow, Registry, RegistryEntry};
use workflow::start_workflow;

pub use context::WorkflowContext;
pub use step::StepOptions;
pub use workflow::WorkflowHandle;

pub use crate::db::status::WorkflowStatusType;

/// The default per-workflow recovery limit (`_DEFAULT_MAX_RECOVERY_ATTEMPTS`).
const DEFAULT_MAX_RECOVERY_ATTEMPTS: i64 = 100;

/// Options for starting a workflow.
#[derive(Debug, Default, Clone)]
pub struct WorkflowOptions {
    /// Set an explicit workflow id (for exactly-once / idempotent invocation). Defaults to a UUIDv4.
    pub workflow_id: Option<String>,
    /// Override the application version recorded for this workflow.
    pub application_version: Option<String>,
    pub authenticated_user: Option<String>,
    pub assumed_role: Option<String>,
    pub authenticated_roles: Option<Vec<String>>,
}

pub(crate) struct DbosInner {
    pub pool: PgPool,
    pub schema: String,
    pub executor_id: String,
    pub application_version: String,
    pub application_id: Option<String>,
    pub registry: Arc<Registry>,
    pub workflow_tasks: TaskTracker,
    #[allow(dead_code)]
    pub cancel: CancellationToken,
    pub poll_interval: Duration,
}

/// A launched DBOS runtime handle. Cheap to clone.
#[derive(Clone)]
pub struct Dbos {
    inner: Arc<DbosInner>,
}

impl Dbos {
    /// Start configuring a runtime.
    pub fn builder(config: Config) -> DbosBuilder {
        DbosBuilder::new(config)
    }

    /// Start a registered workflow and return a handle to its result.
    pub async fn run_workflow<P, R>(
        &self,
        name: &str,
        input: P,
        opts: WorkflowOptions,
    ) -> Result<WorkflowHandle<R>>
    where
        P: Serialize + Send,
    {
        let encoded = encode_input(&input, Format::Portable)?;
        start_workflow::<R>(self.inner.clone(), name, Some(encoded), opts, None).await
    }

    /// Get a polling handle to an existing workflow by id.
    pub fn retrieve_workflow<R>(&self, workflow_id: &str) -> WorkflowHandle<R> {
        WorkflowHandle::polling(workflow_id.to_string(), self.inner.clone())
    }

    /// Stop background tasks and drain in-flight workflows (up to `timeout`), then close the pool.
    pub async fn shutdown(self, timeout: Duration) {
        self.inner.cancel.cancel();
        self.inner.workflow_tasks.close();
        let _ = tokio::time::timeout(timeout, self.inner.workflow_tasks.wait()).await;
        self.inner.pool.close().await;
    }
}

/// Builder for a [`Dbos`] runtime. Register all workflows, then [`launch`](DbosBuilder::launch).
pub struct DbosBuilder {
    config: Config,
    registry: HashMap<String, RegistryEntry>,
    registration_error: Option<DbosError>,
}

impl DbosBuilder {
    pub fn new(config: Config) -> Self {
        DbosBuilder {
            config,
            registry: HashMap::new(),
            registration_error: None,
        }
    }

    /// Register a durable workflow under an explicit `name` (Rust has no reflection-derived name).
    /// The concrete input/output types are captured here so recovery can decode stored inputs.
    pub fn register_workflow<P, R, F, Fut>(mut self, name: &str, f: F) -> Self
    where
        P: DeserializeOwned + Serialize + Send + 'static,
        R: Serialize + DeserializeOwned + Send + 'static,
        F: Fn(WorkflowContext, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        if self.registry.contains_key(name) {
            if self.registration_error.is_none() {
                self.registration_error = Some(DbosError::conflicting_registration(name));
            }
            return self;
        }
        let f = Arc::new(f);
        let handler: ErasedWorkflow = Arc::new(move |ctx, input, fmt| {
            let f = f.clone();
            let fut: BoxFuture<'static, Result<String>> = Box::pin(async move {
                let p: P = decode_input(input.as_deref(), Some(fmt.name()))?;
                let r: R = f(ctx, p).await?;
                encode_value(&r, fmt)
            });
            fut
        });
        self.registry.insert(
            name.to_string(),
            RegistryEntry {
                handler,
                max_retries: DEFAULT_MAX_RECOVERY_ATTEMPTS,
                name: name.to_string(),
                class_name: None,
                config_name: None,
            },
        );
        self
    }

    /// Validate config, connect, migrate the system database, and start the runtime.
    pub async fn launch(self) -> Result<Dbos> {
        if let Some(e) = self.registration_error {
            return Err(e);
        }
        let pc = process_config(self.config)?;
        let pool = match &pc.system_db_pool {
            Some(p) => p.clone(),
            None => connect(pc.database_url.as_deref().unwrap_or_default(), 20).await?,
        };
        run_migrations(&pool, &pc.database_schema).await?;

        let inner = Arc::new(DbosInner {
            pool,
            schema: pc.database_schema,
            executor_id: pc.executor_id,
            application_version: pc.application_version,
            application_id: if pc.application_id.is_empty() {
                None
            } else {
                Some(pc.application_id)
            },
            registry: Arc::new(Registry(self.registry)),
            workflow_tasks: TaskTracker::new(),
            cancel: CancellationToken::new(),
            poll_interval: Duration::from_secs(1),
        });
        // M3 will run crash recovery for this executor here.
        Ok(Dbos { inner })
    }
}
