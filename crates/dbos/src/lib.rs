//! # DBOS Transact for Rust
//!
//! Lightweight **durable workflow orchestration** on top of Postgres. Register ordinary
//! async functions as durable workflows and steps; DBOS checkpoints their progress in
//! Postgres so they recover exactly where they left off after any crash or restart.
//!
//! This is a from-scratch Rust port of the official DBOS Transact SDKs
//! (Go / Python / TypeScript / Java). It is wire-compatible with the DBOS system-database
//! schema, so the same database can be inspected by DBOS tooling.
//!
//! ```no_run
//! use dbos::{Config, Dbos, WorkflowContext, WorkflowOptions, StepOptions};
//!
//! async fn my_workflow(ctx: WorkflowContext, name: String) -> dbos::Result<String> {
//!     let greeting = ctx.run_step("greet", |_| async move { Ok(format!("hello {name}")) }).await?;
//!     Ok(greeting)
//! }
//!
//! # async fn run() -> dbos::Result<()> {
//! let dbos = Dbos::builder(Config::new("myapp", "postgres://localhost/dbos"))
//!     .register_workflow("my_workflow", my_workflow)
//!     .launch()
//!     .await?;
//! let handle = dbos.run_workflow::<_, String>("my_workflow", "world".to_string(), WorkflowOptions::default()).await?;
//! let result = handle.get_result().await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod db;
pub mod error;
pub mod runtime;
pub mod serialize;

pub use config::{Config, Dialect};
pub use error::{DbosError, DbosErrorCode, Result};
pub use runtime::{
    Dbos, DbosBuilder, RegistrationOptions, StepOptions, WorkflowContext, WorkflowHandle,
    WorkflowOptions, WorkflowStatusType,
};

/// A boxed, `Send` future — the return type of type-erased workflow handlers.
pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
