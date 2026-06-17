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
//! The crate is being built milestone by milestone (see `ROADMAP.md`). Currently implemented:
//! - [`error`] — the DBOS error taxonomy (codes match the reference SDKs).
//! - [`config`] — configuration, dialect detection, credential masking.
//! - [`serialize`] — payload serialization (`portable_json` default + `DBOS_JSON` reader).

pub mod config;
pub mod db;
pub mod error;
pub mod serialize;

pub use config::{Config, Dialect};
pub use error::{DbosError, DbosErrorCode, Result};
