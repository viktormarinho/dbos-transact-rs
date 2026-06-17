//! The workflow registry: a name → type-erased handler map.
//!
//! Each registered workflow is erased into a closure that captures its concrete input/output types
//! (`P`/`R`), so crash recovery (and queue dequeue) can decode the *stored serialized input* and
//! re-run the workflow with no runtime type information — only its name.

use std::collections::HashMap;

use super::context::WorkflowContext;
use crate::error::Result;
use crate::serialize::Format;
use crate::BoxFuture;

/// A type-erased workflow body: `(ctx, encoded_input, format) -> encoded_output`.
pub(crate) type ErasedWorkflow = std::sync::Arc<
    dyn Fn(WorkflowContext, Option<String>, Format) -> BoxFuture<'static, Result<String>>
        + Send
        + Sync,
>;

pub(crate) struct RegistryEntry {
    pub handler: ErasedWorkflow,
    pub max_retries: i64,
    #[allow(dead_code)]
    pub name: String,
    pub class_name: Option<String>,
    pub config_name: Option<String>,
}

#[derive(Default)]
pub(crate) struct Registry(pub HashMap<String, RegistryEntry>);

impl Registry {
    pub fn get(&self, name: &str) -> Option<&RegistryEntry> {
        self.0.get(name)
    }
}
