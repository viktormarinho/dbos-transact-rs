//! DBOS error taxonomy.
//!
//! Every DBOS SDK (Go `errors.go`, Python `_error.py`, TS `error.ts`) models errors as a single
//! type carrying a numeric `code` plus optional context fields. We mirror that exactly so error
//! construction and field inspection port 1:1. Codes `1..=17` match the Go `iota + 1` numbering.
//!
//! The optional context fields (which workflow, step, queue, …) are rarely populated and are kept
//! behind a single `Box` so that `Result<T, DbosError>` stays cheap to move on the happy path.

use std::fmt;

/// Crate-wide result type.
pub type Result<T> = std::result::Result<T, DbosError>;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Programmatic error code. Matches the Go reference numbering exactly.
///
/// [`DbosErrorCode::Unspecified`] (`0`) is used for wrapped infrastructure errors (database,
/// serialization) that have no DBOS-specific code — mirroring Go's treatment of code `0` as
/// "unset" (it never matches in [`DbosError::is`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum DbosErrorCode {
    Unspecified = 0,
    ConflictingId = 1,
    Initialization = 2,
    NonExistentWorkflow = 3,
    ConflictingWorkflow = 4,
    WorkflowCancelled = 5,
    UnexpectedStep = 6,
    AwaitedWorkflowCancelled = 7,
    ConflictingRegistration = 8,
    WorkflowUnexpectedType = 9,
    WorkflowExecution = 10,
    StepExecution = 11,
    DeadLetterQueue = 12,
    MaxStepRetriesExceeded = 13,
    QueueDeduplicated = 14,
    PatchingNotEnabled = 15,
    Timeout = 16,
    NoApplicationVersions = 17,
}

impl DbosErrorCode {
    #[inline]
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

impl fmt::Display for DbosErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as i32)
    }
}

/// Optional, rarely-populated context attached to an error. Boxed to keep [`DbosError`] small.
#[derive(Debug, Default)]
struct ErrorContext {
    workflow_id: Option<String>,
    destination_id: Option<String>,
    step_name: Option<String>,
    queue_name: Option<String>,
    deduplication_id: Option<String>,
    step_id: Option<i32>,
    expected_name: Option<String>,
    recorded_name: Option<String>,
    max_retries: Option<i64>,
}

/// Unified DBOS error.
///
/// Carries a [`DbosErrorCode`] plus a human-readable `message` and optional context (which
/// workflow, step, queue, etc.). Display renders as `DBOS Error <code>: <message>` for coded
/// errors, or just `<message>` for wrapped infrastructure errors.
#[derive(Debug)]
pub struct DbosError {
    pub code: DbosErrorCode,
    pub message: String,
    context: Option<Box<ErrorContext>>,
    source: Option<BoxError>,
}

impl DbosError {
    fn new(code: DbosErrorCode, message: String) -> Self {
        DbosError {
            code,
            message,
            context: None,
            source: None,
        }
    }

    /// Lazily get a mutable reference to the (boxed) context, creating it on first use.
    fn ctx(&mut self) -> &mut ErrorContext {
        self.context.get_or_insert_with(Default::default)
    }

    /// The numeric error code (`0` for wrapped infrastructure errors).
    #[inline]
    pub fn code(&self) -> i32 {
        self.code.as_i32()
    }

    /// True if this error has the given code. Mirrors Go's `errors.Is`: code `0`/`Unspecified`
    /// never matches.
    #[inline]
    pub fn is(&self, code: DbosErrorCode) -> bool {
        code != DbosErrorCode::Unspecified && self.code == code
    }

    /// True if this error has the given numeric code (`0` never matches).
    #[inline]
    pub fn is_code(&self, code: i32) -> bool {
        code != 0 && self.code.as_i32() == code
    }

    /// Attach an underlying source error (for error-chain inspection via [`std::error::Error`]).
    pub fn with_source(mut self, source: BoxError) -> Self {
        self.source = Some(source);
        self
    }

    // ---- Context accessors -------------------------------------------------------------------

    pub fn workflow_id(&self) -> Option<&str> {
        self.context.as_ref()?.workflow_id.as_deref()
    }
    pub fn destination_id(&self) -> Option<&str> {
        self.context.as_ref()?.destination_id.as_deref()
    }
    pub fn step_name(&self) -> Option<&str> {
        self.context.as_ref()?.step_name.as_deref()
    }
    pub fn queue_name(&self) -> Option<&str> {
        self.context.as_ref()?.queue_name.as_deref()
    }
    pub fn deduplication_id(&self) -> Option<&str> {
        self.context.as_ref()?.deduplication_id.as_deref()
    }
    pub fn step_id(&self) -> Option<i32> {
        self.context.as_ref()?.step_id
    }
    pub fn expected_name(&self) -> Option<&str> {
        self.context.as_ref()?.expected_name.as_deref()
    }
    pub fn recorded_name(&self) -> Option<&str> {
        self.context.as_ref()?.recorded_name.as_deref()
    }
    pub fn max_retries(&self) -> Option<i64> {
        self.context.as_ref()?.max_retries
    }

    // ---- Constructors (mirror the Go `newXxxError` helpers) ----------------------------------

    pub fn conflicting_id(workflow_id: impl Into<String>) -> Self {
        let workflow_id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::ConflictingId,
            format!("Conflicting workflow ID {workflow_id}"),
        );
        e.ctx().workflow_id = Some(workflow_id);
        e
    }

    pub fn initialization(message: impl fmt::Display) -> Self {
        Self::new(
            DbosErrorCode::Initialization,
            format!("Error initializing DBOS Transact: {message}"),
        )
    }

    pub fn non_existent_workflow(workflow_id: impl Into<String>) -> Self {
        let id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::NonExistentWorkflow,
            format!("workflow {id} does not exist"),
        );
        e.ctx().destination_id = Some(id);
        e
    }

    /// `detail` may be empty, in which case the `": <detail>"` suffix is omitted.
    pub fn conflicting_workflow(workflow_id: impl Into<String>, detail: impl fmt::Display) -> Self {
        let id = workflow_id.into();
        let detail = detail.to_string();
        let mut message = format!("Conflicting workflow invocation with the same ID ({id})");
        if !detail.is_empty() {
            message.push_str(": ");
            message.push_str(&detail);
        }
        let mut e = Self::new(DbosErrorCode::ConflictingWorkflow, message);
        e.ctx().workflow_id = Some(id);
        e
    }

    pub fn workflow_cancelled(workflow_id: impl fmt::Display) -> Self {
        Self::new(
            DbosErrorCode::WorkflowCancelled,
            format!("Workflow {workflow_id} was cancelled"),
        )
    }

    pub fn unexpected_step(
        workflow_id: impl Into<String>,
        step_id: i32,
        expected_name: impl Into<String>,
        recorded_name: impl Into<String>,
    ) -> Self {
        let id = workflow_id.into();
        let expected = expected_name.into();
        let recorded = recorded_name.into();
        let mut e = Self::new(
            DbosErrorCode::UnexpectedStep,
            format!(
                "During execution of workflow {id} step {step_id}, function {recorded} was \
                 recorded when {expected} was expected. Check that your workflow is deterministic."
            ),
        );
        let c = e.ctx();
        c.workflow_id = Some(id);
        c.step_id = Some(step_id);
        c.expected_name = Some(expected);
        c.recorded_name = Some(recorded);
        e
    }

    pub fn awaited_workflow_cancelled(workflow_id: impl Into<String>) -> Self {
        let id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::AwaitedWorkflowCancelled,
            format!("Awaited workflow {id} was cancelled"),
        );
        e.ctx().workflow_id = Some(id);
        e
    }

    pub fn conflicting_registration(name: impl fmt::Display) -> Self {
        Self::new(
            DbosErrorCode::ConflictingRegistration,
            format!("{name} is already registered"),
        )
    }

    pub fn workflow_unexpected_result_type(
        workflow_id: impl Into<String>,
        expected: impl fmt::Display,
        actual: impl fmt::Display,
    ) -> Self {
        let id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::WorkflowUnexpectedType,
            format!("Workflow {id} returned unexpected result type: expected {expected}, got {actual}"),
        );
        e.ctx().workflow_id = Some(id);
        e
    }

    pub fn workflow_unexpected_input_type(
        workflow_name: impl fmt::Display,
        expected: impl fmt::Display,
        actual: impl fmt::Display,
    ) -> Self {
        Self::new(
            DbosErrorCode::WorkflowUnexpectedType,
            format!("Workflow {workflow_name} received unexpected input type: expected {expected}, got {actual}"),
        )
    }

    pub fn workflow_execution(workflow_id: impl Into<String>, err: impl fmt::Display) -> Self {
        let id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::WorkflowExecution,
            format!("Workflow {id} execution error: {err}"),
        );
        e.ctx().workflow_id = Some(id);
        e
    }

    pub fn step_execution(
        workflow_id: impl Into<String>,
        step_name: impl Into<String>,
        err: impl fmt::Display,
    ) -> Self {
        let id = workflow_id.into();
        let step = step_name.into();
        let mut e = Self::new(
            DbosErrorCode::StepExecution,
            format!("Step {step} in workflow {id} execution error: {err}"),
        );
        let c = e.ctx();
        c.workflow_id = Some(id);
        c.step_name = Some(step);
        e
    }

    pub fn dead_letter_queue(workflow_id: impl Into<String>, max_retries: i64) -> Self {
        let id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::DeadLetterQueue,
            format!(
                "Workflow {id} has been moved to the dead-letter queue after exceeding the \
                 maximum of {max_retries} retries"
            ),
        );
        let c = e.ctx();
        c.workflow_id = Some(id);
        c.max_retries = Some(max_retries);
        e
    }

    pub fn max_step_retries_exceeded(
        workflow_id: impl Into<String>,
        step_name: impl Into<String>,
        max_retries: i64,
        err: impl fmt::Display,
    ) -> Self {
        let id = workflow_id.into();
        let step = step_name.into();
        let mut e = Self::new(
            DbosErrorCode::MaxStepRetriesExceeded,
            format!("Step {step} has exceeded its maximum of {max_retries} retries: {err}"),
        );
        let c = e.ctx();
        c.workflow_id = Some(id);
        c.step_name = Some(step);
        c.max_retries = Some(max_retries);
        e
    }

    /// The "awaited workflow exceeded max step retries" variant (Go
    /// `newAwaitedWorkflowMaxStepRetriesExceeded`): same code, distinct message, no step fields.
    pub fn awaited_max_step_retries(workflow_id: impl Into<String>) -> Self {
        let id = workflow_id.into();
        let mut e = Self::new(
            DbosErrorCode::MaxStepRetriesExceeded,
            format!("Awaited workflow {id} has exceeded the maximum number of step retries"),
        );
        e.ctx().workflow_id = Some(id);
        e
    }

    pub fn queue_deduplicated(
        workflow_id: impl Into<String>,
        queue_name: impl Into<String>,
        deduplication_id: impl Into<String>,
    ) -> Self {
        let id = workflow_id.into();
        let queue = queue_name.into();
        let dedup = deduplication_id.into();
        let mut e = Self::new(
            DbosErrorCode::QueueDeduplicated,
            format!(
                "Workflow {id} was deduplicated due to an existing workflow in queue {queue} \
                 with deduplication ID {dedup}"
            ),
        );
        let c = e.ctx();
        c.workflow_id = Some(id);
        c.queue_name = Some(queue);
        c.deduplication_id = Some(dedup);
        e
    }

    pub fn patching_not_enabled() -> Self {
        Self::new(
            DbosErrorCode::PatchingNotEnabled,
            "Patching system is not enabled. Set EnablePatching to true in the DBOS context \
             configuration to use Patch and DeprecatePatch"
                .to_string(),
        )
    }

    pub fn no_application_versions() -> Self {
        Self::new(
            DbosErrorCode::NoApplicationVersions,
            "No application versions are registered".to_string(),
        )
    }

    /// Compose a timeout message exactly as Go's `newTimeoutError`: empty `step_name`/`workflow_id`
    /// /`message` components are omitted.
    pub fn timeout(
        workflow_id: impl Into<String>,
        step_name: impl Into<String>,
        message: impl fmt::Display,
    ) -> Self {
        let id = workflow_id.into();
        let step = step_name.into();
        let extra = message.to_string();

        let mut msg = if step.is_empty() {
            "Operation timed out".to_string()
        } else {
            format!("Step {step} timed out")
        };
        if !id.is_empty() {
            msg.push_str(&format!(" in workflow {id}"));
        }
        if !extra.is_empty() {
            msg.push_str(&format!(": {extra}"));
        }

        let mut e = Self::new(DbosErrorCode::Timeout, msg);
        if !id.is_empty() {
            e.ctx().workflow_id = Some(id);
        }
        if !step.is_empty() {
            e.ctx().step_name = Some(step);
        }
        e
    }

    /// Construct an uncoded error wrapping an arbitrary message (e.g. unknown serialization
    /// format). Renders without the `DBOS Error N:` prefix.
    pub fn other(message: impl fmt::Display) -> Self {
        Self::new(DbosErrorCode::Unspecified, message.to_string())
    }
}

impl fmt::Display for DbosError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.code {
            DbosErrorCode::Unspecified => write!(f, "{}", self.message),
            code => write!(f, "DBOS Error {}: {}", code.as_i32(), self.message),
        }
    }
}

impl std::error::Error for DbosError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|b| b.as_ref() as &(dyn std::error::Error + 'static))
    }
}

impl From<sqlx::Error> for DbosError {
    fn from(e: sqlx::Error) -> Self {
        DbosError::new(DbosErrorCode::Unspecified, format!("database error: {e}"))
            .with_source(Box::new(e))
    }
}

impl From<serde_json::Error> for DbosError {
    fn from(e: serde_json::Error) -> Self {
        DbosError::new(
            DbosErrorCode::Unspecified,
            format!("serialization error: {e}"),
        )
        .with_source(Box::new(e))
    }
}

impl From<base64::DecodeError> for DbosError {
    fn from(e: base64::DecodeError) -> Self {
        DbosError::new(
            DbosErrorCode::Unspecified,
            format!("base64 decode error: {e}"),
        )
        .with_source(Box::new(e))
    }
}

// ---- SQLSTATE classifiers (used by the DB layer to choose control flow) -----------------------

fn sqlstate(e: &sqlx::Error) -> Option<String> {
    use sqlx::error::DatabaseError;
    match e {
        sqlx::Error::Database(db) => DatabaseError::code(db.as_ref()).map(|c| c.into_owned()),
        _ => None,
    }
}

/// `23505 unique_violation`.
pub fn is_unique_violation(e: &sqlx::Error) -> bool {
    sqlstate(e).as_deref() == Some("23505")
}

/// `23503 foreign_key_violation`.
pub fn is_foreign_key_violation(e: &sqlx::Error) -> bool {
    sqlstate(e).as_deref() == Some("23503")
}

/// Transient contention: `40001 serialization_failure` or `40P01 deadlock_detected`.
pub fn is_contention(e: &sqlx::Error) -> bool {
    matches!(sqlstate(e).as_deref(), Some("40001") | Some("40P01"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_code_and_message() {
        let e = DbosError::conflicting_id("wf-1");
        assert_eq!(e.to_string(), "DBOS Error 1: Conflicting workflow ID wf-1");
        assert_eq!(e.code(), 1);
        assert!(e.is(DbosErrorCode::ConflictingId));
        assert!(e.is_code(1));
        assert!(!e.is_code(2));
        assert_eq!(e.workflow_id(), Some("wf-1"));
    }

    #[test]
    fn initialization_wraps_message() {
        let e = DbosError::initialization("missing required config field: appName");
        assert_eq!(e.code(), 2);
        assert_eq!(
            e.to_string(),
            "DBOS Error 2: Error initializing DBOS Transact: missing required config field: appName"
        );
    }

    #[test]
    fn conflicting_workflow_optional_detail() {
        let with = DbosError::conflicting_workflow("wf-2", "boom");
        assert_eq!(
            with.to_string(),
            "DBOS Error 4: Conflicting workflow invocation with the same ID (wf-2): boom"
        );
        let without = DbosError::conflicting_workflow("wf-2", "");
        assert_eq!(
            without.to_string(),
            "DBOS Error 4: Conflicting workflow invocation with the same ID (wf-2)"
        );
    }

    #[test]
    fn unexpected_step_message_and_fields() {
        let e = DbosError::unexpected_step("wf-3", 4, "expectedFn", "recordedFn");
        assert_eq!(e.code(), 6);
        assert_eq!(e.step_id(), Some(4));
        assert_eq!(e.expected_name(), Some("expectedFn"));
        assert_eq!(e.recorded_name(), Some("recordedFn"));
        assert!(e
            .to_string()
            .contains("function recordedFn was recorded when expectedFn was expected"));
        assert!(e
            .to_string()
            .contains("Check that your workflow is deterministic."));
    }

    #[test]
    fn timeout_composition() {
        assert_eq!(
            DbosError::timeout("", "", "").to_string(),
            "DBOS Error 16: Operation timed out"
        );
        assert_eq!(
            DbosError::timeout("wf", "stepA", "deadline").to_string(),
            "DBOS Error 16: Step stepA timed out in workflow wf: deadline"
        );
        assert_eq!(
            DbosError::timeout("wf", "", "").to_string(),
            "DBOS Error 16: Operation timed out in workflow wf"
        );
    }

    #[test]
    fn dead_letter_and_max_retries() {
        let dlq = DbosError::dead_letter_queue("wf", 100);
        assert_eq!(dlq.code(), 12);
        assert_eq!(dlq.max_retries(), Some(100));

        let mre = DbosError::max_step_retries_exceeded("wf", "stepB", 3, "io error");
        assert_eq!(mre.code(), 13);
        assert_eq!(
            mre.to_string(),
            "DBOS Error 13: Step stepB has exceeded its maximum of 3 retries: io error"
        );
        let awaited = DbosError::awaited_max_step_retries("wf");
        assert_eq!(awaited.code(), 13);
        assert!(awaited
            .to_string()
            .contains("Awaited workflow wf has exceeded"));
    }

    #[test]
    fn serde_error_is_uncoded() {
        let json_err = serde_json::from_str::<i32>("not json").unwrap_err();
        let e: DbosError = json_err.into();
        assert_eq!(e.code(), 0);
        assert!(!e.is_code(0)); // code 0 never "matches"
        assert!(e.to_string().starts_with("serialization error:"));
        assert!(std::error::Error::source(&e).is_some());
    }

    #[test]
    fn error_is_small() {
        // The whole point of boxing the context: keep Result<T, DbosError> cheap to move.
        assert!(
            std::mem::size_of::<DbosError>() <= 64,
            "DbosError is {} bytes",
            std::mem::size_of::<DbosError>()
        );
    }
}
