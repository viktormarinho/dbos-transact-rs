//! Step execution: retry-with-backoff and stored-error decoding. Ports Go `executeStepWithRetry`.

use std::future::Future;
use std::time::Duration;

use crate::error::{DbosError, Result};
use crate::serialize::{deserialize_workflow_error, DecodedError};

const DEFAULT_BASE_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_MAX_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_BACKOFF_FACTOR: f64 = 2.0;

/// Per-step retry configuration. Defaults to **no retries** (`max_retries = 0`), matching the Go
/// SDK — a step runs exactly once unless retries are explicitly requested.
#[derive(Debug, Clone, Default)]
pub struct StepOptions {
    pub max_retries: u32,
    pub base_interval: Option<Duration>,
    pub max_interval: Option<Duration>,
    pub backoff_factor: Option<f64>,
}

/// Run a step body, retrying on error per `opts`. With `max_retries == 0`, the first error is
/// returned as-is. With retries, attempts total `max_retries + 1`; once exhausted the joined
/// errors are wrapped in [`DbosError::max_step_retries_exceeded`].
pub(crate) async fn execute_step_with_retry<R, Fut>(
    opts: &StepOptions,
    step_name: &str,
    workflow_id: &str,
    mut run: impl FnMut() -> Fut,
) -> Result<R>
where
    Fut: Future<Output = Result<R>>,
{
    let base = opts.base_interval.unwrap_or(DEFAULT_BASE_INTERVAL);
    let max_interval = opts.max_interval.unwrap_or(DEFAULT_MAX_INTERVAL);
    let factor = opts.backoff_factor.unwrap_or(DEFAULT_BACKOFF_FACTOR);

    let mut errors: Vec<String> = Vec::new();
    match run().await {
        Ok(v) => return Ok(v),
        Err(e) => {
            if opts.max_retries == 0 {
                return Err(e);
            }
            errors.push(e.to_string());
        }
    }

    for retry in 1..=opts.max_retries {
        let delay = if retry == 1 {
            base
        } else {
            let secs = (base.as_secs_f64() * factor.powi((retry - 1) as i32))
                .min(max_interval.as_secs_f64());
            Duration::from_secs_f64(secs)
        };
        tokio::time::sleep(delay).await;
        match run().await {
            Ok(v) => return Ok(v),
            Err(e) => errors.push(e.to_string()),
        }
    }

    Err(DbosError::max_step_retries_exceeded(
        workflow_id,
        step_name,
        opts.max_retries as i64,
        errors.join("\n"),
    ))
}

/// Reconstruct a step error from its stored serialized form.
pub(crate) fn decoded_step_error(err_str: &str, serialization: Option<&str>) -> DbosError {
    match deserialize_workflow_error(Some(err_str), serialization) {
        Some(DecodedError::Plain(s)) => DbosError::other(s),
        Some(DecodedError::Portable(pe)) => DbosError::other(pe.message),
        None => DbosError::other("step failed"),
    }
}
