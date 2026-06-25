//! The LISTEN/NOTIFY listener: a single background task that wakes the in-memory waiters used by
//! `recv` / `get_event` when a message or event arrives. Ports Go `notificationListenerLoop`.
//!
//! A 1-second poll arm and a wake-all-on-reconnect serve as the safety net for any notification
//! missed while the listener was disconnected (so correctness never depends on a delivered NOTIFY).

use std::sync::Arc;
use std::time::Duration;

use sqlx::postgres::PgListener;
use tokio_util::sync::CancellationToken;

use super::DbosInner;
use crate::db::notifications::{NOTIFICATIONS_CHANNEL, WORKFLOW_EVENTS_CHANNEL};
use crate::db::streams::STREAMS_CHANNEL;

/// In-memory waiter registries keyed by `"dest::topic"` / `"workflow::key"`.
pub(crate) type WaiterRegistry = dashmap::DashMap<String, Arc<tokio::sync::Notify>>;

/// Removes its registry key on drop, so a waiter is always cleaned up on every exit path.
pub(crate) struct WaiterGuard<'a> {
    registry: &'a WaiterRegistry,
    key: String,
}

impl<'a> WaiterGuard<'a> {
    pub(crate) fn new(registry: &'a WaiterRegistry, key: String) -> Self {
        WaiterGuard { registry, key }
    }
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        self.registry.remove(&self.key);
    }
}

/// Wake every registered waiter so it re-probes the database.
fn wake_all(inner: &DbosInner) {
    for entry in inner.notifications_waiters.iter() {
        entry.value().notify_waiters();
    }
    for entry in inner.events_waiters.iter() {
        entry.value().notify_waiters();
    }
    for entry in inner.streams_waiters.iter() {
        entry.value().notify_waiters();
    }
}

pub(crate) async fn run_listener(inner: Arc<DbosInner>, token: CancellationToken) {
    let mut poll = tokio::time::interval(Duration::from_secs(1));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if token.is_cancelled() {
            return;
        }
        let mut listener = match PgListener::connect_with(&inner.pool).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, "notification listener connect failed; retrying");
                tokio::select! {
                    _ = token.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                }
            }
        };
        if let Err(e) = listener
            .listen_all([
                NOTIFICATIONS_CHANNEL,
                WORKFLOW_EVENTS_CHANNEL,
                STREAMS_CHANNEL,
            ])
            .await
        {
            tracing::warn!(error = %e, "LISTEN failed; retrying");
            continue;
        }
        // Re-probe everything: we may have missed notifications before (re)connecting.
        wake_all(&inner);

        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = poll.tick() => wake_all(&inner),
                res = listener.recv() => match res {
                    Ok(notification) => {
                        let registry = match notification.channel() {
                            NOTIFICATIONS_CHANNEL => &inner.notifications_waiters,
                            WORKFLOW_EVENTS_CHANNEL => &inner.events_waiters,
                            STREAMS_CHANNEL => &inner.streams_waiters,
                            _ => continue,
                        };
                        if let Some(notify) = registry.get(notification.payload()) {
                            notify.notify_waiters();
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "notification listener disconnected; reconnecting");
                        break; // reconnect (outer loop)
                    }
                }
            }
        }
    }
}
