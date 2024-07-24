use std::panic::Location;
use std::time::Instant;

use futures::Future;
use tokio::task::JoinHandle;

use crate::statsd::{SystemCounters, SystemTimers};

/// Spawns a new asynchronous task, returning a [`JoinHandle`] for it.
///
/// This is in instrumented spawn variant of Tokio's [`tokio::spawn`].
#[track_caller]
#[allow(clippy::disallowed_methods)]
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let location = Location::caller();
    let task_id = format!("{}:{}", location.file(), location.line());

    relay_statsd::metric!(
        counter(SystemCounters::RuntimeTasksCreated) += 1,
        id = &task_id
    );

    let created = Instant::now();
    tokio::spawn(async move {
        let result = future.await;

        relay_statsd::metric!(
            timer(SystemTimers::RuntimeTasksFinished) = created.elapsed(),
            id = &task_id
        );

        result
    })
}
