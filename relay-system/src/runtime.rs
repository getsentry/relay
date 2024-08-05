use std::time::Instant;

use futures::Future;
use tokio::task::JoinHandle;

use crate::statsd::{SystemCounters, SystemTimers};

/// Spawns a new asynchronous task, returning a [`JoinHandle`] for it.
///
/// This is in instrumented spawn variant of Tokio's [`tokio::spawn`].
#[macro_export]
macro_rules! spawn {
    ($future:expr) => {{
        static _TASK_ID: ::std::sync::OnceLock<String> = ::std::sync::OnceLock::new();
        let task_id = _TASK_ID.get_or_init(|| {
            let location = ::std::panic::Location::caller();
            format!("{}:{}", location.file(), location.line())
        });
        $crate::_spawn_inner(task_id, $future)
    }};
}

#[doc(hidden)]
#[allow(clippy::disallowed_methods)]
pub fn _spawn_inner<F>(task_id: &'static str, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    relay_statsd::metric!(
        counter(SystemCounters::RuntimeTasksCreated) += 1,
        id = task_id
    );

    let created = Instant::now();
    tokio::spawn(async move {
        let result = future.await;

        relay_statsd::metric!(
            timer(SystemTimers::RuntimeTasksFinished) = created.elapsed(),
            id = task_id
        );

        result
    })
}

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;

    #[test]
    fn test_spawn_spawns_a_future() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let captures = relay_statsd::with_capturing_test_client(|| {
            rt.block_on(async {
                let _ = crate::spawn!(async {}).await;
            })
        });

        assert_debug_snapshot!(captures, @r###"
        [
            "runtime.task.spawn.created:1|c|#id:relay-system/src/runtime.rs:60",
            "runtime.task.spawn.finished:0|ms|#id:relay-system/src/runtime.rs:60",
        ]
        "###);
    }
}
