use futures::Future;
use tokio::task::JoinHandle;

use crate::statsd::SystemCounters;

/// Spawns a new asynchronous task, returning a [`JoinHandle`] for it.
///
/// This is in instrumented spawn variant of Tokio's [`tokio::spawn`].
#[macro_export]
macro_rules! spawn {
    ($future:expr) => {{
        static _TASK_ID: ::std::sync::OnceLock<$crate::TaskId> = ::std::sync::OnceLock::new();
        let task_id = _TASK_ID.get_or_init(|| (*::std::panic::Location::caller()).into());
        $crate::_spawn_inner(task_id, $future)
    }};
}

#[doc(hidden)]
#[allow(clippy::disallowed_methods)]
pub fn _spawn_inner<F>(task_id: &'static TaskId, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(Task::new(task_id, future))
}

/// An internal id for a spawned task.
#[doc(hidden)]
pub struct TaskId {
    id: String,
    file: String,
    line: String,
}

impl From<std::panic::Location<'_>> for TaskId {
    fn from(value: std::panic::Location<'_>) -> Self {
        Self {
            id: format!("{}:{}", value.file(), value.line()),
            file: value.file().to_owned(),
            line: value.line().to_string(),
        }
    }
}

pin_project_lite::pin_project! {
    /// Wraps a future and emits related task metrics.
    struct Task<T> {
        id: &'static TaskId,
        #[pin]
        inner: T,
    }

    impl<T> PinnedDrop for Task<T> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            relay_statsd::metric!(
                counter(SystemCounters::RuntimeTaskTerminated) += 1,
                id = this.id.id.as_str(),
                file = this.id.file.as_str(),
                line = this.id.line.as_str(),
            );
        }
    }
}

impl<T> Task<T> {
    fn new(id: &'static TaskId, inner: T) -> Self {
        relay_statsd::metric!(
            counter(SystemCounters::RuntimeTaskCreated) += 1,
            id = id.id.as_str(),
            file = id.file.as_str(),
            line = id.line.as_str(),
        );
        Self { id, inner }
    }
}

impl<T: Future> Future for Task<T> {
    type Output = T::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        this.inner.poll(cx)
    }
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

        #[cfg(not(windows))]
        assert_debug_snapshot!(captures, @r###"
        [
            "runtime.task.spawn.created:1|c|#id:relay-system/src/runtime.rs:103,file:relay-system/src/runtime.rs,line:103",
            "runtime.task.spawn.terminated:1|c|#id:relay-system/src/runtime.rs:103,file:relay-system/src/runtime.rs,line:103",
        ]
        "###);
        #[cfg(windows)]
        assert_debug_snapshot!(captures, @r###"
        [
            "runtime.task.spawn.created:1|c|#id:relay-system\\src\\runtime.rs:103,file:relay-system\\src\\runtime.rs,line:103",
            "runtime.task.spawn.terminated:1|c|#id:relay-system\\src\\runtime.rs:103,file:relay-system\\src\\runtime.rs,line:103",
        ]
        "###);
    }
}
