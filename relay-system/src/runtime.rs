use futures::Future;
use tokio::task::JoinHandle;

use crate::statsd::SystemCounters;
use crate::Service;

/// Spawns an instrumented task with an automatically generated [`TaskId`].
///
/// Returns a [`JoinHandle`].
#[macro_export]
macro_rules! spawn {
    ($future:expr) => {{
        static _PARTS: ::std::sync::OnceLock<(String, String, String)> =
            ::std::sync::OnceLock::new();
        let (id, file, line) = _PARTS.get_or_init(|| {
            let caller = *::std::panic::Location::caller();
            let id = format!("{}:{}", caller.file(), caller.line());
            (id, caller.file().to_owned(), caller.line().to_string())
        });
        $crate::spawn(
            $crate::TaskId::_from_location(id.as_str(), file.as_str(), line.as_str()),
            $future,
        )
    }};
}

/// Spawns a new asynchronous task, returning a [`JoinHandle`] for it.
///
/// This is in instrumented spawn variant of Tokio's [`tokio::spawn`].
#[allow(clippy::disallowed_methods)]
pub fn spawn<F>(task_id: TaskId, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(Task::new(task_id, future))
}

/// An identifier for tasks spawned by [`spawn()`], used to log metrics.
pub struct TaskId {
    id: &'static str,
    file: Option<&'static str>,
    line: Option<&'static str>,
}

impl TaskId {
    /// Create a task ID based on the service's name.
    pub fn for_service<S: Service>() -> Self {
        Self {
            id: S::name(),
            file: None,
            line: None,
        }
    }

    #[doc(hidden)]
    pub fn _from_location(id: &'static str, file: &'static str, line: &'static str) -> Self {
        Self {
            id,
            file: Some(file),
            line: Some(line),
        }
    }

    fn emit_metric(&self, metric: SystemCounters) {
        let Self { id, file, line } = self;
        relay_statsd::metric!(
            counter(metric) += 1,
            id = id,
            file = file.unwrap_or_default(),
            line = line.unwrap_or_default()
        );
    }
}

pin_project_lite::pin_project! {
    /// Wraps a future and emits related task metrics.
    struct Task<T> {
        id: TaskId,
        #[pin]
        inner: T,
    }

    impl<T> PinnedDrop for Task<T> {
        fn drop(this: Pin<&mut Self>) {
            this.id.emit_metric(SystemCounters::RuntimeTaskTerminated);
        }
    }
}

impl<T> Task<T> {
    fn new(id: TaskId, inner: T) -> Self {
        id.emit_metric(SystemCounters::RuntimeTaskCreated);
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

    use crate::{Service, TaskId};

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
            "runtime.task.spawn.created:1|c|#id:relay-system/src/runtime.rs:124,file:relay-system/src/runtime.rs,line:124",
            "runtime.task.spawn.terminated:1|c|#id:relay-system/src/runtime.rs:124,file:relay-system/src/runtime.rs,line:124",
        ]
        "###);
        #[cfg(windows)]
        assert_debug_snapshot!(captures, @r###"
        [
            "runtime.task.spawn.created:1|c|#id:relay-system\\src\\runtime.rs:124,file:relay-system\\src\\runtime.rs,line:124",
            "runtime.task.spawn.terminated:1|c|#id:relay-system\\src\\runtime.rs:124,file:relay-system\\src\\runtime.rs,line:124",
        ]
        "###);
    }

    #[test]
    fn test_spawn_with_custom_id() {
        struct Foo;
        impl Service for Foo {
            type Interface = ();
            async fn run(self, _rx: crate::Receiver<Self::Interface>) {}
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let captures = relay_statsd::with_capturing_test_client(|| {
            rt.block_on(async {
                let _ = crate::spawn(TaskId::for_service::<Foo>(), async {}).await;
            })
        });

        assert_debug_snapshot!(captures, @r###"
        [
            "runtime.task.spawn.created:1|c|#id:relay_system::runtime::tests::test_spawn_with_custom_id::Foo,file:,line:",
            "runtime.task.spawn.terminated:1|c|#id:relay_system::runtime::tests::test_spawn_with_custom_id::Foo,file:,line:",
        ]
        "###);
    }
}
