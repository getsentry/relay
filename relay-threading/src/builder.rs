use std::any::Any;
use std::future::Future;
use std::io;
use std::sync::Arc;

use crate::pool::{AsyncPool, Thread};
use crate::pool::{CustomSpawn, DefaultSpawn, ThreadSpawn};

/// A builder for constructing an [`AsyncPool`] with custom configurations.
///
/// Use this builder to fine-tune the performance and threading behavior of your asynchronous
/// task pool.
pub struct AsyncPoolBuilder<S = DefaultSpawn> {
    pub(crate) runtime: tokio::runtime::Handle,
    pub(crate) thread_name: Option<Box<dyn FnMut(usize) -> String>>,
    #[allow(clippy::type_complexity)]
    pub(crate) thread_panic_handler: Option<Arc<dyn Fn(Box<dyn Any + Send>) + Send + Sync>>,
    #[allow(clippy::type_complexity)]
    pub(crate) task_panic_handler: Option<Arc<dyn Fn(Box<dyn Any + Send>) + Send + Sync>>,
    pub(crate) spawn_handler: S,
    pub(crate) num_threads: usize,
    pub(crate) max_concurrency: usize,
}

impl AsyncPoolBuilder<DefaultSpawn> {
    /// Creates a new [`AsyncPoolBuilder`] with default settings.
    pub fn new(runtime: tokio::runtime::Handle) -> AsyncPoolBuilder<DefaultSpawn> {
        AsyncPoolBuilder {
            runtime,
            thread_name: None,
            thread_panic_handler: None,
            task_panic_handler: None,
            spawn_handler: DefaultSpawn,
            num_threads: 1,
            max_concurrency: 1,
        }
    }
}

impl<S> AsyncPoolBuilder<S>
where
    S: ThreadSpawn,
{
    /// Specifies a custom hook to generate the thread name given the thread index within the
    /// [`AsyncPool`].
    pub fn thread_name<F>(mut self, thread_name: F) -> Self
    where
        F: FnMut(usize) -> String + 'static,
    {
        self.thread_name = Some(Box::new(thread_name));
        self
    }

    /// Specifies a custom hook to handle the panic happening in one of the threads of the
    /// [`AsyncPool`].
    ///
    /// The `panic_handler` is called once for each panicking thread with the error caught in the
    /// panicking as argument.
    pub fn thread_panic_handler<F>(mut self, panic_handler: F) -> Self
    where
        F: Fn(Box<dyn Any + Send>) + Send + Sync + 'static,
    {
        self.thread_panic_handler = Some(Arc::new(panic_handler));
        self
    }

    /// Specifies a custom hook to handle the panic happening in one of the tasks of the
    /// [`AsyncPool`].
    ///
    /// The `panic_handler` is called once for each panicking task with the error caught in the
    /// panicking as argument.
    pub fn task_panic_handler<F>(mut self, panic_handler: F) -> Self
    where
        F: Fn(Box<dyn Any + Send>) + Send + Sync + 'static,
    {
        self.task_panic_handler = Some(Arc::new(panic_handler));
        self
    }

    /// Specifies a custom hook to dynamically adjust thread settings for the [`AsyncPool`].
    pub fn spawn_handler<F>(self, spawn_handler: F) -> AsyncPoolBuilder<CustomSpawn<F>>
    where
        F: FnMut(Thread) -> io::Result<()>,
    {
        AsyncPoolBuilder {
            runtime: self.runtime,
            thread_name: self.thread_name,
            thread_panic_handler: self.thread_panic_handler,
            task_panic_handler: self.task_panic_handler,
            spawn_handler: CustomSpawn::new(spawn_handler),
            num_threads: self.num_threads,
            max_concurrency: self.max_concurrency,
        }
    }

    /// Sets the number of executor threads for running tasks in the [`AsyncPool`].
    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }

    /// Adjusts the maximum number of concurrent tasks allowed per executor in the [`AsyncPool`].
    ///
    /// The max concurrency determines how many futures can be polled simultaneously.
    pub fn max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = max_concurrency;
        self
    }

    /// Finalizes the configuration and constructs an operational [`AsyncPool`] for executing tasks.
    pub fn build<F>(self) -> Result<AsyncPool<F>, io::Error>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        AsyncPool::new(self)
    }
}
