use std::any::Any;
use std::future::Future;
use std::io;
use std::sync::Arc;

use crate::pool::{AsyncPool, Thread};
use crate::pool::{CustomSpawn, DefaultSpawn, ThreadSpawn};

/// Type alias for a thread safe closure that is used for panic handling across the code.
pub(crate) type PanicHandler = dyn Fn(Box<dyn Any + Send>) + Send + Sync;

/// [`AsyncPoolBuilder`] provides a flexible way to configure and build an [`AsyncPool`] for executing
/// asynchronous tasks concurrently on dedicated threads.
///
/// This builder enables you to customize the number of threads, concurrency limits, thread naming,
/// and panic handling strategies.
pub struct AsyncPoolBuilder<S = DefaultSpawn> {
    pub(crate) runtime: tokio::runtime::Handle,
    pub(crate) thread_name: Option<Box<dyn FnMut(usize) -> String>>,
    pub(crate) thread_panic_handler: Option<Arc<PanicHandler>>,
    pub(crate) task_panic_handler: Option<Arc<PanicHandler>>,
    pub(crate) spawn_handler: S,
    pub(crate) num_threads: usize,
    pub(crate) max_concurrency: usize,
}

impl AsyncPoolBuilder<DefaultSpawn> {
    /// Initializes a new [`AsyncPoolBuilder`] with default settings.
    ///
    /// The builder is tied to the provided [`tokio::runtime::Handle`] and prepares to configure an [`AsyncPool`].
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
    /// Specifies a custom naming convention for threads in the [`AsyncPool`].
    ///
    /// The provided closure receives the thread's index and returns a name,
    /// which can be useful for debugging and logging.
    pub fn thread_name<F>(mut self, thread_name: F) -> Self
    where
        F: FnMut(usize) -> String + 'static,
    {
        self.thread_name = Some(Box::new(thread_name));
        self
    }

    /// Sets a custom panic handler for threads in the [`AsyncPool`].
    ///
    /// If a thread panics, the provided handler will be invoked so that you can perform
    /// custom error handling or cleanup.
    pub fn thread_panic_handler<F>(mut self, panic_handler: F) -> Self
    where
        F: Fn(Box<dyn Any + Send>) + Send + Sync + 'static,
    {
        self.thread_panic_handler = Some(Arc::new(panic_handler));
        self
    }

    /// Sets a custom panic handler for tasks executed by the [`AsyncPool`].
    ///
    /// This handler is used to manage panics that occur during task execution, allowing for graceful
    /// error handling.
    pub fn task_panic_handler<F>(mut self, panic_handler: F) -> Self
    where
        F: Fn(Box<dyn Any + Send>) + Send + Sync + 'static,
    {
        self.task_panic_handler = Some(Arc::new(panic_handler));
        self
    }

    /// Configures a custom thread spawning procedure for the [`AsyncPool`].
    ///
    /// This method allows you to adjust thread settings (e.g. naming, stack size) before thread creation,
    /// making it possible to apply application-specific configurations.
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

    /// Sets the number of worker threads for the [`AsyncPool`].
    ///
    /// This determines how many dedicated threads will be available for running tasks concurrently.
    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }

    /// Sets the maximum number of concurrent tasks per thread in the [`AsyncPool`].
    ///
    /// This controls how many futures can be polled simultaneously on each worker thread.
    pub fn max_concurrency(mut self, max_concurrency: usize) -> Self {
        self.max_concurrency = max_concurrency;
        self
    }

    /// Constructs an [`AsyncPool`] based on the configured settings.
    ///
    /// Finalizing the builder sets up dedicated worker threads and configures the executor
    /// to enforce the specified concurrency limits.
    pub fn build<F>(self) -> Result<AsyncPool<F>, io::Error>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        AsyncPool::new(self)
    }
}
