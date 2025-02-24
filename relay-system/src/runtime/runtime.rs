use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::runtime::metrics::TokioCallbackMetrics;
use crate::Handle;

/// A Relay async runtime.
///
/// This is a thin wrapper around a Tokio [`tokio::runtime::Runtime`],
/// configured for Relay.
pub struct Runtime {
    rt: tokio::runtime::Runtime,
    handle: Handle,
}

impl Runtime {
    /// Creates a [`Builder`] to create and configure a new runtime.
    pub fn builder(name: &'static str) -> Builder {
        Builder::new(name)
    }

    /// Returns a [`Handle`] to this runtime.
    ///
    /// The [`Handle`] can be freely cloned and used to spawn services.
    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    /// Runs a future to completion on this runtime.
    ///
    /// See also: [`tokio::runtime::Runtime::block_on`].
    #[track_caller]
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.rt.block_on(future)
    }
}

/// Configures a Relay [`Runtime`].
pub struct Builder {
    name: &'static str,
    builder: tokio::runtime::Builder,
}

impl Builder {
    fn new(name: &'static str) -> Self {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.thread_name(name).enable_all();

        Self { name, builder }
    }

    /// Configures the amount of worker threads available to the runtime.
    ///
    /// The default value is the number of cores available to the system.
    ///
    /// See also: [`tokio::runtime::Builder::worker_threads`].
    pub fn worker_threads(&mut self, num: usize) -> &mut Self {
        self.builder.worker_threads(num);
        self
    }

    /// Configures the amount of threads in the dynamic thread pool of the runtime.
    ///
    /// See also: [`tokio::runtime::Builder::max_blocking_threads`].
    pub fn max_blocking_threads(&mut self, num: usize) -> &mut Self {
        self.builder.max_blocking_threads(num);
        self
    }

    /// Configures the idle timeout of threads in the dynamic thread pool.
    ///
    /// See also: [`tokio::runtime::Builder::thread_keep_alive`].
    pub fn thread_keep_alive(&mut self, duration: Duration) -> &mut Self {
        self.builder.thread_keep_alive(duration);
        self
    }

    /// Creates the configured [`Runtime`].
    pub fn build(&mut self) -> Runtime {
        let tokio_cb_metrics = Arc::new(TokioCallbackMetrics::default());
        tokio_cb_metrics.register(&mut self.builder);

        let rt = self
            .builder
            .build()
            .expect("creating the Tokio runtime should never fail");

        Runtime {
            handle: Handle::new(self.name, rt.handle().clone(), tokio_cb_metrics),
            rt,
        }
    }
}
