use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::runtime::metrics::TokioCallbackMetrics;
use crate::{Addr, Receiver, RuntimeMetrics, Service, ServiceRegistry, TaskId};

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

    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    /// Runs a future to completion on this runtime.
    ///
    /// See also: [`tokio::runtime::Runtime::block_on`].
    #[track_caller]
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.handle.block_on(future)
    }
}

#[derive(Clone)]
pub struct Handle {
    inner: Arc<HandleInner>,
}

impl Handle {
    /// Returns a new [`RuntimeMetrics`] handle for this runtime.
    pub fn metrics(&self) -> RuntimeMetrics {
        Arc::clone(&self.inner.tokio_cb_metrics)
            .into_metrics(self.inner.name, self.inner.tokio.metrics())
    }

    /// Starts a service and starts tracking its join handle, exposing an [`Addr`] for message passing.
    pub fn start<S: Service>(&mut self, service: S) -> Addr<S::Interface> {
        let (addr, rx) = crate::channel(S::name());
        self.start_with(service, rx);
        addr
    }

    /// Starts a service and starts tracking its join handle, given a predefined receiver.
    pub fn start_with<S: Service>(&mut self, service: S, rx: Receiver<S::Interface>) {
        self.inner.services.start_in(&self.inner.tokio, service, rx);
    }

    /// Returns a new unique [`ServiceSet`] to spawn services and await their termination.
    pub fn service_set(&self) -> ServiceSet {
        self.inner.services.new_set()
    }

    /// Runs a future to completion on this runtime.
    ///
    /// See also: [`tokio::runtime::Runtime::block_on`].
    #[track_caller]
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.inner.tokio.block_on(future)
    }
}

struct HandleInner {
    name: &'static str,
    services: ServiceRegistry,
    tokio: tokio::runtime::Handle,
    tokio_cb_metrics: Arc<TokioCallbackMetrics>,
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
        let cb_metrics = Arc::new(TokioCallbackMetrics::default());
        cb_metrics.register(&mut self.builder);

        let rt = self
            .builder
            .build()
            .expect("creating the Tokio runtime should never fail");

        Runtime {
            name: self.name,
            services: Arc::new(ServiceRegistry::new()),
            rt,
            cb_metrics,
        }
    }
}
