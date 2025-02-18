use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::StreamExt;

use crate::runtime::metrics::TokioCallbackMetrics;
use crate::{RuntimeMetrics, ServiceJoinHandle, ServiceRegistry, ServiceSpawn};

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

#[derive(Debug)]
struct HandleInner {
    name: &'static str,
    services: ServiceRegistry,
    tokio: tokio::runtime::Handle,
    tokio_cb_metrics: Arc<TokioCallbackMetrics>,
}

/// Handle to the [`Runtime`].
///
/// The handle is internally reference-counted and can be freely cloned.
/// A handle can be obtained using the [`Runtime::handle`] method.
#[derive(Debug, Clone)]
pub struct Handle {
    inner: Arc<HandleInner>,
}

impl Handle {
    /// Returns a new [`RuntimeMetrics`] handle for this runtime.
    pub fn metrics(&self) -> RuntimeMetrics {
        Arc::clone(&self.inner.tokio_cb_metrics)
            .into_metrics(self.inner.name, self.inner.tokio.metrics())
    }

    /// Returns a new unique [`ServiceSet`] to spawn services and await their termination.
    pub fn service_set(&self) -> ServiceSet {
        ServiceSet {
            handle: Arc::clone(&self.inner),
            services: Default::default(),
        }
    }
}

impl ServiceSpawn for Handle {
    fn start_obj(&self, service: crate::ServiceObj) {
        self.inner.services.start_in(&self.inner.tokio, service);
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
            handle: Handle {
                inner: Arc::new(HandleInner {
                    name: self.name,
                    services: ServiceRegistry::new(),
                    tokio: rt.handle().clone(),
                    tokio_cb_metrics,
                }),
            },
            rt,
        }
    }
}

/// Spawns and keeps track of running services.
///
/// A [`ServiceSet`] can be awaited for the completion of all started services
/// on this [`ServiceSet`].
///
/// Every service started on this [`ServiceSet`] is attached to the [`Handle`]
/// this set was created from, using [`Handle::service_set`].
pub struct ServiceSet {
    handle: Arc<HandleInner>,
    services: FuturesUnordered<ServiceJoinHandle>,
}

impl ServiceSet {
    /// Awaits until all services have finished.
    ///
    /// Panics if one of the spawned services has panicked.
    pub async fn join(&mut self) {
        while let Some(res) = self.services.next().await {
            if let Some(panic) = res.err().and_then(|e| e.into_panic()) {
                // Re-trigger panic to terminate the process:
                std::panic::resume_unwind(panic);
            }
        }
    }
}

impl ServiceSpawn for ServiceSet {
    fn start_obj(&self, service: crate::ServiceObj) {
        let handle = self.handle.services.start_in(&self.handle.tokio, service);
        self.services.push(handle);
    }
}
