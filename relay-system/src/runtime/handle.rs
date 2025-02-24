use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt as _;

use crate::runtime::metrics::TokioCallbackMetrics;
use crate::{RuntimeMetrics, ServiceJoinHandle, ServiceRegistry, ServiceSpawn, ServicesMetrics};

#[derive(Debug)]
struct HandleInner {
    name: &'static str,
    services: ServiceRegistry,
    tokio: tokio::runtime::Handle,
    tokio_cb_metrics: Arc<TokioCallbackMetrics>,
}

/// Handle to the [`Runtime`](crate::Runtime).
///
/// The handle is internally reference-counted and can be freely cloned.
/// A handle can be obtained using the [`Runtime::handle`](crate::Runtime::handle) method.
#[derive(Debug, Clone)]
pub struct Handle {
    inner: Arc<HandleInner>,
}

impl Handle {
    pub(crate) fn new(
        name: &'static str,
        tokio: tokio::runtime::Handle,
        tokio_cb_metrics: Arc<TokioCallbackMetrics>,
    ) -> Self {
        Self {
            inner: Arc::new(HandleInner {
                name,
                services: ServiceRegistry::new(),
                tokio,
                tokio_cb_metrics,
            }),
        }
    }

    /// Returns a new [`RuntimeMetrics`] handle for this runtime.
    pub fn metrics(&self) -> RuntimeMetrics {
        Arc::clone(&self.inner.tokio_cb_metrics)
            .into_metrics(self.inner.name, self.inner.tokio.metrics())
    }

    /// Returns all service metrics of all currently running services.
    ///
    /// Unlike [`Self::metrics`], this is not a handle to the metrics.
    pub fn current_services_metrics(&self) -> ServicesMetrics {
        self.inner.services.metrics()
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
