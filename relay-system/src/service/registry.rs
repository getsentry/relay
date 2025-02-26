use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::service::monitor::{RawMetrics, ServiceMonitor};

use crate::service::status::{ServiceJoinHandle, ServiceStatusJoinHandle};
use crate::{ServiceObj, TaskId};

/// A point in time snapshot of all started services and their [`ServiceMetrics`].
pub struct ServicesMetrics(BTreeMap<ServiceId, ServiceMetrics>);

impl ServicesMetrics {
    /// Returns an iterator of all service identifiers and their [`ServiceMetrics`].
    pub fn iter(&self) -> impl Iterator<Item = (ServiceId, ServiceMetrics)> + '_ {
        self.0.iter().map(|(id, metrics)| (*id, *metrics))
    }
}

/// Collected metrics of a single service.
#[derive(Debug, Clone, Copy)]
pub struct ServiceMetrics {
    /// Amount of times the service was polled.
    pub poll_count: u64,
    /// Total amount of time the service was busy.
    ///
    /// The busy duration starts at zero when the service is created and is increased
    /// whenever the service is spending time processing work. Using this value can
    /// indicate the load of the given service.
    ///
    /// This number is monotonically increasing. It is never decremented or reset to zero.
    pub total_busy_duration: Duration,
    /// Approximate utilization of the service based on its [`Self::total_busy_duration`].
    ///
    /// This value is a percentage in the range from `[0-100]` and recomputed periodically.
    ///
    /// The measure is only updated when the when the service is polled. A service which
    /// spends a long time idle may not have this measure updated for a long time.
    pub utilization: u8,
}

/// A per runtime unique identifier for a started service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServiceId {
    task: TaskId,
    instance_id: u32,
}

impl ServiceId {
    /// Returns the name of the service.
    pub fn name(&self) -> &'static str {
        self.task.id()
    }

    /// Returns a for this service unique instance id.
    ///
    /// The combination of [`Self::name`] and [`Self::instance_id`] is unique for each runtime.
    pub fn instance_id(&self) -> u32 {
        self.instance_id
    }
}

#[derive(Debug)]
pub(crate) struct Registry {
    inner: Mutex<Inner>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                services: Default::default(),
            }),
        }
    }

    pub fn start_in(
        &self,
        handle: &tokio::runtime::Handle,
        service: ServiceObj,
    ) -> ServiceJoinHandle {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.start_in(handle, service)
    }

    pub fn metrics(&self) -> ServicesMetrics {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        ServicesMetrics(inner.metrics().collect())
    }
}

#[derive(Debug)]
struct Inner {
    services: BTreeMap<TaskId, ServiceGroup>,
}

impl Inner {
    fn start_in(
        &mut self,
        handle: &tokio::runtime::Handle,
        service: ServiceObj,
    ) -> ServiceJoinHandle {
        let task_id = TaskId::from(&service);
        let group = self.services.entry(task_id).or_default();

        // Cleanup group, evicting all terminated services, while we're at it.
        group.instances.retain(|s| !s.handle.is_finished());

        let id = ServiceId {
            task: task_id,
            instance_id: group.next_instance_id,
        };
        group.next_instance_id += 1;

        // Services are allowed to process as much work as possible before yielding to other,
        // lower priority tasks. We want to prioritize service backlogs over creating more work
        // for these services.
        let future = tokio::task::unconstrained(service.future);
        let future = ServiceMonitor::wrap(future);
        let metrics = Arc::clone(future.metrics());

        let jh = crate::runtime::spawn_in(handle, task_id, future);
        let (sjh, sjhe) = crate::service::status::split(jh);

        let service = Service {
            instance_id: id.instance_id,
            metrics,
            handle: sjh,
        };
        group.instances.push(service);

        sjhe
    }

    fn metrics(&self) -> impl Iterator<Item = (ServiceId, ServiceMetrics)> + '_ {
        self.services.iter().flat_map(|(task_id, group)| {
            group.instances.iter().map(|service| {
                let id = ServiceId {
                    task: *task_id,
                    instance_id: service.instance_id,
                };

                let metrics = ServiceMetrics {
                    poll_count: service.metrics.poll_count.load(Ordering::Relaxed),
                    total_busy_duration: Duration::from_nanos(
                        service.metrics.total_duration_ns.load(Ordering::Relaxed),
                    ),
                    utilization: service.metrics.utilization.load(Ordering::Relaxed),
                };

                (id, metrics)
            })
        })
    }
}

#[derive(Debug, Default)]
struct ServiceGroup {
    next_instance_id: u32,
    instances: Vec<Service>,
}

#[derive(Debug)]
struct Service {
    instance_id: u32,
    metrics: Arc<RawMetrics>,
    handle: ServiceStatusJoinHandle,
}
