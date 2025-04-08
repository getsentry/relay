use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::monitor::{RawMetrics, TimedFuture};

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
    /// The measure is only updated when the service is polled. A service which
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

        // Services are allowed to process as much work as possible before yielding to other,
        // lower priority tasks. We want to prioritize service backlogs over creating more work
        // for these services.
        let future = tokio::task::unconstrained(service.future);
        let future = TimedFuture::wrap(future);
        let metrics = Arc::clone(future.metrics());

        let task_handle = crate::runtime::spawn_in(handle, task_id, future);
        let (status_handle, handle) = crate::service::status::split(task_handle);

        group.add(metrics, status_handle);

        handle
    }

    fn metrics(&self) -> impl Iterator<Item = (ServiceId, ServiceMetrics)> + '_ {
        self.services.iter().flat_map(|(task_id, group)| {
            group.iter().map(|service| {
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

/// Logical grouping for all service instances of the same service.
///
/// A single service can be started multiple times, each individual
/// instance of a specific service is tracked in this group.
///
/// The group keeps track of a unique per service identifier,
/// which stays unique for the duration of the runtime.
///
/// It also holds a list of all currently alive service instances.
#[derive(Debug, Default)]
struct ServiceGroup {
    /// Next unique per-service id.
    ///
    /// The next instance started for this group will be assigned the id
    /// and the id is incremented in preparation for the following instance.
    next_instance_id: u32,
    /// All currently alive service instances or instances that have stopped
    /// but are not yet remove from the list.
    instances: Vec<ServiceInstance>,
}

impl ServiceGroup {
    /// Adds a started service to the service group.
    pub fn add(&mut self, metrics: Arc<RawMetrics>, handle: ServiceStatusJoinHandle) {
        // Cleanup the group, evicting all finished services, while we're at it.
        self.instances.retain(|s| !s.handle.is_finished());

        let instance_id = self.next_instance_id;
        self.next_instance_id += 1;

        let service = ServiceInstance {
            instance_id,
            metrics,
            handle,
        };

        self.instances.push(service);
    }

    /// Returns an iterator over all currently alive services.
    pub fn iter(&self) -> impl Iterator<Item = &ServiceInstance> {
        self.instances.iter().filter(|s| !s.handle.is_finished())
    }
}

/// Collection of metadata the registry tracks per service instance.
#[derive(Debug)]
struct ServiceInstance {
    /// The per service group unique id for this instance.
    instance_id: u32,
    /// A raw handle for all metrics tracked for this instance.
    ///
    /// The handle gives raw access to all tracked metrics, these metrics
    /// should be treated as **read-only**.
    metrics: Arc<RawMetrics>,
    /// A handle to the service instance.
    ///
    /// The handle has information about the completion status of the service.
    handle: ServiceStatusJoinHandle,
}
