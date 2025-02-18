use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::service::monitor::{RawMetrics, ServiceMonitor};

use crate::service::status::{ServiceJoinHandle, ServiceJoinHandleNoError};
use crate::{ServiceObj, TaskId};

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

    fn metrics(&self) -> BTreeMap<ServiceId, Metrics> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.metrics().collect()
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

        let id = ServiceId {
            task: task_id,
            instance_id: group.next_instance_id,
        };
        group.next_instance_id += 1;

        let future = ServiceMonitor::wrap(id, service.future);
        let metrics = Arc::clone(&future.metrics());

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

    fn metrics(&self) -> impl Iterator<Item = (ServiceId, Metrics)> + '_ {
        self.services.iter().flat_map(|(task_id, group)| {
            group.instances.iter().map(|service| {
                let id = ServiceId {
                    task: *task_id,
                    instance_id: service.instance_id,
                };

                let metrics = Metrics {
                    poll_count: service.metrics.poll_count.load(Ordering::Relaxed),
                    total_poll_duration: Duration::from_nanos(
                        service.metrics.total_duration_ns.load(Ordering::Relaxed),
                    ),
                };

                (id, metrics)
            })
        })
    }
}

#[derive(Debug)]
pub struct Metrics {
    pub poll_count: u64,
    pub total_poll_duration: Duration,
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
    handle: ServiceJoinHandleNoError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServiceId {
    task: TaskId,
    instance_id: u32,
}
