use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::service::monitor::{RawMetrics, ServiceMonitor};

use crate::{Receiver, TaskId};

pub struct Registry {
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

    pub fn start_in<S: crate::Service>(
        &self,
        handle: &tokio::runtime::Handle,
        service: S,
        rx: Receiver<S::Interface>,
    ) {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.start_in(handle, service, rx);
    }

    fn metrics(&self) -> BTreeMap<ServiceId, Metrics> {
        let inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        inner.metrics().collect()
    }
}

struct Inner {
    services: BTreeMap<TaskId, ServiceGroup>,
}

impl Inner {
    fn start_in<S: crate::Service>(
        &mut self,
        handle: &tokio::runtime::Handle,
        service: S,
        rx: Receiver<S::Interface>,
    ) {
        let task_id = TaskId::for_service::<S>();
        let group = self.services.entry(task_id).or_default();

        let id = ServiceId {
            task: task_id,
            instance_id: group.next_instance_id,
        };
        group.next_instance_id += 1;

        let future = ServiceMonitor::wrap(id, service.run(rx));

        let service = Service {
            instance_id: id.instance_id,
            metrics: Arc::clone(&future.metrics()),
            handle: crate::runtime::spawn_in(handle, task_id, future),
        };
        group.instances.push(service);
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

pub struct Metrics {
    pub poll_count: u64,
    pub total_poll_duration: Duration,
}

#[derive(Default)]
struct ServiceGroup {
    next_instance_id: u32,
    instances: Vec<Service>,
}

struct Service {
    instance_id: u32,
    metrics: Arc<RawMetrics>,
    handle: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServiceId {
    task: TaskId,
    instance_id: u32,
}
