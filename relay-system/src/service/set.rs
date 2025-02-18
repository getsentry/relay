use std::sync::Arc;

use crate::{Receiver, Service, ServiceRegistry};

pub struct ServiceSet {
    registry: Arc<ServiceRegistry>,
    handle: tokio::runtime::Handle,
    handles: u32,
}

impl ServiceSet {
    pub fn start<S: Service>(&mut self, service: S, rx: Receiver<S::Interface>) {
        self.registry.start_in(&self.handle, service, rx);
    }
}
