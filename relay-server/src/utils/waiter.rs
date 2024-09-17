use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::Notify;

/// Construct used to wait on a boolean variable only if it's set to `true`.
#[derive(Debug)]
pub struct Waiter {
    wait: RwLock<AtomicBool>,
    notify: Notify,
}

impl Waiter {
    pub fn new(wait: bool) -> Arc<Self> {
        Arc::new(Self {
            wait: RwLock::new(AtomicBool::new(wait)),
            notify: Notify::new(),
        })
    }

    pub fn set_wait(&self, wait: bool) {
        self.wait.write().unwrap().store(wait, Ordering::SeqCst);

        // If we are not waiting anymore, we want to notify all the subscribed futures that they
        // are ready to try and proceed.
        if !wait {
            self.notify.notify_waiters();
        }
    }

    pub async fn try_wait(&self) {
        if !self.wait.read().unwrap().load(Ordering::Relaxed) {
            return;
        }

        // If we have to wait, we will suspend on waiting for a notification.
        self.notify.notified().await;
    }
}
