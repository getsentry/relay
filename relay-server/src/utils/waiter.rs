use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Struct holding both the boolean state and the notifier.
#[derive(Debug)]
struct Inner {
    wait: AtomicBool,
    notify: Notify,
}

/// Construct used to wait on a boolean variable only if it's set to `true`.
#[derive(Debug, Clone)]
pub struct Waiter {
    inner: Arc<Inner>,
}

impl Waiter {
    pub fn new(wait: bool) -> Self {
        Self {
            inner: Arc::new(Inner {
                wait: AtomicBool::new(wait),
                notify: Notify::new(),
            }),
        }
    }

    pub fn set_wait(&self, wait: bool) {
        self.inner.wait.store(wait, Ordering::SeqCst);

        // If we are not waiting anymore, notify all the subscribed futures
        if !wait {
            self.inner.notify.notify_waiters();
        }
    }

    pub async fn try_wait(&self) {
        if !self.inner.wait.load(Ordering::Relaxed) {
            return;
        }

        // Suspend on waiting for a notification if waiting is required
        self.inner.notify.notified().await;
    }
}
