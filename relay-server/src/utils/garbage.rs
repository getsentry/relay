use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;

/// Garbage disposal agent.
///
/// Spawns a background thread which drops items sent to it via [`GarbageDisposal::dispose`].
pub struct GarbageDisposal<T> {
    tx: mpsc::Sender<T>,
    queue_size: Arc<AtomicUsize>,
}

impl<T: Send + 'static> GarbageDisposal<T> {
    /// Returns a new instance plus a handle to join on the background thread.
    /// Currently only used in tests.
    fn new_joinable() -> (Self, JoinHandle<()>) {
        let (tx, rx) = mpsc::channel();

        let queue_size = Arc::new(AtomicUsize::new(0));
        let queue_size_clone = queue_size.clone();
        let join_handle = std::thread::spawn(move || {
            relay_log::debug!("Start garbage collection thread");
            while let Ok(object) = rx.recv() {
                queue_size_clone.fetch_sub(1, Ordering::Relaxed); // Wraps around on overflow
                drop(object);
            }
            relay_log::debug!("Stop garbage collection thread");
        });

        (Self { tx, queue_size }, join_handle)
    }

    /// Spawns a new garbage disposal instance.
    /// Every instance has its own background thread that received items to be dropped via
    /// [`Self::dispose`].
    /// When the instance is dropped, the background thread stops automatically.
    pub fn new() -> Self {
        let (instance, _) = Self::new_joinable();
        instance
    }

    /// Defers dropping an object by sending it to the background thread.
    pub fn dispose(&self, object: T) {
        self.queue_size.fetch_add(1, Ordering::Relaxed);
        self.tx
            .send(object)
            .map_err(|e| {
                relay_log::error!("Failed to send object to garbage disposal thread, drop here");
                drop(e.0);
            })
            .ok();
    }

    /// Get current queue size.
    pub fn queue_size(&self) -> usize {
        self.queue_size.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        thread::ThreadId,
    };

    use super::GarbageDisposal;

    struct SomeStruct {
        thread_ids: Arc<Mutex<Vec<ThreadId>>>,
    }

    impl Drop for SomeStruct {
        fn drop(&mut self) {
            self.thread_ids
                .lock()
                .unwrap()
                .push(std::thread::current().id())
        }
    }

    #[test]
    fn test_garbage_disposal() {
        let thread_ids = Arc::new(Mutex::new(Vec::<ThreadId>::new()));

        let x1 = SomeStruct {
            thread_ids: thread_ids.clone(),
        };
        drop(x1);

        let x2 = SomeStruct {
            thread_ids: thread_ids.clone(),
        };

        let (garbage, join_handle) = GarbageDisposal::new_joinable();
        garbage.dispose(x2);
        drop(garbage); // breaks the while loop by dropping rx
        join_handle.join().ok(); // wait for thread to finish its work

        let thread_ids = thread_ids.lock().unwrap();
        assert_eq!(thread_ids.len(), 2);
        assert_eq!(thread_ids[0], std::thread::current().id());
        assert!(thread_ids[0] != thread_ids[1]);
    }
}
