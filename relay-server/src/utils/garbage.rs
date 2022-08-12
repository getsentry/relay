use std::sync::mpsc;
use std::thread::JoinHandle;

pub struct GarbageDisposal<T> {
    tx: Option<mpsc::Sender<T>>,
    join_handle: Option<JoinHandle<()>>,
}

impl<T: Send + 'static> GarbageDisposal<T> {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();

        let join_handle = std::thread::spawn(move || {
            relay_log::debug!("Start garbage collection thread");
            while let Ok(object) = rx.recv() {
                // TODO: Log size of channel queue as a gauge here
                relay_log::trace!(
                    "Dropping object {:?} of type {}",
                    &object as *const T,
                    std::any::type_name::<T>()
                );
                drop(object);
            }
            relay_log::debug!("Stop garbage collection thread");
        });

        Self {
            tx: Some(tx),
            join_handle: Some(join_handle),
        }
    }

    pub fn dispose(&self, object: T) {
        let tx = self.tx.as_ref().expect("Join handle not initialized");
        tx.send(object)
            .map_err(|e| {
                relay_log::error!("Failed to send object to garbage disposal thread, drop here");
                drop(e.0);
            })
            .ok();
    }
}

impl<T> Drop for GarbageDisposal<T> {
    fn drop(&mut self) {
        // Cut off the sender:
        drop(self.tx.take());
        // Wait for receiver to empty its queue:
        if let Some(join_handle) = self.join_handle.take() {
            if join_handle.join().is_err() {
                relay_log::error!("Failed to join on garbage disposal thread");
            }
        }
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

        let garbage = GarbageDisposal::new();
        garbage.dispose(x2);
        drop(garbage); // Join garbage disposal thread

        let thread_ids = thread_ids.lock().unwrap();
        assert_eq!(thread_ids.len(), 2);
        assert_eq!(thread_ids[0], std::thread::current().id());
        assert!(thread_ids[0] != thread_ids[1]);
    }
}
