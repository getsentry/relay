use std::sync::mpsc;

pub struct GarbageDisposal<T> {
    tx: mpsc::Sender<T>,
}

impl<T: Send + 'static> GarbageDisposal<T> {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            relay_log::info!("Start garbage collection thread");
            while let Ok(object) = rx.recv() {
                // TODO: Log size of channel queue as a gauge here
                relay_log::trace!(
                    "Dropping object {:?} of type {}",
                    &object as *const T,
                    std::any::type_name::<T>()
                );
                drop(object);
            }

            // TODO: We create a memory leak if this thread silently crashes
        });

        Self { tx }
    }

    pub fn dispose(&self, object: T) {
        self.tx
            .send(object)
            .map_err(|e| {
                relay_log::error!("Unable to send object to garbage collector thread, drop here");
                drop(e.0);
            })
            .ok();
    }
}

#[cfg(test)]
mod tests {
    use super::GarbageDisposal;

    struct SomeStruct {
        should_go_to_garbage_collector: bool,
        created_on_thread: std::thread::ThreadId,
    }

    impl SomeStruct {
        fn simple() -> Self {
            Self {
                should_go_to_garbage_collector: false,
                created_on_thread: std::thread::current().id(),
            }
        }

        fn garbage_collected() -> Self {
            Self {
                should_go_to_garbage_collector: true,
                created_on_thread: std::thread::current().id(),
            }
        }
    }

    impl Drop for SomeStruct {
        fn drop(&mut self) {
            dbg!("DROPPING");
            let different_thread = std::thread::current().id() != self.created_on_thread;
            assert_eq!(different_thread, self.should_go_to_garbage_collector);
        }
    }

    #[test]
    fn test_garbage_disposal() {
        let x1 = SomeStruct::simple();
        drop(x1);

        let x2 = SomeStruct::garbage_collected();
        let gc = GarbageDisposal::new();
        gc.dispose(x2);
    }
}
