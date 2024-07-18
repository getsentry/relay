use std::thread;
use tokio::runtime::Handle;

pub use rayon::{ThreadPool, ThreadPoolBuildError};

/// Used to create a new [`ThreadPool`] thread pool.
pub struct ThreadPoolBuilder {
    name: &'static str,
    runtime: Option<Handle>,
    num_threads: usize,
}

impl ThreadPoolBuilder {
    /// Creates a new named thread pool builder.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            runtime: None,
            num_threads: 0,
        }
    }

    /// Sets the number of threads to be used in the rayon threadpool.
    ///
    /// See also [`rayon::ThreadPoolBuilder::num_threads`].
    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }

    /// Sets the tokio runtime which will be made available in the workers.
    pub fn runtime(mut self, runtime: Handle) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Creates and returns the thread pool.
    pub fn build(self) -> Result<ThreadPool, ThreadPoolBuildError> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(self.num_threads)
            .thread_name(move |id| format!("pool-{name}-{id}", name = self.name))
            // In case of panic, log that there was a panic but keep the thread alive and don't
            // exist.
            .panic_handler(move |_panic| {
                relay_log::error!("thread in pool {name} paniced!", name = self.name)
            })
            .spawn_handler(|thread| {
                let mut b = thread::Builder::new();
                if let Some(name) = thread.name() {
                    b = b.name(name.to_owned());
                }
                if let Some(stack_size) = thread.stack_size() {
                    b = b.stack_size(stack_size);
                }
                let runtime = self.runtime.clone();
                b.spawn(move || {
                    let _guard = runtime.as_ref().map(|runtime| runtime.enter());
                    thread.run()
                })?;
                Ok(())
            })
            .build()
    }
}
