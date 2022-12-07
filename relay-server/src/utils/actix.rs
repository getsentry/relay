use actix::System;
use tokio::runtime::Runtime;

/// Constructs a tokio [`Runtime`] configured for running [services](relay_system::Service).
///
/// For compatibility, this runtime also registers the actix [`System`]. This is required for
/// interoperability with actors. To send messages to those actors, use
/// [`compat::send`](relay_system::compat::send).
///
/// # Panics
///
/// The calling thread must have the actix system enabled, panics if this is invoked in a thread
/// where actix is not enabled.
pub fn create_runtime(threads: usize) -> Runtime {
    let system = System::current();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .on_thread_start(move || System::set_current(system.clone()))
        .build()
        .unwrap()
}
