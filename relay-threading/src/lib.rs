//! # Relay Threading
//!
//! This module provides a robust threading framework for Relay, designed to efficiently manage and execute
//! asynchronous workloads. At its core is a thread-based asynchronous task pool that offers:
//!
//! - **Flexible Configuration**: Fine-tune thread counts, naming patterns, panic handling strategies,
//!   and concurrency limits through a builder pattern.
//! - **Task Multiplexing**: Distribute tasks across dedicated worker threads.
//! - **Panic Recovery**: Built-in mechanisms to gracefully handle and recover from panics, both at the
//!   thread and individual task level
//! - **Tokio Integration**: Seamlessly integrates with Tokio runtime for async task execution
//!
//! ## Concurrency Model
//!
//! The pool maintains a set of dedicated worker threads, each capable of executing multiple async tasks
//! concurrently up to a configurable limit. This architecture ensures efficient resource utilization
//! while preventing any single thread from becoming overwhelmed.
//!
//! The pool maintains a bounded queue with a capacity of twice the number of worker threads. This
//! design allows new tasks to be queued while existing ones are being processed, ensuring smooth
//! task handoff between producers and consumers. The bounded nature of the queue provides natural
//! backpressure - when workers are overwhelmed, task submission will block until capacity becomes
//! available, preventing resource exhaustion.
//!
//! ## Usage Example
//!
//! ```rust
//! use relay_threading::{AsyncPoolBuilder, AsyncPool};
//! use tokio::runtime::Handle;
//!
//! // Create a runtime handle (for example purposes, using the current runtime)
//! let runtime_handle = Handle::current();
//!
//! // Build an async pool with 4 threads and a max of 100 concurrent tasks per thread
//! let pool: AsyncPool<_> = AsyncPoolBuilder::new(runtime_handle)
//!     .num_threads(4)
//!     .max_concurrency(100)
//!     .build()
//!     .expect("Failed to build async pool");
//!
//! // Schedule a task to be executed by the pool
//! pool.spawn(async {
//!     // Place your asynchronous task logic here
//! });
//! ```
//!
//! ## Error Handling
//!
//! Both the async pool and its task multiplexer support custom panic handlers, allowing graceful
//! recovery from panics in either thread execution or individual tasks.

mod builder;
mod multiplexing;
mod pool;

pub use self::builder::*;
pub use self::pool::*;
