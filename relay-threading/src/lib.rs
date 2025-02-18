//! # Relay Threading
//!
//! This module provides threading abstractions for Relay, offering a set of utilities for managing
//! asynchronous work. It includes a thread-based asynchronous task pool, a flexible builder for
//! configuring pool parameters (thread naming, panic handling, concurrency limits), and mechanisms for
//! multiplexing tasks across dedicated threads with built-in panic recovery.
//!
//! ## Features
//!
//! - **AsyncPool**: A thread-based asynchronous pool for executing futures concurrently on dedicated threads.
//! - **AsyncPoolBuilder**: A configurable builder to construct an [`AsyncPool`] with custom settings,
//!   including thread naming, panic handlers (for both threads and tasks), custom spawn handlers, and
//!   concurrency limits.
//! - **Multiplexed Execution**: A task multiplexer that drives a collection of asynchronous tasks while
//!   respecting a specified concurrency limit. It handles panics gracefully via an optional panic callback.
//! - **Custom Thread Spawning**: Supports customized thread creation, allowing usage of system defaults or
//!   custom configurations through a provided spawn handler.
//!
//! ## Modules
//!
//! - **builder**: Contains the [`AsyncPoolBuilder`] which configures and constructs an [`AsyncPool`].
//! - **multiplexing**: Provides the [`Multiplexed`] future that manages and executes multiple asynchronous tasks concurrently.
//! - **pool**: Implements the [`AsyncPool`] and related types, such as [`Thread`]. Together they encapsulate the logic
//!   for scheduling tasks across multiple threads.
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
//!
//! For more details on specific functionalities, refer to the documentation in the submodules:
//! - [`builder`]
//! - [`multiplexing`]
//! - [`pool`]

mod builder;
mod multiplexing;
mod pool;

pub use self::builder::*;
pub use self::pool::*;
