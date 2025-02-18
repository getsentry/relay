mod handle;
mod metrics;
#[expect(
    clippy::module_inception,
    reason = "contains the Runtime struct, follows tokio"
)]
mod runtime;
mod spawn;

pub use self::metrics::RuntimeMetrics;
pub use self::runtime::{Builder, Handle, Runtime};
pub use self::spawn::{spawn, spawn_in, TaskId};
