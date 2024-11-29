mod metrics;
#[expect(
    clippy::module_inception,
    reason = "contains the Runtime struct, follows tokio"
)]
mod runtime;
mod spawn;

pub use self::metrics::RuntimeMetrics;
pub use self::runtime::{Builder, Runtime};
pub use self::spawn::{spawn, TaskId};
