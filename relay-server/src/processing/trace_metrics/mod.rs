use relay_event_schema::protocol::TraceMetric;
use relay_protocol::Annotated;

use crate::envelope::WithHeader;
use crate::processing::Counted;
use crate::services::outcome::DiscardReason;

pub mod process;
pub mod store;
pub mod validate;

pub use self::process::{SerializedTraceMetricsContainer, expand, normalize, scrub};
pub use self::store::{Context, convert};
pub use self::validate::validate;

pub type ExpandedTraceMetrics = Vec<WithHeader<TraceMetric>>;

pub type SerializedTraceMetrics = Vec<Vec<u8>>;

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid trace metric")]
    Invalid(DiscardReason),
}

impl Counted for WithHeader<TraceMetric> {
    fn quantities(&self) -> Vec<(relay_quotas::DataCategory, usize)> {
        vec![(relay_quotas::DataCategory::TraceMetric, 1)]
    }
}
