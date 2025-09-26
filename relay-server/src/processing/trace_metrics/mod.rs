use relay_event_schema::protocol::TraceMetric;

use crate::envelope::WithHeader;
use crate::processing::Counted;
use crate::services::outcome::DiscardReason;
use smallvec::smallvec;

pub mod process;
pub mod store;
pub mod validate;

pub use self::process::{SerializedTraceMetricsContainer, expand, normalize, scrub};
#[cfg(feature = "processing")]
pub use self::store::{Context, convert};
pub use self::validate::validate;

pub type ExpandedTraceMetrics = Vec<WithHeader<TraceMetric>>;

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid trace metric")]
    Invalid(DiscardReason),
}

impl crate::managed::OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<relay_quotas::Outcome>, Self::Error) {
        let outcome = match &self {
            Self::Invalid(reason) => Some(relay_quotas::Outcome::Invalid(*reason)),
        };
        (outcome, self)
    }
}

impl Counted for WithHeader<TraceMetric> {
    fn quantities(&self) -> smallvec::SmallVec<[(relay_quotas::DataCategory, usize); 2]> {
        smallvec![(relay_quotas::DataCategory::TraceMetric, 1)]
    }
}

impl Counted for SerializedTraceMetricsContainer {
    fn quantities(&self) -> smallvec::SmallVec<[(relay_quotas::DataCategory, usize); 2]> {
        let count = self.metrics.len();
        smallvec![(relay_quotas::DataCategory::TraceMetric, count)]
    }
}

impl Counted for Vec<WithHeader<TraceMetric>> {
    fn quantities(&self) -> smallvec::SmallVec<[(relay_quotas::DataCategory, usize); 2]> {
        smallvec![(relay_quotas::DataCategory::TraceMetric, self.len())]
    }
}
