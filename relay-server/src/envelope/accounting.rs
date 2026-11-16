//! Shared accounting contract and delegation for envelope items.
//!
//! Ordinary quantities describe the serialized item without parsing its payload. Rejection
//! accounting may discover additional quantities and excludes the envelope's logical event.

use super::Item;
use crate::managed::{Counted, Quantities};

/// Per-item rejection accounting, excluding the logical event recorded once per envelope.
/// Call only after deciding to reject an item that has not already been rate limited:
/// transaction rejection may discover embedded spans by parsing the payload.
pub(crate) trait RejectionQuantities: Counted {
    fn rejected_quantities(&self) -> Quantities {
        self.quantities()
    }
}

impl Item {
    /// Returns quantities used for rate limits and outcomes.
    pub fn quantities(&self) -> Quantities {
        match self {
            Self::Event(item) => item.quantities(),
            Self::Transaction(item) => item.quantities(),
            Self::Security(item) => item.quantities(),
            Self::Attachment(item) => item.quantities(),
            Self::FormData(item) => item.quantities(),
            Self::RawSecurity(item) => item.quantities(),
            Self::UnrealReport(item) => item.quantities(),
            Self::UserReport(item) => item.quantities(),
            Self::UserReportV2(item) => item.quantities(),
            Self::Session(item) => item.quantities(),
            Self::Sessions(item) => item.quantities(),
            Self::MetricBuckets(item) => item.quantities(),
            Self::ClientReport(item) => item.quantities(),
            Self::Profile(item) => item.quantities(),
            Self::ReplayEvent(item) => item.quantities(),
            Self::ReplayRecording(item) => item.quantities(),
            Self::ReplayVideo(item) => item.quantities(),
            Self::CheckIn(item) => item.quantities(),
            Self::Log(item) => item.quantities(),
            Self::TraceMetric(item) => item.quantities(),
            Self::Span(item) => item.quantities(),
            Self::ProfileChunk(item) => item.quantities(),
            Self::Integration(item) => item.quantities(),
            Self::Unknown(item) => item.quantities(),
        }
    }
}
