//! Accounting quantities for concrete envelope items.
//!
//! Ordinary quantities describe the serialized item without parsing its payload. Rejection
//! accounting may discover additional quantities and excludes the envelope's logical event.

use super::*;
use crate::integrations::Integration;
use crate::managed::{Counted, Quantities};
use relay_profiling::ProfileType;
use relay_quotas::DataCategory;
use smallvec::smallvec;

/// Per-item rejection accounting, excluding the logical event recorded once per envelope.
/// Call only after deciding to reject an item that has not already been rate limited:
/// transaction rejection may discover embedded spans by parsing the payload.
pub(crate) trait RejectionQuantities:
    Counted + std::ops::DerefMut<Target = ItemData>
{
    fn rejected_quantities(&self) -> Quantities {
        self.quantities()
    }
}

impl Counted for EventItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::Error, 1)]
    }
}

impl RejectionQuantities for EventItem {
    fn rejected_quantities(&self) -> Quantities {
        // The logical event is accounted for once per envelope.
        Quantities::new()
    }
}

impl Counted for TransactionItem {
    fn quantities(&self) -> Quantities {
        let spans = 1 + self.span_count().unwrap_or(0) as usize;
        smallvec![
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
            (DataCategory::Span, spans),
            (DataCategory::SpanIndexed, spans),
        ]
    }
}

impl RejectionQuantities for TransactionItem {
    // Include the transaction's own span; the logical transaction is recorded separately.
    // Read embedded spans from the payload when the count header is absent or invalid.
    fn rejected_quantities(&self) -> Quantities {
        let spans = 1 + self.count_embedded_spans();
        smallvec![
            (DataCategory::Span, spans),
            (DataCategory::SpanIndexed, spans),
        ]
    }
}

impl Counted for SecurityItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::Security, 1)]
    }
}

impl RejectionQuantities for SecurityItem {
    fn rejected_quantities(&self) -> Quantities {
        // The logical event is accounted for once per envelope.
        Quantities::new()
    }
}

impl Counted for AttachmentItem {
    fn quantities(&self) -> Quantities {
        // Dying messages are merged into event data.
        if self.rate_limited()
            || self.attachment_type() == Some(AttachmentType::NintendoSwitchDyingMessage)
        {
            Quantities::new()
        } else {
            smallvec![
                (DataCategory::Attachment, self.attachment_body_size()),
                (DataCategory::AttachmentItem, 1),
            ]
        }
    }
}

impl RejectionQuantities for AttachmentItem {}

impl Counted for FormDataItem {
    fn quantities(&self) -> Quantities {
        Quantities::new()
    }
}

impl RejectionQuantities for FormDataItem {}

impl Counted for RawSecurityItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::Security, 1)]
    }
}

impl RejectionQuantities for RawSecurityItem {
    fn rejected_quantities(&self) -> Quantities {
        // The logical event is accounted for once per envelope.
        Quantities::new()
    }
}

impl Counted for UnrealReportItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::Error, 1)]
    }
}

impl RejectionQuantities for UnrealReportItem {
    fn rejected_quantities(&self) -> Quantities {
        // The logical event is accounted for once per envelope.
        Quantities::new()
    }
}

impl Counted for UserReportItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::UserReportV2, 1)]
    }
}

impl RejectionQuantities for UserReportItem {}

impl Counted for UserReportV2Item {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::UserReportV2, 1)]
    }
}

impl RejectionQuantities for UserReportV2Item {
    fn rejected_quantities(&self) -> Quantities {
        // The logical event is accounted for once per envelope.
        Quantities::new()
    }
}

impl Counted for SessionItem {
    fn quantities(&self) -> Quantities {
        smallvec![(
            DataCategory::Session,
            self.item_count().unwrap_or(1) as usize,
        )]
    }
}

impl RejectionQuantities for SessionItem {}

impl Counted for SessionsItem {
    fn quantities(&self) -> Quantities {
        smallvec![(
            DataCategory::Session,
            self.item_count().unwrap_or(1) as usize,
        )]
    }
}

impl RejectionQuantities for SessionsItem {}

impl Counted for MetricBucketsItem {
    fn quantities(&self) -> Quantities {
        Quantities::new()
    }
}

impl RejectionQuantities for MetricBucketsItem {}

impl Counted for ClientReportItem {
    fn quantities(&self) -> Quantities {
        Quantities::new()
    }
}

impl RejectionQuantities for ClientReportItem {}

impl Counted for ProfileItem {
    fn quantities(&self) -> Quantities {
        let count = self.item_count().unwrap_or(1) as usize;
        let mut quantities = smallvec![
            (DataCategory::Profile, count),
            (DataCategory::ProfileIndexed, count),
        ];
        if let Some(profile_type) = self.profile_type() {
            let category = match profile_type {
                ProfileType::Backend => DataCategory::ProfileBackend,
                ProfileType::Ui => DataCategory::ProfileUi,
            };
            quantities.push((category, count));
        }
        quantities
    }
}

impl RejectionQuantities for ProfileItem {}

impl Counted for ReplayEventItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::Replay, 1)]
    }
}

impl RejectionQuantities for ReplayEventItem {}

impl Counted for ReplayRecordingItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::Replay, 1)]
    }
}

impl RejectionQuantities for ReplayRecordingItem {}

impl Counted for ReplayVideoItem {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::Replay, 1)]
    }
}

impl RejectionQuantities for ReplayVideoItem {}

impl Counted for CheckInItem {
    fn quantities(&self) -> Quantities {
        smallvec![(
            DataCategory::Monitor,
            self.item_count().unwrap_or(1) as usize,
        )]
    }
}

impl RejectionQuantities for CheckInItem {}

impl Counted for LogItem {
    fn quantities(&self) -> Quantities {
        smallvec![
            (
                DataCategory::LogItem,
                self.item_count().unwrap_or(1) as usize,
            ),
            (DataCategory::LogByte, self.len().max(1)),
        ]
    }
}

impl RejectionQuantities for LogItem {}

impl Counted for TraceMetricItem {
    fn quantities(&self) -> Quantities {
        smallvec![
            (
                DataCategory::TraceMetric,
                self.item_count().unwrap_or(1) as usize,
            ),
            (DataCategory::TraceMetricByte, self.len().max(1)),
        ]
    }
}

impl RejectionQuantities for TraceMetricItem {}

impl Counted for SpanItem {
    fn quantities(&self) -> Quantities {
        let count = self.item_count().unwrap_or(1) as usize;
        smallvec![
            (DataCategory::Span, count),
            (DataCategory::SpanIndexed, count),
        ]
    }
}

impl RejectionQuantities for SpanItem {}

impl Counted for ProfileChunkItem {
    fn quantities(&self) -> Quantities {
        let category = match self.profile_type() {
            Some(ProfileType::Backend) => DataCategory::ProfileChunk,
            Some(ProfileType::Ui) => DataCategory::ProfileChunkUi,
            None => return Quantities::new(),
        };
        smallvec![(category, self.item_count().unwrap_or(1) as usize)]
    }
}

impl RejectionQuantities for ProfileChunkItem {}

impl Counted for IntegrationItem {
    fn quantities(&self) -> Quantities {
        match self.integration_header() {
            None => Quantities::new(),
            Some(Integration::Logs(_)) => smallvec![
                (DataCategory::LogItem, 1),
                (DataCategory::LogByte, self.len().max(1)),
            ],
            Some(Integration::Spans(_)) => {
                smallvec![(DataCategory::Span, 1), (DataCategory::SpanIndexed, 1)]
            }
        }
    }
}

impl RejectionQuantities for IntegrationItem {}

impl Counted for UnknownItem {
    fn quantities(&self) -> Quantities {
        Quantities::new()
    }
}

impl RejectionQuantities for UnknownItem {}

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
