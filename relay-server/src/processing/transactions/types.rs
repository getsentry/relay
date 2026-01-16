use relay_event_schema::protocol::{Event, SpanV2};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use relay_statsd::metric;

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Quantities};
use crate::metrics_extraction::transactions::ExtractedMetrics;
use crate::processing::transactions::ExpandedTransaction;
use crate::statsd::RelayTimers;

/// Flags extracted from transaction item headers.
///
/// Ideally `metrics_extracted` and `spans_extracted` will not be needed in the future. Unsure
/// about `fully_normalized`.
#[derive(Debug, Default)]
pub struct Flags {
    pub metrics_extracted: bool,
    pub spans_extracted: bool,
    pub fully_normalized: bool,
}
