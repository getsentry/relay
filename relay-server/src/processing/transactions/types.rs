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

// /// A payload that will be dropped by dynamic sampling.
// #[derive(Debug)]
// pub struct UnsampledPayload {
//     pub event: Annotated<Event>,
//     pub flags: Flags,
//     pub attachments: Items,
//     pub profile: Option<Item>,
//     pub extracted_spans: Vec<Item>,
// }

#[derive(Debug)]
pub struct Payload {
    pub event: Annotated<Event>,
    pub flags: Flags,
    pub attachments: Items,
    pub profile: Option<Item>,
    pub extracted_spans: Vec<Item>,
}

impl Payload {
    fn count_embedded_spans_and_self(&self) -> usize {
        1 + self
            .event
            .value()
            .and_then(|e| e.spans.value())
            .map_or(0, Vec::len)
    }
}

impl Counted for Payload {
    fn quantities(&self) -> Quantities {
        let Self {
            event,
            flags,
            attachments,
            profile,
            extracted_spans,
        } = self;
        let mut quantities = smallvec::smallvec![
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
        ];

        let span_count = if !extracted_spans.is_empty() {
            extracted_spans.len()
        } else if !flags.spans_extracted {
            self.count_embedded_spans_and_self()
        } else {
            0
        };
        quantities.extend([
            (DataCategory::Span, span_count),
            (DataCategory::SpanIndexed, span_count),
        ]);

        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        quantities
    }
}

/// The residual of a payload after it has been marked for dropping by dynamic sampling.
#[derive(Debug)]
pub struct UnsampledPayload {
    event: Annotated<Event>,
    attachments: Items,
}

impl UnsampledPayload {
    /// Splits the expanded payload into a residual payload for outcome reporting and an optional profile item.
    pub fn from_expanded(expanded: ExpandedTransaction) -> (Self, Option<Item>) {
        let ExpandedTransaction {
            headers: _,
            event,
            flags: _,
            attachments,
            profile,
        } = expanded;
        (Self { event, attachments }, profile)
    }
}

impl Counted for UnsampledPayload {
    fn quantities(&self) -> Quantities {
        let Self { event, attachments } = self;
        let mut quantities = smallvec::smallvec![(DataCategory::TransactionIndexed, 1),];

        let span_count = 1 + event
            .value()
            .and_then(|e| e.spans.value())
            .map_or(0, Vec::len);

        quantities.extend([(DataCategory::SpanIndexed, span_count)]);

        quantities.extend(attachments.quantities());

        quantities
    }
}

#[derive(Debug)]
pub enum SampledPayload {
    /// We still have a transaction + child items, and it counts as both indexed + total.
    Keep { payload: Payload },
    /// All we have left is a profile.
    Drop { profile: Option<Item> },
}

pub struct WithMetrics {
    pub headers: EnvelopeHeaders,
    pub payload: SampledPayload,
    pub metrics: ExtractedMetrics,
}

impl Counted for WithMetrics {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            payload,
            metrics,
        } = self;
        match payload {
            SampledPayload::Keep { payload } => {
                // As long as we have a payload, the metrics do not carry any data category.
                payload.quantities()
            }
            SampledPayload::Drop { profile } => {
                // When the payload is gone, the metrics carry the data category.
                // FIXME: Verify that metrics never carry profile outcomes.
                let mut quantities = profile.quantities();
                quantities.extend(metrics.quantities());
                quantities
            }
        }
    }
}

#[derive(Debug)]
pub struct WithHeaders {
    pub headers: EnvelopeHeaders,
    pub payload: SampledPayload,
}

impl WithHeaders {
    pub fn serialize_envelope(self) -> Result<Box<Envelope>, serde_json::Error> {
        let Self { headers, payload } = self;
        let mut items = Items::new();
        match payload {
            SampledPayload::Keep { payload } => {
                let span_count = payload.count_embedded_spans_and_self() - 1;
                let Payload {
                    event,
                    flags,
                    attachments,
                    profile,
                    extracted_spans,
                } = payload;

                items.extend(attachments);
                items.extend(profile);
                items.extend(extracted_spans);

                // To be compatible with previous code, add the transaction at the end:
                let data = metric!(timer(RelayTimers::EventProcessingSerialization), {
                    event.to_json()?
                });
                let mut item = Item::new(ItemType::Transaction);
                item.set_payload(ContentType::Json, data);

                let Flags {
                    metrics_extracted,
                    spans_extracted,
                    fully_normalized,
                } = flags;
                item.set_metrics_extracted(metrics_extracted);
                item.set_spans_extracted(spans_extracted);
                item.set_fully_normalized(fully_normalized);

                item.set_span_count(Some(span_count));

                items.push(item);
            }
            SampledPayload::Drop { profile } => {
                items.extend(profile);
            }
        }

        Ok(Envelope::from_parts(headers, items))
    }
}

impl Counted for WithHeaders {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            payload,
        } = self;
        match payload {
            SampledPayload::Keep { payload } => payload.quantities(),
            SampledPayload::Drop { profile } => profile.quantities(),
        }
    }
}
