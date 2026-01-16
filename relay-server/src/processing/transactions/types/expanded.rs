use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use relay_statsd::metric;
use smallvec::smallvec;

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Managed, Quantities, Rejected};
use crate::processing::spans::{Indexed, TotalAndIndexed};
use crate::processing::transactions::Error;
use crate::processing::transactions::types::Flags;
use crate::processing::{Context, RateLimited, RateLimiter};
use crate::statsd::RelayTimers;

/// A transaction after parsing.
///
/// The type parameter indicates whether metrics were already extracted, which changes how
/// we count the transaction (total vs indexed).
#[derive(Debug)]
pub struct ExpandedTransaction<C = TotalAndIndexed> {
    pub headers: EnvelopeHeaders,
    pub event: Annotated<Event>,
    pub flags: Flags,
    pub attachments: Items,
    pub profile: Option<Item>,
    pub extracted_spans: Vec<Item>,
    #[expect(unused, reason = "marker field, only set never read")]
    pub category: C,
}

impl<T> ExpandedTransaction<T> {
    fn count_embedded_spans_and_self(&self) -> usize {
        1 + self
            .event
            .value()
            .and_then(|e| e.spans.value())
            .map_or(0, Vec::len)
    }
}

impl ExpandedTransaction<TotalAndIndexed> {
    pub fn into_indexed(self) -> ExpandedTransaction<Indexed> {
        let Self {
            headers,
            event,
            flags,
            attachments,
            profile,
            extracted_spans,
            category: _,
        } = self;
        ExpandedTransaction {
            headers,
            event,
            flags,
            attachments,
            profile,
            extracted_spans,
            category: Indexed,
        }
    }
}

impl Counted for ExpandedTransaction<TotalAndIndexed> {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            event: _,
            flags,
            attachments,
            profile,
            extracted_spans,
            category: _,
        } = self;
        let mut quantities = smallvec![
            (DataCategory::TransactionIndexed, 1),
            (DataCategory::Transaction, 1)
        ];

        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        let span_count = if flags.spans_extracted {
            extracted_spans.len()
        } else {
            debug_assert!(extracted_spans.is_empty());
            self.count_embedded_spans_and_self()
        };
        if span_count > 0 {
            quantities.extend([
                (DataCategory::SpanIndexed, span_count),
                (DataCategory::Span, span_count),
            ]);
        }

        quantities
    }
}

impl Counted for ExpandedTransaction<Indexed> {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            event: _,
            flags,
            attachments,
            profile,
            extracted_spans,
            category: _,
        } = self;
        let mut quantities = smallvec![(DataCategory::TransactionIndexed, 1),];

        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        let span_count = if flags.spans_extracted {
            extracted_spans.len()
        } else {
            debug_assert!(self.extracted_spans.is_empty());
            self.count_embedded_spans_and_self()
        };
        if span_count > 0 {
            quantities.extend([(DataCategory::SpanIndexed, span_count)]);
        }

        quantities
    }
}

impl RateLimited for Managed<Box<ExpandedTransaction<TotalAndIndexed>>> {
    type Output = Self;
    type Error = Error;

    async fn enforce<R>(
        mut self,
        mut rate_limiter: R,
        _ctx: Context<'_>,
    ) -> Result<Self::Output, Rejected<Self::Error>>
    where
        R: RateLimiter,
    {
        let scoping = self.scoping();

        // If there is a transaction limit, drop everything.
        // This also affects profiles that lost their transaction due to sampling.
        let limits = rate_limiter
            .try_consume(scoping.item(DataCategory::Transaction), 1)
            .await;
        if !limits.is_empty() {
            return Err(self.reject_err(Error::from(limits)));
        }

        // There is no limit on "total", but if metrics have already been extracted then
        // also check the "indexed" limit:
        if self.flags.metrics_extracted {
            let limits = rate_limiter
                .try_consume(scoping.item(DataCategory::TransactionIndexed), 1)
                .await;
            if !limits.is_empty() {
                let error = Error::from(limits);
                return Err(self.reject_err(error));
            }
        }

        let attachment_quantities = self.attachments.quantities();

        // Check profile limits:
        for (category, quantity) in self.profile.quantities() {
            let limits = rate_limiter
                .try_consume(scoping.item(category), quantity)
                .await;

            if !limits.is_empty() {
                self.modify(|this, record_keeper| {
                    record_keeper.reject_err(Error::from(limits), this.profile.take());
                });
            }
        }

        // Check attachment limits:
        for (category, quantity) in attachment_quantities {
            let limits = rate_limiter
                .try_consume(scoping.item(category), quantity)
                .await;

            if !limits.is_empty() {
                self.modify(|this, record_keeper| {
                    record_keeper
                        .reject_err(Error::from(limits), std::mem::take(&mut this.attachments));
                });
            }
        }

        // We assume that span extraction happens after metrics extraction, so safe to check both
        // categories:
        if !self.extracted_spans.is_empty() {
            for category in [DataCategory::Span, DataCategory::SpanIndexed] {
                let limits = rate_limiter
                    .try_consume(scoping.item(category), self.extracted_spans.len())
                    .await;

                if !limits.is_empty() {
                    self.modify(|this, record_keeper| {
                        record_keeper.reject_err(
                            Error::from(limits),
                            std::mem::take(&mut this.extracted_spans),
                        );
                    });
                }
            }
        }

        Ok(self)
    }
}

impl<T> ExpandedTransaction<T> {
    // TODO: should only exist or `TotalAndIndexed`, Indexed should go straight to kafka.
    pub fn serialize_envelope(self) -> Result<Box<Envelope>, serde_json::Error> {
        let mut items = Items::new();

        let span_count = self.count_embedded_spans_and_self() - 1;
        let ExpandedTransaction {
            headers,
            event,
            flags,
            attachments,
            profile,
            extracted_spans,
            category: _,
        } = self;

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

        Ok(Envelope::from_parts(headers, items))
    }
}
