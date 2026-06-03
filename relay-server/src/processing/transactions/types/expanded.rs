use either::Either;
use relay_event_schema::protocol::{Event, SpanV2};
use relay_profiling::{ProfileMetadata, ProfileType};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use relay_sampling::evaluation::SamplingDecision;
use relay_statsd::metric;
use smallvec::smallvec;

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Managed, Quantities, Rejected};
use crate::metrics_extraction::ExtractedMetrics;
use crate::processing::spans::{Indexed, TotalAndIndexed};
use crate::processing::transactions::Error;
use crate::processing::transactions::process::split_indexed_and_total;
use crate::processing::{Context, CountRateLimited, RateLimited, RateLimiter};
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
    pub spans_rate_limited: bool,
}

/// A transaction after parsing.
///
/// The type parameter indicates whether metrics were already extracted, which changes how
/// we count the transaction (total vs indexed).
#[derive(Debug)]
pub struct ExpandedTransaction<C = TotalAndIndexed> {
    pub headers: EnvelopeHeaders,
    pub event: Annotated<Event>,
    pub flags: Flags,
    pub attachments: Vec<Item>,
    pub profile: Option<ExpandedProfile>,
    #[expect(unused, reason = "marker field, only set never read")]
    pub category: C,
}

impl<T> ExpandedTransaction<T> {
    pub fn count_embedded_spans_and_self(&self) -> usize {
        1 + self
            .event
            .value()
            .and_then(|e| e.spans.value())
            .map_or(0, Vec::len)
    }
}

impl ExpandedTransaction<TotalAndIndexed> {
    /// Change the marker type of this transaction.
    ///
    /// Once converted, the payload will not count toward indexed categories.
    pub fn into_indexed(self) -> ExpandedTransaction<Indexed> {
        let Self {
            headers,
            event,
            flags,
            attachments,
            profile,
            category: _,
        } = self;
        ExpandedTransaction {
            headers,
            event,
            flags,
            attachments,
            profile,
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
            category: _,
        } = self;
        let mut quantities = smallvec![
            (DataCategory::TransactionIndexed, 1),
            (DataCategory::Transaction, 1)
        ];

        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        if !flags.spans_extracted {
            let span_count = self.count_embedded_spans_and_self();
            quantities.extend([
                (DataCategory::SpanIndexed, span_count),
                (DataCategory::Span, span_count),
            ]);
        };

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
            category: _,
        } = self;
        let mut quantities = smallvec![(DataCategory::TransactionIndexed, 1),];

        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        if !flags.spans_extracted {
            let span_count = self.count_embedded_spans_and_self();
            quantities.extend([(DataCategory::SpanIndexed, span_count)]);
        };

        quantities
    }
}

impl RateLimited for Managed<Box<ExpandedTransaction<TotalAndIndexed>>> {
    type Output = Managed<Either<Box<ExpandedTransaction<TotalAndIndexed>>, ExtractedMetrics>>;
    type Error = Error;

    async fn enforce<R>(
        mut self,
        mut rate_limiter: R,
        ctx: Context<'_>,
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
        let limits = rate_limiter
            .try_consume(scoping.item(DataCategory::TransactionIndexed), 1)
            .await;
        if !limits.is_empty() {
            let error = Error::from(limits);
            let (indexed, metrics) = split_indexed_and_total(self, ctx, SamplingDecision::Keep);
            let _ = indexed.reject_err(error);

            return Ok(metrics.map(|metrics, _| Either::Right(metrics)));
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
                break;
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
                break;
            }
        }

        Ok(self.map(|work, _| Either::Left(work)))
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
            category: _,
        } = self;

        items.extend(attachments);
        if let Some(profile) = profile {
            items.push(profile.serialize_item());
        }

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
            spans_rate_limited: _,
        } = flags;
        item.set_metrics_extracted(metrics_extracted);
        item.set_spans_extracted(spans_extracted);
        item.set_fully_normalized(fully_normalized);

        item.set_span_count(Some(span_count));

        items.push(item);

        Ok(Envelope::from_parts(headers, items))
    }
}

/// A profile after extracting metadata.
#[derive(Debug)]
pub struct ExpandedProfile {
    /// Parsed metadata from the [`Self::item`].
    pub meta: ProfileMetadata,
    /// The raw transaction profile received in an envelope.
    pub item: Item,
}

impl ExpandedProfile {
    pub fn serialize_item(mut self) -> Item {
        self.item.set_platform(self.meta.platform);
        self.item
    }
}

impl Counted for ExpandedProfile {
    fn quantities(&self) -> Quantities {
        smallvec![
            (DataCategory::Profile, 1),
            (DataCategory::ProfileIndexed, 1),
            (
                match self.meta.profile_type() {
                    ProfileType::Backend => DataCategory::ProfileBackend,
                    ProfileType::Ui => DataCategory::ProfileUi,
                },
                1
            ),
        ]
    }
}

/// Spans which have been extracted from a [`ExpandedTransaction`].
#[derive(Debug)]
pub struct ExtractedSpans(pub Vec<Annotated<SpanV2>>);

impl ExtractedSpans {
    pub fn into_indexed(self) -> ExtractedIndexedSpans {
        ExtractedIndexedSpans(self.0)
    }
}

impl Counted for ExtractedSpans {
    fn quantities(&self) -> Quantities {
        smallvec![
            (DataCategory::Span, self.0.len()),
            (DataCategory::SpanIndexed, self.0.len()),
        ]
    }
}

impl CountRateLimited for Managed<ExtractedSpans> {
    type Error = Error;
}

/// [`ExtractedSpans`] which had their metrics extracted.
#[derive(Debug)]
pub struct ExtractedIndexedSpans(pub Vec<Annotated<SpanV2>>);

impl ExtractedIndexedSpans {
    #[cfg(feature = "processing")]
    pub fn into_iter(self) -> impl IntoIterator<Item = ExtractedIndexedSpan> {
        self.0.into_iter().map(ExtractedIndexedSpan)
    }
}

impl Counted for ExtractedIndexedSpans {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::SpanIndexed, self.0.len())]
    }
}

/// A single extracted span which had its metrics extracted.
#[derive(Debug)]
#[cfg(feature = "processing")]
pub struct ExtractedIndexedSpan(pub Annotated<SpanV2>);

#[cfg(feature = "processing")]
impl Counted for ExtractedIndexedSpan {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::SpanIndexed, 1)]
    }
}
