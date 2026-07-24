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
use crate::processing::transactions::Error;
use crate::processing::transactions::process::{get_metrics_config, split_indexed_and_total};
use crate::processing::utils::types::{Indexed, TotalAndIndexed, TotalCategory};
use crate::processing::{Context, CountRateLimited, RateLimited, RateLimiter};
use crate::statsd::RelayTimers;

/// Flags extracted from transaction item headers.
///
/// Unsure whether `fully_normalized` will be needed in the future.
#[derive(Debug, Default)]
pub struct Flags {
    pub fully_normalized: bool,
}

/// Whether spans embedded in a transaction have been extracted into standalone spans.
///
/// Payloads start out as [`SpansEmbedded`] and transition to [`SpansExtracted`] during
/// span extraction, transferring ownership of the span categories to the extracted spans.
pub trait SpanExtraction {
    /// Whether the embedded spans have been extracted.
    const EXTRACTED: bool;
}

/// Spans are still embedded in the transaction and count towards the span categories.
#[derive(Copy, Clone, Debug)]
pub struct SpansEmbedded;

impl SpanExtraction for SpansEmbedded {
    const EXTRACTED: bool = false;
}

/// Spans have been extracted from the transaction and no longer count towards it.
#[derive(Copy, Clone, Debug)]
pub struct SpansExtracted;

impl SpanExtraction for SpansExtracted {
    const EXTRACTED: bool = true;
}

/// A transaction after parsing.
///
/// The `C` type parameter indicates whether metrics were already extracted, which changes how
/// we count the transaction (total vs indexed). The `S` type parameter indicates whether the
/// embedded spans were already extracted, which changes whether they count towards the
/// transaction.
#[derive(Debug)]
pub struct ExpandedTransaction<C = TotalAndIndexed, S = SpansEmbedded> {
    pub headers: EnvelopeHeaders,
    pub event: Annotated<Event>,
    pub flags: Flags,
    pub attachments: Vec<Item>,
    pub profile: Option<ExpandedProfile>,
    pub category: C,
    pub span_extraction: S,
}

impl<C, S> ExpandedTransaction<C, S> {
    pub fn count_embedded_spans_and_self(&self) -> usize {
        1 + self
            .event
            .value()
            .and_then(|e| e.spans.value())
            .map_or(0, Vec::len)
    }
}

impl<S> ExpandedTransaction<TotalAndIndexed, S> {
    /// Change the marker type of this transaction.
    ///
    /// Once converted, the payload will only count toward indexed categories.
    pub fn into_indexed(self) -> ExpandedTransaction<Indexed, S> {
        let Self {
            headers,
            event,
            flags,
            attachments,
            profile,
            category: _,
            span_extraction,
        } = self;
        ExpandedTransaction {
            headers,
            event,
            flags,
            attachments,
            profile,
            category: Indexed,
            span_extraction,
        }
    }
}

impl<C> ExpandedTransaction<C, SpansEmbedded> {
    /// Change the marker type of this transaction.
    ///
    /// Once converted, the embedded spans no longer count towards the transaction,
    /// ownership of the span categories was transferred to the extracted spans.
    pub fn into_spans_extracted(self) -> ExpandedTransaction<C, SpansExtracted> {
        let Self {
            headers,
            event,
            flags,
            attachments,
            profile,
            category,
            span_extraction: _,
        } = self;
        ExpandedTransaction {
            headers,
            event,
            flags,
            attachments,
            profile,
            category,
            span_extraction: SpansExtracted,
        }
    }
}

impl<C: TotalCategory, S: SpanExtraction> Counted for ExpandedTransaction<C, S> {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            event: _,
            flags: _,
            attachments,
            profile,
            category: _,
            span_extraction: _,
        } = self;
        let mut quantities = smallvec![(DataCategory::TransactionIndexed, 1)];
        if C::HAS_TOTAL {
            quantities.push((DataCategory::Transaction, 1));
        }

        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        if !S::EXTRACTED {
            let span_count = self.count_embedded_spans_and_self();
            quantities.push((DataCategory::SpanIndexed, span_count));
            if C::HAS_TOTAL {
                quantities.push((DataCategory::Span, span_count));
            }
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
            let (indexed, metrics) =
                split_indexed_and_total(self, ctx, SamplingDecision::Keep, get_metrics_config(ctx));
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

impl ExpandedTransaction<TotalAndIndexed, SpansEmbedded> {
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
            span_extraction: _,
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

        let Flags { fully_normalized } = flags;
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
