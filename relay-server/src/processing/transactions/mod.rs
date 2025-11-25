#![expect(unused)]
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use relay_base_schema::events::EventType;
use relay_dynamic_config::{ErrorBoundary, Feature};
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::{Event, Metrics, SpanV2};
use relay_protocol::{Annotated, Empty, get_value};
use relay_quotas::{DataCategory, RateLimits};
#[cfg(feature = "processing")]
use relay_redis::AsyncRedisClient;
use relay_sampling::evaluation::{ReservoirCounters, ReservoirEvaluator, SamplingDecision};
use relay_statsd::metric;
use sentry::Data;
use smallvec::smallvec;

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::processing::spans::{Indexed, TotalAndIndexed};
use crate::processing::transactions::profile::{Profile, ProfileWithHeaders};
use crate::processing::utils::event::{
    EventFullyNormalized, EventMetricsExtracted, FiltersStatus, SpansExtracted, event_type,
};
use crate::processing::{
    Context, CountRateLimited, Forward, ForwardContext, Output, Processor, QuotaRateLimiter,
    RateLimited, RateLimiter, utils,
};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{ProcessingError, ProcessingExtractedMetrics};
#[cfg(feature = "processing")]
use crate::services::store::StoreEnvelope;
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::{SamplingResult, should_filter};

#[cfg(feature = "processing")]
use crate::managed::TypedEnvelope;
#[cfg(feature = "processing")]
use crate::services::processor::ProcessingGroup;

pub mod extraction;
mod process;
pub mod profile;
pub mod spans;

/// Errors that occur during transaction processing.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid JSON")]
    InvalidJson(#[from] serde_json::Error),
    #[error("envelope processor failed")]
    ProcessingFailed(#[from] ProcessingError),
    #[error("rate limited")]
    RateLimited(RateLimits),
}

impl From<RateLimits> for Error {
    fn from(value: RateLimits) -> Self {
        Self::RateLimited(value)
    }
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::InvalidJson(_) => Outcome::Invalid(DiscardReason::InvalidJson),
            Self::ProcessingFailed(e) => match e {
                ProcessingError::InvalidTransaction => {
                    Outcome::Invalid(DiscardReason::InvalidTransaction)
                }
                ProcessingError::EventFiltered(key) => Outcome::Filtered(key.clone()),
                _other => {
                    relay_log::error!(
                        error = &self as &dyn std::error::Error,
                        "internal error: trace metric processing failed"
                    );
                    Outcome::Invalid(DiscardReason::Internal)
                }
            },
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Outcome::RateLimited(reason_code)
            }
        };
        (Some(outcome), self)
    }
}

/// A processor for transactions.
pub struct TransactionProcessor {
    limiter: Arc<QuotaRateLimiter>,
    geoip_lookup: GeoIpLookup,
    #[cfg(feature = "processing")]
    quotas_client: Option<AsyncRedisClient>,
}

impl TransactionProcessor {
    /// Creates a new transaction processor.
    pub fn new(
        limiter: Arc<QuotaRateLimiter>,
        geoip_lookup: GeoIpLookup,
        #[cfg(feature = "processing")] quotas_client: Option<AsyncRedisClient>,
    ) -> Self {
        Self {
            limiter,
            geoip_lookup,
            #[cfg(feature = "processing")]
            quotas_client,
        }
    }
}

impl Processor for TransactionProcessor {
    type UnitOfWork = SerializedTransaction;
    type Output = TransactionOutput;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let headers = envelope.envelope().headers().clone();

        #[allow(unused_mut)]
        let mut event = envelope
            .envelope_mut()
            .take_item_by(|item| matches!(*item.ty(), ItemType::Transaction))?;

        // Count number of spans by shallow-parsing the event.
        // Needed for accounting but not in prod, because the event is immediately parsed afterwards.
        #[cfg(debug_assertions)]
        event.ensure_span_count();

        let attachments = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Attachment));

        let profile = envelope
            .envelope_mut()
            .take_item_by(|item| matches!(*item.ty(), ItemType::Profile));

        let work = SerializedTransaction {
            headers,
            event,
            attachments,
            profile,
        };

        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        work: Managed<Self::UnitOfWork>,
        mut ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let scoping = work.scoping();
        let project_id = work.scoping().project_id;
        let mut metrics = Metrics::default();

        relay_log::trace!("Expand transaction");
        let mut work = process::expand(work)?;

        relay_log::trace!("Prepare transaction data");
        process::prepare_data(&mut work, &mut ctx, &mut metrics)?;

        relay_log::trace!("Normalize transaction");
        let mut work = process::normalize(work, ctx, &self.geoip_lookup)?;

        relay_log::trace!("Filter transaction");
        let filters_status = process::run_inbound_filters(&work, ctx)?;

        #[cfg(feature = "processing")]
        let quotas_client = self.quotas_client.as_ref();
        #[cfg(not(feature = "processing"))]
        let quotas_client = None;
        relay_log::trace!("Sample transaction");
        let sampling_result =
            process::run_dynamic_sampling(&work, ctx, filters_status, quotas_client).await;

        #[cfg(feature = "processing")]
        let server_sample_rate = sampling_result.sample_rate();

        if let Some(outcome) = sampling_result.into_dropped_outcome() {
            relay_log::trace!("Process profile transaction");
            let work = process::process_profile(work, ctx, SamplingDecision::Drop);
            relay_log::trace!("Extract transaction metrics");
            let (work, extracted_metrics) =
                process::extract_metrics(work, ctx, SamplingDecision::Drop)?;

            let headers = work.headers.clone();
            let mut profile = process::drop_after_sampling(work, ctx, outcome);
            if let Some(profile) = profile.as_mut() {
                self.limiter.enforce_quotas(profile, ctx).await?;
            }

            return Ok(Output {
                main: profile.map(TransactionOutput::OnlyProfile),
                metrics: Some(extracted_metrics),
            });
        }

        // Need to scrub the transaction before extracting spans.
        relay_log::trace!("Scrubbing transaction");
        work = process::scrub(work, ctx)?;

        #[cfg(feature = "processing")]
        if ctx.config.processing_enabled() {
            // Process profiles before extracting metrics, to make sure they are removed if they are invalid.
            relay_log::trace!("Process transaction profile");
            let work = process::process_profile(work, ctx, SamplingDecision::Keep);

            relay_log::trace!("Extract transaction metrics");
            let (indexed, extracted_metrics) =
                process::extract_metrics(work, ctx, SamplingDecision::Keep)?;

            relay_log::trace!("Extract spans");
            let mut indexed = process::extract_spans(indexed, ctx, server_sample_rate);

            relay_log::trace!("Enforce quotas (processing)");
            self.limiter.enforce_quotas(&mut indexed, ctx).await?;

            if !indexed.flags.fully_normalized {
                relay_log::error!(
                    tags.project = %project_id,
                    tags.ty = event_type(&indexed.event).map(|e| e.to_string()).unwrap_or("none".to_owned()),
                    "ingested event without normalizing"
                );
            };

            relay_log::trace!("Done");
            return Ok(Output {
                main: Some(TransactionOutput::Indexed(indexed)),
                metrics: Some(extracted_metrics),
            });
        }

        relay_log::trace!("Enforce quotas");
        self.limiter.enforce_quotas(&mut work, ctx).await?;

        relay_log::trace!("Done");
        Ok(Output {
            main: Some(TransactionOutput::TotalAndIndexed(work)),
            metrics: None,
        })
    }
}

/// A transaction in its serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedTransaction {
    headers: EnvelopeHeaders,
    event: Item,
    attachments: Items,
    profile: Option<Item>,
}

impl Counted for SerializedTransaction {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            event,
            attachments,
            profile,
        } = self;
        debug_assert!(!event.spans_extracted());
        let mut quantities = event.quantities(); // counts spans based on `span_count` header.
        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        quantities
    }
}

/// A transaction after parsing.
///
/// The type parameter indicates whether metrics were already extracted, which changes how
/// we count the transaction (total vs indexed).
#[derive(Debug)]
pub struct ExpandedTransaction<C> {
    headers: EnvelopeHeaders,
    event: Annotated<Event>,
    flags: Flags,
    attachments: Items,
    profile: Option<Item>,
    extracted_spans: ExtractedSpans,

    #[expect(unused, reason = "marker field, only set never read")]
    category: C,
}

impl From<ExpandedTransaction<TotalAndIndexed>> for ExpandedTransaction<Indexed> {
    fn from(value: ExpandedTransaction<TotalAndIndexed>) -> Self {
        let ExpandedTransaction {
            headers,
            event,
            flags,
            attachments,
            profile,
            extracted_spans,
            category: _,
        } = value;
        Self {
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

impl<T> ExpandedTransaction<T> {
    fn serialize_envelope(self) -> Result<Box<Envelope>, serde_json::Error> {
        let span_count = self.count_embedded_spans_and_self() - 1;
        let Self {
            headers,
            event,
            flags:
                Flags {
                    metrics_extracted,
                    spans_extracted,
                    fully_normalized,
                },
            attachments,
            profile,
            extracted_spans,
            category: _,
        } = self;

        let mut items = smallvec![];

        items.extend(attachments);
        items.extend(profile);
        items.extend(extracted_spans.0);

        // To be compatible with previous code, add the transaction at the end:
        let data = metric!(timer(RelayTimers::EventProcessingSerialization), {
            event.to_json()?
        });
        let mut item = Item::new(ItemType::Transaction);
        item.set_payload(ContentType::Json, data);

        item.set_metrics_extracted(metrics_extracted);
        item.set_spans_extracted(spans_extracted);
        item.set_fully_normalized(fully_normalized);
        item.set_span_count(Some(span_count));

        items.push(item);

        Ok(Envelope::from_parts(headers, items))
    }
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

impl Counted for ExpandedTransaction<TotalAndIndexed> {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            event,
            flags,
            attachments,
            profile,
            extracted_spans,
            category: _,
        } = self;
        debug_assert!(!flags.metrics_extracted);
        let mut quantities = smallvec![
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
        ];

        // For now, span extraction happens after metrics extraction:
        debug_assert!(!flags.spans_extracted);
        debug_assert!(extracted_spans.0.is_empty());

        let span_count = self.count_embedded_spans_and_self();
        quantities.extend([
            (DataCategory::Span, span_count),
            (DataCategory::SpanIndexed, span_count),
        ]);

        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        quantities
    }
}

impl Counted for ExpandedTransaction<Indexed> {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            event,
            flags,
            attachments,
            profile,
            extracted_spans,
            category: _,
        } = self;
        debug_assert!(flags.metrics_extracted);
        let mut quantities = smallvec![(DataCategory::TransactionIndexed, 1)];
        if !flags.spans_extracted {
            // TODO: encode this flag into the type and remove `extracted_spans` from the "BeforeSpanExtraction" type.
            debug_assert!(extracted_spans.0.is_empty());
            let span_count = self.count_embedded_spans_and_self();
            quantities.push((DataCategory::SpanIndexed, span_count));
        }

        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        if !extracted_spans.0.is_empty() {
            debug_assert!(flags.spans_extracted);
            // For now, span extraction always happens at the very end:
            debug_assert!(flags.metrics_extracted);
            quantities.extend(extracted_spans.quantities());
        }

        quantities
    }
}

impl RateLimited for Managed<ExpandedTransaction<TotalAndIndexed>> {
    type Error = Error;

    async fn enforce<R>(
        &mut self,
        mut rate_limiter: R,
        ctx: Context<'_>,
    ) -> Result<(), Rejected<Self::Error>>
    where
        R: RateLimiter,
    {
        let scoping = self.scoping();

        // These debug assertions are here because this function does not check indexed or extracted
        // span limits.
        // TODO: encode flags into types instead.
        debug_assert!(!self.flags.metrics_extracted);
        debug_assert!(!self.flags.spans_extracted);

        let ExpandedTransaction {
            headers: _,
            event,
            flags,
            attachments,
            profile,
            extracted_spans,
            category: _,
        } = self.as_ref();

        // If there is a transaction limit, drop everything.
        // This also affects profiles that lost their transaction due to sampling.
        let limits = rate_limiter
            .try_consume(scoping.item(DataCategory::Transaction), 1)
            .await;

        if !limits.is_empty() {
            let error = Error::from(limits);
            return Err(self.reject_err(error));
        }

        // We do not check indexed quota at this point, because metrics have not been extracted
        // from the transaction yet.

        let attachment_quantities = attachments.quantities();

        // Check profile limits:
        for (category, quantity) in profile.quantities() {
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

        Ok(())
    }
}

impl RateLimited for Managed<ExpandedTransaction<Indexed>> {
    type Error = Error;

    async fn enforce<R>(
        &mut self,
        mut rate_limiter: R,
        ctx: Context<'_>,
    ) -> Result<(), Rejected<Self::Error>>
    where
        R: RateLimiter,
    {
        let scoping = self.scoping();

        // These debug assertions are here because this function does not check indexed or extracted
        // span limits.
        // TODO: encode flags into types instead.
        debug_assert!(self.flags.metrics_extracted);

        let ExpandedTransaction {
            headers: _,
            event,
            flags,
            attachments,
            profile,
            extracted_spans,
            category: _,
        } = self.as_ref();

        // If there is a transaction limit, drop everything.
        // This also affects profiles that lost their transaction due to sampling.
        let mut limits = rate_limiter
            .try_consume(scoping.item(DataCategory::Transaction), 0)
            .await; // TODO: test if consume with 0 works

        if limits.is_empty() {
            let indexed_limits = rate_limiter
                .try_consume(scoping.item(DataCategory::TransactionIndexed), 1)
                .await;
            limits.merge(indexed_limits);
        }

        if !limits.is_empty() {
            let error = Error::from(limits);
            return Err(self.reject_err(error));
        }

        let attachment_quantities = attachments.quantities();
        let span_quantities = extracted_spans.quantities();

        // Check profile limits:
        for (category, quantity) in profile.quantities() {
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

        // Check span limits:
        for (category, quantity) in span_quantities {
            let limits = rate_limiter
                .try_consume(scoping.item(category), quantity)
                .await;

            if !limits.is_empty() {
                self.modify(|this, record_keeper| {
                    record_keeper.reject_err(
                        Error::from(limits),
                        std::mem::take(&mut this.extracted_spans.0),
                    );
                });
            }
        }

        Ok(())
    }
}

/// Wrapper for spans extracted from a transaction.
///
/// Needed to not emit the total category for spans.
#[derive(Debug)]
struct ExtractedSpans(Vec<Item>);

impl Counted for ExtractedSpans {
    fn quantities(&self) -> Quantities {
        // For now, extracted spans are always extracted after metrics extraction. This might change
        // in the future.
        debug_assert!(
            self.0
                .iter()
                .all(|i| i.ty() == &ItemType::Span && i.metrics_extracted())
        );

        if self.0.is_empty() {
            return smallvec![];
        }

        smallvec![(DataCategory::SpanIndexed, self.0.len())]
    }
}

/// Flags extracted from transaction item headers.
///
/// Ideally `metrics_extracted` and `spans_extracted` will not be needed in the future. Unsure
/// about `fully_normalized`.
#[derive(Debug, Default)]
struct Flags {
    metrics_extracted: bool,
    spans_extracted: bool,
    fully_normalized: bool,
}

/// Output of the transaction processor.
#[derive(Debug)]
pub enum TransactionOutput {
    TotalAndIndexed(Managed<ExpandedTransaction<TotalAndIndexed>>),
    Indexed(Managed<ExpandedTransaction<Indexed>>),
    OnlyProfile(Managed<ProfileWithHeaders>),
}

impl TransactionOutput {
    #[cfg(test)]
    pub fn event(self) -> Option<Annotated<Event>> {
        match self {
            Self::TotalAndIndexed(managed) => Some(managed.accept(|x| x).event),
            Self::Indexed(managed) => Some(managed.accept(|x| x).event),
            Self::OnlyProfile(managed) => None,
        }
    }
}

impl Forward for TransactionOutput {
    fn serialize_envelope(
        self,
        ctx: ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        match self {
            TransactionOutput::TotalAndIndexed(managed) => managed.try_map(|output, _| {
                output
                    .serialize_envelope()
                    .map_err(drop)
                    .with_outcome(Outcome::Invalid(DiscardReason::Internal))
            }),
            TransactionOutput::Indexed(managed) => managed.try_map(|output, record_keeper| {
                // TODO(follow-up): `Counted` impl of `Box<Envelope>` is wrong.
                // But we will send structured data to the store soon instead of an envelope,
                // then this problem is circumvented.
                record_keeper.lenient(DataCategory::Transaction);
                record_keeper.lenient(DataCategory::Span);
                output
                    .serialize_envelope()
                    .map_err(drop)
                    .with_outcome(Outcome::Invalid(DiscardReason::Internal))
            }),
            TransactionOutput::OnlyProfile(profile) => {
                Ok(profile.map(|ProfileWithHeaders { headers, item }, _| {
                    Envelope::from_parts(headers, smallvec![item])
                }))
            }
        }
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
        ctx: ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let envelope: ManagedEnvelope = self.serialize_envelope(ctx)?.into();

        s.send(StoreEnvelope {
            envelope: envelope.into_processed(),
        });

        Ok(())
    }
}
