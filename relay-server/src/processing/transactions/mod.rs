#![expect(unused)]
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use either::Either;
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
use smallvec::{SmallVec, smallvec};

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType, Items};
#[cfg(feature = "processing")]
use crate::managed::TypedEnvelope;
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::metrics_extraction::transactions::ExtractedMetrics;
#[cfg(feature = "processing")]
use crate::processing::StoreHandle;
use crate::processing::spans::{Indexed, TotalAndIndexed};
use crate::processing::transactions::process::SamplingOutput;
use crate::processing::transactions::profile::{Profile, ProfileWithHeaders};
use crate::processing::transactions::types::{Flags, SampledTransaction};
use crate::processing::utils::event::{
    EventFullyNormalized, EventMetricsExtracted, FiltersStatus, SpansExtracted, event_type,
};
use crate::processing::{
    Context, CountRateLimited, Forward, ForwardContext, Output, Processor, QuotaRateLimiter,
    RateLimited, RateLimiter, utils,
};
use crate::services::outcome::{DiscardReason, Outcome};
#[cfg(feature = "processing")]
use crate::services::processor::ProcessingGroup;
use crate::services::processor::{ProcessingError, ProcessingExtractedMetrics};
#[cfg(feature = "processing")]
use crate::services::store::StoreEnvelope;
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::{SamplingResult, should_filter};

pub mod extraction;
mod process;
pub mod profile;
pub mod spans;
mod types;

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
                        "internal error: transaction processing failed"
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

        let profiles = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Profile));

        let work = SerializedTransaction {
            headers,
            event,
            attachments,
            profiles,
        };

        Some(Managed::with_meta_from(envelope, work))
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
        let (work, _server_sample_rate) =
            match process::run_dynamic_sampling(work, ctx, filters_status, quotas_client).await? {
                SamplingOutput::Keep {
                    payload,
                    sample_rate,
                } => (payload, sample_rate),
                SamplingOutput::Drop { metrics, profile } => {
                    return Ok(Output {
                        main: profile.map(TransactionOutput::Profile),
                        metrics: Some(metrics),
                    });
                }
            };

        #[cfg(feature = "processing")]
        let server_sample_rate = _server_sample_rate;

        relay_log::trace!("Processing profiles");
        let work = process::process_profile(work, ctx);

        relay_log::trace!("Enforce quotas");
        let work = self.limiter.enforce_quotas(work, ctx).await?;

        // Need to scrub the transaction before extracting spans.
        relay_log::trace!("Scrubbing transaction");
        let mut work = process::scrub(work, ctx)?;

        #[cfg(feature = "processing")]
        if ctx.config.processing_enabled() {
            use crate::processing::transactions::process::split_indexed_and_total;

            if !work.flags.fully_normalized {
                relay_log::error!(
                    tags.project = %project_id,
                    tags.ty = event_type(&work.event).map(|e| e.to_string()).unwrap_or("none".to_owned()),
                    "ingested event without normalizing"
                );
            };

            relay_log::trace!("Extract spans");
            work = process::extract_spans(work, ctx, server_sample_rate);

            let (indexed, metrics) = split_indexed_and_total(work, ctx, SamplingDecision::Keep)?;
            return Ok(Output {
                main: Some(TransactionOutput::Indexed(indexed)),
                metrics: Some(metrics),
            });
        }

        Ok(Output {
            main: Some(TransactionOutput::Full(work)),
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
    profiles: SmallVec<[Item; 3]>,
}

impl Counted for SerializedTransaction {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            event,
            attachments,
            profiles,
        } = self;
        debug_assert!(!event.spans_extracted());
        let mut quantities = event.quantities(); // counts spans based on `span_count` header.
        quantities.extend(attachments.quantities());
        quantities.extend(profiles.quantities());

        quantities
    }
}

/// A transaction after parsing.
///
/// The type parameter indicates whether metrics were already extracted, which changes how
/// we count the transaction (total vs indexed).
#[derive(Debug)]
pub struct ExpandedTransaction<C = TotalAndIndexed> {
    headers: EnvelopeHeaders,
    event: Annotated<Event>,
    flags: Flags,
    attachments: Items,
    profile: Option<Item>,
    extracted_spans: Vec<Item>,
    #[expect(unused, reason = "marker field, only set never read")]
    category: C,
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
    fn into_indexed(self) -> ExpandedTransaction<Indexed> {
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
            event,
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
            self.extracted_spans.len()
        } else {
            debug_assert!(self.extracted_spans.is_empty());
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
            event,
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
            self.extracted_spans.len()
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

/// Wrapper for spans extracted from a transaction.
///
/// Needed to not emit the total category for spans.
#[derive(Debug)]
struct ExtractedSpans(Vec<Item>);

impl Counted for ExtractedSpans {
    fn quantities(&self) -> Quantities {
        if self.0.is_empty() {
            return smallvec![];
        }

        smallvec![(DataCategory::SpanIndexed, self.0.len())]
    }
}

/// Output of the transaction processor.
#[derive(Debug)]
pub enum TransactionOutput {
    Full(Managed<Box<ExpandedTransaction>>),
    Profile(Managed<Item>),
    Indexed(Managed<Box<ExpandedTransaction<Indexed>>>),
}

impl TransactionOutput {
    #[cfg(test)]
    pub fn event(self) -> Option<Annotated<Event>> {
        match self {
            TransactionOutput::Full(managed) => Some(managed.accept(|x| x).event),
            TransactionOutput::Profile(managed) => None,
            TransactionOutput::Indexed(managed) => Some(managed.accept(|x| x).event),
        }
    }
}

impl Forward for TransactionOutput {
    fn serialize_envelope(
        self,
        ctx: ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        match self {
            TransactionOutput::Full(managed) => managed.try_map(|work, record_keeper| {
                work.serialize_envelope()
                    .map_err(drop)
                    .with_outcome(Outcome::Invalid(DiscardReason::Internal))
            }),
            TransactionOutput::Profile(managed) => todo!(),
            TransactionOutput::Indexed(managed) => todo!(),
        }
        // self.try_map(|work, record_keeper| {
        //     let output = work
        //         .serialize_envelope()
        //         .map_err(drop)
        //         .with_outcome(Outcome::Invalid(DiscardReason::Internal));
        //     record_keeper.lenient(DataCategory::Transaction); // TODO
        //     record_keeper.lenient(DataCategory::Span); // TODO
        //     output
        // })
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: StoreHandle<'_>,
        ctx: ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let envelope: ManagedEnvelope = self.serialize_envelope(ctx)?.into();

        s.store(StoreEnvelope {
            envelope: envelope.into_processed(),
        });

        Ok(())
    }
}
