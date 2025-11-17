#![expect(unused)]
use std::collections::BTreeMap;
use std::sync::Arc;

use relay_base_schema::events::EventType;
use relay_dynamic_config::{ErrorBoundary, Feature};
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::{Event, Metrics, SpanV2};
use relay_protocol::{Annotated, Empty};
use relay_quotas::{DataCategory, RateLimits};
#[cfg(feature = "processing")]
use relay_redis::AsyncRedisClient;
use relay_sampling::evaluation::{ReservoirCounters, ReservoirEvaluator, SamplingDecision};
use relay_statsd::metric;
use smallvec::smallvec;

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::processing::transactions::profile::{Profile, ProfileWithHeaders};
use crate::processing::utils::event::{
    EventFullyNormalized, EventMetricsExtracted, FiltersStatus, SpansExtracted, event_type,
};
use crate::processing::{
    Context, Forward, ForwardContext, Output, Processor, QuotaRateLimiter, RateLimited,
    RateLimiter, utils,
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

        let transaction = envelope
            .envelope_mut()
            .take_item_by(|item| matches!(*item.ty(), ItemType::Transaction))?;

        let attachments = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Attachment));

        let profile = envelope
            .envelope_mut()
            .take_item_by(|item| matches!(*item.ty(), ItemType::Profile));

        let work = SerializedTransaction {
            headers,
            transaction,
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

        let mut work = process::parse(work)?;

        process::prepare_data(&mut work, &mut ctx, &mut metrics)?;

        let mut work = process::normalize(work, ctx, &self.geoip_lookup)?;

        let filters_status = process::run_inbound_filters(&work, ctx)?;

        #[cfg(feature = "processing")]
        let quotas_client = self.quotas_client.as_ref();
        #[cfg(not(feature = "processing"))]
        let quotas_client = None;
        let sampling_result =
            process::run_dynamic_sampling(&work, ctx, filters_status, quotas_client).await;

        #[cfg(feature = "processing")]
        let server_sample_rate = sampling_result.sample_rate();

        if let Some(outcome) = sampling_result.into_dropped_outcome() {
            let work = process::process_profile(work, ctx, SamplingDecision::Drop);
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
        work = process::scrub(work, ctx)?;

        #[cfg(feature = "processing")]
        if ctx.config.processing_enabled() {
            // Process profiles before extracting metrics, to make sure they are removed if they are invalid.
            let work = process::process_profile(work, ctx, SamplingDecision::Keep);

            let (indexed, extracted_metrics) =
                process::extract_metrics(work, ctx, SamplingDecision::Keep)?;

            let mut indexed = process::extract_spans(indexed, ctx, server_sample_rate);

            self.limiter.enforce_quotas(&mut indexed, ctx).await?;

            if !indexed.flags.fully_normalized {
                relay_log::error!(
                    tags.project = %project_id,
                    tags.ty = event_type(&indexed.transaction.0).map(|e| e.to_string()).unwrap_or("none".to_owned()),
                    "ingested event without normalizing"
                );
            };

            return Ok(Output {
                main: Some(TransactionOutput::Indexed(indexed)),
                metrics: Some(extracted_metrics),
            });
        }

        self.limiter.enforce_quotas(&mut work, ctx).await?;

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
    transaction: Item,
    attachments: Items,
    profile: Option<Item>,
}

impl Counted for SerializedTransaction {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            transaction,
            attachments,
            profile,
        } = self;
        let mut quantities = transaction.quantities();
        debug_assert!(!transaction.spans_extracted());
        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        let span_count = (transaction.span_count() + 1) as usize;
        quantities.extend([
            (DataCategory::Span, span_count),
            (DataCategory::SpanIndexed, span_count),
        ]);

        quantities
    }
}

/// A transaction after parsing.
///
/// The type parameter indicates whether metrics were already extracted, which changes how
/// we count the transaction (total vs indexed).
#[derive(Debug)]
pub struct ExpandedTransaction<T> {
    headers: EnvelopeHeaders,
    transaction: T, // might be empty
    flags: Flags,
    attachments: Items,
    profile: Option<Item>,
    extracted_spans: ExtractedSpans,
}

impl From<ExpandedTransaction<Transaction>> for ExpandedTransaction<IndexedTransaction> {
    fn from(value: ExpandedTransaction<Transaction>) -> Self {
        let ExpandedTransaction {
            headers,
            transaction,
            flags,
            attachments,
            profile,
            extracted_spans,
        } = value;
        Self {
            headers,
            transaction: IndexedTransaction::from(transaction),
            flags,
            attachments,
            profile,
            extracted_spans,
        }
    }
}

impl<T: Into<Annotated<Event>>> ExpandedTransaction<T> {
    fn serialize_envelope(self) -> Result<Box<Envelope>, serde_json::Error> {
        let Self {
            headers,
            transaction,
            flags:
                Flags {
                    metrics_extracted,
                    spans_extracted,
                    fully_normalized,
                },
            attachments,
            profile,
            extracted_spans,
        } = self;

        let event = transaction.into();

        let mut items = smallvec![];
        if !event.is_empty() {
            let data = metric!(timer(RelayTimers::EventProcessingSerialization), {
                event.to_json()?
            });
            let mut item = Item::new(ItemType::Transaction);
            item.set_payload(ContentType::Json, data);

            item.set_metrics_extracted(metrics_extracted);
            item.set_spans_extracted(spans_extracted);
            item.set_fully_normalized(fully_normalized);

            items.push(item);
        }
        items.extend(attachments);
        items.extend(profile);
        items.extend(extracted_spans.0);

        Ok(Envelope::from_parts(headers, items))
    }
}

impl<T: Counted + AsRef<Annotated<Event>>> Counted for ExpandedTransaction<T> {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            transaction,
            flags,
            attachments,
            profile,
            extracted_spans,
        } = self;

        let mut quantities = transaction.quantities();
        if !flags.spans_extracted {
            // TODO: encode this flag into the type and remove `extracted_spans` from the "BeforeSpanExtraction" type.
            debug_assert!(extracted_spans.0.is_empty());
            let span_count = 1 + transaction
                .as_ref()
                .value()
                .and_then(|e| e.spans.value())
                .map_or(0, Vec::len);
            quantities.push((DataCategory::SpanIndexed, span_count));
            // TODO: instead of looking at the flag, depend on `T`
            if !flags.metrics_extracted {
                quantities.push((DataCategory::Span, span_count));
            }
        }

        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        if !extracted_spans.0.is_empty() {
            // For now, span extraction always happens at the very end:
            debug_assert!(flags.metrics_extracted);
        }
        quantities.extend(extracted_spans.quantities());

        quantities
    }
}

impl<T: Counted + AsRef<Annotated<Event>>> RateLimited for Managed<ExpandedTransaction<T>> {
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

        let ExpandedTransaction {
            headers: _,
            transaction,
            flags,
            attachments,
            profile,
            extracted_spans,
        } = self.as_ref();

        // If there is a transaction limit, drop everything.
        // This also affects profiles that lost their transaction due to sampling.
        for (category, quantity) in transaction.quantities() {
            let limits = rate_limiter
                .try_consume(scoping.item(category), quantity)
                .await;

            if !limits.is_empty() {
                return Err(self.reject_err(Error::from(limits)));
            }
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

/// A wrapper for transactions that counts the total and indexed category.
#[derive(Debug)]
pub struct Transaction(Annotated<Event>);

impl AsRef<Annotated<Event>> for Transaction {
    fn as_ref(&self) -> &Annotated<Event> {
        &self.0
    }
}

impl AsMut<Annotated<Event>> for Transaction {
    fn as_mut(&mut self) -> &mut Annotated<Event> {
        &mut self.0
    }
}

impl From<Transaction> for Annotated<Event> {
    fn from(val: Transaction) -> Self {
        val.0
    }
}

impl Counted for Transaction {
    fn quantities(&self) -> Quantities {
        debug_assert!(
            self.0
                .value()
                .is_none_or(|event| event.ty.value() == Some(&EventType::Transaction))
        );
        smallvec![
            (DataCategory::TransactionIndexed, 1),
            (DataCategory::Transaction, 1),
        ]
    }
}

/// Same as [`Transaction`], but only reports the `TransactionIndexed` quantity.
///
/// After dynamic sampling & metrics extraction, the total category is owned by `ExtractedMetrics`.
#[derive(Debug)]
pub struct IndexedTransaction(Annotated<Event>);

impl AsRef<Annotated<Event>> for IndexedTransaction {
    fn as_ref(&self) -> &Annotated<Event> {
        &self.0
    }
}

impl AsMut<Annotated<Event>> for IndexedTransaction {
    fn as_mut(&mut self) -> &mut Annotated<Event> {
        &mut self.0
    }
}

impl From<IndexedTransaction> for Annotated<Event> {
    fn from(val: IndexedTransaction) -> Self {
        val.0
    }
}

impl From<Transaction> for IndexedTransaction {
    fn from(value: Transaction) -> Self {
        Self(value.0)
    }
}

impl Counted for IndexedTransaction {
    fn quantities(&self) -> Quantities {
        debug_assert!(
            self.0
                .value()
                .is_none_or(|event| event.ty.value() == Some(&EventType::Transaction))
        );
        smallvec![(DataCategory::TransactionIndexed, 1)]
    }
}

/// Output of the transaction processor.
#[derive(Debug)]
pub enum TransactionOutput {
    Full(Managed<ExpandedTransaction<Transaction>>),
    Indexed(Managed<ExpandedTransaction<IndexedTransaction>>),
    OnlyProfile(Managed<ProfileWithHeaders>),
}

impl TransactionOutput {
    #[cfg(test)]
    pub fn event(self) -> Option<Annotated<Event>> {
        match self {
            Self::Full(managed) => Some(managed.accept(|x| x).transaction.0),
            Self::Indexed(managed) => Some(managed.accept(|x| x).transaction.0),
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
            TransactionOutput::Full(managed) => managed.try_map(|output, _| {
                output
                    .serialize_envelope()
                    .map_err(drop)
                    .with_outcome(Outcome::Invalid(DiscardReason::Internal))
            }),
            TransactionOutput::Indexed(managed) => managed.try_map(|output, _| {
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
        // TODO: split out spans into a separate message.
        let envelope: ManagedEnvelope = self.serialize_envelope(ctx)?.into();

        s.send(StoreEnvelope {
            envelope: envelope.into_processed(),
        });

        Ok(())
    }
}
