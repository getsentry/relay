#![expect(unused)]
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
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::processing::transactions::profile::{Profile, ProfileWithHeaders};
use crate::processing::utils::event::{
    EventFullyNormalized, EventMetricsExtracted, FiltersStatus, SpansExtracted, event_type,
};
use crate::processing::{Forward, Processor, QuotaRateLimiter, RateLimited, utils};
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

use super::Context;

pub mod extraction;
mod process;
pub mod profile;
pub mod spans;

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
    ) -> Result<super::Output<Self::Output>, Rejected<Self::Error>> {
        let scoping = work.scoping();
        let project_id = work.scoping().project_id;
        let mut metrics = Metrics::default();
        let mut extracted_metrics = ProcessingExtractedMetrics::new();

        let mut work = process::parse(work)?;

        process::prepare_data(&mut work, &mut ctx, &mut metrics)?;

        let mut work = process::normalize(work, ctx, &self.geoip_lookup)?;

        let filters_status = process::run_inbound_filters(&work, ctx)?;

        let sampling_result =
            process::run_dynamic_sampling(&work, ctx, filters_status, &self.quotas_client).await;

        #[cfg(feature = "processing")]
        let server_sample_rate = sampling_result.sample_rate();

        if let Some(outcome) = sampling_result.into_dropped_outcome() {
            let work = process::process_profile(work, ctx, SamplingDecision::Drop);
            let work = process::extract_metrics(
                work,
                ctx,
                SamplingDecision::Drop,
                &mut extracted_metrics,
            )?;

            let headers = work.headers.clone();
            let profile = process::drop_after_sampling(work, ctx, outcome);
            let metrics = profile.wrap(extracted_metrics.into_inner());
            let mut profile = profile.transpose();
            if let Some(profile) = profile.as_mut() {
                self.limiter.enforce_quotas(profile, ctx).await?;
            }

            return Ok(super::Output {
                main: profile.map(|managed| {
                    TransactionOutput::OnlyProfile(
                        managed.map(|Profile(item), _| ProfileWithHeaders { headers, item }),
                    )
                }),
                metrics: Some(metrics),
            });
        }

        // Need to scrub the transaction before extracting spans.
        work = process::scrub(work, ctx)?;

        #[cfg(feature = "processing")]
        if ctx.config.processing_enabled() {
            // Process profiles before extracting metrics, to make sure they are removed if they are invalid.
            let work = process::process_profile(work, ctx, SamplingDecision::Keep);

            let indexed = process::extract_metrics(
                work,
                ctx,
                SamplingDecision::Keep,
                &mut extracted_metrics,
            )?;

            let mut indexed = process::extract_spans(indexed, ctx, server_sample_rate);

            self.limiter.enforce_quotas(&mut indexed, ctx).await?;

            if !indexed.flags.fully_normalized {
                relay_log::error!(
                    tags.project = %project_id,
                    tags.ty = event_type(&indexed.transaction.0).map(|e| e.to_string()).unwrap_or("none".to_owned()),
                    "ingested event without normalizing"
                );
            };

            let metrics = indexed.wrap(extracted_metrics.into_inner());
            return Ok(super::Output {
                main: Some(TransactionOutput::Indexed(indexed)),
                metrics: Some(metrics),
            });
        }

        self.limiter.enforce_quotas(&mut work, ctx).await?;

        let metrics = work.wrap(extracted_metrics.into_inner());
        Ok(super::Output {
            main: Some(TransactionOutput::Full(work)),
            metrics: Some(metrics),
        })
    }
}

/// A transaction in its serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedTransaction {
    headers: EnvelopeHeaders,
    transaction: Item,
    attachments: smallvec::SmallVec<[Item; 3]>,
    profile: Option<Item>,
}

impl SerializedTransaction {
    fn items(&self) -> impl Iterator<Item = &Item> {
        let Self {
            headers: _,
            transaction,
            attachments,
            profile,
        } = self;
        std::iter::once(transaction)
            .chain(attachments)
            .chain(profile.iter())
    }
}

impl Counted for SerializedTransaction {
    fn quantities(&self) -> Quantities {
        let mut quantities = Quantities::new();
        // IDEA: `#[derive(Counted)]`
        for item in self.items() {
            // NOTE: This assumes non-overlapping item quantities.
            quantities.extend(item.quantities());
        }
        quantities
    }
}

#[derive(Debug)]
pub struct ExpandedTransaction<T> {
    headers: EnvelopeHeaders,
    transaction: T, // might be empty
    flags: Flags,
    attachments: smallvec::SmallVec<[Item; 3]>,
    profile: Option<Item>,
    extracted_spans: Vec<Item>,
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
        items.extend(extracted_spans);

        Ok(Envelope::from_parts(headers, items))
    }
}

impl<T: Counted> Counted for ExpandedTransaction<T> {
    fn quantities(&self) -> Quantities {
        let mut quantities = Quantities::new();
        let Self {
            headers: _,
            transaction,
            flags: _, // TODO: might be used to conditionally count embedded spans.
            attachments,
            profile,
            extracted_spans,
        } = self;

        quantities.extend(transaction.quantities());
        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());
        quantities.extend(extracted_spans.quantities());

        quantities
    }
}

impl<T: Counted> RateLimited for Managed<ExpandedTransaction<T>> {
    type Error = Error;

    async fn enforce<R>(
        &mut self,
        mut rate_limiter: R,
        ctx: Context<'_>,
    ) -> Result<(), Rejected<Self::Error>>
    where
        R: super::RateLimiter,
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

        // Check attachment limits:
        for (category, quantity) in span_quantities {
            let limits = rate_limiter
                .try_consume(scoping.item(category), quantity)
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

        Ok(())
    }
}

#[derive(Debug, Default)]
struct Flags {
    metrics_extracted: bool,
    spans_extracted: bool,
    fully_normalized: bool,
}

#[derive(Debug)]
pub struct Transaction(Annotated<Event>);

impl AsMut<Annotated<Event>> for Transaction {
    fn as_mut(&mut self) -> &mut Annotated<Event> {
        &mut self.0
    }
}

impl Into<Annotated<Event>> for Transaction {
    fn into(self) -> Annotated<Event> {
        self.0
    }
}

impl Counted for Transaction {
    fn quantities(&self) -> Quantities {
        match self.0.value() {
            Some(_) => smallvec![
                (DataCategory::TransactionIndexed, 1),
                (DataCategory::Transaction, 1),
            ],
            None => smallvec![],
        }
    }
}

/// Same as [`Transaction`], but only reports the `TransactionIndexed` quantity.
///
/// After dynamic sampling & metrics extraction, the total category is owned by `ExtractedMetrics`.
#[derive(Debug)]
pub struct IndexedTransaction(Annotated<Event>);

impl AsMut<Annotated<Event>> for IndexedTransaction {
    fn as_mut(&mut self) -> &mut Annotated<Event> {
        &mut self.0
    }
}

impl Into<Annotated<Event>> for IndexedTransaction {
    fn into(self) -> Annotated<Event> {
        self.0
    }
}

impl From<Transaction> for IndexedTransaction {
    fn from(value: Transaction) -> Self {
        Self(value.0)
    }
}

impl Counted for IndexedTransaction {
    fn quantities(&self) -> Quantities {
        match self.0.value() {
            Some(_) => smallvec![(DataCategory::TransactionIndexed, 1),],
            None => smallvec![],
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum TransactionOutput {
    Full(Managed<ExpandedTransaction<Transaction>>),
    Indexed(Managed<ExpandedTransaction<IndexedTransaction>>),
    OnlyProfile(Managed<ProfileWithHeaders>),
}

impl Forward for TransactionOutput {
    fn serialize_envelope(
        self,
        ctx: super::ForwardContext<'_>,
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
        ctx: super::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        // TODO: split out spans
        let envelope: ManagedEnvelope = self.serialize_envelope(ctx)?.into();

        s.send(StoreEnvelope {
            envelope: envelope.into_processed(),
        });

        Ok(())
    }
}
