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
use crate::processing::transactions::profile::Profile;
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

        let work = process::normalize(work, ctx, &self.geoip_lookup)?;

        let filters_status = process::run_inbound_filters(&work, ctx)?;

        let sampling_result =
            process::run_dynamic_sampling(&work, ctx, filters_status, &self.quotas_client).await;

        #[cfg(feature = "processing")]
        let server_sample_rate = sampling_result.sample_rate();

        if let Some(outcome) = sampling_result.into_dropped_outcome() {
            return process::drop_after_sampling(
                work,
                extracted_metrics,
                outcome,
                ctx,
                &self.limiter,
            )
            .await;
        }

        // Need to scrub the transaction before extracting spans.
        //
        // Unconditionally scrub to make sure PII is removed as early as possible.
        // 5 - SCRUB
        work.try_modify(|work, _| {
            utils::event::scrub(&mut work.transaction.0, ctx.project_info)?;
            utils::attachments::scrub(work.attachments.iter_mut(), ctx.project_info);
            Ok::<_, Error>(())
        })?;

        #[cfg(feature = "processing")]
        if ctx.config.processing_enabled() {
            // Process profiles before extracting metrics, to make sure they are removed if they are invalid.
            let mut profile_id = None;
            work.try_modify(|work, r| {
                use crate::processing::transactions::extraction::ExtractMetricsContext;

                // 6 - PROCESS PROFILE
                if let Some(profile) = work.profile.as_mut() {
                    profile.set_sampled(false);
                    let result = profile::process(
                        profile,
                        work.headers.meta().client_addr(),
                        work.transaction.0.value(),
                        &ctx,
                    );
                    match result {
                        Err(outcome) => {
                            r.reject_err(outcome, work.profile.take());
                        }
                        Ok(p) => profile_id = Some(p),
                    }
                }

                profile::transfer_id(&mut work.transaction.0, profile_id);
                profile::scrub_profiler_id(&mut work.transaction.0);

                // 7 - EXTRACT METRICS

                // Always extract metrics in processing Relays for sampled items.
                work.flags.metrics_extracted = extraction::extract_metrics(
                    &mut work.transaction.0,
                    &mut extracted_metrics,
                    ExtractMetricsContext {
                        dsc: work.headers.dsc(),
                        project_id,
                        ctx: &ctx,
                        sampling_decision: SamplingDecision::Keep,
                        metrics_extracted: work.flags.metrics_extracted,
                        spans_extracted: work.flags.spans_extracted,
                    },
                )?
                .0;

                // 8 - Extract spans
                if let Some(results) = spans::extract_from_event(
                    work.headers.dsc(),
                    &work.transaction.0,
                    ctx.global_config,
                    ctx.config,
                    server_sample_rate,
                    EventMetricsExtracted(work.flags.metrics_extracted),
                    SpansExtracted(work.flags.spans_extracted),
                ) {
                    work.flags.spans_extracted = true;
                    for result in results {
                        match result {
                            Ok(item) => work.extracted_spans.push(item),
                            Err(_) => r.reject_err(
                                Outcome::Invalid(DiscardReason::InvalidSpan),
                                SpanV2::default(),
                            ),
                        }
                    }
                }

                Ok::<_, Error>(())
            })?;
        }

        // 9 - RATE LIMIT
        self.limiter.enforce_quotas(&mut work, ctx).await?;

        if ctx.config.processing_enabled() && !work.flags.fully_normalized {
            relay_log::error!(
                tags.project = %project_id,
                tags.ty = event_type(&work.transaction.0).map(|e| e.to_string()).unwrap_or("none".to_owned()),
                "ingested event without normalizing"
            );
        };

        let metrics = work.wrap(extracted_metrics.into_inner());
        Ok(super::Output {
            main: Some(TransactionOutput(work)),
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
pub struct ExpandedTransaction {
    headers: EnvelopeHeaders,
    transaction: Transaction, // might be empty
    flags: Flags,
    attachments: smallvec::SmallVec<[Item; 3]>,
    profile: Option<Item>,
    extracted_spans: Vec<Item>,
}

impl ExpandedTransaction {
    fn serialize_envelope(self) -> Result<Box<Envelope>, serde_json::Error> {
        let Self {
            headers,
            transaction: Transaction(event),
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

impl Counted for ExpandedTransaction {
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

impl RateLimited for Managed<ExpandedTransaction> {
    type Error = Error;

    async fn enforce<T>(
        &mut self,
        mut rate_limiter: T,
        ctx: Context<'_>,
    ) -> Result<(), Rejected<Self::Error>>
    where
        T: super::RateLimiter,
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
struct Transaction(Annotated<Event>);

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

#[derive(Debug)]
pub struct TransactionOutput(Managed<ExpandedTransaction>);

impl Forward for TransactionOutput {
    fn serialize_envelope(
        self,
        ctx: super::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        self.0.try_map(|output, _| {
            output
                .serialize_envelope()
                .map_err(drop)
                .with_outcome(Outcome::Invalid(DiscardReason::Internal))
        })
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
        ctx: super::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let Self(output) = self;

        // TODO: split out envelope
        let envelope: ManagedEnvelope = output
            .try_map(|tx, _| {
                tx.serialize_envelope()
                    .map_err(|_| (Outcome::Invalid(DiscardReason::Internal), ()))
            })?
            .into();

        s.send(StoreEnvelope {
            envelope: envelope.into_processed(),
        });

        Ok(())
    }
}
