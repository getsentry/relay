use std::sync::Arc;

use relay_base_schema::events::EventType;
use relay_cogs::Token;
use relay_dynamic_config::{ErrorBoundary, Feature};
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::{Event, Metrics};
use relay_protocol::{Annotated, Empty};
use relay_quotas::{DataCategory, RateLimits};
#[cfg(feature = "processing")]
use relay_redis::AsyncRedisClient;
use relay_sampling::evaluation::{ReservoirCounters, ReservoirEvaluator, SamplingDecision};
use relay_statsd::metric;
use smallvec::{SmallVec, smallvec};

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::processing::transactions::profile::Profile;
use crate::processing::utils::attachments;
use crate::processing::utils::event::{
    EventFullyNormalized, EventMetricsExtracted, FiltersStatus, SpansExtracted, event_type,
};
use crate::processing::utils::transaction::ExtractMetricsContext;
use crate::processing::{Forward, Processor, QuotaRateLimiter, RateLimited, utils};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{ProcessingError, ProcessingExtractedMetrics};
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::{SamplingResult, should_filter};
#[cfg(feature = "processing")]
use crate::{processing::spans::store, services::store::StoreEnvelope};

mod process;
pub mod profile;
mod spans;

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
    #[cfg(feature = "processing")]
    reservoir_counters: ReservoirCounters,
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

        // The envelope might contain only a profile as a leftover after dynamic sampling.
        // In this case, `transaction` is `None`.
        let transaction = envelope
            .envelope_mut()
            .take_item_by(|item| matches!(*item.ty(), ItemType::Transaction));

        // Attachments are only allowed if a transaction exists.
        let attachments = match transaction {
            Some(_) => envelope
                .envelope_mut()
                .take_items_by(|item| matches!(*item.ty(), ItemType::Attachment)),
            None => smallvec::smallvec![], // no attachments allowed.
        };

        // A profile is only allowed if a transaction exists, or if it is marked as not sampled,
        // in which case it is a leftover from a transaction that was dropped by dynamic sampling.
        let profile = envelope.envelope_mut().take_item_by(|item| {
            matches!(*item.ty(), ItemType::Profile) && (transaction.is_some() || !item.sampled())
        });

        if transaction.is_none() && profile.is_none() && attachments.is_empty() {
            return None;
        }

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
        mut ctx: super::Context<'_>,
    ) -> Result<super::Output<Self::Output>, Rejected<Self::Error>> {
        let project_id = work.scoping().project_id;
        let mut event_fully_normalized = EventFullyNormalized(
            work.headers.meta().request_trust().is_trusted()
                && work
                    .transaction
                    .as_ref()
                    .map_or(false, Item::fully_normalized),
        );
        let mut event_metrics_extracted = EventMetricsExtracted(false);
        let mut spans_extracted = SpansExtracted(false);
        let mut metrics = Metrics::default();
        let mut extracted_metrics = ProcessingExtractedMetrics::new();

        let mut work = work.try_map(|w, _| {
            let SerializedTransaction {
                headers,
                transaction: transaction_item,
                attachments,
                profile,
            } = w;
            let mut transaction = Annotated::empty();
            if let Some(transaction_item) = transaction_item.as_ref() {
                transaction = metric!(timer(RelayTimers::EventProcessingDeserialize), {
                    Annotated::<Event>::from_json_bytes(&transaction_item.payload())
                })?;
                if let Some(event) = transaction.value_mut() {
                    event.ty = EventType::Transaction.into();
                }
            }
            Ok::<_, Error>(ExpandedTransaction {
                headers,
                transaction: Transaction(transaction),
                attachments,
                profile,
            })
        })?;

        let mut profile_id = None;
        let mut run_dynamic_sampling = false;
        work.try_modify(|work, record_keeper| {
            if let Some(profile_item) = work.profile.as_mut() {
                let feature = Feature::Profiling;
                if should_filter(ctx.config, ctx.project_info, feature) {
                    record_keeper.reject_err(
                        Outcome::Invalid(DiscardReason::FeatureDisabled(feature)),
                        work.profile.take(),
                    );
                } else if work.transaction.0.value().is_none() && profile_item.sampled() {
                    // A profile with `sampled=true` should never be without a transaction
                    record_keeper.reject_err(
                        Outcome::Invalid(DiscardReason::Profiling("missing_transaction")),
                        work.profile.take(),
                    );
                } else {
                    match relay_profiling::parse_metadata(&profile_item.payload(), project_id) {
                        Ok(id) => {
                            profile_id = Some(id);
                        }
                        Err(err) => {
                            record_keeper.reject_err(
                                Outcome::Invalid(DiscardReason::Profiling(
                                    relay_profiling::discard_reason(err),
                                )),
                                work.profile.take(),
                            );
                        }
                    }
                }
            }

            let event = &mut work.transaction.0;
            profile::transfer_id(event, profile_id);

            utils::dsc::validate_and_set_dsc(&mut work.headers, event, &mut ctx);

            utils::event::finalize(
                &work.headers,
                event,
                work.attachments.iter(),
                &mut metrics,
                &ctx.config,
            );

            event_fully_normalized = utils::event::normalize(
                &work.headers,
                event,
                event_fully_normalized,
                project_id,
                &ctx,
                &self.geoip_lookup,
            )?;

            let filter_run = utils::event::filter(&work.headers, event, &ctx)
                .map_err(|e| ProcessingError::EventFiltered(e))?;

            // Always run dynamic sampling on processing Relays,
            // but delay decision until inbound filters have been fully processed.
            // Also, we require transaction metrics to be enabled before sampling.
            run_dynamic_sampling = (matches!(filter_run, FiltersStatus::Ok)
                || ctx.config.processing_enabled())
                && matches!(&ctx.project_info.config.transaction_metrics, Some(ErrorBoundary::Ok(c)) if c.is_enabled());

            Ok::<_,Error>(())
        })?;

        let sampling_result = match run_dynamic_sampling {
            true => {
                #[allow(unused_mut)]
                let mut reservoir = ReservoirEvaluator::new(Arc::clone(&self.reservoir_counters));
                #[cfg(feature = "processing")]
                if let Some(quotas_client) = self.quotas_client.as_ref() {
                    reservoir.set_redis(work.scoping().organization_id, quotas_client);
                }
                utils::dynamic_sampling::run(
                    work.headers.dsc(),
                    &work.transaction.0,
                    &ctx,
                    Some(&reservoir),
                )
                .await
            }
            false => SamplingResult::Pending,
        };

        relay_statsd::metric!(
            counter(RelayCounters::SamplingDecision) += 1,
            decision = sampling_result.decision().as_str(),
            item = "transaction"
        );

        #[cfg(feature = "processing")]
        let server_sample_rate = sampling_result.sample_rate();

        if let Some(outcome) = sampling_result.into_dropped_outcome() {
            // Process profiles before dropping the transaction, if necessary.
            // Before metric extraction to make sure the profile count is reflected correctly.

            work.try_modify(|work, r| {
                if let Some(profile) = work.profile.as_mut() {
                    profile.set_sampled(false);
                    let result = profile::process(
                        profile,
                        work.headers.meta().client_addr(),
                        work.transaction.0.value(),
                        &ctx,
                    );
                    if let Err(outcome) = result {
                        r.reject_err(outcome, work.profile.take());
                    }
                }
                // Extract metrics here, we're about to drop the event/transaction.
                event_metrics_extracted = utils::transaction::extract_metrics(
                    &mut work.transaction.0,
                    &mut extracted_metrics,
                    ExtractMetricsContext {
                        dsc: work.headers.dsc(),
                        project_id,
                        ctx: &ctx,
                        sampling_decision: SamplingDecision::Drop,
                        event_metrics_extracted,
                        spans_extracted,
                    },
                )?;
                Ok::<_, Error>(())
            });

            // .map_err(Error::ProcessingFailed)
            // .reject(&work)?;

            let (work, profile) = work.split_once(|mut work| {
                let profile = work.profile.take();
                (work, profile)
            });

            // reject everything but the profile:
            // FIXME: track non-extracted spans as well.
            // -> Type TransactionWithEmbeddedSpans
            let _ = work.reject_err(outcome);

            // If we have a profile left, we need to make sure there is quota for this profile.
            let profile = profile.transpose();
            if let Some(profile) = profile {
                let mut profile = profile.map(|p, _| Profile(p));
                self.limiter.enforce_quotas(&mut profile, ctx).await?;
            }

            let metrics = work.wrap(extracted_metrics.into_inner());
            return Ok(super::Output {
                main: Some(TransactionOutput(work)),
                metrics: Some(metrics),
            });
        }

        // let _post_ds = cogs.start_category("post_ds"); // FIXME

        // Need to scrub the transaction before extracting spans.
        //
        // Unconditionally scrub to make sure PII is removed as early as possible.

        work.try_modify(|work, _| {
            utils::event::scrub(&mut work.transaction.0, ctx.project_info)?;
            utils::attachments::scrub(work.attachments.iter_mut(), ctx.project_info);
            Ok::<_, Error>(())
        });

        if cfg!(feature = "processing") && ctx.config.processing_enabled() {
            // Process profiles before extracting metrics, to make sure they are removed if they are invalid.
            let mut profile_id = None;
            work.try_modify(|work, r| {
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

                // Always extract metrics in processing Relays for sampled items.
                event_metrics_extracted = utils::transaction::extract_metrics(
                    &mut work.transaction.0,
                    &mut extracted_metrics,
                    ExtractMetricsContext {
                        dsc: work.headers.dsc(),
                        project_id,
                        ctx: &ctx,
                        sampling_decision: SamplingDecision::Drop,
                        event_metrics_extracted,
                        spans_extracted,
                    },
                )?;

                let mut span_items = vec![];

                spans_extracted = spans::extract_from_event(
                    work.headers.dsc(),
                    &work.transaction.0,
                    ctx.global_config,
                    ctx.config,
                    server_sample_rate,
                    event_metrics_extracted,
                    spans_extracted,
                    |item| span_items.push(item),
                    r,
                );

                Ok::<_, Error>(())
            });
        }

        self.limiter.enforce_quotas(&mut work, ctx).await?;

        // // Event may have been dropped because of a quota and the envelope can be empty.
        // if event.value().is_some() {
        //     event::serialize(
        //         managed_envelope,
        //         &mut event,
        //         event_fully_normalized,
        //         event_metrics_extracted,
        //         spans_extracted,
        //     )?;
        // }

        if ctx.config.processing_enabled() && !event_fully_normalized.0 {
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
    transaction: Option<Item>,
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
        transaction
            .into_iter()
            .chain(attachments.into_iter())
            .chain(profile.into_iter())
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
    attachments: smallvec::SmallVec<[Item; 3]>,
    profile: Option<Item>,
}

impl ExpandedTransaction {
    fn serialize_envelope(self) -> Result<Box<Envelope>, serde_json::Error> {
        let Self {
            headers,
            transaction: Transaction(event),
            attachments,
            profile,
        } = self;

        let mut items = smallvec![];
        if !event.is_empty() {
            let data = metric!(timer(RelayTimers::EventProcessingSerialization), {
                event.to_json()?
            });
            let mut item = Item::new(ItemType::Transaction);
            item.set_payload(ContentType::Json, data);

            // TODO: set flags

            items.push(item);
        }
        items.extend(attachments);
        items.extend(profile.into_iter());

        Ok(Envelope::from_parts(headers, items))
    }
}

impl Counted for ExpandedTransaction {
    fn quantities(&self) -> Quantities {
        let mut quantities = Quantities::new();
        let Self {
            headers: _,
            transaction,
            attachments,
            profile,
        } = self;

        quantities.extend(transaction.quantities());
        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        quantities
    }
}

impl RateLimited for Managed<ExpandedTransaction> {
    type Error = Error;

    async fn enforce<T>(
        &mut self,
        mut rate_limiter: T,
        ctx: super::Context<'_>,
    ) -> Result<(), Rejected<Self::Error>>
    where
        T: super::RateLimiter,
    {
        let scoping = self.scoping();

        let ExpandedTransaction {
            headers: _,
            transaction,
            attachments,
            profile,
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

        // TODO: check extracted span limits

        Ok(())
    }
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

// pub struct CountSpans<'a>(&'a Event);

// impl Counted for CountSpans<'_> {
//     fn quantities(&self) -> Quantities {
//         let mut quantities = self.0.quantities();
//         quantities.extend(self.0.spans.quantities());
//         quantities
//     }
// }

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

        let (envelope, spans) = output.split_once(
            |ExpandedTransaction {
                 headers,
                 transaction,
                 attachments,
                 profile,
                 //  extracted_spans,
             }| {
                let mut envelope = Envelope::try_from_event(headers, transaction)
                    .map_err(|e| output.internal_error("failed to create envelope from event"))?;
                for item in attachments {
                    envelope.add_item(item);
                }

                (envelope, extracted_spans)
            },
        );

        // Send transaction & attachments in envelope:
        s.send(StoreEnvelope { envelope });

        // Send spans:
        let ctx = store::Context {
            server_sample_rate: spans.server_sample_rate,
            retention: ctx.retention(|r| r.span.as_ref()),
        };
        for span in spans.split(|v| v) {
            if let Ok(span) = span.try_map(|span, _| store::convert(span, &ctx)) {
                s.send(span);
            };
        }

        Ok(())
    }
}
