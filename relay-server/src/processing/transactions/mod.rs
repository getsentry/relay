use std::sync::Arc;

use relay_base_schema::events::EventType;
use relay_dynamic_config::{ErrorBoundary, Feature};
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::{Event, Metrics, SpanV2};
use relay_filter::FilterStatKey;
use relay_protocol::Annotated;
use relay_quotas::{DataCategory, RateLimits};
#[cfg(feature = "processing")]
use relay_redis::AsyncRedisClient;
use relay_sampling::evaluation::{ReservoirCounters, ReservoirEvaluator, SamplingDecision};
use relay_statsd::metric;
use smallvec::SmallVec;

use crate::Envelope;
use crate::envelope::ContainerItems;
use crate::envelope::{ContainerWriteError, EnvelopeHeaders, Item, ItemContainer, ItemType};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::processing::transactions::profile::Profile;
use crate::processing::utils::event::{
    EventFullyNormalized, EventMetricsExtracted, FiltersStatus, SpansExtracted, event_type,
};
use crate::processing::utils::transaction::ExtractMetricsContext;
use crate::processing::{Forward, Processor, QuotaRateLimiter, utils};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{ProcessingError, ProcessingExtractedMetrics};
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::{SamplingResult, should_filter};
#[cfg(feature = "processing")]
use crate::{processing::spans::store, services::store::StoreEnvelope};

mod process;
mod profile;

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
        ctx: super::Context<'_>,
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

        let mut event = Annotated::empty();
        if let Some(transaction_item) = work.transaction {
            let result = metric!(timer(RelayTimers::EventProcessingDeserialize), {
                Annotated::<Event>::from_json_bytes(&transaction_item.payload())
            });
            event = result.map_err(Error::from).reject(&work)?;
            if let Some(event) = event.value_mut() {
                event.ty = EventType::Transaction.into();
            }
        }

        let (transaction_part, profile) = work.split_once(|w| {
            // TODO: transaction_part should be of a type without a profile field
            let profile = w.profile.take();
            (w, profile)
        });

        let mut profile_id = None;
        if let Some(profile_item) = profile.as_ref() {
            let feature = Feature::Profiling;
            if should_filter(ctx.config, ctx.project_info, feature) {
                profile.reject_err(Outcome::Invalid(DiscardReason::FeatureDisabled(feature)));
            } else if transaction_part.transaction.is_none() && profile_item.sampled() {
                // A profile with `sampled=true` should never be without a transaction
                profile.reject_err(Outcome::Invalid(DiscardReason::Profiling(
                    "missing_transaction",
                )));
            } else {
                match relay_profiling::parse_metadata(&profile_item.payload(), project_id) {
                    Ok(id) => {
                        profile_id = Some(id);
                    }
                    Err(err) => {
                        profile.reject_err(Outcome::Invalid(DiscardReason::Profiling(
                            relay_profiling::discard_reason(err),
                        )));
                    }
                }
            }
        }

        profile::transfer_id(&mut event, profile_id);

        let work = transaction_part.merge(profile).map(|(mut t, p), _| {
            t.profile = p;
            t
        });

        work.modify(|w, r| utils::dsc::validate_and_set_dsc(&mut w.headers, &event, &mut ctx));

        utils::event::finalize(
            &work.headers,
            &mut event,
            work.attachments.iter(),
            &mut metrics,
            &ctx.config,
        );

        event_fully_normalized = utils::event::normalize(
            &work.headers,
            &mut event,
            event_fully_normalized,
            project_id,
            &ctx,
            &self.geoip_lookup,
        )
        .map_err(Error::from)
        .reject(&work)?;

        let filter_run = utils::event::filter(&work.headers, &mut event, &ctx)
            .map_err(|e| ProcessingError::EventFiltered(e).into())
            .reject(&work)?;

        // Always run dynamic sampling on processing Relays,
        // but delay decision until inbound filters have been fully processed.
        // Also, we require transaction metrics to be enabled before sampling.
        let run_dynamic_sampling = (matches!(filter_run, FiltersStatus::Ok)
            || ctx.config.processing_enabled())
            && matches!(&ctx.project_info.config.transaction_metrics, Some(ErrorBoundary::Ok(c)) if c.is_enabled());

        let sampling_result = match run_dynamic_sampling {
            true => {
                #[allow(unused_mut)]
                let mut reservoir = ReservoirEvaluator::new(Arc::clone(&self.reservoir_counters));
                #[cfg(feature = "processing")]
                if let Some(quotas_client) = self.quotas_client.as_ref() {
                    reservoir.set_redis(work.scoping().organization_id, quotas_client);
                }
                utils::dynamic_sampling::run(work.headers.dsc(), &mut event, &ctx, Some(&reservoir))
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

        // TODO: earlier
        let work = work.map(|w, _| {
            let SerializedTransaction {
                headers,
                transaction: _,
                attachments,
                profile,
            } = w;
            ExpandedTransaction {
                headers,
                transaction: event,
                attachments,
                profile,
            }
        });

        if let Some(outcome) = sampling_result.into_dropped_outcome() {
            // Process profiles before dropping the transaction, if necessary.
            // Before metric extraction to make sure the profile count is reflected correctly.

            work.modify(|work, r| {
                if let Some(profile) = work.profile.as_mut() {
                    profile.set_sampled(false);
                    let result = profile::process(
                        profile,
                        work.headers.meta().client_addr(),
                        event.value(),
                        &ctx,
                    );
                    if let Err(outcome) = result {
                        r.reject_err(outcome, work.profile.take());
                    }
                }
            });

            // Extract metrics here, we're about to drop the event/transaction.
            event_metrics_extracted = utils::transaction::extract_metrics(
                &mut event,
                &mut extracted_metrics,
                ExtractMetricsContext {
                    dsc: work.headers.dsc(),
                    project_id,
                    ctx: &ctx,
                    sampling_decision: SamplingDecision::Drop,
                    event_metrics_extracted,
                    spans_extracted,
                },
            )
            .map_err(Error::ProcessingFailed)
            .reject(&work)?;

            let (work, profile) = work.split_once(|w| {
                let profile = w.profile.take();
                (w, profile)
            });

            // reject everything but the profile:
            // FIXME: track non-extracted spans as well.
            // -> Type TransactionWithEmbeddedSpans
            let _ = work.reject_err(outcome);

            // If we have a profile left, we need to make sure there is quota for this profile.
            if let Some(profile) = profile.transpose() {
                let mut profile = profile.map(|p, _| Profile(p));
                if self.limiter.enforce_quotas(&mut profile, ctx).await.is_ok() {
                    work.profile = Some(profile.0);
                }
            }

            return Ok(super::Output {
                main: Some(TransactionOutput(work)),
                metrics: Some(work.wrap(extracted_metrics.into_inner())),
            });
        }

        // let _post_ds = cogs.start_category("post_ds");

        // // Need to scrub the transaction before extracting spans.
        // //
        // // Unconditionally scrub to make sure PII is removed as early as possible.
        // event::scrub(&mut event, ctx.project_info)?;

        // attachment::scrub(managed_envelope, ctx.project_info);

        // if_processing!(self.inner.config, {
        //     // Process profiles before extracting metrics, to make sure they are removed if they are invalid.
        //     let profile_id = profile::process(
        //         managed_envelope,
        //         &mut event,
        //         ctx.global_config,
        //         ctx.config,
        //         ctx.project_info,
        //     );
        //     profile::transfer_id(&mut event, profile_id);
        //     profile::scrub_profiler_id(&mut event);

        //     // Always extract metrics in processing Relays for sampled items.
        //     event_metrics_extracted = self.extract_transaction_metrics(
        //         managed_envelope,
        //         &mut event,
        //         &mut extracted_metrics,
        //         project_id,
        //         ctx.project_info,
        //         SamplingDecision::Keep,
        //         event_metrics_extracted,
        //         spans_extracted,
        //     )?;

        //     if ctx.project_info.has_feature(Feature::ExtractSpansFromEvent) {
        //         spans_extracted = span::extract_from_event(
        //             managed_envelope,
        //             &event,
        //             ctx.global_config,
        //             ctx.config,
        //             server_sample_rate,
        //             event_metrics_extracted,
        //             spans_extracted,
        //         );
        //     }
        // });

        // event = self
        //     .enforce_quotas(managed_envelope, event, &mut extracted_metrics, ctx)
        //     .await?;

        // if_processing!(self.inner.config, {
        //     event = span::maybe_discard_transaction(managed_envelope, event, ctx.project_info);
        // });

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

        // if ctx.config.processing_enabled() && !event_fully_normalized.0 {
        //     relay_log::error!(
        //         tags.project = %project_id,
        //         tags.ty = event_type(&event).map(|e| e.to_string()).unwrap_or("none".to_owned()),
        //         "ingested event without normalizing"
        //     );
        // };

        Ok(super::Output {
            main: Some(TransactionOutput(work)),
            metrics: Some(work.wrap(extracted_metrics.into_inner())),
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
    transaction: Annotated<Event>, // might be empty
    attachments: smallvec::SmallVec<[Item; 3]>,
    profile: Option<Item>,
}

impl Counted for ExpandedTransaction {
    fn quantities(&self) -> Quantities {
        let mut quantities = Quantities::new();

        if self.transaction.value().is_some() {
            quantities.extend([
                (DataCategory::TransactionIndexed, 1),
                (DataCategory::Transaction, 1),
            ]);
        }
        quantities.extend(self.attachments.quantities());
        quantities.extend(self.profile.quantities());

        quantities
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
                for item in attachment_items {
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
