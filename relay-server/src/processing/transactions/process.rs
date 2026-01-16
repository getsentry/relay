use std::sync::Arc;

use either::Either;
use hyper::ext;
use relay_base_schema::events::EventType;
use relay_dynamic_config::ErrorBoundary;
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::{Event, Metrics, SpanV2};
use relay_profiling::ProfileError;
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use relay_redis::AsyncRedisClient;
use relay_sampling::evaluation::{ReservoirEvaluator, SamplingDecision};
use relay_statsd::metric;
use smallvec::smallvec;

use crate::envelope::Item;
use crate::managed::{Counted, Managed, ManagedResult, Quantities, RecordKeeper, Rejected};
use crate::metrics_extraction::transactions::ExtractedMetrics;
use crate::processing::spans::{Indexed, TotalAndIndexed};
use crate::processing::transactions::extraction::{self, ExtractMetricsContext};
use crate::processing::transactions::profile::{Profile, ProfileWithHeaders};
use crate::processing::transactions::types::{SampledTransaction, WithMetrics};
use crate::processing::transactions::{
    Error, ExpandedTransaction, ExtractedSpans, Flags, IndexedWrapper, SerializedTransaction,
    TransactionOutput, profile, spans,
};
use crate::processing::utils::event::{
    EventFullyNormalized, EventMetricsExtracted, FiltersStatus, SpansExtracted,
};
use crate::processing::{Context, Output, QuotaRateLimiter, utils};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{ProcessingError, ProcessingExtractedMetrics};
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::SamplingResult;

/// Parses the event payload.
///
/// This function boxes the resulting [`ExpandedTransaction`] because keeping it on the stack
/// causes stack overflows in unit tests when run without optimizations.
pub fn expand(
    work: Managed<SerializedTransaction>,
) -> Result<Managed<Box<ExpandedTransaction>>, Rejected<Error>> {
    work.try_map(|work, record_keeper| {
        let SerializedTransaction {
            headers,
            event: transaction_item,
            attachments,
            profiles,
        } = work;
        let mut event = metric!(timer(RelayTimers::EventProcessingDeserialize), {
            Annotated::<Event>::from_json_bytes(&transaction_item.payload())
        })?;
        if let Some(event) = event.value_mut() {
            event.ty = EventType::Transaction.into();
        }
        let flags = Flags {
            metrics_extracted: transaction_item.metrics_extracted(),
            spans_extracted: transaction_item.spans_extracted(),
            fully_normalized: headers.meta().request_trust().is_trusted()
                && transaction_item.fully_normalized(),
        };
        validate_flags(&flags);

        let mut profiles = profiles.into_iter();

        // Accept at most one profile:
        let profile = profiles.next();
        for additional_profile in profiles {
            record_keeper.reject_err(
                Outcome::Invalid(DiscardReason::Profiling(relay_profiling::discard_reason(
                    &ProfileError::TooManyProfiles,
                ))),
                additional_profile,
            );
        }

        #[cfg(debug_assertions)]
        {
            // Fix broken span count headers
            use relay_protocol::get_value;
            let embedded = get_value!(event.spans).map_or(0, Vec::len);
            let diff = embedded as isize - transaction_item.span_count() as isize;
            record_keeper.modify_by(DataCategory::Span, diff);
            record_keeper.modify_by(DataCategory::SpanIndexed, diff);
        }

        Ok::<_, Error>(Box::new(ExpandedTransaction {
            headers,
            event,
            flags,
            attachments,
            profile,
            extracted_spans: vec![],
            category: TotalAndIndexed,
        }))
    })
}

/// Validates the following assumption:
/// 1. Metrics are only extracted in non-processing relays if the sampling decision is "drop".
/// 2. That means that if we see a new transaction, it cannot yet have metrics extracted.
fn validate_flags(flags: &Flags) {
    if flags.metrics_extracted {
        relay_log::error!("Received a transaction which already had its metrics extracted.");
    }
}

/// Validates and massages the data.
pub fn prepare_data(
    work: &mut Managed<Box<ExpandedTransaction>>,
    ctx: &mut Context<'_>,
    metrics: &mut Metrics,
) -> Result<(), Rejected<Error>> {
    let scoping = work.scoping();
    work.try_modify(|work, record_keeper| {
        let profile_id = profile::filter(work, record_keeper, *ctx, scoping.project_id);
        profile::transfer_id(&mut work.event, profile_id);
        profile::remove_context_if_rate_limited(&mut work.event, scoping, *ctx);

        utils::dsc::validate_and_set_dsc(&mut work.headers, &work.event, ctx);

        // HACKish: The span extraction killswitch relies on a random number, so evaluate it only
        // once and then pretend that spans were already extracted.
        let span_extraction_sample_rate = ctx
            .global_config
            .options
            .span_extraction_sample_rate
            .unwrap_or(1.0);
        if crate::utils::sample(span_extraction_sample_rate).is_discard() {
            work.flags.spans_extracted = true;

            // The kill switch is an emergency measure which breaks the product, so we accept
            // omitting outcomes here:
            record_keeper.lenient(DataCategory::Span);
            record_keeper.lenient(DataCategory::SpanIndexed);
        }

        utils::event::finalize(
            &work.headers,
            &mut work.event,
            work.attachments.iter(),
            metrics,
            ctx.config,
        )
        .map_err(Error::from)
    })?;
    Ok(())
}

/// Normalizes the transaction event.
pub fn normalize(
    work: Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
    geoip_lookup: &GeoIpLookup,
) -> Result<Managed<Box<ExpandedTransaction>>, Rejected<Error>> {
    let project_id = work.scoping().project_id;
    work.try_map(|mut work, _| {
        work.flags.fully_normalized = utils::event::normalize(
            &work.headers,
            &mut work.event,
            EventFullyNormalized(work.flags.fully_normalized),
            project_id,
            ctx,
            geoip_lookup,
        )?
        .0;
        Ok::<_, Error>(work)
    })
}

/// Rejects the entire unit of work if one of the project's filters matches.
pub fn run_inbound_filters(
    work: &Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
) -> Result<FiltersStatus, Rejected<Error>> {
    utils::event::filter(&work.headers, &work.event, &ctx)
        .map_err(ProcessingError::EventFiltered)
        .map_err(Error::from)
        .reject(work)
}

/// The result of dynamic sampling.
pub enum SamplingOutput {
    /// The decision was retain, maintain full transaction.
    Keep(Managed<Box<ExpandedTransaction>>),
    /// The decision was discard keep only extracted metrics and an optional profile.
    Drop {
        metrics: Managed<ExtractedMetrics>,
        profile: Option<Managed<Item>>,
    },
}

/// Computes the sampling decision for a transaction and associated items.
pub async fn run_dynamic_sampling(
    work: Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
    filters_status: FiltersStatus,
    quotas_client: Option<&AsyncRedisClient>,
) -> Result<SamplingOutput, Rejected<Error>> {
    let sampling_result =
        make_dynamic_sampling_decision(&work, ctx, filters_status, quotas_client).await;

    let sampling_match = match sampling_result {
        SamplingResult::Match(m) if m.decision().is_drop() => m,
        keep => {
            // TODO: needed?
            // work.modify(|spans, _| {
            //     spans.server_sample_rate = sampling_result.sample_rate();
            // });
            return Ok(SamplingOutput::Keep(work));
        }
    };

    // At this point the decision is to drop the payload.
    let (payload, remainder) = split_indexed_and_total(work, ctx, SamplingDecision::Drop)?;

    // Need to process the profile before the event gets dropped:
    let payload = process_profile(payload, ctx);

    let outcome = Outcome::FilteredSampling(sampling_match.into_matched_rules().into());
    let _ = payload.reject_err(outcome);

    let (metrics, profile) = remainder.split_once(|x| x);
    Ok(SamplingOutput::Drop {
        metrics,
        profile: profile.transpose(),
    })
}

/// Computes the dynamic sampling decision for the unit of work, but does not perform action on data.
async fn make_dynamic_sampling_decision(
    work: &Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
    filters_status: FiltersStatus,
    quotas_client: Option<&AsyncRedisClient>,
) -> SamplingResult {
    let sampling_result =
        do_make_dynamic_sampling_decision(work, ctx, filters_status, quotas_client).await;
    relay_statsd::metric!(
        counter(RelayCounters::SamplingDecision) += 1,
        decision = sampling_result.decision().as_str(),
        item = "transaction"
    );
    sampling_result
}

async fn do_make_dynamic_sampling_decision(
    work: &Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
    filters_status: FiltersStatus,
    quotas_client: Option<&AsyncRedisClient>,
) -> SamplingResult {
    // Always run dynamic sampling on processing Relays,
    // but delay decision until inbound filters have been fully processed.
    // Also, we require transaction metrics to be enabled before sampling.
    let should_run = matches!(filters_status, FiltersStatus::Ok) || ctx.config.processing_enabled();

    let can_extract_metrics = matches!(&ctx.project_info.config.transaction_metrics, Some(ErrorBoundary::Ok(c)) if c.is_enabled());
    if !(should_run && can_extract_metrics) {
        return SamplingResult::Pending;
    }

    #[allow(unused_mut)]
    let mut reservoir = ReservoirEvaluator::new(Arc::clone(ctx.reservoir_counters));
    #[cfg(feature = "processing")]
    if let Some(quotas_client) = quotas_client {
        reservoir.set_redis(work.scoping().organization_id, quotas_client);
    }
    utils::dynamic_sampling::run(
        work.headers.dsc(),
        work.event.value(),
        &ctx,
        Some(&reservoir),
    )
    .await
}

type IndexedAndMetrics = (
    Managed<Box<ExpandedTransaction<Indexed>>>,
    Managed<(ExtractedMetrics, Option<Item>)>,
);

/// Splits transaction into indexed payload and metrics representing the total counts.
///
/// Dynamic sampling internal function, outside users should use the safer, use case driven variants
/// [`try_split_indexed_and_total`] and [`reject_indexed_spans`].
fn split_indexed_and_total(
    mut work: Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
    sampling_decision: SamplingDecision,
) -> Result<IndexedAndMetrics, Rejected<Error>> {
    let scoping = work.scoping();

    let mut metrics = ProcessingExtractedMetrics::new();
    let mut profile = None;
    let r = work.try_modify(|work, _| {
        work.flags.metrics_extracted = extraction::extract_metrics(
            &mut work.event,
            &mut metrics,
            ExtractMetricsContext {
                dsc: work.headers.dsc(),
                project_id: scoping.project_id,
                ctx,
                sampling_decision,
                metrics_extracted: work.flags.metrics_extracted,
                spans_extracted: work.flags.spans_extracted,
            },
        )?
        .0;
        profile = work.profile.take();
        if let Some(profile) = profile.as_mut() {
            profile.set_sampled(sampling_decision.is_keep());
        }
        Ok::<_, Error>(())
    });

    Ok(work.split_once(|work| {
        (
            Box::new(work.into_indexed()),
            (metrics.into_inner(), profile),
        )
    }))
}

/// Processes the profile attached to the transaction.
pub fn process_profile(
    work: Managed<Box<ExpandedTransaction<_>>>,
    ctx: Context<'_>,
) -> Managed<Box<ExpandedTransaction>> {
    work.map(|mut work, record_keeper| {
        let mut profile_id = None;
        if let Some(profile) = work.profile.as_mut() {
            let result = profile::process(
                profile,
                work.headers.meta().client_addr(),
                work.event.value(),
                &ctx,
            );
            match result {
                Err(outcome) => {
                    record_keeper.reject_err(outcome, work.profile.take());
                }
                Ok(id) => profile_id = Some(id),
            };
        }
        profile::transfer_id(&mut work.event, profile_id);
        profile::scrub_profiler_id(&mut work.event);

        work
    })
}

/// Extracts transaction & span metrics from the payload.
fn extract_metrics(
    mut work: Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
    drop_with_outcome: Option<Outcome>,
) -> Result<Managed<WithMetrics>, Rejected<Error>> {
    let project_id = work.scoping().project_id;

    let sampling_decision = match drop_with_outcome {
        Some(_) => SamplingDecision::Drop,
        None => SamplingDecision::Keep,
    };

    work.try_map(|mut work, record_keeper| {
        if work.flags.metrics_extracted {
            // txn and span counts will be off.
            record_keeper.lenient(DataCategory::Transaction);
            record_keeper.lenient(DataCategory::Span);
        }

        let mut metrics = ProcessingExtractedMetrics::new();
        if sampling_decision.is_drop() || ctx.is_processing() {
            work.flags.metrics_extracted = extraction::extract_metrics(
                &mut work.event,
                &mut metrics,
                ExtractMetricsContext {
                    dsc: work.headers.dsc(),
                    project_id,
                    ctx,
                    sampling_decision,
                    metrics_extracted: work.flags.metrics_extracted,
                    spans_extracted: work.flags.spans_extracted,
                },
            )?
            .0;
        }

        let metrics = metrics.into_inner();
        let headers = work.headers.clone();
        let output = match drop_with_outcome {
            Some(outcome) => {
                let profile = work.profile.take();
                record_keeper.reject_err(outcome, IndexedWrapper(work));
                WithMetrics {
                    headers,
                    payload: SampledTransaction::Drop { profile },
                    metrics,
                }
            }
            None => WithMetrics {
                headers,
                payload: SampledTransaction::Keep { payload: work },
                metrics,
            },
        };

        // Adds items in this data category.
        record_keeper.lenient(DataCategory::MetricBucket);

        Ok::<_, Error>(output)
    })
}

/// Converts the spans embedded in the transaction into top-level span items.
#[cfg(feature = "processing")]
pub fn extract_spans(
    mut work: Managed<WithMetrics>,
    ctx: Context<'_>,
    server_sample_rate: Option<f64>,
) -> Managed<WithMetrics> {
    work.modify(|mut work, r| {
        let WithMetrics {
            headers,
            payload,
            metrics,
        } = work;
        match payload {
            SampledTransaction::Keep { payload } => {
                if let Some(results) = spans::extract_from_event(
                    headers.dsc(),
                    &payload.event,
                    ctx.global_config,
                    ctx.config,
                    server_sample_rate,
                    EventMetricsExtracted(payload.flags.metrics_extracted),
                    SpansExtracted(payload.flags.spans_extracted),
                ) {
                    payload.flags.spans_extracted = true;
                    for result in results {
                        match result {
                            Ok(item) => payload.extracted_spans.push(item),
                            Err(_) => {
                                r.reject_err(
                                    Outcome::Invalid(DiscardReason::InvalidSpan),
                                    IndexedSpans(1),
                                );
                            }
                        }
                    }
                }
            }
            SampledTransaction::Drop { profile } => {}
        }
    });
    work
}

/// Runs PiiProcessors on the event and its attachments.
pub fn scrub(
    work: Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
) -> Result<Managed<Box<ExpandedTransaction>>, Rejected<Error>> {
    work.try_map(|mut work, _| {
        utils::event::scrub(&mut work.event, ctx.project_info)?;
        utils::attachments::scrub(work.attachments.iter_mut(), ctx.project_info);
        Ok::<_, Error>(work)
    })
}

struct IndexedSpans(usize);

impl Counted for IndexedSpans {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::SpanIndexed, self.0)]
    }
}
