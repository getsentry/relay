use std::sync::Arc;

use relay_base_schema::events::EventType;
use relay_dynamic_config::ErrorBoundary;
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::{Event, Metrics, SpanV2};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use relay_redis::AsyncRedisClient;
use relay_sampling::evaluation::{ReservoirEvaluator, SamplingDecision};
use relay_statsd::metric;
use smallvec::smallvec;

use crate::envelope::Item;
use crate::managed::{Counted, Managed, ManagedResult, Quantities, RecordKeeper, Rejected};
use crate::metrics_extraction::transactions::ExtractedMetrics;
use crate::processing::transactions::extraction::ExtractMetricsContext;
use crate::processing::transactions::profile::Profile;
use crate::processing::transactions::{
    Error, ExpandedTransaction, ExtractedSpans, Flags, IndexedTransaction, SerializedTransaction,
    Transaction, TransactionOutput, profile, spans,
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
pub fn parse(
    work: Managed<SerializedTransaction>,
) -> Result<Managed<ExpandedTransaction<Transaction>>, Rejected<Error>> {
    work.try_map(|work, _| {
        let SerializedTransaction {
            headers,
            transaction: transaction_item,
            attachments,
            profile,
        } = work;
        let mut transaction = metric!(timer(RelayTimers::EventProcessingDeserialize), {
            Annotated::<Event>::from_json_bytes(&transaction_item.payload())
        })?;
        if let Some(event) = transaction.value_mut() {
            event.ty = EventType::Transaction.into();
        }
        let flags = Flags {
            metrics_extracted: transaction_item.metrics_extracted(),
            spans_extracted: transaction_item.spans_extracted(),
            fully_normalized: headers.meta().request_trust().is_trusted()
                && transaction_item.fully_normalized(),
        };
        validate_flags(&flags);

        Ok::<_, Error>(ExpandedTransaction {
            headers,
            transaction: Transaction(transaction),
            flags,
            attachments,
            profile,
            extracted_spans: ExtractedSpans(vec![]),
        })
    })
}

/// Validates the following assumption:
/// 1. Metrics are only extracted in non-processing relays if the sampling decision is "drop".
/// 2. That means that if we see a new transaction, it cannot yet have metrics extracted.
fn validate_flags(flags: &Flags) {
    debug_assert!(!flags.metrics_extracted);
    if flags.metrics_extracted {
        relay_log::error!("Received a transaction which already had its metrics extracted.");
    }
}

/// Validates and massages the data.
pub fn prepare_data(
    work: &mut Managed<ExpandedTransaction<Transaction>>,
    ctx: &mut Context<'_>,
    metrics: &mut Metrics,
) -> Result<(), Rejected<Error>> {
    let scoping = work.scoping();
    work.try_modify(|work, record_keeper| {
        let profile_id = profile::filter(work, record_keeper, *ctx, scoping.project_id);
        let event = &mut work.transaction.0;
        profile::transfer_id(event, profile_id);
        profile::remove_context_if_rate_limited(event, scoping, *ctx);

        utils::dsc::validate_and_set_dsc(&mut work.headers, event, ctx);

        utils::event::finalize(
            &work.headers,
            event,
            work.attachments.iter(),
            metrics,
            ctx.config,
        )
        .map_err(Error::from)
    })?;
    Ok(())
}

pub fn normalize(
    work: Managed<ExpandedTransaction<Transaction>>,
    ctx: Context<'_>,
    geoip_lookup: &GeoIpLookup,
) -> Result<Managed<ExpandedTransaction<Transaction>>, Rejected<Error>> {
    let project_id = work.scoping().project_id;
    work.try_map(|mut work, _| {
        work.flags.fully_normalized = utils::event::normalize(
            &work.headers,
            &mut work.transaction.0,
            EventFullyNormalized(work.flags.fully_normalized),
            project_id,
            ctx,
            geoip_lookup,
        )?
        .0;
        Ok::<_, Error>(work)
    })
}

pub fn run_inbound_filters(
    work: &Managed<ExpandedTransaction<Transaction>>,
    ctx: Context<'_>,
) -> Result<FiltersStatus, Rejected<Error>> {
    utils::event::filter(&work.headers, &work.transaction.0, &ctx)
        .map_err(ProcessingError::EventFiltered)
        .map_err(Error::from)
        .reject(work)
}

pub async fn run_dynamic_sampling(
    work: &Managed<ExpandedTransaction<Transaction>>,
    ctx: Context<'_>,
    filters_status: FiltersStatus,
    quotas_client: &Option<AsyncRedisClient>,
) -> SamplingResult {
    let sampling_result = do_run_dynamic_sampling(work, ctx, filters_status, quotas_client).await;
    relay_statsd::metric!(
        counter(RelayCounters::SamplingDecision) += 1,
        decision = sampling_result.decision().as_str(),
        item = "transaction"
    );
    sampling_result
}

async fn do_run_dynamic_sampling(
    work: &Managed<ExpandedTransaction<Transaction>>,
    ctx: Context<'_>,
    filters_status: FiltersStatus,
    quotas_client: &Option<AsyncRedisClient>,
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
    if let Some(quotas_client) = quotas_client.as_ref() {
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

/// Finishes transaction and profile processing when the dynamic sampling decision was "drop".
pub fn drop_after_sampling(
    mut work: Managed<ExpandedTransaction<IndexedTransaction>>,
    ctx: Context<'_>,
    outcome: Outcome,
) -> Managed<Option<Profile>> {
    let (mut work, profile) = work.split_once(|mut work| {
        let profile = work.profile.take();
        (work, profile)
    });

    // reject everything but the profile:
    let _ = work.reject_err(outcome.clone());

    profile.map(|item, _| item.map(Profile))
}

pub fn process_profile(
    work: Managed<ExpandedTransaction<Transaction>>,
    ctx: Context<'_>,
    sampling_decision: SamplingDecision,
) -> Managed<ExpandedTransaction<Transaction>> {
    work.map(|mut work, record_keeper| {
        let mut profile_id = None;
        if let Some(profile) = work.profile.as_mut() {
            profile.set_sampled(sampling_decision.is_keep());
            let result = profile::process(
                profile,
                work.headers.meta().client_addr(),
                work.transaction.0.value(),
                &ctx,
            );
            match result {
                Err(outcome) => {
                    record_keeper.reject_err(outcome, work.profile.take());
                }
                Ok(id) => profile_id = Some(id),
            };
        }
        profile::transfer_id(&mut work.transaction.0, profile_id);
        profile::scrub_profiler_id(&mut work.transaction.0);

        work
    })
}

type IndexedWithMetrics = (
    Managed<ExpandedTransaction<IndexedTransaction>>,
    Managed<ExtractedMetrics>,
);

pub fn extract_metrics(
    work: Managed<ExpandedTransaction<Transaction>>,
    ctx: Context<'_>,
    sampling_decision: SamplingDecision,
) -> Result<IndexedWithMetrics, Rejected<Error>> {
    let project_id = work.scoping().project_id;

    let mut metrics = ProcessingExtractedMetrics::new();
    let indexed = work.try_map(|mut work, record_keeper| {
        // Extract metrics here, we're about to drop the event/transaction.
        work.flags.metrics_extracted = super::extraction::extract_metrics(
            &mut work.transaction.0,
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
        Ok::<_, Error>(ExpandedTransaction::<IndexedTransaction>::from(work))
    })?;
    let metrics = indexed.wrap(metrics.into_inner());
    Ok((indexed, metrics))
}

pub fn extract_spans(
    work: Managed<ExpandedTransaction<IndexedTransaction>>,
    ctx: Context<'_>,
    server_sample_rate: Option<f64>,
) -> Managed<ExpandedTransaction<IndexedTransaction>> {
    work.map(|mut work, r| {
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
                    Ok(item) => work.extracted_spans.0.push(item),
                    Err(_) => r.reject_err(
                        Outcome::Invalid(DiscardReason::InvalidSpan),
                        // HACK to get correct outcomes for spans. TODO: should this only emit SpanIndexed?
                        SpanV2::default(),
                    ),
                }
            }
        }
        work
    })
}

pub fn scrub<T>(
    work: Managed<ExpandedTransaction<T>>,
    ctx: Context<'_>,
) -> Result<Managed<ExpandedTransaction<T>>, Rejected<Error>>
where
    T: Counted + AsRef<Annotated<Event>> + AsMut<Annotated<Event>>,
{
    work.try_map(|mut work, _| {
        utils::event::scrub(work.transaction.as_mut(), ctx.project_info)?;
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
