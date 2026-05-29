use relay_base_schema::events::EventType;
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::{Event, Metrics};
use relay_profiling::{ProfileError, ProfileType};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use relay_sampling::evaluation::SamplingDecision;
use relay_statsd::metric;
use smallvec::smallvec;

use crate::envelope::Item;
use crate::managed::{Counted, Managed, ManagedResult, Quantities, RecordKeeper, Rejected};
use crate::metrics_extraction::ExtractedMetrics;
use crate::processing::spans::{Indexed, TotalAndIndexed};
use crate::processing::transactions::extraction::{self, ExtractMetricsContext};
use crate::processing::transactions::spans;
use crate::processing::transactions::types::{
    ExpandedProfile, ExpandedTransaction, ExtractedIndexedSpans, ExtractedSpans, Flags,
    StandaloneProfile,
};
use crate::processing::transactions::{Error, SerializedTransaction, profile};
use crate::processing::utils::event::{EventFullyNormalized, FiltersStatus};
use crate::processing::{Context, utils};
use crate::services::outcome::{DiscardItemType, DiscardReason, Outcome};
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
            spans_rate_limited: false,
        };
        validate_flags(&flags);

        let profile = expand_profile(profiles, record_keeper);

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

fn expand_profile(
    profiles: Vec<Item>,
    record_keeper: &mut RecordKeeper<'_>,
) -> Option<ExpandedProfile> {
    let mut profiles = profiles.into_iter();

    // Accept at most one profile:
    let profile = profiles.next()?;
    for additional_profile in profiles {
        record_keeper.reject_err(
            Outcome::Invalid(DiscardReason::Profiling(relay_profiling::discard_reason(
                &ProfileError::TooManyProfiles,
            ))),
            additional_profile,
        );
    }

    let meta = match relay_profiling::parse_metadata(&profile.payload()) {
        Ok(meta) => meta,
        Err(err) => {
            record_keeper.reject_err(
                Outcome::Invalid(DiscardReason::Profiling(relay_profiling::discard_reason(
                    &err,
                ))),
                profile,
            );
            return None;
        }
    };

    // If the profile type is new information, we now count the profile in an additional data category.
    if profile.profile_type().is_none() {
        record_keeper.modify_by(
            match meta.profile_type() {
                ProfileType::Backend => DataCategory::ProfileBackend,
                ProfileType::Ui => DataCategory::ProfileUi,
            },
            1,
        );
    }

    Some(ExpandedProfile {
        meta,
        item: profile,
    })
}

/// Validates and massages the data.
pub fn prepare_data(
    work: &mut Managed<Box<ExpandedTransaction>>,
    ctx: &mut Context<'_>,
    metrics: &mut Metrics,
) -> Result<(), Rejected<Error>> {
    let scoping = work.scoping();
    work.try_modify(|work, record_keeper| {
        profile::filter(work, record_keeper, *ctx);
        profile::transfer_id(&mut work.event, work.profile.as_ref().map(|p| p.meta.id));
        profile::remove_context_if_rate_limited(&mut work.event, scoping, *ctx);

        utils::dsc::validate_and_set_dsc(&mut work.headers, &work.event, ctx);

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
    work.try_map(|mut work, r| {
        let original_span_count = work.count_embedded_spans_and_self();

        work.flags.fully_normalized = utils::event::normalize(
            &work.headers,
            &mut work.event,
            EventFullyNormalized(work.flags.fully_normalized),
            project_id,
            ctx,
            geoip_lookup,
        )?
        .0;

        // Normalization may have trimmed spans:
        debug_assert!(!work.flags.spans_extracted);
        let new_span_count = work.count_embedded_spans_and_self();
        if let Some(trimmed) = original_span_count.checked_sub(new_span_count)
            && trimmed > 0
        {
            r.reject_err(
                Outcome::Invalid(DiscardReason::ItemTooLarge(DiscardItemType::Span)),
                [
                    (DataCategory::Span, trimmed),
                    (DataCategory::SpanIndexed, trimmed),
                ],
            );
        }

        Ok::<_, Error>(work)
    })
}

/// Rejects the entire unit of work if one of the project's filters matches.
pub fn run_inbound_filters(
    work: &Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
) -> Result<FiltersStatus, Rejected<Error>> {
    utils::event::filter(&work.headers, &work.event, ctx)
        .map_err(ProcessingError::EventFiltered)
        .map_err(Error::from)
        .reject(work)
}

/// The result of dynamic sampling.
pub enum SamplingOutput {
    /// The decision was retain, maintain full transaction.
    Keep {
        payload: Managed<Box<ExpandedTransaction>>,
        sample_rate: Option<f64>,
    },
    /// The decision was discard keep only extracted metrics and an optional profile.
    Drop {
        metrics: Managed<ExtractedMetrics>,
        profile: Option<Managed<Box<StandaloneProfile>>>,
    },
}

/// Computes the sampling decision for a transaction and associated items.
pub fn run_dynamic_sampling(
    payload: Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
    filters_status: FiltersStatus,
) -> SamplingOutput {
    let sampling_result = make_dynamic_sampling_decision(&payload, ctx, filters_status);

    let sampling_match = match sampling_result {
        SamplingResult::Match(m) if m.decision().is_drop() => m,
        keep => {
            return SamplingOutput::Keep {
                payload,
                sample_rate: keep.sample_rate(),
            };
        }
    };

    // At this point the decision is to drop the payload.
    let (payload, metrics) = split_indexed_and_total(payload, ctx, SamplingDecision::Drop);

    let (payload, profile) = payload.split_once(|mut payload, _| {
        let profile = payload.profile.take().map(|profile| StandaloneProfile {
            profile,
            // Actually no need to clone here, since we do drop the remaining transaction after,
            // for simplicity sake we clone for now.
            headers: payload.headers.clone(),
        });

        (payload, profile)
    });

    let outcome = Outcome::FilteredSampling(sampling_match.into_matched_rules().into());
    let _ = payload.reject_err(outcome);

    SamplingOutput::Drop {
        metrics,
        profile: profile.transpose().map(Managed::boxed),
    }
}

/// Computes the dynamic sampling decision for the unit of work, but does not perform action on data.
fn make_dynamic_sampling_decision(
    work: &Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
    filters_status: FiltersStatus,
) -> SamplingResult {
    let sampling_result = do_make_dynamic_sampling_decision(work, ctx, filters_status);
    relay_statsd::metric!(
        counter(RelayCounters::SamplingDecision) += 1,
        decision = sampling_result.decision().as_str(),
        item = "transaction"
    );
    sampling_result
}

fn do_make_dynamic_sampling_decision(
    work: &Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
    filters_status: FiltersStatus,
) -> SamplingResult {
    // Always run dynamic sampling on processing Relays,
    // but delay decision until inbound filters have been fully processed.
    // Also, we require transaction metrics to be enabled before sampling.
    let should_run = matches!(filters_status, FiltersStatus::Ok) || ctx.config.processing_enabled();
    if !should_run {
        return SamplingResult::Pending;
    }

    utils::dynamic_sampling::run(work.headers.dsc(), work.event.value(), &ctx)
}

type IndexedTransactionAndSpanAndMetrics = (
    Managed<Box<ExpandedTransaction<Indexed>>>,
    Option<Managed<ExtractedIndexedSpans>>,
    Managed<ExtractedMetrics>,
);

/// Splits transaction into indexed payload and metrics representing the total counts.
///
/// Like [`split_indexed_and_total`] but works with [`ExtractedSpans`].
pub fn split_indexed_and_total_with_extracted_spans(
    transaction: Managed<Box<ExpandedTransaction>>,
    spans: Option<Managed<ExtractedSpans>>,
    ctx: Context<'_>,
) -> IndexedTransactionAndSpanAndMetrics {
    let scoping = transaction.scoping();

    let (transaction, metrics) = transaction.split_once(|mut tx, r| {
        r.lenient(DataCategory::MetricBucket);

        let mut metrics = ProcessingExtractedMetrics::new();

        let had_metrics_extracted = tx.flags.metrics_extracted;
        tx.flags.metrics_extracted = extraction::extract_metrics(
            &mut tx.event,
            &mut metrics,
            ExtractMetricsContext {
                dsc: tx.headers.dsc(),
                project_id: scoping.project_id,
                ctx,
                sampling_decision: SamplingDecision::Keep,
                metrics_extracted: tx.flags.metrics_extracted,
                extract_span_metrics: spans.is_some(),
            },
        )
        .0;

        if had_metrics_extracted || !tx.flags.metrics_extracted {
            // Invalid config or invalid original transaction
            r.lenient(DataCategory::Transaction);
            r.lenient(DataCategory::Span);
        }

        // This really is a bug, we ignore here.
        //
        // Transactions are counted using a span metric, as transaction payloads should
        // eventually be fully transformed into spans.
        //
        // Since there is no span metric extracted for this transaction, as we already extracted
        // the spans from the transaction, there is no now metric carrying the transaction category.
        //
        // After extracting span metrics the count is accurate again, but attached to the span metrics.
        // Unless, the spans have been rate limited, which is an actual potential bug which we
        // ignore here for two reasons:
        //  - Span rate limits should be applied to transaction as well
        //  - Long-term transactions will no longer exist
        if spans.is_none() {
            r.lenient(DataCategory::Transaction);
        }

        // Since we just extracted span metrics, which account for the total spans, we need also fix
        // these counts, later we correct this again.
        if let Some(spans) = &spans {
            r.modify_by(DataCategory::Span, spans.0.len() as isize);
        }

        (Box::new(tx.into_indexed()), metrics.into_inner())
    });

    // In an ideal world we would use these extracted spans to also extract span metrics instead of
    // re-using the transaction to get the metrics and risking differences in metrics.
    //
    // The master plan on how to clean this up:
    //  1. Migrate dynamic sampling to EAP
    //  2. Emit total category outcomes in Relay instead of as a metric
    //  3. Remove all span metrics, including extraction (possible after 1., and 2.)
    let spans = spans.map(|spans| {
        spans.map(|spans, r| {
            if let Some((c, q)) = metrics
                .quantities()
                .iter()
                .find(|(c, _)| *c == DataCategory::Span)
            {
                // "Insurance" that metrics extracted from the transaction spans match the extracted
                // spans.
                r.modify_by(*c, -(*q as isize));
            } else if transaction.flags.metrics_extracted {
                // This `metrics_extracted` flag really isn't necessary anymore and should be deleted.
                r.lenient(DataCategory::Span);
            }

            spans.into_indexed()
        })
    });

    (transaction, spans, metrics)
}

type IndexedAndMetrics = (
    Managed<Box<ExpandedTransaction<Indexed>>>,
    Managed<ExtractedMetrics>,
);

/// Splits transaction into indexed payload and metrics representing the total counts.
pub fn split_indexed_and_total(
    mut work: Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
    sampling_decision: SamplingDecision,
) -> IndexedAndMetrics {
    let scoping = work.scoping();

    let had_metrics_extracted = work.flags.metrics_extracted;
    let extract_span_metrics = !work.flags.spans_rate_limited;

    let mut metrics = ProcessingExtractedMetrics::new();
    work.modify(|work, _| {
        work.flags.metrics_extracted = extraction::extract_metrics(
            &mut work.event,
            &mut metrics,
            ExtractMetricsContext {
                dsc: work.headers.dsc(),
                project_id: scoping.project_id,
                ctx,
                sampling_decision,
                metrics_extracted: work.flags.metrics_extracted,
                extract_span_metrics,
            },
        )
        .0;
    });

    work.split_once(|work, r| {
        r.lenient(DataCategory::MetricBucket);
        if had_metrics_extracted || !work.flags.metrics_extracted {
            // Invalid config or invalid original transaction
            r.lenient(DataCategory::Transaction);
            r.lenient(DataCategory::Span);
        }
        if work.flags.spans_rate_limited {
            // This really is a bug, we ignore here.
            //
            // Transactions are counted using a span metric, as transaction payloads should
            // eventually be fully transformed into spans. But this is for now only the case for
            // metrics, the payloads are lagging behind.
            //
            // If spans are rate limited, there is no metric to attach the transaction to ->
            // we're losing a transaction here.
            //
            //  If and once we apply span rate limits to transactions this case will also be fixed,
            //  as it can no longer happen.
            r.lenient(DataCategory::Transaction);
        }

        (Box::new(work.into_indexed()), metrics.into_inner())
    })
}

/// Processes the profile attached to the transaction.
pub fn process_profile(work: &mut Managed<Box<ExpandedTransaction>>, ctx: Context<'_>) {
    work.modify(|work, record_keeper| {
        if let Some(profile) = work.profile.as_mut()
            && let Err(outcome) = profile::process(
                &mut profile.item,
                work.headers.meta().client_addr(),
                work.event.value(),
                &ctx,
            )
        {
            record_keeper.reject_err(outcome, work.profile.take());
        };

        let profile_id = work.profile.as_ref().map(|profile| profile.meta.id);
        profile::transfer_id(&mut work.event, profile_id);
        profile::scrub_profiler_id(&mut work.event);
    });
}

/// Converts the spans embedded in the transaction into top-level span items.
///
/// Only extracts spans in processing.
pub fn extract_spans(
    transaction: Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
    server_sample_rate: Option<f64>,
) -> (Managed<ExtractedSpans>, Managed<Box<ExpandedTransaction>>) {
    // This could be validated with additional typing, a different type for transactions which have
    // spans extracted.
    debug_assert!(
        !transaction.flags.spans_extracted,
        "spans can only be extracted once"
    );

    transaction.split_once(|mut tx, r| {
        let spans = spans::extract_from_event(tx.headers.dsc(), &tx.event, ctx, server_sample_rate)
            .into_iter()
            .filter_map(|span| match span {
                Ok(span) => Some(span),
                Err(()) => {
                    r.reject_err(
                        Outcome::Invalid(DiscardReason::InvalidSpan),
                        IndexedSpans(1),
                    );
                    None
                }
            })
            .collect();

        // Once spans are extracted, they are no longer counted towards the transaction.
        tx.flags.spans_extracted = true;

        (ExtractedSpans(spans), tx)
    })
}

/// Runs PiiProcessors on the event and its attachments.
pub fn scrub(
    work: Managed<Box<ExpandedTransaction>>,
    ctx: Context<'_>,
) -> Result<Managed<Box<ExpandedTransaction>>, Rejected<Error>> {
    work.try_map(|mut work, records| {
        utils::event::scrub(&mut work.event, ctx.project_info)?;
        utils::attachments::scrub(work.attachments.iter_mut(), ctx.project_info, Some(records));
        Ok::<_, Error>(work)
    })
}

struct IndexedSpans(usize);

impl Counted for IndexedSpans {
    fn quantities(&self) -> Quantities {
        smallvec![(DataCategory::SpanIndexed, self.0)]
    }
}
