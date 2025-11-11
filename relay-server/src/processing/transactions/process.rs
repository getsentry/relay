use relay_base_schema::events::EventType;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;
use relay_sampling::evaluation::SamplingDecision;
use relay_statsd::metric;

use crate::managed::{Managed, Rejected};
use crate::processing::transactions::profile::Profile;
use crate::processing::transactions::{
    Error, ExpandedTransaction, Flags, SerializedTransaction, Transaction, TransactionOutput,
    profile,
};
use crate::processing::utils::event::EventMetricsExtracted;
use crate::processing::utils::transaction::ExtractMetricsContext;
use crate::processing::{Context, Output, QuotaRateLimiter, utils};
use crate::services::outcome::Outcome;
use crate::services::processor::ProcessingExtractedMetrics;
use crate::statsd::RelayTimers;

/// Parses the event payload.
pub fn expand(work: SerializedTransaction) -> Result<ExpandedTransaction, Error> {
    let SerializedTransaction {
        headers,
        transaction: transaction_item,
        attachments,
        profile,
    } = work;
    let mut transaction = Annotated::empty();
    let mut flags = Flags::default();
    if let Some(transaction_item) = transaction_item.as_ref() {
        transaction = metric!(timer(RelayTimers::EventProcessingDeserialize), {
            Annotated::<Event>::from_json_bytes(&transaction_item.payload())
        })?;
        if let Some(event) = transaction.value_mut() {
            event.ty = EventType::Transaction.into();
        }
        flags = Flags {
            metrics_extracted: transaction_item.metrics_extracted(),
            spans_extracted: transaction_item.spans_extracted(),
            fully_normalized: headers.meta().request_trust().is_trusted()
                && transaction_item.fully_normalized(),
        };
    }

    Ok(ExpandedTransaction {
        headers,
        transaction: Transaction(transaction),
        flags,
        attachments,
        profile,
        extracted_spans: vec![],
    })
}

/// Finishes transaction and profile processing when the dynamic sampling decision was "drop".
pub async fn drop_after_sampling(
    mut work: Managed<ExpandedTransaction>,
    mut extracted_metrics: ProcessingExtractedMetrics,
    outcome: Outcome,
    ctx: Context<'_>,
    limiter: &QuotaRateLimiter,
) -> Result<Output<TransactionOutput>, Rejected<Error>> {
    // Process profiles before dropping the transaction, if necessary.
    // Before metric extraction to make sure the profile count is reflected correctly.
    let project_id = work.scoping().project_id;
    work.try_modify(|work, record_keeper| {
        if let Some(profile) = work.profile.as_mut() {
            profile.set_sampled(false);
            let result = profile::process(
                profile,
                work.headers.meta().client_addr(),
                work.transaction.0.value(),
                &ctx,
            );
            if let Err(outcome) = result {
                record_keeper.reject_err(outcome, work.profile.take());
            }
        }
        // Extract metrics here, we're about to drop the event/transaction.
        // TODO: move `utils` to `transaction`.
        work.flags.metrics_extracted = utils::transaction::extract_metrics(
            &mut work.transaction.0,
            &mut extracted_metrics,
            ExtractMetricsContext {
                dsc: work.headers.dsc(),
                project_id,
                ctx: &ctx,
                sampling_decision: SamplingDecision::Drop,
                metrics_extracted: EventMetricsExtracted(work.flags.metrics_extracted),
                spans_extracted: work.flags.spans_extracted,
            },
        )?
        .0;
        Ok::<_, Error>(())
    })?;

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
        limiter.enforce_quotas(&mut profile, ctx).await?;
    }

    let metrics = work.wrap(extracted_metrics.into_inner());
    Ok(Output {
        main: Some(TransactionOutput(work)),
        metrics: Some(metrics),
    })
}
