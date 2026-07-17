use std::sync::Arc;

use relay_cogs::{AppFeature, FeatureWeights};
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::Metrics;
use relay_quotas::RateLimits;

use crate::envelope::ItemType;
use crate::managed::{Managed, ManagedEnvelope, OutcomeError, Rejected};
use crate::processing::transactions::process::SamplingOutput;
use crate::processing::transactions::types::{SerializedTransaction, TransactionOutput};
use crate::processing::utils::attachments;
use crate::processing::utils::event::event_type;
use crate::processing::{Context, Output, Processor, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::ProcessingError;

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
}

impl TransactionProcessor {
    /// Creates a new transaction processor.
    pub fn new(limiter: Arc<QuotaRateLimiter>, geoip_lookup: GeoIpLookup) -> Self {
        Self {
            limiter,
            geoip_lookup,
        }
    }
}

impl Processor for TransactionProcessor {
    type Input = SerializedTransaction;
    type Output = TransactionOutput;
    type Error = Error;

    fn cogs() -> FeatureWeights {
        AppFeature::Transactions.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let headers = envelope.envelope().headers().clone();

        let mut event = envelope
            .envelope_mut()
            .take_item_by(|item| matches!(*item.ty(), ItemType::Transaction))?;

        // Count number of spans by shallow-parsing the event.
        // Needed for accounting but not in prod, because the event is immediately parsed afterwards.
        #[cfg(debug_assertions)]
        event.ensure_span_count();

        let attachments = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Attachment))
            .into_vec();

        let profiles = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Profile))
            .into_vec();

        let work = SerializedTransaction {
            headers,
            event,
            attachments,
            profiles,
        };

        Some(Managed::with_meta_from_managed_envelope(envelope, work))
    }

    async fn process(
        &self,
        tx: Managed<Self::Input>,
        mut ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let project_id = tx.scoping().project_id;
        let mut metrics = Metrics::default();

        relay_log::trace!("Expand transaction");
        let mut tx = process::expand(tx)?;

        relay_log::trace!("Validate attachments");
        attachments::validate_attachments(&mut tx, |t| &mut t.attachments, ctx);

        relay_log::trace!("Prepare transaction data");
        process::prepare_data(&mut tx, &mut ctx, &mut metrics)?;

        relay_log::trace!("Normalize transaction");
        let mut tx = process::normalize(tx, ctx, &self.geoip_lookup)?;

        relay_log::trace!("Filter transaction");
        let filters_status = process::run_inbound_filters(&tx, ctx)?;

        relay_log::trace!("Processing profile");
        process::process_profile(&mut tx, ctx);

        relay_log::trace!("Sample transaction");
        let (sampling_output, metrics_config) =
            process::run_dynamic_sampling(tx, ctx, filters_status);
        let (tx, server_sample_rate) = match sampling_output {
            SamplingOutput::Keep {
                payload,
                sample_rate,
            } => (payload, sample_rate),
            SamplingOutput::Drop {
                metrics,
                mut profile,
            } => {
                // Remaining profile needs to be rate limited:
                if let Some(p) = profile {
                    profile = self.limiter.enforce_quotas(p, ctx).await.ok();
                }
                return Ok(Output {
                    main: profile.map(TransactionOutput::Profile),
                    metrics: Some(metrics),
                });
            }
        };

        // Need to scrub the transaction before extracting spans.
        relay_log::trace!("Scrubbing transaction");
        #[allow(unused_mut)]
        let mut tx = process::scrub(tx, ctx)?;

        relay_log::trace!("Enforce quotas");
        let tx = self.limiter.enforce_quotas(tx, ctx).await?;
        let tx = match tx.transpose() {
            either::Either::Left(tx) => tx,
            either::Either::Right(metrics) => return Ok(Output::metrics(metrics)),
        };

        if ctx.is_processing() {
            if !tx.flags.fully_normalized {
                relay_log::error!(
                    tags.project = %project_id,
                    tags.ty = event_type(&tx.event).map(|e| e.to_string()).unwrap_or("none".to_owned()),
                    "ingested event without normalizing"
                );
            };

            let (spans, tx) = process::extract_spans(tx, ctx, server_sample_rate);
            let spans = self.limiter.enforce_quotas(spans, ctx).await.ok();

            let (transaction, spans, metrics) =
                process::split_indexed_and_total_with_extracted_spans(
                    tx,
                    spans,
                    ctx,
                    metrics_config,
                );

            return Ok(Output {
                main: Some(TransactionOutput::Indexed { spans, transaction }),
                metrics: Some(metrics),
            });
        }

        Ok(Output {
            main: Some(TransactionOutput::Full(tx)),
            metrics: None,
        })
    }
}
