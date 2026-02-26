use std::sync::Arc;

use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::Metrics;
use relay_quotas::RateLimits;
use relay_redis::AsyncRedisClient;
use relay_sampling::evaluation::SamplingDecision;

use crate::envelope::ItemType;
use crate::managed::{Managed, ManagedEnvelope, OutcomeError, Rejected};
use crate::processing::transactions::process::{SamplingOutput, split_indexed_and_total};
use crate::processing::transactions::types::{SerializedTransaction, TransactionOutput};
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
    quotas_client: Option<AsyncRedisClient>,
}

impl TransactionProcessor {
    /// Creates a new transaction processor.
    pub fn new(
        limiter: Arc<QuotaRateLimiter>,
        geoip_lookup: GeoIpLookup,
        quotas_client: Option<AsyncRedisClient>,
    ) -> Self {
        Self {
            limiter,
            geoip_lookup,
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

        let mut event = envelope
            .envelope_mut()
            .take_item_by(|item| matches!(item.ty(), ItemType::Transaction))?;

        // Count number of spans by shallow-parsing the event.
        // Needed for accounting but not in prod, because the event is immediately parsed afterwards.
        #[cfg(debug_assertions)]
        event.ensure_span_count();

        let attachments = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.ty(), ItemType::Attachment));

        let profiles = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.ty(), ItemType::Profile));

        let work = SerializedTransaction {
            headers,
            event,
            attachments,
            profiles,
        };

        Some(Managed::with_meta_from(envelope, work))
    }

    async fn process(
        &self,
        work: Managed<Self::UnitOfWork>,
        mut ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let project_id = work.scoping().project_id;
        let mut metrics = Metrics::default();

        relay_log::trace!("Expand transaction");
        let mut work = process::expand(work)?;

        relay_log::trace!("Prepare transaction data");
        process::prepare_data(&mut work, &mut ctx, &mut metrics)?;

        relay_log::trace!("Normalize transaction");
        let mut work = process::normalize(work, ctx, &self.geoip_lookup)?;

        relay_log::trace!("Filter transaction");
        let filters_status = process::run_inbound_filters(&work, ctx)?;

        let quotas_client = self.quotas_client.as_ref();

        relay_log::trace!("Processing profile");
        process::process_profile(&mut work, ctx);

        relay_log::trace!("Sample transaction");
        let (work, server_sample_rate) =
            match process::run_dynamic_sampling(work, ctx, filters_status, quotas_client).await? {
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
        let mut work = process::scrub(work, ctx)?;

        work = process::extract_spans(work, ctx, server_sample_rate);

        relay_log::trace!("Enforce quotas");
        let work = self.limiter.enforce_quotas(work, ctx).await?;
        let work = match work.transpose() {
            either::Either::Left(work) => work,
            either::Either::Right(metrics) => return Ok(Output::metrics(metrics)),
        };

        if ctx.is_processing() {
            if !work.flags.fully_normalized {
                relay_log::error!(
                    tags.project = %project_id,
                    tags.ty = event_type(&work.event).map(|e| e.to_string()).unwrap_or("none".to_owned()),
                    "ingested event without normalizing"
                );
            };

            let (indexed, metrics) = split_indexed_and_total(work, ctx, SamplingDecision::Keep)?;

            return Ok(Output {
                main: Some(TransactionOutput::Indexed(indexed)),
                metrics: Some(metrics),
            });
        }

        Ok(Output {
            main: Some(TransactionOutput::Full(work)),
            metrics: None,
        })
    }
}
