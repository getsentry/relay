#![expect(unused)]
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use relay_base_schema::events::EventType;
use relay_dynamic_config::{ErrorBoundary, Feature};
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::{Event, Metrics, SpanV2};
use relay_protocol::{Annotated, Empty, get_value};
use relay_quotas::{DataCategory, RateLimits};
#[cfg(feature = "processing")]
use relay_redis::AsyncRedisClient;
use relay_sampling::evaluation::{ReservoirCounters, ReservoirEvaluator, SamplingDecision};
use relay_statsd::metric;
use sentry::Data;
use smallvec::{SmallVec, smallvec};

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType, Items};
#[cfg(feature = "processing")]
use crate::managed::TypedEnvelope;
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
#[cfg(feature = "processing")]
use crate::processing::StoreHandle;
use crate::processing::spans::{Indexed, TotalAndIndexed};
use crate::processing::transactions::profile::{Profile, ProfileWithHeaders};
use crate::processing::transactions::types::{Flags, SampledPayload, WithHeaders, WithMetrics};
use crate::processing::utils::event::{
    EventFullyNormalized, EventMetricsExtracted, FiltersStatus, SpansExtracted, event_type,
};
use crate::processing::{
    Context, CountRateLimited, Forward, ForwardContext, Output, Processor, QuotaRateLimiter,
    RateLimited, RateLimiter, utils,
};
use crate::services::outcome::{DiscardReason, Outcome};
#[cfg(feature = "processing")]
use crate::services::processor::ProcessingGroup;
use crate::services::processor::{ProcessingError, ProcessingExtractedMetrics};
#[cfg(feature = "processing")]
use crate::services::store::StoreEnvelope;
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::{SamplingResult, should_filter};

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
    #[cfg(feature = "processing")]
    quotas_client: Option<AsyncRedisClient>,
}

impl TransactionProcessor {
    /// Creates a new transaction processor.
    pub fn new(
        limiter: Arc<QuotaRateLimiter>,
        geoip_lookup: GeoIpLookup,
        #[cfg(feature = "processing")] quotas_client: Option<AsyncRedisClient>,
    ) -> Self {
        Self {
            limiter,
            geoip_lookup,
            #[cfg(feature = "processing")]
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

        #[allow(unused_mut)]
        let mut event = envelope
            .envelope_mut()
            .take_item_by(|item| matches!(*item.ty(), ItemType::Transaction))?;

        // Count number of spans by shallow-parsing the event.
        // Needed for accounting but not in prod, because the event is immediately parsed afterwards.
        #[cfg(debug_assertions)]
        event.ensure_span_count();

        let attachments = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Attachment));

        let profiles = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Profile));

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
        let scoping = work.scoping();
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

        #[cfg(feature = "processing")]
        let quotas_client = self.quotas_client.as_ref();
        #[cfg(not(feature = "processing"))]
        let quotas_client = None;
        relay_log::trace!("Sample transaction");
        let sampling_result =
            process::run_dynamic_sampling(&work, ctx, filters_status, quotas_client).await;

        #[cfg(feature = "processing")]
        let server_sample_rate = sampling_result.sample_rate();

        relay_log::trace!("Processing profiles");
        let work = process::process_profile(work, ctx, sampling_result.decision());

        relay_log::trace!("Enforce quotas");
        let mut work = self.limiter.enforce_quotas(work, ctx).await?;

        relay_log::trace!("Extract transaction metrics");
        let mut work = process::extract_metrics(work, ctx, sampling_result.into_dropped_outcome())?;

        // Need to scrub the transaction before extracting spans.
        relay_log::trace!("Scrubbing transaction");
        work = process::scrub(work, ctx)?;

        #[cfg(feature = "processing")]
        if ctx.config.processing_enabled() {
            if let SampledPayload::Keep { payload } = &work.payload
                && !payload.flags.fully_normalized
            {
                relay_log::error!(
                    tags.project = %project_id,
                    tags.ty = event_type(&payload.event).map(|e| e.to_string()).unwrap_or("none".to_owned()),
                    "ingested event without normalizing"
                );
            };

            relay_log::trace!("Extract spans");
            work = process::extract_spans(work, ctx, server_sample_rate);
        }

        let (main, metrics) = work.split_once(
            |WithMetrics {
                 headers,
                 payload,
                 metrics,
             }| (WithHeaders { headers, payload }, metrics),
        );

        relay_log::trace!("Done");
        Ok(Output {
            main: Some(TransactionOutput(main)),
            metrics: Some(metrics),
        })
    }
}

/// A transaction in its serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedTransaction {
    headers: EnvelopeHeaders,
    event: Item,
    attachments: Items,
    profiles: SmallVec<[Item; 3]>,
}

impl Counted for SerializedTransaction {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            event,
            attachments,
            profiles,
        } = self;
        debug_assert!(!event.spans_extracted());
        let mut quantities = event.quantities(); // counts spans based on `span_count` header.
        quantities.extend(attachments.quantities());
        quantities.extend(profiles.quantities());

        quantities
    }
}

/// A transaction after parsing.
///
/// The type parameter indicates whether metrics were already extracted, which changes how
/// we count the transaction (total vs indexed).
#[derive(Debug)]
pub struct ExpandedTransaction {
    headers: EnvelopeHeaders,
    event: Annotated<Event>,
    flags: Flags,
    attachments: Items,
    profile: Option<Item>,
    extracted_spans: Vec<Item>,
}

impl ExpandedTransaction {
    fn count_embedded_spans_and_self(&self) -> usize {
        1 + self
            .event
            .value()
            .and_then(|e| e.spans.value())
            .map_or(0, Vec::len)
    }
}

impl Counted for ExpandedTransaction {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            event,
            flags,
            attachments,
            profile,
            extracted_spans,
        } = self;
        let mut quantities = smallvec![(DataCategory::TransactionIndexed, 1),];

        let span_count = if flags.spans_extracted {
            debug_assert!(!self.extracted_spans.is_empty());
            self.extracted_spans.len()
        } else {
            debug_assert!(self.extracted_spans.is_empty());
            self.count_embedded_spans_and_self()
        };
        quantities.extend([(DataCategory::SpanIndexed, span_count)]);

        if !flags.metrics_extracted {
            // The transaction still carries ownership over the total categories:
            quantities.extend([
                (DataCategory::Transaction, 1),
                (DataCategory::Span, span_count),
            ]);
        }

        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        quantities
    }
}

impl RateLimited for Managed<ExpandedTransaction> {
    type Error = Error;
    type Output = Self;

    async fn enforce<R>(
        mut self,
        mut rate_limiter: R,
        ctx: Context<'_>,
    ) -> Result<Self, Rejected<Self::Error>>
    where
        R: RateLimiter,
    {
        let scoping = self.scoping();

        // These debug assertions are here because this function does not check indexed or extracted
        // span limits.
        // TODO: encode flags into types instead.
        debug_assert!(!self.flags.metrics_extracted);
        debug_assert!(!self.flags.spans_extracted);

        // FIXME: check indexed category if metrics extracted.

        // If there is a transaction limit, drop everything.
        // This also affects profiles that lost their transaction due to sampling.
        let limits = rate_limiter
            .try_consume(scoping.item(DataCategory::Transaction), 1)
            .await;

        if !limits.is_empty() {
            let error = Error::from(limits);
            return Err(self.reject_err(error));
        }

        // There is no limit on "total", but if metrics have already been extracted then
        // also check the "indexed" limit:
        if self.flags.metrics_extracted {
            let limits = rate_limiter
                .try_consume(scoping.item(DataCategory::TransactionIndexed), 1)
                .await;

            if !limits.is_empty() {
                let error = Error::from(limits);
                return Err(self.reject_err(error));
            }
        }

        let attachment_quantities = self.attachments.quantities();

        // Check profile limits:
        for (category, quantity) in self.profile.quantities() {
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

        // We assume that span extraction happens after metrics extraction, so safe to check both
        // categories:
        if !self.extracted_spans.is_empty() {
            for category in [DataCategory::Span, DataCategory::SpanIndexed] {
                let limits = rate_limiter
                    .try_consume(scoping.item(category), self.extracted_spans.len())
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
        }

        Ok(self)
    }
}

/// Wrapper for spans extracted from a transaction.
///
/// Needed to not emit the total category for spans.
#[derive(Debug)]
struct ExtractedSpans(Vec<Item>);

impl Counted for ExtractedSpans {
    fn quantities(&self) -> Quantities {
        if self.0.is_empty() {
            return smallvec![];
        }

        smallvec![(DataCategory::SpanIndexed, self.0.len())]
    }
}

/// Output of the transaction processor.
#[derive(Debug)]
pub struct TransactionOutput(Managed<WithHeaders>);

impl TransactionOutput {
    #[cfg(test)]
    pub fn event(self) -> Option<Annotated<Event>> {
        match self.0.accept(|x| x).payload {
            SampledPayload::Keep { payload } => Some(payload.event),
            SampledPayload::Drop { profile } => None,
        }
    }
}

impl Forward for TransactionOutput {
    fn serialize_envelope(
        self,
        ctx: ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        self.0.try_map(|work, _| {
            work.serialize_envelope()
                .map_err(drop)
                .with_outcome(Outcome::Invalid(DiscardReason::Internal))
        })
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: StoreHandle<'_>,
        ctx: ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let envelope: ManagedEnvelope = self.serialize_envelope(ctx)?.into();

        s.store(StoreEnvelope {
            envelope: envelope.into_processed(),
        });

        Ok(())
    }
}
