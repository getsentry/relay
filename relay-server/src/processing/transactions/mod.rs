use std::sync::Arc;

use relay_base_schema::events::EventType;
use relay_dynamic_config::Feature;
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::{Event, Metrics, SpanV2};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use relay_statsd::metric;
use smallvec::SmallVec;

use crate::Envelope;
use crate::envelope::ContainerItems;
use crate::envelope::{ContainerWriteError, EnvelopeHeaders, Item, ItemContainer, ItemType};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::processing::{Forward, Processor, QuotaRateLimiter, utils};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{ProcessingError, ProcessingExtractedMetrics};
use crate::statsd::RelayTimers;
use crate::utils::should_filter;
#[cfg(feature = "processing")]
use crate::{processing::spans::store, services::store::StoreEnvelope};

mod process;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid JSON")]
    InvalidJson(#[from] serde_json::Error),
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Error::InvalidJson(_) => Outcome::Invalid(DiscardReason::InvalidJson),
        };
        (Some(outcome), self)
    }
}

/// A processor for transactions.
pub struct TransactionProcessor {
    limiter: Arc<QuotaRateLimiter>,
    geo_lookup: GeoIpLookup,
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
                match relay_profiling::parse_metadata(
                    &profile_item.payload(),
                    ctx.project_info.project_id.unwrap(), // FIXME
                ) {
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

        transfer_profile_id(&mut event, profile_id);

        transaction_part
            .modify(|w, r| utils::dsc::validate_and_set_dsc(&mut w.headers, &event, &mut ctx));

        event::finalize(
            managed_envelope,
            &mut event,
            &mut metrics,
            &self.inner.config,
        )?;

        event_fully_normalized = self.normalize_event(
            managed_envelope,
            &mut event,
            project_id,
            ctx.project_info,
            event_fully_normalized,
        )?;

        let filter_run = event::filter(
            managed_envelope,
            &mut event,
            ctx.project_info,
            ctx.global_config,
        )?;

        // Always run dynamic sampling on processing Relays,
        // but delay decision until inbound filters have been fully processed.
        let run_dynamic_sampling =
            matches!(filter_run, FiltersStatus::Ok) || self.inner.config.processing_enabled();

        let reservoir = self.new_reservoir_evaluator(
            managed_envelope.scoping().organization_id,
            reservoir_counters,
        );

        let sampling_result = match run_dynamic_sampling {
            true => {
                dynamic_sampling::run(
                    managed_envelope,
                    &mut event,
                    ctx.config,
                    ctx.project_info,
                    ctx.sampling_project_info,
                    &reservoir,
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
            profile::process(
                managed_envelope,
                &mut event,
                ctx.global_config,
                ctx.config,
                ctx.project_info,
            );
            // Extract metrics here, we're about to drop the event/transaction.
            event_metrics_extracted = self.extract_transaction_metrics(
                managed_envelope,
                &mut event,
                &mut extracted_metrics,
                project_id,
                ctx.project_info,
                SamplingDecision::Drop,
                event_metrics_extracted,
                spans_extracted,
            )?;

            dynamic_sampling::drop_unsampled_items(
                managed_envelope,
                event,
                outcome,
                spans_extracted,
            );

            // At this point we have:
            //  - An empty envelope.
            //  - An envelope containing only processed profiles.
            // We need to make sure there are enough quotas for these profiles.
            event = self
                .enforce_quotas(
                    managed_envelope,
                    Annotated::empty(),
                    &mut extracted_metrics,
                    ctx,
                )
                .await?;

            return Ok(Some(extracted_metrics));
        }

        let _post_ds = cogs.start_category("post_ds");

        // Need to scrub the transaction before extracting spans.
        //
        // Unconditionally scrub to make sure PII is removed as early as possible.
        event::scrub(&mut event, ctx.project_info)?;

        attachment::scrub(managed_envelope, ctx.project_info);

        if_processing!(self.inner.config, {
            // Process profiles before extracting metrics, to make sure they are removed if they are invalid.
            let profile_id = profile::process(
                managed_envelope,
                &mut event,
                ctx.global_config,
                ctx.config,
                ctx.project_info,
            );
            profile::transfer_id(&mut event, profile_id);
            profile::scrub_profiler_id(&mut event);

            // Always extract metrics in processing Relays for sampled items.
            event_metrics_extracted = self.extract_transaction_metrics(
                managed_envelope,
                &mut event,
                &mut extracted_metrics,
                project_id,
                ctx.project_info,
                SamplingDecision::Keep,
                event_metrics_extracted,
                spans_extracted,
            )?;

            if ctx.project_info.has_feature(Feature::ExtractSpansFromEvent) {
                spans_extracted = span::extract_from_event(
                    managed_envelope,
                    &event,
                    ctx.global_config,
                    ctx.config,
                    server_sample_rate,
                    event_metrics_extracted,
                    spans_extracted,
                );
            }
        });

        event = self
            .enforce_quotas(managed_envelope, event, &mut extracted_metrics, ctx)
            .await?;

        if_processing!(self.inner.config, {
            event = span::maybe_discard_transaction(managed_envelope, event, ctx.project_info);
        });

        // Event may have been dropped because of a quota and the envelope can be empty.
        if event.value().is_some() {
            event::serialize(
                managed_envelope,
                &mut event,
                event_fully_normalized,
                event_metrics_extracted,
                spans_extracted,
            )?;
        }

        if self.inner.config.processing_enabled() && !event_fully_normalized.0 {
            relay_log::error!(
                tags.project = %project_id,
                tags.ty = event_type(&event).map(|e| e.to_string()).unwrap_or("none".to_owned()),
                "ingested event without normalizing"
            );
        };

        Ok(Some(extracted_metrics))
    }
}

/// Transfers the profile ID from the profile item to the transaction item.
///
/// The profile id may be `None` when the envelope does not contain a profile,
/// in that case the profile context is removed.
/// Some SDKs send transactions with profile ids but omit the profile in the envelope.
fn transfer_profile_id(event: &mut Annotated<Event>, profile_id: Option<ProfileId>) {
    let Some(event) = event.value_mut() else {
        return;
    };

    match profile_id {
        Some(profile_id) => {
            let contexts = event.contexts.get_or_insert_with(Contexts::new);
            contexts.add(ProfileContext {
                profile_id: Annotated::new(profile_id),
                ..ProfileContext::default()
            });
        }
        None => {
            if let Some(contexts) = event.contexts.value_mut()
                && let Some(profile_context) = contexts.get_mut::<ProfileContext>()
            {
                profile_context.profile_id = Annotated::empty();
            }
        }
    }
}

/// New type representing the normalization state of the event.
#[derive(Copy, Clone)]
struct EventFullyNormalized(bool);

impl EventFullyNormalized {
    /// Returns `true` if the event is fully normalized, `false` otherwise.
    pub fn new(envelope: &Envelope) -> Self {
        let event_fully_normalized = envelope.meta().request_trust().is_trusted()
            && envelope
                .items()
                .any(|item| item.creates_event() && item.fully_normalized());

        Self(event_fully_normalized)
    }
}

/// New type representing whether metrics were extracted from transactions/spans.
#[derive(Debug, Copy, Clone)]
struct EventMetricsExtracted(bool);

/// New type representing whether spans were extracted.
#[derive(Debug, Copy, Clone)]
struct SpansExtracted(bool);

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
    data: ExpandedData,
}

impl ExpandedTransaction {
    fn serialize_envelope(self) -> Result<Box<Envelope>, ContainerWriteError> {
        let Self { headers, data } = self;

        match data {
            ExpandedData::Transaction {
                transaction,
                attachments,
                profile,
                extracted_spans,
            } => {
                let mut envelope = Envelope::try_from_event(headers, transaction)?;

                if !extracted_spans.is_empty() {
                    let mut item = Item::new(ItemType::Span);
                    ItemContainer::from(extracted_spans).write_to(&mut item)?;
                    envelope.add_item(item);
                }

                for item in attachment_items {
                    envelope.add_item(item);
                }
                for item in profile_items {
                    envelope.add_item(item);
                }

                Ok(envelope)
            }
            ExpandedData::Profile(item) => Ok(Envelope::from_parts(headers, [item].into())),
        }
    }
}

impl Counted for ExpandedTransaction {
    fn quantities(&self) -> Quantities {
        let Self { headers: _, data } = self;
        data.quantities()
    }
}

enum ExpandedData {
    /// A standard transaction item with optional profile & attachment(s).
    Transaction {
        transaction: Event,
        attachments: SmallVec<[Item; 3]>,
        profile: Option<Item>,
        #[cfg(feature = "processing")]
        extracted_spans: ContainerItems<SpanV2>,
    },
    /// A profile left over after the transaction has been dropped by dynamic sampling.
    Profile(Item),
}

impl Counted for ExpandedData {
    fn quantities(&self) -> Quantities {
        match self {
            Self::Transaction {
                transaction,
                attachments,
                profile,
                extracted_spans,
            } => {
                let mut quantities = Quantities::new();
                quantities.extend(transaction.quantities());
                quantities.extend(attachments.quantities());
                if let Some(profile) = profile {
                    quantities.extend(attachments.quantities());
                }
                quantities.extend(extracted_spans).quantities();
                quantities
            }
            Self::Profile(item) => item.quantities(),
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

        let (envelope, spans) =
            output.split_once(|ExpandedTransaction { headers, data }| match data {
                ExpandedData::Transaction {
                    transaction,
                    attachments,
                    profile,
                    extracted_spans,
                } => {
                    let mut envelope =
                        Envelope::try_from_event(headers, transaction).map_err(|e| {
                            output.internal_error("failed to create envelope from event")
                        })?;
                    for item in attachment_items {
                        envelope.add_item(item);
                    }

                    (envelope, extracted_spans)
                }
                ExpandedData::Profile(item) => {
                    (Envelope::from_parts(headers, [item].into()), vec![])
                }
            });

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
