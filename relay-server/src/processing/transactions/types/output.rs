#[cfg(feature = "processing")]
use relay_dynamic_config::Feature;
#[cfg(feature = "processing")]
use relay_protocol::Annotated;

use crate::Envelope;
use crate::managed::{Managed, ManagedResult, Rejected};
#[cfg(feature = "processing")]
use crate::processing::StoreHandle;
use crate::processing::spans::Indexed;
use crate::processing::transactions::types::{
    ExpandedTransaction, ExtractedIndexedSpans, StandaloneProfile,
};
use crate::processing::{Forward, ForwardContext};
use crate::services::outcome::{DiscardReason, Outcome};

/// Output of the transaction processor.
#[derive(Debug)]
pub enum TransactionOutput {
    /// The transaction has not been dropped by dynamic sampling.
    Full(Managed<Box<ExpandedTransaction>>),
    /// The transaction has been dropped by dynamic sampling, only an optional profile remains.
    Profile(Managed<Box<StandaloneProfile>>),
    /// The transaction has not been dropped by dynamic sampling, and metrics have been extracted.
    ///
    /// This is used in processing relays.
    Indexed {
        spans: Option<Managed<ExtractedIndexedSpans>>,
        transaction: Managed<Box<ExpandedTransaction<Indexed>>>,
    },
}

impl Forward for TransactionOutput {
    fn serialize_envelope(
        self,
        _ctx: ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        match self {
            TransactionOutput::Full(managed) => managed.try_map(|work, _| {
                work.serialize_envelope()
                    .map_err(drop)
                    .with_outcome(Outcome::Invalid(DiscardReason::Internal))
            }),
            TransactionOutput::Profile(profile) => {
                Ok(profile.map(|profile, _| profile.serialize_envelope()))
            }
            TransactionOutput::Indexed { spans, transaction } => {
                if let Some(spans) = spans {
                    let _ = spans.internal_error("indexed spans can only be stored");
                };
                Err(transaction.internal_error("an indexed transaction can only be stored"))
            }
        }
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: StoreHandle<'_>,
        ctx: ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let (spans, transaction) = match self {
            TransactionOutput::Full(managed) => {
                return Err(managed.internal_error("only indexed transactions can be stored"));
            }
            TransactionOutput::Profile(profile) => {
                s.send_to_store(profile.map(|p, _| store::convert_profile(p.profile, false, ctx)));
                return Ok(());
            }
            TransactionOutput::Indexed { spans, transaction } => (spans, transaction),
        };

        let performance_issues_spans = ctx
            .project_info
            .has_feature(Feature::PerformanceIssuesSpans);

        if let Some(spans) = spans {
            let event_id = transaction.headers.event_id();
            let retention = ctx.retention(|r| r.span.as_ref());

            for span in spans.split(|spans| spans.into_iter()) {
                if let Ok(mut span) =
                    span.try_map(|span, _| store::convert_span(span, event_id, retention))
                {
                    if performance_issues_spans && *(span.item.is_segment.value().unwrap_or(&false))
                    {
                        span.modify(|span, _| {
                            span.performance_issues_spans = true;
                        });
                    }
                    s.send_to_store(span)
                };
            }
        }

        let (profile, transaction) = transaction.split_once(|mut tx, _| (tx.profile.take(), tx));
        if let Some(profile) = profile.transpose() {
            s.send_to_store(profile.map(|p, _| store::convert_profile(p, true, ctx)));
        }

        let envelope = transaction.try_map(|mut work, record_keeper| {
            // TODO: This should raise an error, Indexed output should go straight to Kafka
            // instead of an envelope. As long as we have this hack, ignore bookkeeping
            record_keeper.lenient(relay_quotas::DataCategory::Transaction);

            if let Some(event) = work.event.value_mut()
                && performance_issues_spans
            {
                event.performance_issues_spans = Annotated::new(true);
            }

            work.serialize_envelope()
                .map_err(drop)
                .with_outcome(Outcome::Invalid(DiscardReason::Internal))
        })?;

        s.send_envelope(envelope.into());

        Ok(())
    }
}

#[cfg(feature = "processing")]
mod store {
    use relay_event_schema::protocol::EventId;
    use relay_protocol::Annotated;

    use super::*;

    use crate::managed::Counted as _;
    use crate::processing::Retention;
    use crate::processing::transactions::types::{ExpandedProfile, ExtractedIndexedSpan};
    use crate::services::store::{StoreProfile, StoreSpanV2};

    pub fn convert_profile(
        profile: ExpandedProfile,
        sampled: bool,
        ctx: ForwardContext<'_>,
    ) -> StoreProfile {
        let retention_days = ctx.event_retention().standard;

        StoreProfile {
            retention_days,
            quantities: profile.quantities(),
            profile: {
                let mut item = profile.serialize_item();
                item.set_sampled(sampled);
                item
            },
        }
    }

    pub fn convert_span(
        span: ExtractedIndexedSpan,
        event_id: Option<EventId>,
        retentions: Retention,
    ) -> Result<Box<StoreSpanV2>, Outcome> {
        let span = match span.0 {
            Annotated(Some(span), _) => span,
            Annotated(None, meta) => {
                relay_log::debug!("dropping empty span with meta {meta:?}");
                return Err(Outcome::Invalid(DiscardReason::InvalidSpan));
            }
        };

        Ok(Box::new(StoreSpanV2 {
            routing_key: span.trace_id.value().copied().map(Into::into),
            retention_days: retentions.standard,
            downsampled_retention_days: retentions.downsampled,
            event_id,
            performance_issues_spans: false,
            item: span,
        }))
    }
}
