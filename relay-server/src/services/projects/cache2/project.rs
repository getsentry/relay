use std::sync::Arc;

use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_quotas::{CachedRateLimits, DataCategory, MetricNamespaceScoping, RateLimits};
use relay_sampling::evaluation::ReservoirCounters;

use crate::envelope::ItemType;
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::projects::cache2::state::SharedProject;
use crate::services::projects::project::ProjectState;
use crate::utils::{CheckLimits, Enforcement, EnvelopeLimiter, ManagedEnvelope};

/// A loaded project.
pub struct Project<'a> {
    shared: SharedProject,
    config: &'a Config,
}

impl<'a> Project<'a> {
    pub(crate) fn new(shared: SharedProject, config: &'a Config) -> Self {
        Self { shared, config }
    }

    pub fn project_state(&self) -> &ProjectState {
        self.shared.project_state()
    }

    pub fn rate_limits(&self) -> &CachedRateLimits {
        self.shared.cached_rate_limits()
    }

    pub fn reservoir_counters(&self) -> &ReservoirCounters {
        self.shared.reservoir_counters()
    }

    /// Runs the checks on incoming envelopes.
    ///
    /// See, [`crate::services::projects::cache::CheckEnvelope`] for more information
    ///
    /// * checks the rate limits
    /// * validates the envelope meta in `check_request` - determines whether the given request
    ///   should be accepted or discarded
    ///
    /// IMPORTANT: If the [`ProjectState`] is invalid, the `check_request` will be skipped and only
    /// rate limits will be validated. This function **must not** be called in the main processing
    /// pipeline.
    pub fn check_envelope(
        &self,
        mut envelope: ManagedEnvelope,
    ) -> Result<CheckedEnvelope, DiscardReason> {
        let state = match self.project_state() {
            ProjectState::Enabled(state) => Some(Arc::clone(&state)),
            ProjectState::Disabled => {
                // TODO(jjbayer): We should refactor this function to either return a Result or
                // handle envelope rejections internally, but not both.
                envelope.reject(Outcome::Invalid(DiscardReason::ProjectId));
                return Err(DiscardReason::ProjectId);
            }
            ProjectState::Pending => None,
        };

        let mut scoping = envelope.scoping();

        if let Some(ref state) = state {
            scoping = state.scope_request(envelope.envelope().meta());
            envelope.scope(scoping);

            if let Err(reason) = state.check_envelope(envelope.envelope(), &self.config) {
                envelope.reject(Outcome::Invalid(reason));
                return Err(reason);
            }
        }

        let current_limits = self.rate_limits().current_limits();

        let quotas = state.as_deref().map(|s| s.get_quotas()).unwrap_or(&[]);
        let envelope_limiter = EnvelopeLimiter::new(CheckLimits::NonIndexed, |item_scoping, _| {
            Ok(current_limits.check_with_quotas(quotas, item_scoping))
        });

        let (mut enforcement, mut rate_limits) =
            envelope_limiter.compute(envelope.envelope_mut(), &scoping)?;

        let check_nested_spans = state
            .as_ref()
            .is_some_and(|s| s.has_feature(Feature::ExtractSpansFromEvent));

        // If we can extract spans from the event, we want to try and count the number of nested
        // spans to correctly emit negative outcomes in case the transaction itself is dropped.
        if check_nested_spans {
            sync_spans_to_enforcement(&envelope, &mut enforcement);
        }

        enforcement.apply_with_outcomes(&mut envelope);

        envelope.update();

        // Special case: Expose active rate limits for all metric namespaces if there is at least
        // one metrics item in the Envelope to communicate backoff to SDKs. This is necessary
        // because `EnvelopeLimiter` cannot not check metrics without parsing item contents.
        if envelope.envelope().items().any(|i| i.ty().is_metrics()) {
            let mut metrics_scoping = scoping.item(DataCategory::MetricBucket);
            metrics_scoping.namespace = MetricNamespaceScoping::Any;
            rate_limits.merge(current_limits.check_with_quotas(quotas, metrics_scoping));
        }

        let envelope = if envelope.envelope().is_empty() {
            // Individual rate limits have already been issued above
            envelope.reject(Outcome::RateLimited(None));
            None
        } else {
            Some(envelope)
        };

        Ok(CheckedEnvelope {
            envelope,
            rate_limits,
        })
    }
}

/// A checked envelope and associated rate limits.
///
/// Items violating the rate limits have been removed from the envelope. If all items are removed
/// from the envelope, `None` is returned in place of the envelope.
#[derive(Debug)]
pub struct CheckedEnvelope {
    pub envelope: Option<ManagedEnvelope>,
    pub rate_limits: RateLimits,
}

/// Adds category limits for the nested spans inside a transaction.
///
/// On the fast path of rate limiting, we do not have nested spans of a transaction extracted
/// as top-level spans, thus if we limited a transaction, we want to count and emit negative
/// outcomes for each of the spans nested inside that transaction.
fn sync_spans_to_enforcement(envelope: &ManagedEnvelope, enforcement: &mut Enforcement) {
    if !enforcement.is_event_active() {
        return;
    }

    let spans_count = count_nested_spans(envelope);
    if spans_count == 0 {
        return;
    }

    if enforcement.event.is_active() {
        enforcement.spans = enforcement.event.clone_for(DataCategory::Span, spans_count);
    }

    if enforcement.event_indexed.is_active() {
        enforcement.spans_indexed = enforcement
            .event_indexed
            .clone_for(DataCategory::SpanIndexed, spans_count);
    }
}

/// Counts the nested spans inside the first transaction envelope item inside the [`Envelope`](crate::envelope::Envelope).
fn count_nested_spans(envelope: &ManagedEnvelope) -> usize {
    #[derive(Debug, serde::Deserialize)]
    struct PartialEvent {
        spans: crate::utils::SeqCount,
    }

    envelope
        .envelope()
        .items()
        .find(|item| *item.ty() == ItemType::Transaction && !item.spans_extracted())
        .and_then(|item| serde_json::from_slice::<PartialEvent>(&item.payload()).ok())
        // We do + 1, since we count the transaction itself because it will be extracted
        // as a span and counted during the slow path of rate limiting.
        .map_or(0, |event| event.spans.0 + 1)
}
