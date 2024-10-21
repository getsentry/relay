use std::sync::Arc;

use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_quotas::{CachedRateLimits, DataCategory, MetricNamespaceScoping, RateLimits};
use relay_sampling::evaluation::ReservoirCounters;

use crate::envelope::ItemType;
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::projects::cache::state::SharedProject;
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

#[cfg(test)]
mod tests {
    use crate::envelope::{ContentType, Envelope, Item};
    use crate::extractors::RequestMeta;
    use crate::services::processor::ProcessingGroup;
    use relay_base_schema::project::ProjectId;
    use relay_event_schema::protocol::EventId;
    use relay_test::mock_service;
    use serde_json::json;
    use smallvec::SmallVec;

    use super::*;

    #[test]
    fn get_state_expired() {
        for expiry in [9999, 0] {
            let config = Arc::new(
                Config::from_json_value(json!(
                    {
                        "cache": {
                            "project_expiry": expiry,
                            "project_grace_period": 0,
                            "eviction_interval": 9999 // do not evict
                        }
                    }
                ))
                .unwrap(),
            );

            // Initialize project with a state
            let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
            let project_info = ProjectInfo {
                project_id: Some(ProjectId::new(123)),
                ..Default::default()
            };
            let mut project = Project::new(project_key, config.clone());
            project.state = ProjectFetchState::enabled(project_info);

            if expiry > 0 {
                // With long expiry, should get a state
                assert!(matches!(project.current_state(), ProjectState::Enabled(_)));
            } else {
                // With 0 expiry, project should expire immediately. No state can be set.
                assert!(matches!(project.current_state(), ProjectState::Pending));
            }
        }
    }

    #[tokio::test]
    async fn test_stale_cache() {
        let (addr, _) = mock_service("project_cache", (), |&mut (), _| {});

        let config = Arc::new(
            Config::from_json_value(json!(
                {
                    "cache": {
                        "project_expiry": 100,
                        "project_grace_period": 0,
                        "eviction_interval": 9999 // do not evict
                    }
                }
            ))
            .unwrap(),
        );

        let channel = StateChannel::new();

        // Initialize project with a state.
        let project_key = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let mut project = Project::new(project_key, config);
        project.state_channel = Some(channel);
        project.state = ProjectFetchState::allowed();

        assert!(project.next_fetch_attempt.is_none());
        // Try to update project with errored project state.
        project.update_state(&addr, ProjectFetchState::pending(), false);
        // Since we got invalid project state we still keep the old one meaning there
        // still must be the project id set.
        assert!(matches!(project.current_state(), ProjectState::Enabled(_)));
        assert!(project.next_fetch_attempt.is_some());

        // This tests that we actually initiate the backoff and the backoff mechanism works:
        // * first call to `update_state` with invalid ProjectState starts the backoff, but since
        //   it's the first attemt, we get Duration of 0.
        // * second call to `update_state` here will bumpt the `next_backoff` Duration to somehing
        //   like ~ 1s
        // * and now, by calling `fetch_state` we test that it's a noop, since if backoff is active
        //   we should never fetch
        // * without backoff it would just panic, not able to call the ProjectCache service
        let channel = StateChannel::new();
        project.state_channel = Some(channel);
        project.update_state(&addr, ProjectFetchState::pending(), false);
        project.fetch_state(addr, false);
    }

    fn create_project(config: Option<serde_json::Value>) -> Project {
        let project_key = ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap();
        let mut project = Project::new(project_key, Arc::new(Config::default()));
        let mut project_info = ProjectInfo {
            project_id: Some(ProjectId::new(42)),
            ..Default::default()
        };
        let mut public_keys = SmallVec::new();
        public_keys.push(PublicKeyConfig {
            public_key: project_key,
            numeric_id: None,
        });
        project_info.public_keys = public_keys;
        if let Some(config) = config {
            project_info.config = serde_json::from_value(config).unwrap();
        }
        project.state = ProjectFetchState::enabled(project_info);
        project
    }

    fn request_meta() -> RequestMeta {
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        RequestMeta::new(dsn)
    }

    #[test]
    fn test_track_nested_spans_outcomes() {
        let mut project = create_project(Some(json!({
            "features": [
                "organizations:indexed-spans-extraction"
            ],
            "quotas": [{
               "id": "foo",
               "categories": ["transaction"],
               "window": 3600,
               "limit": 0,
               "reasonCode": "foo",
           }]
        })));

        let mut envelope = Envelope::from_request(Some(EventId::new()), request_meta());

        let mut transaction = Item::new(ItemType::Transaction);
        transaction.set_payload(
            ContentType::Json,
            r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "type": "transaction",
  "transaction": "I have a stale timestamp, but I'm recent!",
  "start_timestamp": 1,
  "timestamp": 2,
  "contexts": {
    "trace": {
      "trace_id": "ff62a8b040f340bda5d830223def1d81",
      "span_id": "bd429c44b67a3eb4"
    }
  },
  "spans": [
    {
      "span_id": "bd429c44b67a3eb4",
      "start_timestamp": 1,
      "timestamp": null,
      "trace_id": "ff62a8b040f340bda5d830223def1d81"
    },
    {
      "span_id": "bd429c44b67a3eb5",
      "start_timestamp": 1,
      "timestamp": null,
      "trace_id": "ff62a8b040f340bda5d830223def1d81"
    }
  ]
}"#,
        );

        envelope.add_item(transaction);

        let (outcome_aggregator, mut outcome_aggregator_rx) = Addr::custom();
        let (test_store, _) = Addr::custom();

        let managed_envelope = ManagedEnvelope::new(
            envelope,
            outcome_aggregator.clone(),
            test_store,
            ProcessingGroup::Transaction,
        );

        let _ = project.check_envelope(managed_envelope);
        drop(outcome_aggregator);

        let expected = [
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
            (DataCategory::Span, 3),
            (DataCategory::SpanIndexed, 3),
        ];

        for (expected_category, expected_quantity) in expected {
            let outcome = outcome_aggregator_rx.blocking_recv().unwrap();
            assert_eq!(outcome.category, expected_category);
            assert_eq!(outcome.quantity, expected_quantity);
        }
    }
}
