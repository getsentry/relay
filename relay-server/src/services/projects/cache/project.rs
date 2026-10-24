use std::marker::PhantomData;
use std::sync::Arc;

use relay_config::ConfigSnapshot;
use relay_quotas::{CachedRateLimits, RateLimits};

use crate::Envelope;
use crate::managed::{Managed, Rejected};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::projects::cache::state::SharedProject;
use crate::services::projects::project::ProjectState;
use crate::utils::enforce_rate_limits;

/// A loaded project.
pub struct Project<'a> {
    shared: SharedProject,
    config: ConfigSnapshot,
    // This lifetime is a leftover from before we started introducing a reloadable
    // configuration. It's not yet removed to keep changes a bit more isolated to config.
    //
    // This will be removed in a follow-up PR. I promise.
    _lifetime: PhantomData<&'a ()>,
}

impl<'a> Project<'a> {
    pub(crate) fn new(shared: SharedProject, config: ConfigSnapshot) -> Self {
        Self {
            shared,
            config,
            _lifetime: PhantomData,
        }
    }

    /// Returns a reference to the currently cached project state.
    pub fn state(&self) -> &ProjectState {
        self.shared.project_state()
    }

    /// Returns a reference to the currently cached rate limits.
    pub fn rate_limits(&self) -> &CachedRateLimits {
        self.shared.cached_rate_limits()
    }

    /// Checks the envelope against project configuration and rate limits.
    ///
    /// When `fetched`, then the project state is ensured to be up to date. When `cached`, an outdated
    /// project state may be used, or otherwise the envelope is passed through unaltered.
    ///
    /// To check the envelope, this runs:
    ///  - Validate origins and public keys
    ///  - Quotas with a limit of `0`
    ///  - Cached rate limits
    pub fn check_envelope(
        &self,
        envelope: &mut Managed<Box<Envelope>>,
    ) -> Result<RateLimits, Rejected<DiscardReason>> {
        let state = match self.state() {
            ProjectState::Enabled(state) => Some(Arc::clone(state)),
            ProjectState::Dummy => None,
            ProjectState::Disabled => {
                // TODO(jjbayer): We should refactor this function to either return a Result or
                // handle envelope rejections internally, but not both.
                let err = envelope
                    .reject_err(Outcome::Invalid(DiscardReason::ProjectId))
                    .map(|_| DiscardReason::ProjectId);
                return Err(err);
            }
            ProjectState::Pending => None,
        };

        if let Some(ref state) = state {
            let scoping = state.scope_request(envelope.meta());
            envelope.scope(scoping);

            if let Err(reason) = state.check_envelope(envelope, &self.config) {
                return Err(envelope
                    .reject_err(Outcome::Invalid(reason))
                    .map(|_| reason));
            }
        }

        let current_limits = self.rate_limits().current_limits();

        let quotas = state.as_deref().map(|s| s.get_quotas()).unwrap_or(&[]);

        Ok(enforce_rate_limits(envelope, &current_limits, quotas))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::envelope::{ContentType, Envelope, Item, ItemType};
    use crate::extractors::RequestMeta;
    use crate::services::projects::project::{ProjectInfo, PublicKeyConfig};
    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_event_schema::protocol::EventId;
    use relay_quotas::{DataCategory, ReasonCode};
    use serde_json::json;
    use smallvec::smallvec;

    use super::*;

    fn create_project(config: ConfigSnapshot, data: Option<serde_json::Value>) -> Project<'static> {
        let mut project_info = ProjectInfo {
            project_id: Some(ProjectId::new(42)),
            ..Default::default()
        };
        project_info.public_keys = smallvec![PublicKeyConfig {
            public_key: ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
            numeric_id: None,
        }];

        if let Some(data) = data {
            project_info.config = serde_json::from_value(data).unwrap();
        }

        Project::new(
            SharedProject::for_test(ProjectState::Enabled(project_info.into())),
            config,
        )
    }

    fn request_meta() -> RequestMeta {
        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        RequestMeta::new(dsn)
    }

    fn get_span_count(envelope: &Envelope) -> usize {
        envelope.items().next().unwrap().span_count().unwrap_or(0) as usize
    }

    #[tokio::test]
    async fn test_track_nested_spans_outcomes() {
        let config = relay_config::Config::default().current();
        let project = create_project(
            config,
            Some(json!({
                "quotas": [{
                   "id": "foo",
                   "categories": ["transaction"],
                   "window": 3600,
                   "limit": 0,
                   "reasonCode": "foo",
               }]
            })),
        );

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

        let (outcome_aggregator, mut outcome_aggregator_rx) = relay_system::Addr::custom();

        let mut managed_envelope = Managed::from_envelope(envelope, outcome_aggregator);

        assert_eq!(get_span_count(&managed_envelope), 0); // not written yet
        project.check_envelope(&mut managed_envelope).unwrap();

        let expected = [
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
            (DataCategory::Span, 3),
            (DataCategory::SpanIndexed, 3),
        ];

        let mut actual = BTreeMap::new();
        while let Ok(outcome) = outcome_aggregator_rx.try_recv() {
            assert_eq!(
                outcome.outcome,
                Outcome::RateLimited(Some(ReasonCode::new("foo")))
            );
            *actual.entry(outcome.category).or_insert(0) += outcome.quantity;
        }
        assert_eq!(actual, BTreeMap::from(expected));
    }

    #[tokio::test]
    async fn test_track_nested_spans_outcomes_predefined() {
        let config = relay_config::Config::default().current();
        let project = create_project(
            config,
            Some(json!({
                "quotas": [{
                   "id": "foo",
                   "categories": ["transaction"],
                   "window": 3600,
                   "limit": 0,
                   "reasonCode": "foo",
               }]
            })),
        );

        let mut envelope = Envelope::from_request(Some(EventId::new()), request_meta());

        let mut transaction = Item::new(ItemType::Transaction);
        transaction.set_span_count(Some(666));
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
  "spans": []
}"#,
        );

        envelope.add_item(transaction);

        let (outcome_aggregator, mut outcome_aggregator_rx) = relay_system::Addr::custom();

        let mut managed_envelope = Managed::from_envelope(envelope, outcome_aggregator);

        assert_eq!(get_span_count(&managed_envelope), 666);
        project.check_envelope(&mut managed_envelope).unwrap();

        let expected = [
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
            (DataCategory::Span, 667),
            (DataCategory::SpanIndexed, 667),
        ];

        let mut actual = BTreeMap::new();
        while let Ok(outcome) = outcome_aggregator_rx.try_recv() {
            assert_eq!(
                outcome.outcome,
                Outcome::RateLimited(Some(ReasonCode::new("foo")))
            );
            *actual.entry(outcome.category).or_insert(0) += outcome.quantity;
        }
        assert_eq!(actual, BTreeMap::from(expected));
    }
}
