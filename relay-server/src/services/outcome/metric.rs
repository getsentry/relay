use std::collections::BTreeMap;
use std::sync::LazyLock;

use relay_config::Config;
use relay_metrics::{Bucket, BucketMetadata, BucketValue, MetricName, UnixTimestamp};

#[cfg(any(test, feature = "processing"))]
use crate::services::outcome::OutcomeId;
use crate::services::outcome::{Outcome, TrackOutcome};

/// Metric MRI for [`OutcomeId::ACCEPTED`] outcomes.
const OUTCOME_ACCEPTED_MRI: &str = "c:outcomes/accepted@none";
/// Metric MRI for [`OutcomeId::FILTERED`] outcomes.
const FILTERED_MRI: &str = "c:outcomes/filtered@none";
/// Metric MRI for [`OutcomeId::RATE_LIMITED`] outcomes.
const RATE_LIMITED_MRI: &str = "c:outcomes/rate_limited@none";
/// Metric MRI for [`OutcomeId::INVALID`] outcomes.
const INVALID_MRI: &str = "c:outcomes/invalid@none";
/// Metric MRI for [`OutcomeId::ABUSE`] outcomes.
const ABUSE_MRI: &str = "c:outcomes/abuse@none";
/// Metric MRI for [`OutcomeId::CLIENT_DISCARD`] outcomes.
const CLIENT_DISCARD_MRI: &str = "c:outcomes/client_discard@none";
/// Metric MRI for [`OutcomeId::CARDINALITY_LIMITED`] outcomes.
#[cfg(any(test, feature = "processing"))]
const CARDINALITY_LIMITED_MRI: &str = "c:outcomes/cardinality_limited@none";

/// Converts a [`TrackOutcome`] to a metric [`Bucket`].
pub fn to_metric(outcome: &TrackOutcome, config: &Config) -> Bucket {
    static ACCEPTED: LazyLock<MetricName> = LazyLock::new(|| OUTCOME_ACCEPTED_MRI.into());
    static FILTERED: LazyLock<MetricName> = LazyLock::new(|| FILTERED_MRI.into());
    static RATE_LIMITED: LazyLock<MetricName> = LazyLock::new(|| RATE_LIMITED_MRI.into());
    static INVALID: LazyLock<MetricName> = LazyLock::new(|| INVALID_MRI.into());
    static ABUSE: LazyLock<MetricName> = LazyLock::new(|| ABUSE_MRI.into());
    static CLIENT_DISCARD: LazyLock<MetricName> = LazyLock::new(|| CLIENT_DISCARD_MRI.into());

    let TrackOutcome {
        timestamp,
        // Metrics are attached to the project key already, no need to add information from the scoping.
        scoping: _,
        outcome,
        event_id,
        remote_addr,
        category,
        quantity,
    } = outcome;

    let name = match outcome {
        Outcome::Accepted => ACCEPTED.clone(),
        Outcome::Filtered(_) => FILTERED.clone(),
        Outcome::FilteredSampling(_) => FILTERED.clone(),
        Outcome::RateLimited(_) => RATE_LIMITED.clone(),
        Outcome::Invalid(_) => INVALID.clone(),
        Outcome::Abuse => ABUSE.clone(),
        Outcome::ClientDiscard(_) => CLIENT_DISCARD.clone(),
    };

    let tags = {
        let mut tags = BTreeMap::new();
        // `TrackOutcome` can only be created within this Relay -> set the source.
        if let Some(source) = config.outcome_source() {
            tags.insert("source".to_owned(), source.to_owned());
        }
        if let Some(reason) = outcome.to_reason() {
            tags.insert("reason".to_owned(), reason.to_string());
        }
        // We stopped adding event id and remote addr to outcomes for cardinality reasons.
        let _ = event_id;
        let _ = remote_addr;
        if let Some(category) = category.value() {
            tags.insert("category".to_owned(), category.to_string());
        }
        tags
    };

    let timestamp = UnixTimestamp::from_datetime(*timestamp).unwrap_or_else(UnixTimestamp::now);

    Bucket {
        name,
        value: BucketValue::Counter((*quantity).into()),
        timestamp,
        tags,
        width: 0,
        metadata: BucketMetadata::new(timestamp),
    }
}

/// Converts a outcome metric name to a [`OutcomeId`].
///
/// Returns `None` for unknown or invalid metric names.
#[cfg(any(test, feature = "processing"))]
pub fn to_outcome_id(name: &MetricName) -> Option<OutcomeId> {
    Some(match name.as_ref() {
        OUTCOME_ACCEPTED_MRI => OutcomeId::ACCEPTED,
        FILTERED_MRI => OutcomeId::FILTERED,
        RATE_LIMITED_MRI => OutcomeId::RATE_LIMITED,
        INVALID_MRI => OutcomeId::INVALID,
        ABUSE_MRI => OutcomeId::ABUSE,
        CLIENT_DISCARD_MRI => OutcomeId::CLIENT_DISCARD,
        CARDINALITY_LIMITED_MRI => OutcomeId::CARDINALITY_LIMITED,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use relay_base_schema::data_category::DataCategory;
    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::ProjectId;
    use relay_filter::FilterStatKey;
    use relay_metrics::{MetricNamespace, MetricType};
    use relay_quotas::Scoping;

    use crate::services::outcome::{DiscardReason, RuleCategories};

    use super::*;

    fn scoping() -> Scoping {
        Scoping {
            organization_id: OrganizationId::new(42),
            project_id: ProjectId::new(43),
            project_key: "a94ae32be2584e0bbd7a4cbb95971fee".parse().unwrap(),
            key_id: Some(12),
        }
    }

    fn config() -> Config {
        Config::from_json_value(serde_json::json!({
            "outcomes": {
                "source": "I bims",
            }
        }))
        .unwrap()
    }

    #[test]
    fn test_metric_mri_valid() {
        let name = MetricName::from(OUTCOME_ACCEPTED_MRI);
        assert_eq!(name.try_namespace(), Some(MetricNamespace::Outcomes));
        assert_eq!(name.try_name(), Some("accepted"));
        assert_eq!(name.try_type(), Some(MetricType::Counter));

        let name = MetricName::from(FILTERED_MRI);
        assert_eq!(name.try_namespace(), Some(MetricNamespace::Outcomes));
        assert_eq!(name.try_name(), Some("filtered"));
        assert_eq!(name.try_type(), Some(MetricType::Counter));

        let name = MetricName::from(RATE_LIMITED_MRI);
        assert_eq!(name.try_namespace(), Some(MetricNamespace::Outcomes));
        assert_eq!(name.try_name(), Some("rate_limited"));
        assert_eq!(name.try_type(), Some(MetricType::Counter));

        let name = MetricName::from(INVALID_MRI);
        assert_eq!(name.try_namespace(), Some(MetricNamespace::Outcomes));
        assert_eq!(name.try_name(), Some("invalid"));
        assert_eq!(name.try_type(), Some(MetricType::Counter));

        let name = MetricName::from(ABUSE_MRI);
        assert_eq!(name.try_namespace(), Some(MetricNamespace::Outcomes));
        assert_eq!(name.try_name(), Some("abuse"));
        assert_eq!(name.try_type(), Some(MetricType::Counter));

        let name = MetricName::from(CLIENT_DISCARD_MRI);
        assert_eq!(name.try_namespace(), Some(MetricNamespace::Outcomes));
        assert_eq!(name.try_name(), Some("client_discard"));
        assert_eq!(name.try_type(), Some(MetricType::Counter));
    }

    #[test]
    fn test_to_metric_invalid() {
        let outcome = TrackOutcome {
            timestamp: chrono::DateTime::from_timestamp_nanos(123_000_000_000),
            scoping: scoping(),
            outcome: Outcome::Invalid(DiscardReason::InvalidEventId),
            event_id: Some("ec75d3980a1f42638ec45f091c2d9b24".parse().unwrap()),
            remote_addr: Some([192, 168, 2, 1].into()),
            category: DataCategory::Error,
            quantity: 42,
        };

        let bucket = to_metric(&outcome, &config());

        insta::assert_json_snapshot!(&bucket, @r#"
        {
          "timestamp": 123,
          "width": 0,
          "name": "c:outcomes/invalid@none",
          "type": "c",
          "value": 42.0,
          "tags": {
            "category": "1",
            "reason": "invalid_event_id",
            "source": "I bims"
          },
          "metadata": {
            "merges": 1,
            "received_at": 123
          }
        }
        "#);
    }

    #[test]
    fn test_to_metric_accepted() {
        let outcome = TrackOutcome {
            timestamp: chrono::DateTime::from_timestamp_nanos(123_000_000_000),
            scoping: scoping(),
            outcome: Outcome::Accepted,
            event_id: None,
            remote_addr: None,
            category: DataCategory::Error,
            quantity: 42,
        };

        let bucket = to_metric(&outcome, &config());

        insta::assert_json_snapshot!(&bucket, @r#"
        {
          "timestamp": 123,
          "width": 0,
          "name": "c:outcomes/accepted@none",
          "type": "c",
          "value": 42.0,
          "tags": {
            "category": "1",
            "source": "I bims"
          },
          "metadata": {
            "merges": 1,
            "received_at": 123
          }
        }
        "#);
    }

    #[test]
    fn test_to_outcome_id_roundtrip() {
        let outcomes = [
            Outcome::Accepted,
            Outcome::Filtered(FilterStatKey::IpAddress),
            Outcome::FilteredSampling(RuleCategories(Default::default())),
            Outcome::RateLimited(None),
            Outcome::Invalid(DiscardReason::Duplicate),
            Outcome::Abuse,
            Outcome::ClientDiscard("foo".to_owned()),
        ];

        for outcome in outcomes {
            let bucket = to_metric(
                &TrackOutcome {
                    timestamp: chrono::DateTime::from_timestamp_nanos(123_000_000_000),
                    scoping: scoping(),
                    outcome: outcome.clone(),
                    event_id: None,
                    remote_addr: None,
                    category: DataCategory::Error,
                    quantity: 42,
                },
                &config(),
            );

            let id = to_outcome_id(&bucket.name).unwrap();
            assert_eq!(id, outcome.to_outcome_id(), "{} | {outcome:?}", bucket.name);
        }
    }
}
