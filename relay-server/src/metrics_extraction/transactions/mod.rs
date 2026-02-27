use relay_base_schema::events::EventType;
use relay_base_schema::project::ProjectId;
use relay_common::time::UnixTimestamp;
use relay_event_schema::protocol::Event;
use relay_metrics::Bucket;
use relay_sampling::evaluation::SamplingDecision;

use crate::metrics_extraction::IntoMetric;
use crate::metrics_extraction::transactions::types::{
    ExtractMetricsError, TransactionCPRTags, TransactionMetric,
};

pub mod types;

/// Metrics extracted from an envelope.
///
/// Metric extraction derives pre-computed metrics (time series data) from payload items in
/// envelopes. Depending on their semantics, these metrics can be ingested into the same project as
/// the envelope or a different project.
#[derive(Debug, Default)]
pub struct ExtractedMetrics {
    /// Metrics associated with the project of the envelope.
    pub project_metrics: Vec<Bucket>,

    /// Metrics associated with the project of the trace parent.
    pub sampling_metrics: Vec<Bucket>,
}

/// A utility that extracts metrics from transactions.
pub struct TransactionExtractor<'a> {
    pub transaction_from_dsc: Option<&'a str>,
    pub sampling_decision: SamplingDecision,
    pub target_project_id: ProjectId,
}

impl TransactionExtractor<'_> {
    pub fn extract(&self, event: &Event) -> Result<ExtractedMetrics, ExtractMetricsError> {
        let mut metrics = ExtractedMetrics::default();

        if event.ty.value() != Some(&EventType::Transaction) {
            return Ok(metrics);
        }

        let (Some(&_start), Some(&end)) = (event.start_timestamp.value(), event.timestamp.value())
        else {
            relay_log::debug!("failed to extract the start and the end timestamps from the event");
            return Err(ExtractMetricsError::MissingTimestamp);
        };

        let Some(timestamp) = UnixTimestamp::from_datetime(end.into_inner()) else {
            relay_log::debug!("event timestamp is not a valid unix timestamp");
            return Err(ExtractMetricsError::InvalidTimestamp);
        };

        // Internal usage counter
        metrics
            .project_metrics
            .push(TransactionMetric::Usage.into_metric(timestamp));

        // Count per root project for dynamic sampling
        let root_counter_tags = TransactionCPRTags {
            decision: self.sampling_decision.to_string(),
            target_project_id: self.target_project_id,
            transaction: self.transaction_from_dsc.map(|t| t.to_owned()),
        };
        metrics.sampling_metrics.push(
            TransactionMetric::CountPerRootProject {
                tags: root_counter_tags,
            }
            .into_metric(timestamp),
        );

        Ok(metrics)
    }
}

#[cfg(test)]
mod tests {
    use relay_event_normalization::{NormalizationConfig, normalize_event};
    use relay_metrics::BucketValue;
    use relay_protocol::Annotated;

    use super::*;

    #[test]
    fn test_extract_transaction_metrics() {
        let json = r#"
        {
            "type": "transaction",
            "platform": "javascript",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "timestamp": "2021-04-26T08:00:00+0100",
            "release": "1.2.3",
            "transaction": "gEt /api/:version/users/",
            "transaction_info": {"source": "custom"},
            "contexts": {
                "trace": {
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "span_id": "bd429c44b67a3eb4",
                    "op": "myOp",
                    "status": "ok"
                }
            }
        }
        "#;

        let mut event = Annotated::from_json(json).unwrap();
        normalize_event(
            &mut event,
            &NormalizationConfig {
                ..Default::default()
            },
        );

        let extractor = TransactionExtractor {
            transaction_from_dsc: Some("test_transaction"),
            sampling_decision: SamplingDecision::Keep,
            target_project_id: ProjectId::new(42),
        };

        let metrics = extractor.extract(event.value().unwrap()).unwrap();

        // Should only have Usage in project_metrics
        assert_eq!(metrics.project_metrics.len(), 1);
        assert_eq!(
            &*metrics.project_metrics[0].name,
            "c:transactions/usage@none"
        );
        assert!(matches!(
            metrics.project_metrics[0].value,
            BucketValue::Counter(_)
        ));

        // Should only have CountPerRootProject in sampling_metrics
        assert_eq!(metrics.sampling_metrics.len(), 1);
        assert_eq!(
            &*metrics.sampling_metrics[0].name,
            "c:transactions/count_per_root_project@none"
        );
        assert_eq!(metrics.sampling_metrics[0].tags["decision"], "keep");
        assert_eq!(metrics.sampling_metrics[0].tags["target_project_id"], "42");
        assert_eq!(
            metrics.sampling_metrics[0].tags["transaction"],
            "test_transaction"
        );
    }

    #[test]
    fn test_extract_transaction_metrics_non_transaction() {
        let json = r#"
        {
            "type": "error",
            "timestamp": "2021-04-26T08:00:00+0100"
        }
        "#;

        let event = Annotated::<Event>::from_json(json).unwrap();

        let extractor = TransactionExtractor {
            transaction_from_dsc: None,
            sampling_decision: SamplingDecision::Keep,
            target_project_id: ProjectId::new(42),
        };

        let metrics = extractor.extract(event.value().unwrap()).unwrap();
        assert!(metrics.project_metrics.is_empty());
        assert!(metrics.sampling_metrics.is_empty());
    }
}
