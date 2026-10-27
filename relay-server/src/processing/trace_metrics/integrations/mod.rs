use relay_event_schema::protocol::{TraceMetric, TraceMetricHeader};
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, Item, WithHeader};
use crate::integrations::{Integration, TraceMetricsIntegration};
use crate::managed::RecordKeeper;
use crate::processing::trace_metrics::Settings;
use crate::processing::trace_metrics::utils::calculate_size;

mod otel;

/// Expands a trace metric [`Integration`] into a list of trace metrics.
///
/// The function expects *only* trace metric item integrations.
pub fn expand(
    item: Item,
    records: &mut RecordKeeper<'_>,
) -> Option<(Settings, ContainerItems<TraceMetric>)> {
    let integration = match item.integration() {
        Some(Integration::TraceMetrics(integration)) => integration,
        integration => {
            records.internal_error(InvalidIntegration(integration), item);
            return None;
        }
    };

    let mut metrics = Vec::new();
    let produce = |metric: TraceMetric| {
        let byte_size = calculate_size(&metric);

        records.modify_by(DataCategory::TraceMetric, 1);
        records.modify_by(DataCategory::TraceMetricByte, byte_size as isize);

        metrics.push(WithHeader {
            header: Some(TraceMetricHeader {
                byte_size: Some(byte_size),
                other: Default::default(),
            }),
            value: metric.into(),
        });
    };

    let payload = item.payload();

    let settings = match integration {
        TraceMetricsIntegration::OtelV1 { format } => otel::expand(format, &payload, produce),
    };
    let settings = match settings {
        Ok(settings) => settings,
        Err(err) => {
            let _ = records.reject_err(err, &item);
            return None;
        }
    };

    // Undo all the base item quantities, as they will be completely taken over by the parsed
    // contents, which contains an arbitrary amount of items (even 0).
    for (category, quantity) in item.quantities() {
        records.modify_by(category, -(quantity as isize));
    }

    Some((settings, metrics))
}

#[derive(Debug, thiserror::Error)]
#[error("Expected a trace metrics integration, got: {0:?}")]
struct InvalidIntegration(Option<Integration>);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use relay_dynamic_config::{Feature, FeatureSet, ProjectConfig};
    use relay_event_schema::protocol::MetricType;

    use crate::Envelope;
    use crate::envelope::{ContentType, ItemType};
    use crate::extractors::RequestMeta;
    use crate::integrations::OtelFormat;
    use crate::managed::ManagedEnvelope;
    use crate::processing::trace_metrics::TraceMetricsProcessor;
    use crate::processing::{Context, Processor, QuotaRateLimiter};
    use crate::services::projects::project::ProjectInfo;

    use super::*;

    fn otlp_envelope(payload: String) -> Box<Envelope> {
        let meta = RequestMeta::new(
            "https://a94ae32be2584e0bbd7a4cbb95971fee@sentry.io/42"
                .parse()
                .unwrap(),
        );
        let mut envelope = Envelope::from_request(None, meta);
        let mut item = Item::new(ItemType::Integration);
        item.set_payload(
            ContentType::Integration(Integration::TraceMetrics(TraceMetricsIntegration::OtelV1 {
                format: OtelFormat::Json,
            })),
            payload,
        );
        envelope.add_item(item);
        envelope
    }

    /// Runs an OTLP metrics payload through the full trace metrics processor.
    #[tokio::test]
    async fn test_processor_end_to_end() {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        let payload = format!(
            r#"{{"resourceMetrics":[{{"scopeMetrics":[{{"metrics":[
                {{"name":"requests","sum":{{"aggregationTemporality":1,"isMonotonic":true,
                  "dataPoints":[{{"timeUnixNano":"{now}","asInt":"3"}}]}}}},
                {{"name":"ignored","sum":{{"aggregationTemporality":2,
                  "dataPoints":[{{"timeUnixNano":"{now}","asInt":"3"}}]}}}},
                {{"name":"latency","unit":"ms","histogram":{{"aggregationTemporality":1,
                  "dataPoints":[{{"timeUnixNano":"{now}","count":"3","sum":30,
                    "bucketCounts":["1","2"],"explicitBounds":[10],"min":5,"max":15,
                    "exemplars":[{{"timeUnixNano":"{now}","asDouble":12}}]}}]}}}}
            ]}}]}}]}}"#
        );

        #[cfg(feature = "processing")]
        let limiter = QuotaRateLimiter::new(
            crate::services::projects::cache::ProjectCacheHandle::for_test(),
            None,
        );
        #[cfg(not(feature = "processing"))]
        let limiter = QuotaRateLimiter::new();
        let processor = TraceMetricsProcessor::new(Arc::new(limiter));

        let project_info = ProjectInfo {
            config: ProjectConfig {
                features: FeatureSet::from_iter([Feature::TraceMetricsIngestion]),
                ..Default::default()
            },
            ..Default::default()
        };
        let ctx = Context {
            project_info: &project_info,
            ..Context::for_test()
        };

        let (outcomes, mut received) = relay_system::Addr::custom();
        let mut envelope = ManagedEnvelope::new(otlp_envelope(payload), outcomes);
        let serialized = processor.prepare_envelope(&mut envelope).unwrap();
        assert!(envelope.envelope().is_empty());
        envelope.accept();

        let output = processor.process(serialized, ctx).await.unwrap();
        let metrics = output.main.unwrap().0;

        let rows: Vec<_> = metrics
            .metrics
            .iter()
            .map(|m| {
                let m = m.value().unwrap();
                let kind = m
                    .attributes
                    .value()
                    .and_then(|a| a.get_value("sentry._internal.row_kind"))
                    .and_then(|v| v.as_str())
                    .unwrap()
                    .to_owned();
                (
                    m.name.as_str().unwrap().to_owned(),
                    m.ty.value().cloned().unwrap(),
                    kind,
                )
            })
            .collect();

        assert_eq!(
            rows,
            [
                (
                    "requests".to_owned(),
                    MetricType::Counter,
                    "point".to_owned()
                ),
                (
                    "latency".to_owned(),
                    MetricType::Histogram,
                    "bucket".to_owned()
                ),
                (
                    "latency".to_owned(),
                    MetricType::Histogram,
                    "bucket".to_owned()
                ),
                (
                    "latency".to_owned(),
                    MetricType::Histogram,
                    "exemplar".to_owned()
                ),
            ]
        );

        metrics.accept(|_| ());
        // No outcomes, all rows were accepted and the dropped cumulative point is not counted.
        assert!(received.try_recv().is_err());
    }
}
