use relay_event_schema::protocol::{MetricType, TraceMetric};
use relay_protocol::Annotated;

use crate::managed::{Managed, Rejected};
use crate::processing::Context;
use crate::processing::trace_metrics::{
    Error, ExpandedTraceMetrics, Result, SerializedTraceMetrics, get_calculated_byte_size,
};
use crate::services::outcome::DiscardReason;
use crate::statsd::RelayDistributions;

/// Validates that there is only a single trace metric container processed at a time.
///
/// Similar to logs, the `TraceMetric` item must always be sent as an `ItemContainer`, currently it is not allowed to
/// send multiple containers for trace metrics.
///
/// This restriction may be lifted in the future.
///
/// This limit mostly exists to incentivise SDKs to batch multiple trace metrics into a single container,
/// technically it can be removed without issues.
pub fn container(metrics: &Managed<SerializedTraceMetrics>) -> Result<(), Rejected<Error>> {
    // It's fine if there was no trace metric container, as we may accept OTel trace metrics in the future.
    if metrics.metrics.len() > 1 {
        return Err(metrics.reject_err(Error::DuplicateContainer));
    }

    Ok(())
}

/// Validates contained metrics do not exceed the maximum size limit.
///
/// Currently this only considers the maximum metric size configured in the configuration.
///
/// In the future we may want to increase the limit or start trimming excessive
/// attributes/payloads. For now we drop metrics which exceed our size limit.
///
/// This matches the logic defined in [`check_envelope_size_limits`](crate::utils::check_envelope_size_limits),
/// when validating envelope sizes, the actual size of individual metrics is not known and therefore
/// must be enforced consistently after parsing again.
pub fn size(metrics: &mut Managed<ExpandedTraceMetrics>, ctx: Context<'_>) {
    let max_size_bytes = ctx.config.max_trace_metric_size();

    metrics.retain(
        |metrics| &mut metrics.metrics,
        |metric, _| {
            let size = get_calculated_byte_size(metric);
            let is_too_large = size > max_size_bytes;

            relay_statsd::metric!(
                distribution(RelayDistributions::TraceItemCanonicalSize) = size as u64,
                item = "trace_metric",
                too_large = if is_too_large { "true" } else { "false" },
            );

            match is_too_large {
                true => Err(Error::TooLarge),
                false => Ok(()),
            }
        },
    );
}

/// Validates the semantic validity of a trace metric.
pub fn validate(metrics: &mut Managed<ExpandedTraceMetrics>) {
    metrics.retain(
        |metrics| &mut metrics.metrics,
        |metric, _| {
            validate_trace_metric(metric).inspect_err(|err| {
                relay_log::debug!("failed to validate trace metric: {err}");
            })
        },
    );
}

fn validate_trace_metric(metric: &Annotated<TraceMetric>) -> Result<()> {
    match metric.value().and_then(|m| m.ty.value()) {
        Some(MetricType::Gauge | MetricType::Distribution | MetricType::Counter) => {}
        Some(MetricType::Unknown(_)) | None => {
            return Err(Error::Invalid(DiscardReason::InvalidTraceMetric));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::trace_metrics::utils::calculate_size;

    macro_rules! assert_calculated_size_of {
        ($expected:expr, $json:expr) => {{
            let json = $json;

            let metric = Annotated::<TraceMetric>::from_json(json)
                .unwrap()
                .into_value()
                .unwrap();

            let size = calculate_size(&metric);
            assert_eq!(size, $expected, "metric: {json}");
        }};
    }

    #[test]
    fn test_calculate_size_empty_metric_is_1byte() {
        assert_calculated_size_of!(1, r#"{}"#);
    }

    #[test]
    fn test_calculate_size_name_only() {
        assert_calculated_size_of!(
            11,
            r#"{
            "name": "test.metric"
        }"#
        );
    }

    #[test]
    fn test_calculate_size_name_and_value() {
        assert_calculated_size_of!(
            19,
            r#"{
            "name": "test.metric",
            "value": 123.45
        }"#
        );
    }

    #[test]
    fn test_calculate_size_string_attribute() {
        assert_calculated_size_of!(
            43,
            r#"{
            "name": "test.metric",
            "value": 1.0,
            "attributes": {
                "foo": {
                    "value": "ඞ and some more equals 33 bytes",
                    "type": "string"
                }
            }
        }"#
        );
    }

    #[test]
    fn test_calculate_size_integer_attribute() {
        assert_calculated_size_of!(
            30,
            r#"{
            "name": "test.metric",
            "value": 1.0,
            "attributes": {
                "foo": {
                    "value": 12,
                    "type": "integer"
                }
            }
        }"#
        );
    }

    #[test]
    fn test_calculate_size_bool_attribute() {
        assert_calculated_size_of!(
            23,
            r#"{
            "name": "test.metric",
            "value": 1.0,
            "attributes": {
                "foo": {
                    "value": true,
                    "type": "boolean"
                }
            }
        }"#
        );
    }

    #[test]
    fn test_calculate_size_null_attribute() {
        assert_calculated_size_of!(
            22,
            r#"{
            "name": "test.metric",
            "value": 1.0,
            "attributes": {
                "foo": {
                    "value": null,
                    "type": "double"
                }
            }
        }"#
        );
    }

    #[test]
    fn test_calculate_size_full_metric() {
        assert_calculated_size_of!(
            82,
            r#"{
            "timestamp": 946684800.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "name": "http.request.duration",
            "type": "distribution",
            "value": 123.45,
            "attributes": {
                "k1": {
                    "value": "string value",
                    "type": "string"
                },
                "k2": {
                    "value": 18446744073709551615,
                    "type": "integer"
                },
                "k3": {
                    "value": 42.0,
                    "type": "double"
                },
                "k4": {
                    "value": false,
                    "type": "boolean"
                }
            }
        }"#
        );
    }

    #[test]
    fn test_calculate_size_ignores_non_counted_fields() {
        assert_calculated_size_of!(
            19,
            r#"{
            "timestamp": 946684800.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "name": "test.metric",
            "type": "distribution",
            "unit": "millisecond",
            "value": 123.45
        }"#
        );
    }
}
