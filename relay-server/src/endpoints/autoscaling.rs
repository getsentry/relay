use crate::http::StatusCode;
use crate::service::ServiceState;
use crate::services::autoscaling::{AutoscalingData, AutoscalingMessageKind};
use std::fmt::Display;
use std::fmt::Write;

/// Returns internal metrics data for relay.
pub async fn handle(state: ServiceState) -> (StatusCode, String) {
    let data = match state
        .autoscaling()
        .send(AutoscalingMessageKind::Check)
        .await
    {
        Ok(data) => data,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to collect internal metrics".to_string(),
            )
        }
    };

    (StatusCode::OK, to_prometheus_string(&data))
}

/// Serializes the autoscaling data into a prometheus string.
fn to_prometheus_string(data: &AutoscalingData) -> String {
    let mut result = String::with_capacity(2048);

    append_data_row(&mut result, "memory_usage", data.memory_usage, &[]);
    append_data_row(&mut result, "up", data.up, &[]);
    append_data_row(&mut result, "spool_item_count", data.item_count, &[]);
    append_data_row(&mut result, "spool_total_size", data.total_size, &[]);
    for utilization in &data.services_metrics {
        let service_name = extract_service_name(utilization.0);
        append_data_row(
            &mut result,
            "utilization",
            utilization.1,
            &[("relay_service", service_name)],
        );
    }
    append_data_row(
        &mut result,
        "worker_pool_utilization",
        data.worker_pool_utilization,
        &[],
    );
    result
}

fn append_data_row(result: &mut String, label: &str, data: impl Display, tags: &[(&str, &str)]) {
    // Metrics are automatically prefixed with "relay_"
    write!(result, "relay_{label}").unwrap();
    if !tags.is_empty() {
        result.push('{');
        for (idx, (key, value)) in tags.iter().enumerate() {
            if idx > 0 {
                result.push_str(", ");
            }
            write!(result, "{key}=\"{value}\"").unwrap();
        }
        result.push('}');
    }
    writeln!(result, " {data}").unwrap();
}

/// Extracts the concrete Service name from a string with a namespace,
/// In case there are no ':' because a custom name is used, then the full name is returned.
/// For example:
/// * `relay::services::MyService` -> `MyService`.
/// * `aggregator_service` -> `aggregator_service`.
fn extract_service_name(full_name: &str) -> &str {
    full_name
        .rsplit_once(':')
        .map(|(_, s)| s)
        .unwrap_or(full_name)
}

#[cfg(test)]
mod test {
    use crate::endpoints::autoscaling::{append_data_row, extract_service_name};
    use crate::services::autoscaling::{AutoscalingData, ServiceUtilization};

    #[test]
    fn test_extract_service_with_namespace() {
        let service_name = extract_service_name("relay::services::MyService");
        assert_eq!(service_name, "MyService");
    }

    #[test]
    fn test_extract_service_without_namespace() {
        let service_name = extract_service_name("custom_service");
        assert_eq!(service_name, "custom_service");
    }

    #[test]
    fn test_append_no_labels() {
        let mut result = String::new();
        append_data_row(&mut result, "example", 200, &[]);
        assert_eq!(result, "relay_example 200\n");
    }

    #[test]
    fn test_append_single_label() {
        let mut result = String::new();
        append_data_row(&mut result, "example", 200, &[("key", "value")]);
        assert_eq!(result, "relay_example{key=\"value\"} 200\n");
    }

    #[test]
    fn test_append_multiple_labels() {
        let mut result = String::new();
        append_data_row(
            &mut result,
            "example",
            200,
            &[("first_key", "first_value"), ("second_key", "second_value")],
        );
        assert_eq!(
            result,
            "relay_example{first_key=\"first_value\", second_key=\"second_value\"} 200\n"
        );
    }

    #[test]
    fn test_prometheus_serialize() {
        let data = AutoscalingData {
            memory_usage: 0.75,
            up: 1,
            item_count: 10,
            total_size: 30,
            services_metrics: vec![
                ServiceUtilization("test", 10),
                ServiceUtilization("envelope", 50),
            ],
            worker_pool_utilization: 61,
        };
        let result = super::to_prometheus_string(&data);
        assert_eq!(
            result,
            r#"relay_memory_usage 0.75
relay_up 1
relay_spool_item_count 10
relay_spool_total_size 30
relay_utilization{relay_service="test"} 10
relay_utilization{relay_service="envelope"} 50
relay_worker_pool_utilization 61
"#
        );
    }
}
