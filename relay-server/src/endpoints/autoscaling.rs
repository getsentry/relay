use crate::http::StatusCode;
use crate::service::ServiceState;
use crate::services::autoscaling::{AutoscalingData, AutoscalingMessageKind};
use std::fmt::Display;

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

/// Simple function to serialize a well-known format into a prometheus string.
fn to_prometheus_string(data: &AutoscalingData) -> String {
    let mut result = String::with_capacity(128);

    append_data_row(&mut result, "memory_usage", data.memory_usage);
    append_data_row(&mut result, "up", data.up);
    append_data_row(&mut result, "item_count", data.item_count);
    append_data_row(&mut result, "total_size", data.total_size);
    result
}

fn append_data_row(result: &mut String, label: &str, data: impl Display) {
    // Metrics are automatically prefixed with "relay_"
    result.push_str("relay_");
    result.push_str(label);
    result.push(' ');
    result.push_str(&data.to_string());
    result.push('\n');
}

#[cfg(test)]
mod test {
    use crate::services::autoscaling::AutoscalingData;

    #[test]
    fn test_prometheus_serialize() {
        let data = AutoscalingData {
            memory_usage: 0.75,
            up: 1,
            item_count: 10,
            total_size: 30,
        };
        let result = super::to_prometheus_string(&data);
        assert_eq!(
            result,
            r#"relay_memory_usage 0.75
relay_up 1
relay_item_count 10
relay_total_size 30
"#
        );
    }
}
