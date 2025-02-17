use crate::http::StatusCode;
use crate::service::ServiceState;
use crate::services::autoscaling::{AutoscalingData, AutoscalingMessageKind};

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
    let mut result = String::with_capacity(32);
    result.push_str("memory_usage ");
    result.push_str(&data.memory_usage.to_string());
    result.push('\n');
    result.push_str("up ");
    result.push_str(&data.up.to_string());
    result.push('\n');

    result
}

#[cfg(test)]
mod test {
    use crate::services::autoscaling::AutoscalingData;

    #[test]
    fn test_prometheus_serialize() {
        let data = AutoscalingData {
            memory_usage: 0.75,
            up: 1,
        };
        let result = super::to_prometheus_string(&data);
        assert_eq!(result, "memory_usage 0.75\nup 1\n");
    }
}
