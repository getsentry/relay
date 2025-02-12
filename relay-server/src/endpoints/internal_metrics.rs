use crate::http::StatusCode;
use crate::service::ServiceState;
use crate::services::internal_metrics::InternalMetricsMessageKind;
use ahash::{HashMap, HashMapExt};

/// Returns internal metrics data for relay.
pub async fn handle(state: ServiceState) -> (StatusCode, String) {
    let data = match state
        .internal_metrics()
        .send(InternalMetricsMessageKind::Check)
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

    match serde_prometheus::to_string(&data, None, HashMap::new()) {
        Ok(result) => (StatusCode::OK, result),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to collect internal metrics".to_string(),
        ),
    }
}
