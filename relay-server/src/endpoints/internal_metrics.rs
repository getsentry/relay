use crate::http::StatusCode;
use crate::service::ServiceState;
use crate::services::internal_metrics::KedaMetricsMessageKind;
use ahash::{HashMap, HashMapExt};
use axum::extract::Path;
use libc::exit;

pub struct InternalMetricsResult {}

/// Returns internal metrics data for relay.
pub async fn handle(state: ServiceState, Path(content_type): Path<String>) -> (StatusCode, String) {
    let data = match state
        .keda_metrics()
        .send(KedaMetricsMessageKind::Check)
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

    let mut tags: HashMap<&str, &str> = HashMap::new();
    tags.insert("type", content_type.as_str());

    match content_type.as_str() {
        "prometheus" => (
            StatusCode::OK,
            serde_prometheus::to_string(&data, None, tags).unwrap(),
        ),
        "json" => (StatusCode::OK, serde_json::to_string(&data).unwrap()),
        _ => (StatusCode::BAD_REQUEST, "invalid content type".to_string()),
    }
}
