use crate::service::ServiceState;
use crate::services::internal_metrics::KedaMetricsMessageKind;
use ahash::{HashMap, HashMapExt};
use axum::response::IntoResponse;

pub async fn handle(state: ServiceState) -> impl IntoResponse {
    let data = state
        .keda_metrics()
        .send(KedaMetricsMessageKind::Check)
        .await
        .unwrap();

    serde_prometheus::to_string(&data, None, HashMap::new()).unwrap()
}
