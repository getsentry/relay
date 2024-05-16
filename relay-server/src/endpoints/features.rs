use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

use crate::service::ServiceState;

pub async fn handle(state: ServiceState) -> impl IntoResponse {
    (
        StatusCode::OK,
        axum::Json(FeatureSet {
            version: 1,
            options: Option {
                sample_rate: 1.0,
                traces_sample_rate: 1.0,
                user_config: Value::Null,
            },
        }),
    )
}

// TODO: Move this to relay-features.

#[derive(Debug, Serialize)]
struct FeatureSet {
    version: u8,
    options: Option,
}

#[derive(Debug, Serialize)]
struct Option {
    sample_rate: f64,
    traces_sample_rate: f64,
    user_config: Value,
}
