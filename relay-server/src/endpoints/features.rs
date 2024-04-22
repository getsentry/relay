use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;
use std::collections::BTreeMap;

use crate::service::ServiceState;

pub async fn handle(state: ServiceState) -> impl IntoResponse {
    (
        StatusCode::OK,
        axum::Json(FeatureSet {
            version: 0,
            features: vec![Feature {
                key: "hello".to_owned(),
                value: FeatureValue::String("world".to_owned()),
            }],
        }),
    )
}

// TODO: Move this to relay-features.

#[derive(Debug, Serialize)]
struct FeatureSet {
    version: u8,
    features: Vec<Feature>,
}

#[derive(Debug, Serialize)]
struct Feature {
    key: String,
    value: FeatureValue,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum FeatureValue {
    Bool(bool),
    Float(f64),
    Integer(usize),
    String(String),
    Object(BTreeMap<String, serde_json::Value>),
}
