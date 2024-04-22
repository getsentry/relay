use serde::Serialize;
use std::collections::BTreeMap;

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
