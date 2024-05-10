//! Configuration for measurements generated from AI model instrumentation.

use relay_common::glob2::LazyGlob;
use serde::{Deserialize, Serialize};

const MAX_SUPPORTED_VERSION: u16 = 1;

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCosts {
    version: u16,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    costs: Vec<ModelCost>,
}

impl ModelCosts {
    /// `false` if measurement and metrics extraction should be skipped.
    pub fn is_enabled(&self) -> bool {
        self.version > 0 && self.version <= MAX_SUPPORTED_VERSION
    }

    /// Gets the cost per 1000 tokens, if defined for the given model.
    pub fn cost_per_1k_tokens(&self, model_id: &str, for_completion: bool) -> Option<f32> {
        self.costs
            .iter()
            .find(|cost| cost.matches(model_id, for_completion))
            .map(|c| c.cost_per_1k_tokens)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCost {
    model_id: LazyGlob,
    for_completion: bool,
    cost_per_1k_tokens: f32,
}

impl ModelCost {
    /// `true` if this cost definition matches the given model.
    pub fn matches(&self, model_id: &str, for_completion: bool) -> bool {
        self.for_completion == for_completion && self.model_id.compiled().is_match(model_id)
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;
    use serde_json::json;

    use super::*;

    #[test]
    fn roundtrip() {
        let original = json!({
            "version": 1,
            "costs": [
                {"modelId": "babbage-002.ft-*", "forCompletion": false, "costPer1kTokens": 0.0016}
            ]
        });
        let deserialized: ModelCosts = serde_json::from_value(original.clone()).unwrap();
        assert_debug_snapshot!(deserialized, @r###"
        ModelCosts {
            version: 1,
            costs: [
                ModelCost {
                    model_id: LazyGlob("babbage-002.ft-*"),
                    for_completion: false,
                    cost_per_1k_tokens: 0.0016,
                },
            ],
        }
        "###);

        let serialized = serde_json::to_value(deserialized).unwrap();
        assert_eq!(serialized, original);
    }
}
