use std::time::Duration;

use relay_conventions::consts::*;
use relay_event_schema::protocol::Attributes;
use relay_protocol::Annotated;

use crate::ModelCosts;
use crate::span::ai;
use crate::statsd::{Counters, map_origin_to_integration, platform_tag};

/// Normalizes AI attributes.
///
/// This aggressively overwrites existing AI attributes, in order to guarantee a consistent data
/// set for the AI product module.
///
/// As an example, an OTeL user may be manually instrumenting AI request costs on spans but in a
/// local currency. Sentry's AI model requires a consistent cost value, independent of local
/// currencies.
///
/// Callers may choose to only run this normalization in processing mode to not have the
/// normalization run multiple times.
pub fn normalize_ai(
    attributes: &mut Annotated<Attributes>,
    duration: Option<Duration>,
    costs: Option<&ModelCosts>,
) {
    let Some(attributes) = attributes.value_mut() else {
        return;
    };

    // Specifically only apply normalizations if the item is recognized as an AI item by the
    // product.
    if !is_ai_item(attributes) {
        return;
    }

    normalize_ai_type(attributes);
    normalize_total_tokens(attributes);
    normalize_tokens_per_second(attributes, duration);
    normalize_ai_costs(attributes, costs);
}

/// Returns whether the item is should have AI normalizations applied.
fn is_ai_item(attributes: &mut Attributes) -> bool {
    // The product indicator whether we consider an item to be an EAP item.
    if attributes.get_value(GEN_AI_OPERATION_TYPE).is_some() {
        return true;
    }

    // We use the operation name to infer the operation type.
    if attributes.get_value(GEN_AI_OPERATION_NAME).is_some() {
        return true;
    }

    // Older SDKs may only send a (span) op which we also use to infer the operation type.
    let op = attributes.get_value(OP).and_then(|op| op.as_str());
    if op.is_some_and(|op| op.starts_with("gen_ai.") || op.starts_with("ai.")) {
        return true;
    }

    false
}

/// Normalizes the [`GEN_AI_OPERATION_TYPE`] and infers it from the AI operation if it is missing.
fn normalize_ai_type(attributes: &mut Attributes) {
    let op_name = attributes
        .get_value(GEN_AI_OPERATION_NAME)
        .or_else(|| attributes.get_value(OP))
        .and_then(|op| op.as_str())
        .and_then(|op| ai::infer_ai_operation_type(op))
        // This is fine, this normalization only happens for known AI spans.
        .unwrap_or(ai::DEFAULT_AI_OPERATION);

    attributes.insert(GEN_AI_OPERATION_TYPE, op_name.to_owned());
}

/// Calculates the [`GEN_AI_USAGE_TOTAL_TOKENS`] attribute.
fn normalize_total_tokens(attributes: &mut Attributes) {
    let input_tokens = attributes
        .get_value(GEN_AI_USAGE_INPUT_TOKENS)
        .and_then(|v| v.as_f64());

    let output_tokens = attributes
        .get_value(GEN_AI_USAGE_OUTPUT_TOKENS)
        .and_then(|v| v.as_f64());

    if input_tokens.is_none() && output_tokens.is_none() {
        return;
    }

    let total_tokens = input_tokens.unwrap_or(0.0) + output_tokens.unwrap_or(0.0);
    attributes.insert(GEN_AI_USAGE_TOTAL_TOKENS, total_tokens);
}

/// Calculates the [`GEN_AI_RESPONSE_TPS`] attribute.
fn normalize_tokens_per_second(attributes: &mut Attributes, duration: Option<Duration>) {
    let Some(duration) = duration.filter(|d| !d.is_zero()) else {
        return;
    };

    let output_tokens = attributes
        .get_value(GEN_AI_USAGE_OUTPUT_TOKENS)
        .and_then(|v| v.as_f64())
        .filter(|v| *v > 0.0);

    if let Some(output_tokens) = output_tokens {
        let tps = output_tokens / duration.as_secs_f64();
        attributes.insert(GEN_AI_RESPONSE_TPS, tps);
    }
}

/// Calculates model costs and serializes them into attributes.
fn normalize_ai_costs(attributes: &mut Attributes, model_costs: Option<&ModelCosts>) {
    let origin = extract_string_value(attributes, ORIGIN);
    let platform = extract_string_value(attributes, PLATFORM);

    let model_cost = attributes
        .get_value(GEN_AI_REQUEST_MODEL)
        .or_else(|| attributes.get_value(GEN_AI_RESPONSE_MODEL))
        .and_then(|v| v.as_str())
        .and_then(|model| model_costs?.cost_per_token(model));

    let Some(model_cost) = model_cost else { return };

    let get_tokens = |key| {
        attributes
            .get_value(key)
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0)
    };

    let tokens = ai::UsedTokens {
        input_tokens: get_tokens(GEN_AI_USAGE_INPUT_TOKENS),
        input_cached_tokens: get_tokens(GEN_AI_USAGE_INPUT_CACHED_TOKENS),
        input_cache_write_tokens: get_tokens(GEN_AI_USAGE_INPUT_CACHE_WRITE_TOKENS),
        output_tokens: get_tokens(GEN_AI_USAGE_OUTPUT_TOKENS),
        output_reasoning_tokens: get_tokens(GEN_AI_USAGE_OUTPUT_REASONING_TOKENS),
    };

    let integration = map_origin_to_integration(origin);
    let platform = platform_tag(platform);

    let Some(costs) = ai::calculate_costs(model_cost, tokens, integration, platform) else {
        relay_statsd::metric!(
            counter(Counters::GenAiCostCalculationResult) += 1,
            result = "calculation_none",
            integration = integration,
            platform = platform,
        );
        return;
    };

    attributes.insert(GEN_AI_COST_INPUT_TOKENS, costs.input);
    attributes.insert(GEN_AI_COST_OUTPUT_TOKENS, costs.output);
    attributes.insert(GEN_AI_COST_TOTAL_TOKENS, costs.total());
}

fn extract_string_value<'a>(attributes: &'a Attributes, key: &str) -> Option<&'a str> {
    attributes.get_value(key).and_then(|v| v.as_str())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use relay_pattern::Pattern;
    use relay_protocol::{Empty, assert_annotated_snapshot};

    use crate::ModelCostV2;

    use super::*;

    macro_rules! attributes {
        ($($key:expr => $value:expr),* $(,)?) => {
            Attributes::from([
                $(($key.into(), Annotated::new($value.into())),)*
            ])
        };
    }

    fn model_costs() -> ModelCosts {
        ModelCosts {
            version: 2,
            models: HashMap::from([
                (
                    Pattern::new("claude-2.1").unwrap(),
                    ModelCostV2 {
                        input_per_token: 0.01,
                        output_per_token: 0.02,
                        output_reasoning_per_token: 0.03,
                        input_cached_per_token: 0.04,
                        input_cache_write_per_token: 0.0,
                    },
                ),
                (
                    Pattern::new("gpt4-21-04").unwrap(),
                    ModelCostV2 {
                        input_per_token: 0.09,
                        output_per_token: 0.05,
                        output_reasoning_per_token: 0.0,
                        input_cached_per_token: 0.0,
                        input_cache_write_per_token: 0.0,
                    },
                ),
            ]),
        }
    }

    #[test]
    fn test_normalize_ai_all_tokens() {
        let mut attributes = Annotated::new(attributes! {
            "gen_ai.operation.type" => "ai_client".to_owned(),
            "gen_ai.usage.input_tokens" => 1000,
            "gen_ai.usage.output_tokens" => 2000,
            "gen_ai.usage.output_tokens.reasoning" => 1000,
            "gen_ai.usage.input_tokens.cached" => 500,
            "gen_ai.request.model" => "claude-2.1".to_owned(),
        });

        normalize_ai(
            &mut attributes,
            Some(Duration::from_secs(1)),
            Some(&model_costs()),
        );

        assert_annotated_snapshot!(attributes, @r#"
        {
          "gen_ai.cost.input_tokens": {
            "type": "double",
            "value": 25.0
          },
          "gen_ai.cost.output_tokens": {
            "type": "double",
            "value": 50.0
          },
          "gen_ai.cost.total_tokens": {
            "type": "double",
            "value": 75.0
          },
          "gen_ai.operation.type": {
            "type": "string",
            "value": "ai_client"
          },
          "gen_ai.request.model": {
            "type": "string",
            "value": "claude-2.1"
          },
          "gen_ai.response.tokens_per_second": {
            "type": "double",
            "value": 2000.0
          },
          "gen_ai.usage.input_tokens": {
            "type": "integer",
            "value": 1000
          },
          "gen_ai.usage.input_tokens.cached": {
            "type": "integer",
            "value": 500
          },
          "gen_ai.usage.output_tokens": {
            "type": "integer",
            "value": 2000
          },
          "gen_ai.usage.output_tokens.reasoning": {
            "type": "integer",
            "value": 1000
          },
          "gen_ai.usage.total_tokens": {
            "type": "double",
            "value": 3000.0
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_ai_basic_tokens() {
        let mut attributes = Annotated::new(attributes! {
            "gen_ai.operation.type" => "ai_client".to_owned(),
            "gen_ai.usage.input_tokens" => 1000,
            "gen_ai.usage.output_tokens" => 2000,
            "gen_ai.request.model" => "gpt4-21-04".to_owned(),
        });

        normalize_ai(
            &mut attributes,
            Some(Duration::from_millis(500)),
            Some(&model_costs()),
        );

        assert_annotated_snapshot!(attributes, @r#"
        {
          "gen_ai.cost.input_tokens": {
            "type": "double",
            "value": 90.0
          },
          "gen_ai.cost.output_tokens": {
            "type": "double",
            "value": 100.0
          },
          "gen_ai.cost.total_tokens": {
            "type": "double",
            "value": 190.0
          },
          "gen_ai.operation.type": {
            "type": "string",
            "value": "ai_client"
          },
          "gen_ai.request.model": {
            "type": "string",
            "value": "gpt4-21-04"
          },
          "gen_ai.response.tokens_per_second": {
            "type": "double",
            "value": 4000.0
          },
          "gen_ai.usage.input_tokens": {
            "type": "integer",
            "value": 1000
          },
          "gen_ai.usage.output_tokens": {
            "type": "integer",
            "value": 2000
          },
          "gen_ai.usage.total_tokens": {
            "type": "double",
            "value": 3000.0
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_ai_basic_tokens_no_duration_no_cost() {
        let mut attributes = Annotated::new(attributes! {
            "gen_ai.operation.type" => "ai_client".to_owned(),
            "gen_ai.usage.input_tokens" => 1000,
            "gen_ai.usage.output_tokens" => 2000,
            "gen_ai.request.model" => "unknown".to_owned(),
        });

        normalize_ai(&mut attributes, Some(Duration::ZERO), Some(&model_costs()));

        assert_annotated_snapshot!(attributes, @r#"
        {
          "gen_ai.operation.type": {
            "type": "string",
            "value": "ai_client"
          },
          "gen_ai.request.model": {
            "type": "string",
            "value": "unknown"
          },
          "gen_ai.usage.input_tokens": {
            "type": "integer",
            "value": 1000
          },
          "gen_ai.usage.output_tokens": {
            "type": "integer",
            "value": 2000
          },
          "gen_ai.usage.total_tokens": {
            "type": "double",
            "value": 3000.0
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_ai_does_not_overwrite() {
        let mut attributes = Annotated::new(attributes! {
            "gen_ai.operation.type" => "ai_client".to_owned(),
            "gen_ai.usage.input_tokens" => 1000,
            "gen_ai.usage.output_tokens" => 2000,
            "gen_ai.request.model" => "gpt4-21-04".to_owned(),

            "gen_ai.cost.input_tokens" => 999.0,
        });

        normalize_ai(
            &mut attributes,
            Some(Duration::from_millis(500)),
            Some(&model_costs()),
        );

        assert_annotated_snapshot!(attributes, @r#"
        {
          "gen_ai.cost.input_tokens": {
            "type": "double",
            "value": 90.0
          },
          "gen_ai.cost.output_tokens": {
            "type": "double",
            "value": 100.0
          },
          "gen_ai.cost.total_tokens": {
            "type": "double",
            "value": 190.0
          },
          "gen_ai.operation.type": {
            "type": "string",
            "value": "ai_client"
          },
          "gen_ai.request.model": {
            "type": "string",
            "value": "gpt4-21-04"
          },
          "gen_ai.response.tokens_per_second": {
            "type": "double",
            "value": 4000.0
          },
          "gen_ai.usage.input_tokens": {
            "type": "integer",
            "value": 1000
          },
          "gen_ai.usage.output_tokens": {
            "type": "integer",
            "value": 2000
          },
          "gen_ai.usage.total_tokens": {
            "type": "double",
            "value": 3000.0
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_ai_overwrite_costs() {
        let mut attributes = Annotated::new(attributes! {
            "gen_ai.operation.type" => "ai_client".to_owned(),
            "gen_ai.usage.input_tokens" => 1000,
            "gen_ai.usage.output_tokens" => 2000,
            "gen_ai.request.model" => "gpt4-21-04".to_owned(),

            "gen_ai.usage.total_tokens" => 1337,

            "gen_ai.cost.input_tokens" => 99.0,
            "gen_ai.cost.output_tokens" => 99.0,
            "gen_ai.cost.total_tokens" => 123.0,

            "gen_ai.response.tokens_per_second" => 42.0,
        });

        normalize_ai(
            &mut attributes,
            Some(Duration::from_millis(500)),
            Some(&model_costs()),
        );

        assert_annotated_snapshot!(attributes, @r#"
        {
          "gen_ai.cost.input_tokens": {
            "type": "double",
            "value": 90.0
          },
          "gen_ai.cost.output_tokens": {
            "type": "double",
            "value": 100.0
          },
          "gen_ai.cost.total_tokens": {
            "type": "double",
            "value": 190.0
          },
          "gen_ai.operation.type": {
            "type": "string",
            "value": "ai_client"
          },
          "gen_ai.request.model": {
            "type": "string",
            "value": "gpt4-21-04"
          },
          "gen_ai.response.tokens_per_second": {
            "type": "double",
            "value": 4000.0
          },
          "gen_ai.usage.input_tokens": {
            "type": "integer",
            "value": 1000
          },
          "gen_ai.usage.output_tokens": {
            "type": "integer",
            "value": 2000
          },
          "gen_ai.usage.total_tokens": {
            "type": "double",
            "value": 3000.0
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_ai_no_ai_attributes() {
        let mut attributes = Annotated::new(attributes! {
            "gen_ai.usage.input_tokens" => 1000,
            "gen_ai.usage.output_tokens" => 2000,
        });

        normalize_ai(
            &mut attributes,
            Some(Duration::from_millis(500)),
            Some(&model_costs()),
        );

        assert_annotated_snapshot!(&mut attributes, @r#"
        {
          "gen_ai.usage.input_tokens": {
            "type": "integer",
            "value": 1000
          },
          "gen_ai.usage.output_tokens": {
            "type": "integer",
            "value": 2000
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_ai_no_ai_indicator_attribute() {
        let mut attributes = Annotated::new(attributes! {
            "foo" => 123,
        });

        normalize_ai(
            &mut attributes,
            Some(Duration::from_millis(500)),
            Some(&model_costs()),
        );

        assert_annotated_snapshot!(&mut attributes, @r#"
        {
          "foo": {
            "type": "integer",
            "value": 123
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_ai_empty() {
        let mut attributes = Annotated::empty();

        normalize_ai(
            &mut attributes,
            Some(Duration::from_millis(500)),
            Some(&model_costs()),
        );

        assert!(attributes.is_empty());
    }
}
