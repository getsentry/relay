//! AI cost calculation.

use crate::normalize::AiOperationTypeMap;
use crate::{ModelCostV2, ModelCosts};
use relay_event_schema::protocol::{Event, Span, SpanData};
use relay_protocol::{Annotated, Getter, Value};

/// Amount of used tokens for a model call.
#[derive(Debug, Copy, Clone)]
pub struct UsedTokens {
    /// Total amount of input tokens used.
    pub input_tokens: f64,
    /// Amount of cached tokens used.
    ///
    /// This is a subset of [`Self::input_tokens`].
    pub input_cached_tokens: f64,
    /// Amount of cache write tokens used.
    ///
    /// This is a subset of [`Self::input_tokens`].
    pub input_cache_write_tokens: f64,
    /// Total amount of output tokens.
    pub output_tokens: f64,
    /// Total amount of reasoning tokens.
    ///
    /// This is a subset of [`Self::output_tokens`].
    pub output_reasoning_tokens: f64,
}

impl UsedTokens {
    /// Extracts [`UsedTokens`] from [`SpanData`] attributes.
    pub fn from_span_data(data: &SpanData) -> Self {
        macro_rules! get_value {
            ($e:expr) => {
                $e.value().and_then(Value::as_f64).unwrap_or(0.0)
            };
        }

        Self {
            input_tokens: get_value!(data.gen_ai_usage_input_tokens),
            output_tokens: get_value!(data.gen_ai_usage_output_tokens),
            output_reasoning_tokens: get_value!(data.gen_ai_usage_output_tokens_reasoning),
            input_cached_tokens: get_value!(data.gen_ai_usage_input_tokens_cached),
            input_cache_write_tokens: get_value!(data.gen_ai_usage_input_tokens_cache_write),
        }
    }

    /// Returns `true` if any tokens were used.
    pub fn has_usage(&self) -> bool {
        self.input_tokens > 0.0 || self.output_tokens > 0.0
    }

    /// Calculates the total amount of uncached input tokens.
    ///
    /// Subtracts cached tokens from the total token count.
    pub fn raw_input_tokens(&self) -> f64 {
        self.input_tokens - self.input_cached_tokens
    }

    /// Calculates the total amount of raw, non-reasoning output tokens.
    ///
    /// Subtracts reasoning tokens from the total token count.
    pub fn raw_output_tokens(&self) -> f64 {
        self.output_tokens - self.output_reasoning_tokens
    }
}

/// Calculated model call costs.
#[derive(Debug, Copy, Clone)]
pub struct CalculatedCost {
    /// The cost of input tokens used.
    pub input: f64,
    /// The cost of output tokens used.
    pub output: f64,
}

impl CalculatedCost {
    /// The total, input and output, cost.
    pub fn total(&self) -> f64 {
        self.input + self.output
    }
}

/// Calculates the total cost for a model call.
///
/// Returns `None` if no tokens were used.
pub fn calculate_costs(model_cost: &ModelCostV2, tokens: UsedTokens) -> Option<CalculatedCost> {
    if !tokens.has_usage() {
        return None;
    }

    let input = (tokens.raw_input_tokens() * model_cost.input_per_token)
        + (tokens.input_cached_tokens * model_cost.input_cached_per_token)
        + (tokens.input_cache_write_tokens * model_cost.input_cache_write_per_token);

    // For now most of the models do not differentiate between reasoning and output token cost,
    // it costs the same.
    let reasoning_cost = match model_cost.output_reasoning_per_token {
        reasoning_cost if reasoning_cost > 0.0 => reasoning_cost,
        _ => model_cost.output_per_token,
    };

    let output = (tokens.raw_output_tokens() * model_cost.output_per_token)
        + (tokens.output_reasoning_tokens * reasoning_cost);

    Some(CalculatedCost { input, output })
}

/// Calculates the cost of an AI model based on the model cost and the tokens used.
/// Calculated cost is in US dollars.
fn extract_ai_model_cost_data(model_cost: Option<&ModelCostV2>, data: &mut SpanData) {
    let Some(model_cost) = model_cost else { return };

    let used_tokens = UsedTokens::from_span_data(&*data);
    let Some(costs) = calculate_costs(model_cost, used_tokens) else {
        return;
    };

    data.gen_ai_cost_total_tokens
        .set_value(Value::F64(costs.total()).into());

    // Set individual cost components
    data.gen_ai_cost_input_tokens
        .set_value(Value::F64(costs.input).into());
    data.gen_ai_cost_output_tokens
        .set_value(Value::F64(costs.output).into());
}

/// Maps AI-related measurements (legacy) to span data.
fn map_ai_measurements_to_data(span: &mut Span) {
    let measurements = span.measurements.value();
    let data = span.data.get_or_insert_with(SpanData::default);

    let set_field_from_measurement = |target_field: &mut Annotated<Value>,
                                      measurement_key: &str| {
        if let Some(measurements) = measurements
            && target_field.value().is_none()
            && let Some(value) = measurements.get_value(measurement_key)
        {
            target_field.set_value(Value::F64(value.to_f64()).into());
        }
    };

    set_field_from_measurement(&mut data.gen_ai_usage_total_tokens, "ai_total_tokens_used");
    set_field_from_measurement(&mut data.gen_ai_usage_input_tokens, "ai_prompt_tokens_used");
    set_field_from_measurement(
        &mut data.gen_ai_usage_output_tokens,
        "ai_completion_tokens_used",
    );
}

fn set_total_tokens(span: &mut Span) {
    let data = span.data.get_or_insert_with(SpanData::default);

    // It might be that 'total_tokens' is not set in which case we need to calculate it
    if data.gen_ai_usage_total_tokens.value().is_none() {
        let input_tokens = data
            .gen_ai_usage_input_tokens
            .value()
            .and_then(Value::as_f64);
        let output_tokens = data
            .gen_ai_usage_output_tokens
            .value()
            .and_then(Value::as_f64);

        if input_tokens.is_none() && output_tokens.is_none() {
            // don't set total_tokens if there are no input nor output tokens
            return;
        }

        data.gen_ai_usage_total_tokens.set_value(
            Value::F64(input_tokens.unwrap_or(0.0) + output_tokens.unwrap_or(0.0)).into(),
        );
    }
}

/// Extract the additional data into the span
fn extract_ai_data(span: &mut Span, ai_model_costs: &ModelCosts) {
    let duration = span
        .get_value("span.duration")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let data = span.data.get_or_insert_with(SpanData::default);

    // Extracts the response tokens per second
    if data.gen_ai_response_tokens_per_second.value().is_none()
        && duration > 0.0
        && let Some(output_tokens) = data
            .gen_ai_usage_output_tokens
            .value()
            .and_then(Value::as_f64)
    {
        data.gen_ai_response_tokens_per_second
            .set_value(Value::F64(output_tokens / (duration / 1000.0)).into());
    }

    // Extracts the total cost of the AI model used
    if let Some(model_id) = data
        .gen_ai_request_model
        .value()
        .and_then(|val| val.as_str())
        .or_else(|| {
            data.gen_ai_response_model
                .value()
                .and_then(|val| val.as_str())
        })
    {
        extract_ai_model_cost_data(ai_model_costs.cost_per_token(model_id), data)
    }
}

/// Enrich the AI span data
pub fn enrich_ai_span_data(
    span: &mut Span,
    model_costs: Option<&ModelCosts>,
    operation_type_map: Option<&AiOperationTypeMap>,
) {
    if !is_ai_span(span) {
        return;
    }

    map_ai_measurements_to_data(span);
    set_total_tokens(span);

    if let Some(model_costs) = model_costs {
        extract_ai_data(span, model_costs);
    }
    if let Some(operation_type_map) = operation_type_map {
        infer_ai_operation_type(span, operation_type_map);
    }
}

/// Extract the ai data from all of an event's spans
pub fn enrich_ai_event_data(
    event: &mut Event,
    model_costs: Option<&ModelCosts>,
    operation_type_map: Option<&AiOperationTypeMap>,
) {
    let spans = event.spans.value_mut().iter_mut().flatten();
    let spans = spans.filter_map(|span| span.value_mut().as_mut());

    for span in spans {
        enrich_ai_span_data(span, model_costs, operation_type_map);
    }
}

///  Infer AI operation type mapping to a span.
///
/// This function sets the gen_ai.operation.type attribute based on the value of either
/// gen_ai.operation.name or span.op based on the provided operation type map configuration.
fn infer_ai_operation_type(span: &mut Span, operation_type_map: &AiOperationTypeMap) {
    let data = span.data.get_or_insert_with(SpanData::default);
    let op_type = data
        .gen_ai_operation_name
        .value()
        .or(span.op.value())
        .and_then(|op| operation_type_map.get_operation_type(op));

    if let Some(operation_type) = op_type {
        data.gen_ai_operation_type
            .set_value(Some(operation_type.to_owned()));
    }
}

/// Returns true if the span is an AI span.
/// AI spans are spans with either a gen_ai.operation.name attribute or op starting with "ai."
/// (legacy) or "gen_ai." (new).
fn is_ai_span(span: &Span) -> bool {
    let has_ai_op = span
        .data
        .value()
        .and_then(|data| data.gen_ai_operation_name.value())
        .is_some();

    let is_ai_span_op = span
        .op
        .value()
        .is_some_and(|op| op.starts_with("ai.") || op.starts_with("gen_ai."));

    has_ai_op || is_ai_span_op
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use relay_pattern::Pattern;
    use relay_protocol::get_value;

    use super::*;

    #[test]
    fn test_calculate_cost_no_tokens() {
        let cost = calculate_costs(
            &ModelCostV2 {
                input_per_token: 1.0,
                output_per_token: 1.0,
                output_reasoning_per_token: 1.0,
                input_cached_per_token: 1.0,
                input_cache_write_per_token: 1.0,
            },
            UsedTokens::from_span_data(&SpanData::default()),
        );
        assert!(cost.is_none());
    }

    #[test]
    fn test_calculate_cost_full() {
        let cost = calculate_costs(
            &ModelCostV2 {
                input_per_token: 1.0,
                output_per_token: 2.0,
                output_reasoning_per_token: 3.0,
                input_cached_per_token: 0.5,
                input_cache_write_per_token: 0.75,
            },
            UsedTokens {
                input_tokens: 8.0,
                input_cached_tokens: 5.0,
                input_cache_write_tokens: 0.0,
                output_tokens: 15.0,
                output_reasoning_tokens: 9.0,
            },
        )
        .unwrap();

        insta::assert_debug_snapshot!(cost, @r"
        CalculatedCost {
            input: 5.5,
            output: 39.0,
        }
        ");
    }

    #[test]
    fn test_calculate_cost_no_reasoning_cost() {
        let cost = calculate_costs(
            &ModelCostV2 {
                input_per_token: 1.0,
                output_per_token: 2.0,
                // Should fallback to output token cost for reasoning.
                output_reasoning_per_token: 0.0,
                input_cached_per_token: 0.5,
                input_cache_write_per_token: 0.0,
            },
            UsedTokens {
                input_tokens: 8.0,
                input_cached_tokens: 5.0,
                input_cache_write_tokens: 0.0,
                output_tokens: 15.0,
                output_reasoning_tokens: 9.0,
            },
        )
        .unwrap();

        insta::assert_debug_snapshot!(cost, @r"
        CalculatedCost {
            input: 5.5,
            output: 30.0,
        }
        ");
    }

    /// This test shows it is possible to produce negative costs if tokens are not aligned properly.
    ///
    /// The behaviour was desired when initially implemented.
    #[test]
    fn test_calculate_cost_negative() {
        let cost = calculate_costs(
            &ModelCostV2 {
                input_per_token: 2.0,
                output_per_token: 2.0,
                output_reasoning_per_token: 1.0,
                input_cached_per_token: 1.0,
                input_cache_write_per_token: 1.5,
            },
            UsedTokens {
                input_tokens: 1.0,
                input_cached_tokens: 11.0,
                input_cache_write_tokens: 0.0,
                output_tokens: 1.0,
                output_reasoning_tokens: 9.0,
            },
        )
        .unwrap();

        insta::assert_debug_snapshot!(cost, @r"
        CalculatedCost {
            input: -9.0,
            output: -7.0,
        }
        ");
    }

    #[test]
    fn test_calculate_cost_with_cache_writes() {
        let cost = calculate_costs(
            &ModelCostV2 {
                input_per_token: 1.0,
                output_per_token: 2.0,
                output_reasoning_per_token: 3.0,
                input_cached_per_token: 0.5,
                input_cache_write_per_token: 0.75,
            },
            UsedTokens {
                input_tokens: 100.0,
                input_cached_tokens: 20.0,
                input_cache_write_tokens: 30.0,
                output_tokens: 50.0,
                output_reasoning_tokens: 10.0,
            },
        )
        .unwrap();

        insta::assert_debug_snapshot!(cost, @r"
        CalculatedCost {
            input: 112.5,
            output: 110.0,
        }
        ");
    }

    #[test]
    fn test_calculate_cost_backward_compatibility_no_cache_write() {
        // Test that cost calculation works when cache_write field is missing (backward compatibility)
        let mut span_data = SpanData::default();
        span_data.gen_ai_usage_input_tokens = Annotated::new(100.0.into());
        span_data.gen_ai_usage_input_tokens_cached = Annotated::new(20.0.into());
        span_data.gen_ai_usage_output_tokens = Annotated::new(50.0.into());
        // Note: gen_ai_usage_input_tokens_cache_write is NOT set (simulating old data)

        let tokens = UsedTokens::from_span_data(&span_data);

        // Verify cache_write_tokens defaults to 0.0
        assert_eq!(tokens.input_cache_write_tokens, 0.0);

        let cost = calculate_costs(
            &ModelCostV2 {
                input_per_token: 1.0,
                output_per_token: 2.0,
                output_reasoning_per_token: 0.0,
                input_cached_per_token: 0.5,
                input_cache_write_per_token: 0.75,
            },
            tokens,
        )
        .unwrap();

        // Cost should be calculated without cache_write_tokens
        // input: (100 - 20) * 1.0 + 20 * 0.5 + 0 * 0.75 = 80 + 10 + 0 = 90
        // output: 50 * 2.0 = 100
        insta::assert_debug_snapshot!(cost, @r"
        CalculatedCost {
            input: 90.0,
            output: 100.0,
        }
        ");
    }

    /// Test that the AI operation type is inferred from a gen_ai.operation.name attribute.
    #[test]
    fn test_infer_ai_operation_type_from_gen_ai_operation_name() {
        let operation_types = HashMap::from([
            (Pattern::new("*").unwrap(), "ai_client".to_owned()),
            (Pattern::new("invoke_agent").unwrap(), "agent".to_owned()),
            (
                Pattern::new("gen_ai.invoke_agent").unwrap(),
                "agent".to_owned(),
            ),
        ]);

        let operation_type_map = AiOperationTypeMap {
            version: 1,
            operation_types,
        };

        let span = r#"{
            "data": {
                "gen_ai.operation.name": "invoke_agent"
            }
        }"#;
        let mut span = Annotated::from_json(span).unwrap();
        infer_ai_operation_type(span.value_mut().as_mut().unwrap(), &operation_type_map);
        assert_eq!(
            get_value!(span.data.gen_ai_operation_type!).as_str(),
            "agent"
        );
    }

    /// Test that the AI operation type is inferred from a span.op attribute.
    #[test]
    fn test_infer_ai_operation_type_from_span_op() {
        let operation_types = HashMap::from([
            (Pattern::new("*").unwrap(), "ai_client".to_owned()),
            (Pattern::new("invoke_agent").unwrap(), "agent".to_owned()),
            (
                Pattern::new("gen_ai.invoke_agent").unwrap(),
                "agent".to_owned(),
            ),
        ]);
        let operation_type_map = AiOperationTypeMap {
            version: 1,
            operation_types,
        };

        let span = r#"{
            "op": "gen_ai.invoke_agent"
        }"#;
        let mut span = Annotated::from_json(span).unwrap();
        infer_ai_operation_type(span.value_mut().as_mut().unwrap(), &operation_type_map);
        assert_eq!(
            get_value!(span.data.gen_ai_operation_type!).as_str(),
            "agent"
        );
    }

    /// Test that the AI operation type is inferred from a fallback.
    #[test]
    fn test_infer_ai_operation_type_from_fallback() {
        let operation_types = HashMap::from([
            (Pattern::new("*").unwrap(), "ai_client".to_owned()),
            (Pattern::new("invoke_agent").unwrap(), "agent".to_owned()),
            (
                Pattern::new("gen_ai.invoke_agent").unwrap(),
                "agent".to_owned(),
            ),
        ]);

        let operation_type_map = AiOperationTypeMap {
            version: 1,
            operation_types,
        };

        let span = r#"{
            "data": {
                "gen_ai.operation.name": "embeddings"
            }
        }"#;
        let mut span = Annotated::from_json(span).unwrap();
        infer_ai_operation_type(span.value_mut().as_mut().unwrap(), &operation_type_map);
        assert_eq!(
            get_value!(span.data.gen_ai_operation_type!).as_str(),
            "ai_client"
        );
    }

    /// Test that an AI span is detected from a gen_ai.operation.name attribute.
    #[test]
    fn test_is_ai_span_from_gen_ai_operation_name() {
        let span = r#"{
            "data": {
                "gen_ai.operation.name": "chat"
            }
        }"#;
        let span: Span = Annotated::from_json(span).unwrap().into_value().unwrap();
        assert!(is_ai_span(&span));
    }

    /// Test that an AI span is detected from a span.op starting with "ai.".
    #[test]
    fn test_is_ai_span_from_span_op_ai() {
        let span = r#"{
            "op": "ai.chat"
        }"#;
        let span: Span = Annotated::from_json(span).unwrap().into_value().unwrap();
        assert!(is_ai_span(&span));
    }

    /// Test that an AI span is detected from a span.op starting with "gen_ai.".
    #[test]
    fn test_is_ai_span_from_span_op_gen_ai() {
        let span = r#"{
            "op": "gen_ai.chat"
        }"#;
        let span: Span = Annotated::from_json(span).unwrap().into_value().unwrap();
        assert!(is_ai_span(&span));
    }

    /// Test that a non-AI span is detected.
    #[test]
    fn test_is_ai_span_negative() {
        let span = r#"{
        }"#;
        let span: Span = Annotated::from_json(span).unwrap().into_value().unwrap();
        assert!(!is_ai_span(&span));
    }
}
