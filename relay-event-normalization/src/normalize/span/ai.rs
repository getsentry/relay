//! AI cost calculation.

use crate::normalize::AiOperationTypeMap;
use crate::{ModelCostV2, ModelCosts};
use relay_event_schema::protocol::{Event, Measurements, OperationType, SpanData, TraceContext};
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
        + (tokens.input_cached_tokens * model_cost.input_cached_per_token);

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
fn map_ai_measurements_to_data(
    span_data: &mut Annotated<SpanData>,
    measurements: Option<&Measurements>,
) {
    let data = span_data.get_or_insert_with(SpanData::default);

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

fn set_total_tokens(span_data: &mut Annotated<SpanData>) {
    let data = span_data.get_or_insert_with(SpanData::default);

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
fn extract_ai_data(
    span_data: &mut Annotated<SpanData>,
    duration: f64,
    ai_model_costs: &ModelCosts,
) {
    let data = span_data.get_or_insert_with(SpanData::default);

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
    span_data: &mut Annotated<SpanData>,
    span_op: &Annotated<OperationType>,
    measurements: &Annotated<Measurements>,
    duration: f64,
    model_costs: Option<&ModelCosts>,
    operation_type_map: Option<&AiOperationTypeMap>,
) {
    map_ai_measurements_to_data(span_data, measurements.value());

    if !is_ai_span(span_data, span_op) {
        return;
    }

    set_total_tokens(span_data);

    if let Some(model_costs) = model_costs {
        extract_ai_data(span_data, duration, model_costs);
    }
    if let Some(operation_type_map) = operation_type_map {
        infer_ai_operation_type(span_data, span_op, operation_type_map);
    }
}

/// Extract the ai data from all of an event's spans
pub fn enrich_ai_event_data(
    event: &mut Event,
    model_costs: Option<&ModelCosts>,
    operation_type_map: Option<&AiOperationTypeMap>,
) {
    let event_duration = event
        .get_value("event.duration")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    if let Some(trace_context) = event
        .contexts
        .value_mut()
        .as_mut()
        .and_then(|c| c.get_mut::<TraceContext>())
    {
        enrich_ai_span_data(
            &mut trace_context.data,
            &trace_context.op,
            &event.measurements,
            event_duration,
            model_costs,
            operation_type_map,
        );
    }
    let spans = event.spans.value_mut().iter_mut().flatten();
    let spans = spans.filter_map(|span| span.value_mut().as_mut());

    for span in spans {
        let span_duration = span
            .get_value("span.duration")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        enrich_ai_span_data(
            &mut span.data,
            &span.op,
            &span.measurements,
            span_duration,
            model_costs,
            operation_type_map,
        );
    }
}

///  Infer AI operation type mapping to a span.
///
/// This function sets the gen_ai.operation.type attribute based on the value of either
/// gen_ai.operation.name or span.op based on the provided operation type map configuration.
fn infer_ai_operation_type(
    span_data: &mut Annotated<SpanData>,
    span_op: &Annotated<OperationType>,
    operation_type_map: &AiOperationTypeMap,
) {
    let data = span_data.get_or_insert_with(SpanData::default);
    let op_type = data
        .gen_ai_operation_name
        .value()
        .or(span_op.value())
        .and_then(|op| operation_type_map.get_operation_type(op));

    if let Some(operation_type) = op_type {
        data.gen_ai_operation_type
            .set_value(Some(operation_type.to_owned()));
    }
}

/// Returns true if the span is an AI span.
/// AI spans are spans with either a gen_ai.operation.name attribute or op starting with "ai."
/// (legacy) or "gen_ai." (new).
fn is_ai_span(span_data: &Annotated<SpanData>, span_op: &Annotated<OperationType>) -> bool {
    let has_ai_op = span_data
        .value()
        .and_then(|data| data.gen_ai_operation_name.value())
        .is_some();

    let is_ai_span_op = span_op
        .value()
        .is_some_and(|op| op.starts_with("ai.") || op.starts_with("gen_ai."));

    has_ai_op || is_ai_span_op
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use relay_pattern::Pattern;
    use relay_protocol::assert_annotated_snapshot;

    use super::*;

    #[test]
    fn test_calculate_cost_no_tokens() {
        let cost = calculate_costs(
            &ModelCostV2 {
                input_per_token: 1.0,
                output_per_token: 1.0,
                output_reasoning_per_token: 1.0,
                input_cached_per_token: 1.0,
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
            },
            UsedTokens {
                input_tokens: 8.0,
                input_cached_tokens: 5.0,
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
            },
            UsedTokens {
                input_tokens: 8.0,
                input_cached_tokens: 5.0,
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
            },
            UsedTokens {
                input_tokens: 1.0,
                input_cached_tokens: 11.0,
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

        let span_data = r#"{
            "gen_ai.operation.name": "invoke_agent"
        }"#;
        let mut span_data = Annotated::from_json(span_data).unwrap();

        infer_ai_operation_type(&mut span_data, &Annotated::default(), &operation_type_map);

        assert_annotated_snapshot!(&span_data, @r#"
        {
          "gen_ai.operation.name": "invoke_agent",
          "gen_ai.operation.type": "agent"
        }
        "#);
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

        let mut span_data = Annotated::<SpanData>::default();
        infer_ai_operation_type(
            &mut span_data,
            &Annotated::new("gen_ai.invoke_agent".into()),
            &operation_type_map,
        );

        assert_annotated_snapshot!(&span_data, @r#"
        {
          "gen_ai.operation.type": "agent"
        }
        "#);
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

        let span_data = r#"{
            "gen_ai.operation.name": "embeddings"
        }"#;
        let mut span_data = Annotated::from_json(span_data).unwrap();

        infer_ai_operation_type(&mut span_data, &Annotated::default(), &operation_type_map);

        assert_annotated_snapshot!(&span_data, @r#"
        {
          "gen_ai.operation.name": "embeddings",
          "gen_ai.operation.type": "ai_client"
        }
        "#);
    }

    /// Test that an AI span is detected from a gen_ai.operation.name attribute.
    #[test]
    fn test_is_ai_span_from_gen_ai_operation_name() {
        let mut span_data = Annotated::default();
        span_data
            .get_or_insert_with(SpanData::default)
            .gen_ai_operation_name
            .set_value(Some("chat".into()));
        assert!(is_ai_span(&span_data, &Annotated::default()));
    }

    /// Test that an AI span is detected from a span.op starting with "ai.".
    #[test]
    fn test_is_ai_span_from_span_op_ai() {
        assert!(is_ai_span(
            &Annotated::default(),
            &Annotated::new("ai.chat".into())
        ));
    }

    /// Test that an AI span is detected from a span.op starting with "gen_ai.".
    #[test]
    fn test_is_ai_span_from_span_op_gen_ai() {
        assert!(is_ai_span(
            &Annotated::default(),
            &Annotated::new("gen_ai.chat".into())
        ));
    }

    /// Test that a non-AI span is detected.
    #[test]
    fn test_is_ai_span_negative() {
        assert!(!is_ai_span(&Annotated::default(), &Annotated::default()));
    }
}
