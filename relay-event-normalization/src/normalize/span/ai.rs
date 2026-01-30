//! AI cost calculation.

use crate::statsd::{Counters, map_origin_to_integration, platform_tag};
use crate::{ModelCostV2, ModelCosts};
use relay_event_schema::protocol::{
    Event, Measurements, OperationType, Span, SpanData, TraceContext,
};
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
pub fn calculate_costs(
    model_cost: &ModelCostV2,
    tokens: UsedTokens,
    integration: &str,
    platform: &str,
) -> Option<CalculatedCost> {
    if !tokens.has_usage() {
        relay_statsd::metric!(
            counter(Counters::GenAiCostCalculationResult) += 1,
            result = "calculation_none",
            integration = integration,
            platform = platform,
        );
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

    let metric_label = match (input, output) {
        (x, y) if x < 0.0 || y < 0.0 => "calculation_negative",
        (0.0, 0.0) => "calculation_zero",
        _ => "calculation_positive",
    };

    relay_statsd::metric!(
        counter(Counters::GenAiCostCalculationResult) += 1,
        result = metric_label,
        integration = integration,
        platform = platform,
    );

    Some(CalculatedCost { input, output })
}

/// Default AI operation stored in [`GEN_AI_OPERATION_TYPE`](relay_conventions::GEN_AI_OPERATION_TYPE)
/// for AI spans without a well known AI span op.
///
/// See also: [`infer_ai_operation_type`].
pub const DEFAULT_AI_OPERATION: &str = "ai_client";

/// Infers the AI operation from an AI operation name.
///
/// The operation name is usually inferred from the
/// [`GEN_AI_OPERATION_NAME`](relay_conventions::GEN_AI_OPERATION_NAME) span attribute and the span
/// operation.
///
/// Sentry expects the operation type in the
/// [`GEN_AI_OPERATION_TYPE`](relay_conventions::GEN_AI_OPERATION_TYPE) attribute.
///
/// The function returns `None` when the op is not a well known AI operation, callers likely want to default
/// the value to [`DEFAULT_AI_OPERATION`] for AI spans.
pub fn infer_ai_operation_type(op_name: &str) -> Option<&'static str> {
    let ai_op = match op_name {
        // Full matches:
        "ai.run.generateText"
        | "ai.run.generateObject"
        | "gen_ai.invoke_agent"
        | "ai.pipeline.generate_text"
        | "ai.pipeline.generate_object"
        | "ai.pipeline.stream_text"
        | "ai.pipeline.stream_object"
        | "gen_ai.create_agent"
        | "invoke_agent"
        | "create_agent" => "agent",
        "gen_ai.execute_tool" | "execute_tool" => "tool",
        "gen_ai.handoff" | "handoff" => "handoff",
        // Prefix matches:
        op if op.starts_with("ai.streamText.doStream") => "ai_client",
        op if op.starts_with("ai.streamText") => "agent",

        op if op.starts_with("ai.generateText.doGenerate") => "ai_client",
        op if op.starts_with("ai.generateText") => "agent",

        op if op.starts_with("ai.generateObject.doGenerate") => "ai_client",
        op if op.starts_with("ai.generateObject") => "agent",

        op if op.starts_with("ai.toolCall") => "tool",
        // No match:
        _ => return None,
    };

    Some(ai_op)
}

/// Calculates the cost of an AI model based on the model cost and the tokens used.
/// Calculated cost is in US dollars.
fn extract_ai_model_cost_data(
    model_cost: Option<&ModelCostV2>,
    data: &mut SpanData,
    origin: Option<&str>,
    platform: Option<&str>,
) {
    let Some(model_cost) = model_cost else { return };

    let used_tokens = UsedTokens::from_span_data(&*data);
    let integration = map_origin_to_integration(origin);
    let platform = platform_tag(platform);
    let Some(costs) = calculate_costs(model_cost, used_tokens, integration, platform) else {
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
fn map_ai_measurements_to_data(data: &mut SpanData, measurements: Option<&Measurements>) {
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

fn set_total_tokens(data: &mut SpanData) {
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
    data: &mut SpanData,
    duration: f64,
    ai_model_costs: &ModelCosts,
    origin: Option<&str>,
    platform: Option<&str>,
) {
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
        extract_ai_model_cost_data(
            ai_model_costs.cost_per_token(model_id),
            data,
            origin,
            platform,
        )
    }
}

/// Enrich the AI span data
fn enrich_ai_span_data(
    span_data: &mut Annotated<SpanData>,
    span_op: &Annotated<OperationType>,
    measurements: &Annotated<Measurements>,
    duration: f64,
    model_costs: Option<&ModelCosts>,
    origin: Option<&str>,
    platform: Option<&str>,
) {
    if !is_ai_span(span_data, span_op.value()) {
        return;
    }

    let data = span_data.get_or_insert_with(SpanData::default);

    map_ai_measurements_to_data(data, measurements.value());

    set_total_tokens(data);

    if let Some(model_costs) = model_costs {
        extract_ai_data(data, duration, model_costs, origin, platform);
    }

    let ai_op_type = data
        .gen_ai_operation_name
        .value()
        .or(span_op.value())
        .and_then(|op| infer_ai_operation_type(op))
        .unwrap_or(DEFAULT_AI_OPERATION);

    data.gen_ai_operation_type
        .set_value(Some(ai_op_type.to_owned()));
}

/// Enrich the AI span data
pub fn enrich_ai_span(span: &mut Span, model_costs: Option<&ModelCosts>) {
    let duration = span
        .get_value("span.duration")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    enrich_ai_span_data(
        &mut span.data,
        &span.op,
        &span.measurements,
        duration,
        model_costs,
        span.origin.as_str(),
        span.platform.as_str(),
    );
}

/// Extract the ai data from all of an event's spans
pub fn enrich_ai_event_data(event: &mut Event, model_costs: Option<&ModelCosts>) {
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
            trace_context.origin.as_str(),
            event.platform.as_str(),
        );
    }
    let spans = event.spans.value_mut().iter_mut().flatten();
    let spans = spans.filter_map(|span| span.value_mut().as_mut());

    for span in spans {
        let span_duration = span
            .get_value("span.duration")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let span_platform = span.platform.as_str().or_else(|| event.platform.as_str());

        enrich_ai_span_data(
            &mut span.data,
            &span.op,
            &span.measurements,
            span_duration,
            model_costs,
            span.origin.as_str(),
            span_platform,
        );
    }
}

/// Returns true if the span is an AI span.
/// AI spans are spans with either a gen_ai.operation.name attribute or op starting with "ai."
/// (legacy) or "gen_ai." (new).
fn is_ai_span(span_data: &Annotated<SpanData>, span_op: Option<&OperationType>) -> bool {
    let has_ai_op = span_data
        .value()
        .and_then(|data| data.gen_ai_operation_name.value())
        .is_some();

    let is_ai_span_op =
        span_op.is_some_and(|op| op.starts_with("ai.") || op.starts_with("gen_ai."));

    has_ai_op || is_ai_span_op
}

#[cfg(test)]
mod tests {
    use relay_protocol::{FromValue, assert_annotated_snapshot};
    use serde_json::json;

    use super::*;

    fn ai_span_with_data(data: serde_json::Value) -> Span {
        Span {
            op: "gen_ai.test".to_owned().into(),
            data: SpanData::from_value(data.into()),
            ..Default::default()
        }
    }

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
            "test",
            "test",
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
            "test",
            "test",
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
            "test",
            "test",
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
            "test",
            "test",
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
            "test",
            "test",
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
        let span_data = SpanData {
            gen_ai_usage_input_tokens: Annotated::new(100.0.into()),
            gen_ai_usage_input_tokens_cached: Annotated::new(20.0.into()),
            gen_ai_usage_output_tokens: Annotated::new(50.0.into()),
            // Note: gen_ai_usage_input_tokens_cache_write is NOT set (simulating old data)
            ..Default::default()
        };

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
            "test",
            "test",
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
        let mut span = ai_span_with_data(json!({
            "gen_ai.operation.name": "invoke_agent"
        }));

        enrich_ai_span(&mut span, None);

        assert_annotated_snapshot!(&span.data, @r#"
        {
          "gen_ai.operation.name": "invoke_agent",
          "gen_ai.operation.type": "agent"
        }
        "#);
    }

    /// Test that the AI operation type is inferred from a span.op attribute.
    #[test]
    fn test_infer_ai_operation_type_from_span_op() {
        let mut span = Span {
            op: "gen_ai.invoke_agent".to_owned().into(),
            ..Default::default()
        };

        enrich_ai_span(&mut span, None);

        assert_annotated_snapshot!(span.data, @r#"
        {
          "gen_ai.operation.type": "agent"
        }
        "#);
    }

    /// Test that the AI operation type is inferred from a fallback.
    #[test]
    fn test_infer_ai_operation_type_from_fallback() {
        let mut span = ai_span_with_data(json!({
            "gen_ai.operation.name": "embeddings"
        }));

        enrich_ai_span(&mut span, None);

        assert_annotated_snapshot!(&span.data, @r#"
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
        assert!(is_ai_span(&span_data, None));
    }

    /// Test that an AI span is detected from a span.op starting with "ai.".
    #[test]
    fn test_is_ai_span_from_span_op_ai() {
        let span_op: OperationType = "ai.chat".into();
        assert!(is_ai_span(&Annotated::default(), Some(&span_op)));
    }

    /// Test that an AI span is detected from a span.op starting with "gen_ai.".
    #[test]
    fn test_is_ai_span_from_span_op_gen_ai() {
        let span_op: OperationType = "gen_ai.chat".into();
        assert!(is_ai_span(&Annotated::default(), Some(&span_op)));
    }

    /// Test that a non-AI span is detected.
    #[test]
    fn test_is_ai_span_negative() {
        assert!(!is_ai_span(&Annotated::default(), None));
    }

    /// Test enrich_ai_event_data with invoke_agent in trace context and a chat child span.
    #[test]
    fn test_enrich_ai_event_data_invoke_agent_trace_with_chat_span() {
        let event_json = r#"{
            "type": "transaction",
            "timestamp": 1234567892.0,
            "start_timestamp": 1234567889.0,
            "contexts": {
                "trace": {
                    "op": "gen_ai.invoke_agent",
                    "trace_id": "12345678901234567890123456789012",
                    "span_id": "1234567890123456",
                    "data": {
                        "gen_ai.operation.name": "gen_ai.invoke_agent",
                        "gen_ai.usage.input_tokens": 500,
                        "gen_ai.usage.output_tokens": 200
                    }
                }
            },
            "spans": [
                {
                    "op": "gen_ai.chat.completions",
                    "span_id": "1234567890123457",
                    "start_timestamp": 1234567889.5,
                    "timestamp": 1234567890.5,
                    "data": {
                        "gen_ai.operation.name": "chat",
                        "gen_ai.usage.input_tokens": 100,
                        "gen_ai.usage.output_tokens": 50
                    }
                }
            ]
        }"#;

        let mut annotated_event: Annotated<Event> = Annotated::from_json(event_json).unwrap();
        let event = annotated_event.value_mut().as_mut().unwrap();

        enrich_ai_event_data(event, None);

        assert_annotated_snapshot!(&annotated_event, @r#"
        {
          "type": "transaction",
          "timestamp": 1234567892.0,
          "start_timestamp": 1234567889.0,
          "contexts": {
            "trace": {
              "trace_id": "12345678901234567890123456789012",
              "span_id": "1234567890123456",
              "op": "gen_ai.invoke_agent",
              "data": {
                "gen_ai.usage.total_tokens": 700.0,
                "gen_ai.usage.input_tokens": 500,
                "gen_ai.usage.output_tokens": 200,
                "gen_ai.operation.name": "gen_ai.invoke_agent",
                "gen_ai.operation.type": "agent"
              },
              "type": "trace"
            }
          },
          "spans": [
            {
              "timestamp": 1234567890.5,
              "start_timestamp": 1234567889.5,
              "op": "gen_ai.chat.completions",
              "span_id": "1234567890123457",
              "data": {
                "gen_ai.usage.total_tokens": 150.0,
                "gen_ai.usage.input_tokens": 100,
                "gen_ai.usage.output_tokens": 50,
                "gen_ai.operation.name": "chat",
                "gen_ai.operation.type": "ai_client"
              }
            }
          ]
        }
        "#);
    }

    /// Test enrich_ai_event_data with non-AI trace context, invoke_agent parent span, and chat child span.
    #[test]
    fn test_enrich_ai_event_data_nested_agent_and_chat_spans() {
        let event_json = r#"{
            "type": "transaction",
            "timestamp": 1234567892.0,
            "start_timestamp": 1234567889.0,
            "contexts": {
                "trace": {
                    "op": "http.server",
                    "trace_id": "12345678901234567890123456789012",
                    "span_id": "1234567890123456"
                }
            },
            "spans": [
                {
                    "op": "gen_ai.invoke_agent",
                    "span_id": "1234567890123457",
                    "parent_span_id": "1234567890123456",
                    "start_timestamp": 1234567889.5,
                    "timestamp": 1234567891.5,
                    "data": {
                        "gen_ai.operation.name": "invoke_agent",
                        "gen_ai.usage.input_tokens": 500,
                        "gen_ai.usage.output_tokens": 200
                    }
                },
                {
                    "op": "gen_ai.chat.completions",
                    "span_id": "1234567890123458",
                    "parent_span_id": "1234567890123457",
                    "start_timestamp": 1234567890.0,
                    "timestamp": 1234567891.0,
                    "data": {
                        "gen_ai.operation.name": "chat",
                        "gen_ai.usage.input_tokens": 100,
                        "gen_ai.usage.output_tokens": 50
                    }
                }
            ]
        }"#;

        let mut annotated_event: Annotated<Event> = Annotated::from_json(event_json).unwrap();
        let event = annotated_event.value_mut().as_mut().unwrap();

        enrich_ai_event_data(event, None);

        assert_annotated_snapshot!(&annotated_event, @r#"
        {
          "type": "transaction",
          "timestamp": 1234567892.0,
          "start_timestamp": 1234567889.0,
          "contexts": {
            "trace": {
              "trace_id": "12345678901234567890123456789012",
              "span_id": "1234567890123456",
              "op": "http.server",
              "type": "trace"
            }
          },
          "spans": [
            {
              "timestamp": 1234567891.5,
              "start_timestamp": 1234567889.5,
              "op": "gen_ai.invoke_agent",
              "span_id": "1234567890123457",
              "parent_span_id": "1234567890123456",
              "data": {
                "gen_ai.usage.total_tokens": 700.0,
                "gen_ai.usage.input_tokens": 500,
                "gen_ai.usage.output_tokens": 200,
                "gen_ai.operation.name": "invoke_agent",
                "gen_ai.operation.type": "agent"
              }
            },
            {
              "timestamp": 1234567891.0,
              "start_timestamp": 1234567890.0,
              "op": "gen_ai.chat.completions",
              "span_id": "1234567890123458",
              "parent_span_id": "1234567890123457",
              "data": {
                "gen_ai.usage.total_tokens": 150.0,
                "gen_ai.usage.input_tokens": 100,
                "gen_ai.usage.output_tokens": 50,
                "gen_ai.operation.name": "chat",
                "gen_ai.operation.type": "ai_client"
              }
            }
          ]
        }
        "#);
    }

    /// Test enrich_ai_event_data with legacy measurements and span op for operation type.
    #[test]
    fn test_enrich_ai_event_data_legacy_measurements_and_span_op() {
        let event_json = r#"{
            "type": "transaction",
            "timestamp": 1234567892.0,
            "start_timestamp": 1234567889.0,
            "contexts": {
                "trace": {
                    "op": "http.server",
                    "trace_id": "12345678901234567890123456789012",
                    "span_id": "1234567890123456"
                }
            },
            "spans": [
                {
                    "op": "gen_ai.invoke_agent",
                    "span_id": "1234567890123457",
                    "parent_span_id": "1234567890123456",
                    "start_timestamp": 1234567889.5,
                    "timestamp": 1234567891.5,
                    "measurements": {
                        "ai_prompt_tokens_used": {"value": 500.0},
                        "ai_completion_tokens_used": {"value": 200.0}
                    }
                },
                {
                    "op": "ai.chat_completions.create.langchain.ChatOpenAI",
                    "span_id": "1234567890123458",
                    "parent_span_id": "1234567890123457",
                    "start_timestamp": 1234567890.0,
                    "timestamp": 1234567891.0,
                    "measurements": {
                        "ai_prompt_tokens_used": {"value": 100.0},
                        "ai_completion_tokens_used": {"value": 50.0}
                    }
                }
            ]
        }"#;

        let mut annotated_event: Annotated<Event> = Annotated::from_json(event_json).unwrap();
        let event = annotated_event.value_mut().as_mut().unwrap();

        enrich_ai_event_data(event, None);

        assert_annotated_snapshot!(&annotated_event, @r#"
        {
          "type": "transaction",
          "timestamp": 1234567892.0,
          "start_timestamp": 1234567889.0,
          "contexts": {
            "trace": {
              "trace_id": "12345678901234567890123456789012",
              "span_id": "1234567890123456",
              "op": "http.server",
              "type": "trace"
            }
          },
          "spans": [
            {
              "timestamp": 1234567891.5,
              "start_timestamp": 1234567889.5,
              "op": "gen_ai.invoke_agent",
              "span_id": "1234567890123457",
              "parent_span_id": "1234567890123456",
              "data": {
                "gen_ai.usage.total_tokens": 700.0,
                "gen_ai.usage.input_tokens": 500.0,
                "gen_ai.usage.output_tokens": 200.0,
                "gen_ai.operation.type": "agent"
              },
              "measurements": {
                "ai_completion_tokens_used": {
                  "value": 200.0
                },
                "ai_prompt_tokens_used": {
                  "value": 500.0
                }
              }
            },
            {
              "timestamp": 1234567891.0,
              "start_timestamp": 1234567890.0,
              "op": "ai.chat_completions.create.langchain.ChatOpenAI",
              "span_id": "1234567890123458",
              "parent_span_id": "1234567890123457",
              "data": {
                "gen_ai.usage.total_tokens": 150.0,
                "gen_ai.usage.input_tokens": 100.0,
                "gen_ai.usage.output_tokens": 50.0,
                "gen_ai.operation.type": "ai_client"
              },
              "measurements": {
                "ai_completion_tokens_used": {
                  "value": 50.0
                },
                "ai_prompt_tokens_used": {
                  "value": 100.0
                }
              }
            }
          ]
        }
        "#);
    }
}
