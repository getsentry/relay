//! AI cost calculation.

use crate::normalize::AiOperationTypeMap;
use crate::{ModelCostV2, ModelCosts};
use relay_event_schema::protocol::{Event, Span, SpanData};
use relay_protocol::{Annotated, Getter, Value};

/// Calculates the cost of an AI model based on the model cost and the tokens used.
/// Calculated cost is in US dollars.
fn extract_ai_model_cost_data(model_cost: Option<&ModelCostV2>, data: &mut SpanData) {
    let cost_per_token = match model_cost {
        Some(v) => v,
        None => return,
    };

    let input_tokens_used = data
        .gen_ai_usage_input_tokens
        .value()
        .and_then(Value::as_f64);

    let output_tokens_used = data
        .gen_ai_usage_output_tokens
        .value()
        .and_then(Value::as_f64);
    let output_reasoning_tokens_used = data
        .gen_ai_usage_output_tokens_reasoning
        .value()
        .and_then(Value::as_f64);
    let input_cached_tokens_used = data
        .gen_ai_usage_input_tokens_cached
        .value()
        .and_then(Value::as_f64);

    if input_tokens_used.is_none() && output_tokens_used.is_none() {
        return;
    }

    let mut input_cost = 0.0;
    let mut output_cost = 0.0;
    // Cached tokens are subset of the input tokens, so we need to subtract them
    // from the input tokens
    input_cost += cost_per_token.input_per_token
        * (input_tokens_used.unwrap_or(0.0) - input_cached_tokens_used.unwrap_or(0.0));
    input_cost += cost_per_token.input_cached_per_token * input_cached_tokens_used.unwrap_or(0.0);
    // Reasoning tokens are subset of the output tokens, so we need to subtract
    // them from the output tokens
    output_cost += cost_per_token.output_per_token
        * (output_tokens_used.unwrap_or(0.0) - output_reasoning_tokens_used.unwrap_or(0.0));

    if cost_per_token.output_reasoning_per_token > 0.0 {
        // for now most of the models do not differentiate between reasoning and output token cost,
        // it costs the same
        output_cost +=
            cost_per_token.output_reasoning_per_token * output_reasoning_tokens_used.unwrap_or(0.0);
    } else {
        output_cost +=
            cost_per_token.output_per_token * output_reasoning_tokens_used.unwrap_or(0.0);
    }

    let result = input_cost + output_cost;
    // double write during migration period
    // 'gen_ai_usage_total_cost' is deprecated and will be removed in the future
    data.gen_ai_usage_total_cost
        .set_value(Value::F64(result).into());
    data.gen_ai_cost_total_tokens
        .set_value(Value::F64(result).into());

    // Set individual cost components
    data.gen_ai_cost_input_tokens
        .set_value(Value::F64(input_cost).into());
    data.gen_ai_cost_output_tokens
        .set_value(Value::F64(output_cost).into());
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
/// This function maps span.op values to gen_ai.operation.type based on the provided
/// operation type map configuration.
fn infer_ai_operation_type(span: &mut Span, operation_type_map: &AiOperationTypeMap) {
    let data = span.data.get_or_insert_with(SpanData::default);

    if let Some(op) = span.op.value()
        && let Some(operation_type) = operation_type_map.get_operation_type(op)
    {
        data.gen_ai_operation_type
            .set_value(Some(operation_type.to_owned()));
    }
}

/// Returns true if the span is an AI span.
/// AI spans are spans with op starting with "ai." (legacy) or "gen_ai." (new).
fn is_ai_span(span: &Span) -> bool {
    span.op
        .value()
        .is_some_and(|op| op.starts_with("ai.") || op.starts_with("gen_ai."))
}
