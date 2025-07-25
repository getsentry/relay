//! AI cost calculation.

use crate::{ModelCostV2, ModelCosts};
use relay_event_schema::protocol::{Event, Span, SpanData};
use relay_protocol::{Annotated, Getter, Value};

/// Calculates the cost of an AI model based on the model cost and the tokens used.
/// Calculated cost is in US dollars.
fn calculate_ai_model_cost(model_cost: Option<ModelCostV2>, data: &SpanData) -> Option<f64> {
    let cost_per_token = model_cost?;
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
        return None;
    }

    let mut result = 0.0;

    // Cached tokens are subset of the input tokens, so we need to subtract them
    // from the input tokens
    result += cost_per_token.input_per_token
        * (input_tokens_used.unwrap_or(0.0) - input_cached_tokens_used.unwrap_or(0.0));
    result += cost_per_token.input_cached_per_token * input_cached_tokens_used.unwrap_or(0.0);
    // Reasoning tokens are subset of the output tokens, so we need to subtract
    // them from the output tokens
    result += cost_per_token.output_per_token
        * (output_tokens_used.unwrap_or(0.0) - output_reasoning_tokens_used.unwrap_or(0.0));

    if cost_per_token.output_reasoning_per_token > 0.0 {
        // for now most of the models do not differentiate between reasoning and output token cost,
        // it costs the same
        result +=
            cost_per_token.output_reasoning_per_token * output_reasoning_tokens_used.unwrap_or(0.0);
    } else {
        result += cost_per_token.output_per_token * output_reasoning_tokens_used.unwrap_or(0.0);
    }

    Some(result)
}

/// Maps AI-related measurements (legacy) to span data.
pub fn map_ai_measurements_to_data(span: &mut Span) {
    if !is_ai_span(span) {
        return;
    };

    let measurements = span.measurements.value();
    let data = span.data.get_or_insert_with(SpanData::default);

    let set_field_from_measurement = |target_field: &mut Annotated<Value>,
                                      measurement_key: &str| {
        if let Some(measurements) = measurements {
            if target_field.value().is_none() {
                if let Some(value) = measurements.get_value(measurement_key) {
                    target_field.set_value(Value::F64(value.to_f64()).into());
                }
            }
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
pub fn extract_ai_data(span: &mut Span, ai_model_costs: &ModelCosts) {
    if !is_ai_span(span) {
        return;
    }

    let duration = span
        .get_value("span.duration")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let Some(data) = span.data.value_mut() else {
        return;
    };

    // Extracts the response tokens per second
    if data.gen_ai_response_tokens_per_second.value().is_none() && duration > 0.0 {
        if let Some(output_tokens) = data
            .gen_ai_usage_output_tokens
            .value()
            .and_then(Value::as_f64)
        {
            data.gen_ai_response_tokens_per_second
                .set_value(Value::F64(output_tokens / (duration / 1000.0)).into());
        }
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
        if let Some(total_cost) =
            calculate_ai_model_cost(ai_model_costs.cost_per_token(model_id), data)
        {
            data.gen_ai_usage_total_cost
                .set_value(Value::F64(total_cost).into());
        }
    }
}

/// Extract the ai data from all of an event's spans
pub fn enrich_ai_span_data(event: &mut Event, model_costs: Option<&ModelCosts>) {
    let spans = event.spans.value_mut().iter_mut().flatten();
    let spans = spans.filter_map(|span| span.value_mut().as_mut());

    for span in spans {
        map_ai_measurements_to_data(span);
        if let Some(model_costs) = model_costs {
            extract_ai_data(span, model_costs);
        }
    }
}

/// Returns true if the span is an AI span.
/// AI spans are spans with op starting with "ai." (legacy) or "gen_ai." (new).
pub fn is_ai_span(span: &Span) -> bool {
    span.op
        .value()
        .is_some_and(|op| op.starts_with("ai.") || op.starts_with("gen_ai."))
}
