//! AI cost calculation.

use crate::ModelCosts;
use relay_event_schema::protocol::{Event, Span, SpanData};
use relay_protocol::{Annotated, Value};

/// Calculated cost is in US dollars.
fn calculate_ai_model_cost(
    model_id: &str,
    prompt_tokens_used: Option<f64>,
    completion_tokens_used: Option<f64>,
    total_tokens_used: Option<f64>,
    ai_model_costs: &ModelCosts,
) -> Option<f64> {
    if let Some(prompt_tokens) = prompt_tokens_used {
        if let Some(completion_tokens) = completion_tokens_used {
            let mut result = 0.0;
            if let Some(cost_per_1k) = ai_model_costs.cost_per_1k_tokens(model_id, false) {
                result += cost_per_1k * (prompt_tokens / 1000.0)
            }
            if let Some(cost_per_1k) = ai_model_costs.cost_per_1k_tokens(model_id, true) {
                result += cost_per_1k * (completion_tokens / 1000.0)
            }
            return Some(result);
        }
    }
    if let Some(total_tokens) = total_tokens_used {
        ai_model_costs
            .cost_per_1k_tokens(model_id, false)
            .map(|cost| cost * (total_tokens / 1000.0))
    } else {
        None
    }
}

/// Maps AI-related measurements (legacy) to span data.
pub fn map_ai_measurements_to_data(span: &mut Span) {
    if !span.op.value().is_some_and(|op| op.starts_with("ai.")) {
        return;
    };

    let measurements = span.measurements.value();
    let data = span.data.get_or_insert_with(SpanData::default);

    let set_field_from_measurement = |target_field: &mut Annotated<Value>,
                                      measurement_key: &str| {
        if let Some(measurements) = measurements {
            if target_field.value().is_none() {
                if let Some(value) = measurements.get_value(measurement_key) {
                    target_field.set_value(Value::F64(value).into());
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
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        let output_tokens = data
            .gen_ai_usage_output_tokens
            .value()
            .and_then(Value::as_f64)
            .unwrap_or(0.0);
        data.gen_ai_usage_total_tokens
            .set_value(Value::F64(input_tokens + output_tokens).into());
    }
}

/// Extract the gen_ai_usage_total_cost data into the span
pub fn extract_ai_data(span: &mut Span, ai_model_costs: &ModelCosts) {
    if !span.op.value().is_some_and(|op| op.starts_with("ai.")) {
        return;
    }

    let Some(data) = span.data.value_mut() else {
        return;
    };

    let total_tokens_used = data
        .gen_ai_usage_total_tokens
        .value()
        .and_then(Value::as_f64);
    let prompt_tokens_used = data
        .gen_ai_usage_input_tokens
        .value()
        .and_then(Value::as_f64);
    let completion_tokens_used = data
        .gen_ai_usage_output_tokens
        .value()
        .and_then(Value::as_f64);

    if let Some(model_id) = data.ai_model_id.value().and_then(|val| val.as_str()) {
        if let Some(total_cost) = calculate_ai_model_cost(
            model_id,
            prompt_tokens_used,
            completion_tokens_used,
            total_tokens_used,
            ai_model_costs,
        ) {
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
