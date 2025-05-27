//! AI cost calculation.

use crate::ModelCosts;
use relay_event_schema::protocol::{Event, Span, SpanData};
use relay_protocol::Value;

/// Converts a protocol Value to f64 if possible.
pub fn value_to_f64(val: &relay_protocol::Value) -> Option<f64> {
    match val {
        relay_protocol::Value::F64(f) => Some(*f),
        relay_protocol::Value::I64(i) => Some(*i as f64),
        relay_protocol::Value::U64(u) => Some(*u as f64),
        _ => None,
    }
}

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
    let Some(span_op) = span.op.value() else {
        return;
    };

    if !span_op.starts_with("ai.") {
        return;
    }

    let Some(measurements) = span.measurements.value() else {
        return;
    };

    // It might be that there is no data in such a case we want to add it here to have something
    // to put the measurements inside it.
    let data = span.data.get_or_insert_with(SpanData::default);

    if let Some(ai_total_tokens_used) = measurements.get_value("ai_total_tokens_used") {
        data.ai_total_tokens_used
            .set_value(Value::F64(ai_total_tokens_used).into());
    }

    if let Some(ai_prompt_tokens_used) = measurements.get_value("ai_prompt_tokens_used") {
        data.ai_prompt_tokens_used
            .set_value(Value::F64(ai_prompt_tokens_used).into());
    }

    if let Some(ai_completion_tokens_used) = measurements.get_value("ai_completion_tokens_used") {
        data.ai_completion_tokens_used
            .set_value(Value::F64(ai_completion_tokens_used).into());
    }
}

/// Extract the gen_ai_usage_total_cost data into the span.
pub fn extract_ai_data(span: &mut Span, ai_model_costs: &ModelCosts) {
    let Some(span_op) = span.op.value() else {
        return;
    };

    if !span_op.starts_with("ai.") {
        return;
    }

    let Some(data) = span.data.value_mut() else {
        return;
    };

    let total_tokens_used = data.ai_total_tokens_used.value().and_then(value_to_f64);
    let prompt_tokens_used = data.ai_prompt_tokens_used.value().and_then(value_to_f64);
    let completion_tokens_used = data
        .ai_completion_tokens_used
        .value()
        .and_then(value_to_f64);

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

/// Extract the ai_total_cost measurements from all of an event's spans
pub fn normalize_ai_measurements(event: &mut Event, model_costs: Option<&ModelCosts>) {
    if let Some(spans) = event.spans.value_mut() {
        for span in spans {
            if let Some(mut_span) = span.value_mut() {
                map_ai_measurements_to_data(mut_span);
            }
        }
    }
    if let Some(model_costs) = model_costs {
        if let Some(spans) = event.spans.value_mut() {
            for span in spans {
                if let Some(mut_span) = span.value_mut() {
                    extract_ai_data(mut_span, model_costs);
                }
            }
        }
    }
}
