use relay_base_schema::metrics::MetricUnit;
use relay_dynamic_config::ModelCosts;
use relay_event_schema::protocol::{Measurement, Span};

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

/// Extract the ai_total_cost measurement into the span.
/// Note that this can't go in relay_event_normalization because it uses relay_dynamic_config
/// which would cause a circular dependency.
pub fn extract_ai_measurements(span: &mut Span, ai_model_costs: &ModelCosts) {
    let Some(span_op) = span.op.value() else {
        return;
    };

    if !span_op.starts_with("ai.") {
        return;
    }
    let total_tokens_used = span
        .measurements
        .value()
        .and_then(|m| m.get_value("ai_total_tokens_used"));
    let prompt_tokens_used = span
        .measurements
        .value()
        .and_then(|m| m.get_value("ai_prompt_tokens_used"));
    let completion_tokens_used = span
        .measurements
        .value()
        .and_then(|m| m.get_value("ai_completion_tokens_used"));
    if let Some(model_id) = span
        .data
        .value()
        .and_then(|d| d.ai_model_id.value())
        .and_then(|val| val.as_str())
    {
        if let Some(total_cost) = calculate_ai_model_cost(
            model_id,
            prompt_tokens_used,
            completion_tokens_used,
            total_tokens_used,
            ai_model_costs,
        ) {
            span.measurements
                .get_or_insert_with(Default::default)
                .insert(
                    "ai_total_cost".to_owned(),
                    Measurement {
                        value: total_cost.into(),
                        unit: MetricUnit::None.into(),
                    }
                    .into(),
                );
        }
    }
}
