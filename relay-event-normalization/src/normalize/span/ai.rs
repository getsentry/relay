//! AI cost calculation.
use crate::statsd::{Counters, map_origin_to_integration, platform_tag};
use crate::{ModelCostV2, ModelMetadata};
use relay_event_schema::protocol::{
    Event,
    Measurements,
    OperationType,
    Span,
    SpanData,
    TraceContext,
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
        self.input_tokens - self.input_cached_tokens - self.input_cache_write_tokens
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
            result = "calculation_no_tokens",
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

/// Default AI operation stored in [`GEN_AI__OPERATION__TYPE`](relay_conventions::attributes::GEN_AI__OPERATION__TYPE)
/// for AI spans without a well known AI span op.
/// 
/// See also: [`infer_ai_operation_type`].
pub const DEFAULT_AI_OPERATION: &str = "ai_client";

/// Infers the AI operation from an AI operation name.
/// 
/// The operation name is usually inferred from the
/// [`GEN_AI__OPERATION__NAME`](relay_conventions::attributes::GEN_AI__OPERATION__NAME) span attribute and the span
/// operation.
/// 
/// Sentry expects the operation type in the
/// [`GEN_AI__OPERATION__TYPE`](relay_conventions::attributes::GEN_AI__OPERATION__TYPE) attribute.
/// 
/// The function returns `None` if the operation type is unknown.
