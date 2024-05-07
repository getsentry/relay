//! This file maps AI model IDs to their respective costs.
//! Adapted from <https://github.com/langchain-ai/langchain/blob/5b6d1a907df8a7af0f47c5ffd394b3673d9539e5/libs/community/langchain_community/callbacks/openai_info.py#L4>

/// Cost is in US dollars per thousand tokens
fn calculate_cost_per_1k_tokens(model_id: &str, for_completion: bool) -> Option<f64> {
    match (model_id, for_completion) {
        // GPT-4 input
        ("gpt-4", false) => Some(0.03),
        ("gpt-4-0314", false) => Some(0.03),
        ("gpt-4-0613", false) => Some(0.03),
        ("gpt-4-32k", false) => Some(0.06),
        ("gpt-4-32k-0314", false) => Some(0.06),
        ("gpt-4-32k-0613", false) => Some(0.06),
        ("gpt-4-vision-preview", false) => Some(0.01),
        ("gpt-4-1106-preview", false) => Some(0.01),
        ("gpt-4-0125-preview", false) => Some(0.01),
        ("gpt-4-turbo-preview", false) => Some(0.01),
        ("gpt-4-turbo", false) => Some(0.01),
        ("gpt-4-turbo-2024-04-09", false) => Some(0.01),

        // GPT-4 output
        ("gpt-4", true) => Some(0.06),
        ("gpt-4-0314", true) => Some(0.06),
        ("gpt-4-0613", true) => Some(0.06),
        ("gpt-4-32k", true) => Some(0.12),
        ("gpt-4-32k-0314", true) => Some(0.12),
        ("gpt-4-32k-0613", true) => Some(0.12),
        ("gpt-4-vision-preview", true) => Some(0.03),
        ("gpt-4-1106-preview", true) => Some(0.03),
        ("gpt-4-0125-preview", true) => Some(0.03),
        ("gpt-4-turbo-preview", true) => Some(0.03),
        ("gpt-4-turbo", true) => Some(0.03),
        ("gpt-4-turbo-2024-04-09", true) => Some(0.03),

        // GPT-3.5 input
        ("gpt-3.5-turbo", false) => Some(0.0005),
        ("gpt-3.5-turbo-0125", false) => Some(0.0005),
        ("gpt-3.5-turbo-0301", false) => Some(0.0015),
        ("gpt-3.5-turbo-0613", false) => Some(0.0015),
        ("gpt-3.5-turbo-1106", false) => Some(0.001),
        ("gpt-3.5-turbo-instruct", false) => Some(0.0015),
        ("gpt-3.5-turbo-16k", false) => Some(0.003),
        ("gpt-3.5-turbo-16k-0613", false) => Some(0.003),

        // GPT-3.5 output
        ("gpt-3.5-turbo", true) => Some(0.0015),
        ("gpt-3.5-turbo-0125", true) => Some(0.0015),
        ("gpt-3.5-turbo-0301", true) => Some(0.002),
        ("gpt-3.5-turbo-0613", true) => Some(0.002),
        ("gpt-3.5-turbo-1106", true) => Some(0.002),
        ("gpt-3.5-turbo-instruct", true) => Some(0.002),
        ("gpt-3.5-turbo-16k", true) => Some(0.004),
        ("gpt-3.5-turbo-16k-0613", true) => Some(0.004),

        // Azure GPT-35 input
        ("gpt-35-turbo", false) => Some(0.0015), // Azure OpenAI version of ChatGPT
        ("gpt-35-turbo-0301", false) => Some(0.0015), // Azure OpenAI version of ChatGPT
        ("gpt-35-turbo-0613", false) => Some(0.0015),
        ("gpt-35-turbo-instruct", false) => Some(0.0015),
        ("gpt-35-turbo-16k", false) => Some(0.003),
        ("gpt-35-turbo-16k-0613", false) => Some(0.003),

        // Azure GPT-35 output
        ("gpt-35-turbo", true) => Some(0.002), // Azure OpenAI version of ChatGPT
        ("gpt-35-turbo-0301", true) => Some(0.002), // Azure OpenAI version of ChatGPT
        ("gpt-35-turbo-0613", true) => Some(0.002),
        ("gpt-35-turbo-instruct", true) => Some(0.002),
        ("gpt-35-turbo-16k", true) => Some(0.004),
        ("gpt-35-turbo-16k-0613", true) => Some(0.004),

        // Other OpenAI models
        ("text-ada-001", _) => Some(0.0004),
        ("ada", _) => Some(0.0004),
        ("text-babbage-001", _) => Some(0.0005),
        ("babbage", _) => Some(0.0005),
        ("text-curie-001", _) => Some(0.002),
        ("curie", _) => Some(0.002),
        ("text-davinci-003", _) => Some(0.02),
        ("text-davinci-002", _) => Some(0.02),
        ("code-davinci-002", _) => Some(0.02),

        // Fine-tuned OpenAI input
        ("babbage-002-finetuned", false) => Some(0.0016),
        ("davinci-002-finetuned", false) => Some(0.012),
        ("gpt-3.5-turbo-0613-finetuned", false) => Some(0.012),
        ("gpt-3.5-turbo-1106-finetuned", false) => Some(0.012),

        // Fine-tuned OpenAI output
        ("babbage-002-finetuned", true) => Some(0.0016),
        ("davinci-002-finetuned", true) => Some(0.012),
        ("gpt-3.5-turbo-0613-finetuned", true) => Some(0.016),
        ("gpt-3.5-turbo-1106-finetuned", true) => Some(0.016),

        // Azure OpenAI Fine-tuned input
        ("babbage-002-azure-finetuned", false) => Some(0.0004),
        ("davinci-002-azure-finetuned", false) => Some(0.002),
        ("gpt-35-turbo-0613-azure-finetuned", false) => Some(0.0015),

        // Azure OpenAI Fine-tuned output
        ("babbage-002-azure-finetuned", true) => Some(0.0004),
        ("davinci-002-azure-finetuned", true) => Some(0.002),
        ("gpt-35-turbo-0613-azure-finetuned", true) => Some(0.002),

        // Legacy OpenAI Fine-tuned models
        ("ada-finetuned-legacy", _) => Some(0.0016),
        ("babbage-finetuned-legacy", _) => Some(0.0024),
        ("curie-finetuned-legacy", _) => Some(0.012),
        ("davinci-finetuned-legacy", _) => Some(0.12),

        // Anthropic Claude 3 input
        ("claude-3-haiku", false) => Some(0.00025),
        ("claude-3-sonnet", false) => Some(0.003),
        ("claude-3-opus", false) => Some(0.015),

        // Anthropic Claude 3 output
        ("claude-3-haiku", true) => Some(0.00125),
        ("claude-3-sonnet", true) => Some(0.015),
        ("claude-3-opus", true) => Some(0.075),

        // Anthropic Claude 2 input
        (s, false) if s.starts_with("claude-2.") => Some(0.008),
        (s, false) if s.starts_with("claude-instant") => Some(0.0008),

        // Anthropic Claude 2 output
        (s, true) if s.starts_with("claude-2.") => Some(0.024),
        (s, true) if s.starts_with("claude-instant") => Some(0.0024),

        // Cohere command input
        ("command", false) => Some(0.001),
        ("command-light", false) => Some(0.0003),
        ("command-r", false) => Some(0.0005),
        ("command-r-plus", false) => Some(0.003),

        // Cohere command output
        ("command", true) => Some(0.002),
        ("command-light", true) => Some(0.0006),
        ("command-r", true) => Some(0.0015),
        ("command-r-plus", true) => Some(0.015),

        _ => None,
    }
}

/// Calculated cost is in US dollars.
pub fn calculate_ai_model_cost(
    model_id: &str,
    prompt_tokens_used: Option<f64>,
    completion_tokens_used: Option<f64>,
    total_tokens_used: Option<f64>,
) -> Option<f64> {
    let mut normalized_model_id: String = model_id.to_string();
    if let Some(split) = model_id.split_once(".ft-") {
        normalized_model_id.clear();
        normalized_model_id.push_str(split.0);
        normalized_model_id.push_str("-azure-finetuned");
    }
    if let Some(split) = model_id.split_once(":ft-") {
        normalized_model_id.clear();
        normalized_model_id.push_str(split.0);
        normalized_model_id.push_str("-finetuned-legacy");
    }
    if let Some(split) = model_id.split_once("ft:") {
        normalized_model_id.clear();
        normalized_model_id.push_str(split.1);
        normalized_model_id.push_str("-finetuned");
    }
    if let Some(prompt_tokens) = prompt_tokens_used {
        if let Some(completion_tokens) = completion_tokens_used {
            let mut result = 0.0;
            if let Some(cost_per_1k) =
                calculate_cost_per_1k_tokens(normalized_model_id.as_str(), false)
            {
                result += cost_per_1k * (prompt_tokens / 1000.0)
            }
            if let Some(cost_per_1k) =
                calculate_cost_per_1k_tokens(normalized_model_id.as_str(), true)
            {
                result += cost_per_1k * (completion_tokens / 1000.0)
            }
            return Some(result);
        }
    }
    if let Some(total_tokens) = total_tokens_used {
        calculate_cost_per_1k_tokens(normalized_model_id.as_str(), false)
            .map(|cost| cost * (total_tokens / 1000.0))
    } else {
        None
    }
}
