//! This file maps AI model IDs to their respective costs.
//! Adapted from https =>//github.com/langchain-ai/langchain/blob/5b6d1a907df8a7af0f47c5ffd394b3673d9539e5/libs/community/langchain_community/callbacks/openai_info.py#L4

/// Cost is in US dollars per thousand tokens
pub fn calculate_ai_model_cost(model_id: &str) -> Option<f64> {
    match model_id {
        // GPT-4 input
        "gpt-4" => Some(0.03),
        "gpt-4-0314" => Some(0.03),
        "gpt-4-0613" => Some(0.03),
        "gpt-4-32k" => Some(0.06),
        "gpt-4-32k-0314" => Some(0.06),
        "gpt-4-32k-0613" => Some(0.06),
        "gpt-4-vision-preview" => Some(0.01),
        "gpt-4-1106-preview" => Some(0.01),
        "gpt-4-0125-preview" => Some(0.01),
        "gpt-4-turbo-preview" => Some(0.01),
        "gpt-4-turbo" => Some(0.01),
        "gpt-4-turbo-2024-04-09" => Some(0.01),
        // GPT-4 output
        "gpt-4-completion" => Some(0.06),
        "gpt-4-0314-completion" => Some(0.06),
        "gpt-4-0613-completion" => Some(0.06),
        "gpt-4-32k-completion" => Some(0.12),
        "gpt-4-32k-0314-completion" => Some(0.12),
        "gpt-4-32k-0613-completion" => Some(0.12),
        "gpt-4-vision-preview-completion" => Some(0.03),
        "gpt-4-1106-preview-completion" => Some(0.03),
        "gpt-4-0125-preview-completion" => Some(0.03),
        "gpt-4-turbo-preview-completion" => Some(0.03),
        "gpt-4-turbo-completion" => Some(0.03),
        "gpt-4-turbo-2024-04-09-completion" => Some(0.03),
        // GPT-3.5 input
        "gpt-3.5-turbo" => Some(0.0005),
        "gpt-3.5-turbo-0125" => Some(0.0005),
        "gpt-3.5-turbo-0301" => Some(0.0015),
        "gpt-3.5-turbo-0613" => Some(0.0015),
        "gpt-3.5-turbo-1106" => Some(0.001),
        "gpt-3.5-turbo-instruct" => Some(0.0015),
        "gpt-3.5-turbo-16k" => Some(0.003),
        "gpt-3.5-turbo-16k-0613" => Some(0.003),
        // GPT-3.5 output
        "gpt-3.5-turbo-completion" => Some(0.0015),
        "gpt-3.5-turbo-0125-completion" => Some(0.0015),
        "gpt-3.5-turbo-0301-completion" => Some(0.002),
        "gpt-3.5-turbo-0613-completion" => Some(0.002),
        "gpt-3.5-turbo-1106-completion" => Some(0.002),
        "gpt-3.5-turbo-instruct-completion" => Some(0.002),
        "gpt-3.5-turbo-16k-completion" => Some(0.004),
        "gpt-3.5-turbo-16k-0613-completion" => Some(0.004),
        // Azure GPT-35 input
        "gpt-35-turbo" => Some(0.0015), // Azure OpenAI version of ChatGPT
        "gpt-35-turbo-0301" => Some(0.0015), // Azure OpenAI version of ChatGPT
        "gpt-35-turbo-0613" => Some(0.0015),
        "gpt-35-turbo-instruct" => Some(0.0015),
        "gpt-35-turbo-16k" => Some(0.003),
        "gpt-35-turbo-16k-0613" => Some(0.003),
        // Azure GPT-35 output
        "gpt-35-turbo-completion" => Some(0.002), // Azure OpenAI version of ChatGPT
        "gpt-35-turbo-0301-completion" => Some(0.002), // Azure OpenAI version of ChatGPT
        "gpt-35-turbo-0613-completion" => Some(0.002),
        "gpt-35-turbo-instruct-completion" => Some(0.002),
        "gpt-35-turbo-16k-completion" => Some(0.004),
        "gpt-35-turbo-16k-0613-completion" => Some(0.004),
        // Others
        "text-ada-001" => Some(0.0004),
        "ada" => Some(0.0004),
        "text-babbage-001" => Some(0.0005),
        "babbage" => Some(0.0005),
        "text-curie-001" => Some(0.002),
        "curie" => Some(0.002),
        "text-davinci-003" => Some(0.02),
        "text-davinci-002" => Some(0.02),
        "code-davinci-002" => Some(0.02),
        // Fine Tuned input
        "babbage-002-finetuned" => Some(0.0016),
        "davinci-002-finetuned" => Some(0.012),
        "gpt-3.5-turbo-0613-finetuned" => Some(0.012),
        "gpt-3.5-turbo-1106-finetuned" => Some(0.012),
        // Fine Tuned output
        "babbage-002-finetuned-completion" => Some(0.0016),
        "davinci-002-finetuned-completion" => Some(0.012),
        "gpt-3.5-turbo-0613-finetuned-completion" => Some(0.016),
        "gpt-3.5-turbo-1106-finetuned-completion" => Some(0.016),
        // Azure Fine Tuned input
        "babbage-002-azure-finetuned" => Some(0.0004),
        "davinci-002-azure-finetuned" => Some(0.002),
        "gpt-35-turbo-0613-azure-finetuned" => Some(0.0015),
        // Azure Fine Tuned output
        "babbage-002-azure-finetuned-completion" => Some(0.0004),
        "davinci-002-azure-finetuned-completion" => Some(0.002),
        "gpt-35-turbo-0613-azure-finetuned-completion" => Some(0.002),
        // Legacy fine-tuned models
        "ada-finetuned-legacy" => Some(0.0016),
        "babbage-finetuned-legacy" => Some(0.0024),
        "curie-finetuned-legacy" => Some(0.012),
        "davinci-finetuned-legacy" => Some(0.12),

        _ => None,
    }
}
