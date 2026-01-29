use relay_statsd::{CounterMetric, TimerMetric};

pub enum Counters {
    GenAiCostCalculationResult,
}

impl CounterMetric for Counters {
    fn name(&self) -> &'static str {
        match *self {
            Self::GenAiCostCalculationResult => "gen_ai.cost_calculation.result",
        }
    }
}

pub enum Timers {
    /// Measures how log normalization of SQL queries in span description take.
    ///
    /// This metric is tagged with:
    ///  - `mode`: The method used for normalization (either `parser` or `regex`).
    SpanDescriptionNormalizeSQL,
}

impl TimerMetric for Timers {
    fn name(&self) -> &'static str {
        match *self {
            Self::SpanDescriptionNormalizeSQL => "normalize.span.description.sql",
        }
    }
}

/// Maps a span origin to a well-known AI integration name for metrics.
///
/// Origins follow the pattern `auto.<integration>.<source>` or `auto.<category>.<protocol>.<source>`.
/// This function extracts recognized AI integrations for cleaner metric tagging.
pub fn map_origin_to_integration(origin: Option<&str>) -> &'static str {
    match origin {
        Some(o) if o.starts_with("auto.ai.openai_agents") => "openai_agents",
        Some(o) if o.starts_with("auto.ai.openai") => "openai",
        Some(o) if o.starts_with("auto.ai.anthropic") => "anthropic",
        Some(o) if o.starts_with("auto.ai.cohere") => "cohere",
        Some(o) if o.starts_with("auto.vercelai.") => "vercelai",
        Some(o) if o.starts_with("auto.ai.langchain") => "langchain",
        Some(o) if o.starts_with("auto.ai.langgraph") => "langgraph",
        Some(o) if o.starts_with("auto.ai.google_genai") => "google_genai",
        Some(o) if o.starts_with("auto.ai.pydantic_ai") => "pydantic_ai",
        Some(o) if o.starts_with("auto.ai.huggingface_hub") => "huggingface_hub",
        Some(o) if o.starts_with("auto.ai.litellm") => "litellm",
        Some(o) if o.starts_with("auto.ai.mcp_server") => "mcp_server",
        Some(o) if o.starts_with("auto.ai.mcp") => "mcp",
        Some(o) if o.starts_with("auto.ai.claude_agent_sdk") => "claude_agent_sdk",
        Some(o) if o.starts_with("auto.ai.") => "other",
        Some(_) => "other",
        None => "unknown",
    }
}

pub fn platform_tag(platform: Option<&str>) -> &'static str {
    match platform {
        Some("cocoa") => "cocoa",
        Some("csharp") => "csharp",
        Some("edge") => "edge",
        Some("go") => "go",
        Some("java") => "java",
        Some("javascript") => "javascript",
        Some("julia") => "julia",
        Some("native") => "native",
        Some("node") => "node",
        Some("objc") => "objc",
        Some("perl") => "perl",
        Some("php") => "php",
        Some("python") => "python",
        Some("ruby") => "ruby",
        Some("swift") => "swift",
        Some(_) => "other",
        None => "unknown",
    }
}
