macro_rules! convention_attributes {
    ($($name:ident => $attr:literal,)*) => {
        $(pub const $name: &str = $attr;)*

        #[test]
        fn test_attributes_defined_in_conventions() {
            $(
                assert!(crate::attribute_info($name).is_some());
            )*
        }
    };
}

// Attributes which can also be found in Sentry conventions.
convention_attributes!(
    BROWSER_NAME => "sentry.browser.name",
    BROWSER_VERSION => "sentry.browser.version",
    CLIENT_ADDRESS => "client.address",
    CLIENT_SAMPLE_RATE => "sentry.client_sample_rate",
    DB_QUERY_TEXT => "db.query.text",
    DB_STATEMENT => "db.statement",
    DB_SYSTEM_NAME => "db.system.name",
    DESCRIPTION => "sentry.description",
    DSC_ENVIRONMENT => "sentry.dsc.environment",
    DSC_PUBLIC_KEY => "sentry.dsc.public_key",
    DSC_RELEASE => "sentry.dsc.release",
    DSC_SAMPLED => "sentry.dsc.sampled",
    DSC_SAMPLE_RATE => "sentry.dsc.sample_rate",
    DSC_TRACE_ID => "sentry.dsc.trace_id",
    DSC_TRANSACTION => "sentry.dsc.transaction",
    ENVIRONMENT => "sentry.environment",
    FAAS_TRIGGER => "faas.trigger",
    GEN_AI_COST_INPUT_TOKENS => "gen_ai.cost.input_tokens",
    GEN_AI_COST_OUTPUT_TOKENS => "gen_ai.cost.output_tokens",
    GEN_AI_COST_TOTAL_TOKENS => "gen_ai.cost.total_tokens",
    GEN_AI_REQUEST_MODEL => "gen_ai.request.model",
    GEN_AI_RESPONSE_MODEL => "gen_ai.response.model",
    GEN_AI_RESPONSE_TPS => "gen_ai.response.tokens_per_second",
    GEN_AI_SYSTEM => "gen_ai.system",
    GEN_AI_USAGE_INPUT_CACHED_TOKENS => "gen_ai.usage.input_tokens.cached",
    GEN_AI_USAGE_INPUT_TOKENS => "gen_ai.usage.input_tokens",
    GEN_AI_USAGE_OUTPUT_REASONING_TOKENS => "gen_ai.usage.output_tokens.reasoning",
    GEN_AI_USAGE_OUTPUT_TOKENS => "gen_ai.usage.output_tokens",
    GEN_AI_USAGE_TOTAL_TOKENS => "gen_ai.usage.total_tokens",
    GRAPHQL_OPERATION => "sentry.graphql.operation",
    HTTP_PREFETCH => "sentry.http.prefetch",
    HTTP_REQUEST_METHOD => "http.request.method",
    HTTP_RESPONSE_STATUS_CODE => "http.response.status_code",
    HTTP_ROUTE => "http.route",
    HTTP_TARGET => "http.target",
    IS_REMOTE => "sentry.is_remote",
    MESSAGING_SYSTEM => "messaging.system",
    OBSERVED_TIMESTAMP_NANOS => "sentry.observed_timestamp_nanos",
    OP => "sentry.op",
    ORIGIN => "sentry.origin",
    PLATFORM => "sentry.platform",
    PROFILE_ID => "sentry.profile_id",
    RELEASE => "sentry.release",
    RPC_GRPC_STATUS_CODE => "rpc.grpc.status_code",
    RPC_SERVICE => "rpc.service",
    SEGMENT_ID => "sentry.segment.id",
    SEGMENT_NAME => "sentry.segment.name",
    SERVER_ADDRESS => "server.address",
    SPAN_KIND => "sentry.kind",
    STATUS_MESSAGE => "sentry.status.message",
    URL_FULL => "url.full",
    URL_PATH => "url.path",
    USER_AGENT_ORIGINAL => "user_agent.original",
    USER_GEO_CITY => "user.geo.city",
    USER_GEO_COUNTRY_CODE => "user.geo.country_code",
    USER_GEO_REGION => "user.geo.region",
    USER_GEO_SUBDIVISION => "user.geo.subdivision",
);

/// Attributes which are in use by Relay but are not yet defined in the Sentry conventions.
///
/// Really do not add to this list, at all, ever. The only reason this opt-out even exists to make a
/// transition easier for attributes which Relay already uses but aren't yet in conventions.
mod not_yet_defined {}
#[expect(unused, reason = "no special attributes at the moment")]
pub use self::not_yet_defined::*;
