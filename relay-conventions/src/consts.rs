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
    DB_SYSTEM => "db.system",
    DB_SYSTEM_NAME => "db.system.name",
    DB_OPERATION_NAME => "db.operation.name",
    DB_COLLECTION_NAME => "db.collection.name",
    DESCRIPTION => "sentry.description",
    DSC_ENVIRONMENT => "sentry.dsc.environment",
    DSC_PUBLIC_KEY => "sentry.dsc.public_key",
    DSC_RELEASE => "sentry.dsc.release",
    DSC_SAMPLED => "sentry.dsc.sampled",
    DSC_SAMPLE_RATE => "sentry.dsc.sample_rate",
    DSC_TRACE_ID => "sentry.dsc.trace_id",
    DSC_TRANSACTION => "sentry.dsc.transaction",
    ENVIRONMENT => "sentry.environment",
    EVENT_NAME => "event.name",
    FAAS_TRIGGER => "faas.trigger",
    GEN_AI_COST_INPUT_TOKENS => "gen_ai.cost.input_tokens",
    GEN_AI_COST_OUTPUT_TOKENS => "gen_ai.cost.output_tokens",
    GEN_AI_COST_TOTAL_TOKENS => "gen_ai.cost.total_tokens",
    GEN_AI_REQUEST_MODEL => "gen_ai.request.model",
    GEN_AI_RESPONSE_MODEL => "gen_ai.response.model",
    GEN_AI_RESPONSE_TPS => "gen_ai.response.tokens_per_second",
    GEN_AI_SYSTEM => "gen_ai.system",
    GEN_AI_USAGE_INPUT_CACHED_TOKENS => "gen_ai.usage.input_tokens.cached",
    GEN_AI_USAGE_INPUT_CACHE_WRITE_TOKENS => "gen_ai.usage.input_tokens.cache_write",
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
    NORMALIZED_DB_QUERY => "sentry.normalized_db_query",
    NORMALIZED_DB_QUERY_HASH => "sentry.normalized_db_query.hash",
    OBSERVED_TIMESTAMP_NANOS => "sentry.observed_timestamp_nanos",
    OP => "sentry.op",
    ORIGIN => "sentry.origin",
    PLATFORM => "sentry.platform",
    PROFILE_ID => "sentry.profile_id",
    RELEASE => "sentry.release",
    RESOURCE_RENDER_BLOCKING_STATUS => "resource.render_blocking_status",
    RPC_GRPC_STATUS_CODE => "rpc.grpc.status_code",
    RPC_SERVICE => "rpc.service",
    SEGMENT_ID => "sentry.segment.id",
    SEGMENT_NAME => "sentry.segment.name",
    SENTRY_ACTION => "sentry.action",
    SENTRY_CATEGORY => "sentry.category",
    SENTRY_DOMAIN => "sentry.domain",
    SENTRY_GROUP => "sentry.group",
    SENTRY_NORMALIZED_DESCRIPTION => "sentry.normalized_description",
    SENTRY_STATUS_CODE => "sentry.status_code",
    SERVER_ADDRESS => "server.address",
    SPAN_KIND => "sentry.kind",
    STATUS_MESSAGE => "sentry.status.message",
    UI_COMPONENT_NAME => "ui.component_name",
    URL_FULL => "url.full",
    URL_PATH => "url.path",
    URL_SCHEME => "url.scheme",
    URL_DOMAIN => "url.domain",
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
mod not_yet_defined {
    // The legacy http request method attribute used by transactions spans.
    // Could not be added to sentry conventions at the time due to an attribute naming conflict that
    // requires updating the sentry conventions code gen.
    // TODO: replace with conventions defined attribute name once the conventions code gen is updated.
    pub const LEGACY_HTTP_REQUEST_METHOD: &str = "http.request_method";

    pub const WAS_TRANSACTION: &str = "sentry.was_transaction";
}
pub use self::not_yet_defined::*;
