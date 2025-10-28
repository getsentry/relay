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
    DB_QUERY_TEXT => "db.query.text",
    DB_STATEMENT => "db.statement",
    DB_SYSTEM_NAME => "db.system.name",
    DESCRIPTION => "sentry.description",
    FAAS_TRIGGER => "faas.trigger",
    GEN_AI_SYSTEM => "gen_ai.system",
    HTTP_PREFETCH => "sentry.http.prefetch",
    HTTP_REQUEST_METHOD => "http.request.method",
    HTTP_RESPONSE_STATUS_CODE => "http.response.status_code",
    HTTP_ROUTE => "http.route",
    HTTP_TARGET => "http.target",
    MESSAGING_SYSTEM => "messaging.system",
    ENVIRONMENT => "sentry.environment",
    OBSERVED_TIMESTAMP_NANOS => "sentry.observed_timestamp_nanos",
    OBSERVED_TIMESTAMP_NANOS_INTERNAL => "sentry._internal.observed_timestamp_nanos",
    OP => "sentry.op",
    ORIGIN => "sentry.origin",
    PLATFORM => "sentry.platform",
    PROFILE_ID => "sentry.profile_id",
    RELEASE => "sentry.release",
    SERVER_ADDRESS => "server.address",
    RPC_GRPC_STATUS_CODE => "rpc.grpc.status_code",
    RPC_SERVICE => "rpc.service",
    SEGMENT_ID => "sentry.segment.id",
    SEGMENT_NAME => "sentry.segment.name",
    URL_FULL => "url.full",
    URL_PATH => "url.path",
    USER_GEO_CITY => "user.geo.city",
    USER_GEO_COUNTRY_CODE => "user.geo.country_code",
    USER_GEO_REGION => "user.geo.region",
    USER_GEO_SUBDIVISION => "user.geo.subdivision",
    USER_AGENT_ORIGINAL => "user_agent.original",
);

/// Attributes which are in use by Relay but are not yet defined in the Sentry conventions.
mod not_yet_defined {
    // For now, it can be found in the JavaScript SDK:
    // https://github.com/getsentry/sentry-javascript/blob/4756112a9d233ca6f1b0be64d64d892279797a39/packages/opentelemetry/src/semanticAttributes.ts#L4-L5
    pub const GRAPHQL_OPERATION: &str = "sentry.graphql.operation";
    // It was introduced in `a1f1e89` (https://github.com/getsentry/relay/pull/4876)
    // to absorb the `status.message` field from OTEL.
    pub const STATUS_MESSAGE: &str = "sentry.status.message";
}
pub use self::not_yet_defined::*;
