pub const BROWSER_NAME: &str = "sentry.browser.name";
pub const BROWSER_VERSION: &str = "sentry.browser.version";
pub const DB_QUERY_TEXT: &str = "db.query.text";
pub const DB_STATEMENT: &str = "db.statement";
pub const DB_SYSTEM_NAME: &str = "db.system.name";
pub const DESCRIPTION: &str = "sentry.description";
pub const FAAS_TRIGGER: &str = "faas.trigger";
pub const GEN_AI_SYSTEM: &str = "gen_ai.system";
// This attribute is not defined in `sentry-conventions` yet.
//
// For now, it can be found in the JavaScript SDK:
// https://github.com/getsentry/sentry-javascript/blob/4756112a9d233ca6f1b0be64d64d892279797a39/packages/opentelemetry/src/semanticAttributes.ts#L4-L5
pub const GRAPHQL_OPERATION: &str = "sentry.graphql.operation";
pub const HTTP_PREFETCH: &str = "sentry.http.prefetch";
pub const HTTP_REQUEST_METHOD: &str = "http.request.method";
pub const HTTP_RESPONSE_STATUS_CODE: &str = "http.response.status_code";
pub const HTTP_ROUTE: &str = "http.route";
pub const HTTP_TARGET: &str = "http.target";
pub const MESSAGING_SYSTEM: &str = "messaging.system";
pub const OBSERVED_TIMESTAMP_NANOS: &str = "sentry.observed_timestamp_nanos";
pub const OP: &str = "sentry.op";
pub const PLATFORM: &str = "sentry.platform";
pub const PROFILE_ID: &str = "sentry.profile_id";
pub const RELEASE: &str = "sentry.release";
pub const RPC_GRPC_STATUS_CODE: &str = "rpc.grpc.status_code";
pub const RPC_SERVICE: &str = "rpc.service";
pub const SEGMENT_ID: &str = "sentry.segment.id";
pub const SEGMENT_NAME: &str = "sentry.segment.name";
// This attribute is not defined in `sentry-conventions` yet.
//
// It was introduced in `a1f1e89` (https://github.com/getsentry/relay/pull/4876)
// to absorb the `status.message` field from OTEL.
pub const STATUS_MESSAGE: &str = "sentry.status.message";
pub const URL_FULL: &str = "url.full";
pub const URL_PATH: &str = "url.path";
pub const USER_GEO_CITY: &str = "user.geo.city";
pub const USER_GEO_COUNTRY_CODE: &str = "user.geo.country_code";
pub const USER_GEO_REGION: &str = "user.geo.region";
pub const USER_GEO_SUBDIVISION: &str = "user.geo.subdivision";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ATTRIBUTES;

    #[test]
    fn consts_defined() {
        assert!(ATTRIBUTES.contains_key(BROWSER_NAME));
        assert!(ATTRIBUTES.contains_key(BROWSER_VERSION));
        assert!(ATTRIBUTES.contains_key(DESCRIPTION));
        assert!(ATTRIBUTES.contains_key(DB_QUERY_TEXT));
        assert!(ATTRIBUTES.contains_key(DB_STATEMENT));
        assert!(ATTRIBUTES.contains_key(DB_SYSTEM_NAME));
        assert!(ATTRIBUTES.contains_key(FAAS_TRIGGER));
        assert!(ATTRIBUTES.contains_key(GEN_AI_SYSTEM));
        // TODO: "sentry.graphql.operation" is not defined in `sentry-conventions`.
        // assert!(ATTRIBUTES.contains_key(GRAPHQL_OPERATION));
        assert!(ATTRIBUTES.contains_key(HTTP_PREFETCH));
        assert!(ATTRIBUTES.contains_key(HTTP_REQUEST_METHOD));
        assert!(ATTRIBUTES.contains_key(HTTP_RESPONSE_STATUS_CODE));
        assert!(ATTRIBUTES.contains_key(HTTP_ROUTE));
        assert!(ATTRIBUTES.contains_key(HTTP_TARGET));
        assert!(ATTRIBUTES.contains_key(MESSAGING_SYSTEM));
        assert!(ATTRIBUTES.contains_key(OBSERVED_TIMESTAMP_NANOS));
        assert!(ATTRIBUTES.contains_key(OP));
        assert!(ATTRIBUTES.contains_key(PLATFORM));
        assert!(ATTRIBUTES.contains_key(PROFILE_ID));
        assert!(ATTRIBUTES.contains_key(RELEASE));
        assert!(ATTRIBUTES.contains_key(RPC_GRPC_STATUS_CODE));
        assert!(ATTRIBUTES.contains_key(RPC_SERVICE));
        assert!(ATTRIBUTES.contains_key(SEGMENT_ID));
        assert!(ATTRIBUTES.contains_key(SEGMENT_NAME));
        // TODO: "sentry.status.message" is not defined in `sentry-conventions`.
        // assert!(ATTRIBUTES.contains_key(STATUS_MESSAGE));
        assert!(ATTRIBUTES.contains_key(URL_FULL));
        assert!(ATTRIBUTES.contains_key(URL_PATH));
        assert!(ATTRIBUTES.contains_key(USER_GEO_CITY));
        assert!(ATTRIBUTES.contains_key(USER_GEO_COUNTRY_CODE));
        assert!(ATTRIBUTES.contains_key(USER_GEO_REGION));
        assert!(ATTRIBUTES.contains_key(USER_GEO_SUBDIVISION));
    }
}
