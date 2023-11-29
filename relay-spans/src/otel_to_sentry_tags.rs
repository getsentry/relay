use std::collections::BTreeMap;

use once_cell::sync::Lazy;

pub static OTEL_TO_SENTRY_TAGS: Lazy<BTreeMap<&str, &str>> = Lazy::new(|| {
    BTreeMap::from([
        ("enduser.id", "user.id"),
        ("http.request.cookies", "request.cookies"),
        ("http.request.env", "request.env"),
        (
            "http.request.headers.content-type",
            "request.headers.content-type",
        ),
        ("http.request.method", "request.method"),
        ("sentry.environment", "environment"),
        ("sentry.op", "op"),
        ("sentry.origin", "origin"),
        ("sentry.release", "release"),
        ("sentry.sample_rate", "sample_rate"),
        ("sentry.sdk.integrations", "sdk.integrations"),
        ("sentry.sdk.name", "sdk.name"),
        ("sentry.sdk.packages", "sdk.packages"),
        ("sentry.sdk.version", "sdk.version"),
        ("sentry.source", "source"),
        ("sentry.user.email", "user.email"),
        ("sentry.user.geo.city", "user.geo.city"),
        ("sentry.user.geo.country_code", "user.geo.country_code"),
        ("sentry.user.geo.region", "user.geo.region"),
        ("sentry.user.ip_address", "user.ip_address"),
        ("sentry.user.segment", "user.segment"),
        ("sentry.user.username", "user.username"),
        ("url.full", "request.url"),
        ("url.query_string", "request.query_string"),
    ])
});
