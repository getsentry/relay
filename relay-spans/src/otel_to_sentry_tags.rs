use std::collections::BTreeMap;

use once_cell::sync::Lazy;

pub static OTEL_TO_SENTRY_TAGS: Lazy<BTreeMap<&str, &str>> = Lazy::new(|| {
    BTreeMap::from([
        ("sentry.release", "release"),
        ("sentry.environment", "environment"),
        ("sentry.origin", "origin"),
        ("sentry.op", "op"),
        ("sentry.source", "source"),
        ("sentry.sample_rate", "sample_rate"),
        ("enduser.id", "user.id"),
        ("sentry.user.username", "user.username"),
        ("sentry.user.email", "user.email"),
        ("sentry.user.ip_address", "user.ip_address"),
        ("sentry.user.segment", "user.segment"),
        ("sentry.user.geo.city", "user.geo.city"),
        ("sentry.user.geo.country_code", "user.geo.country_code"),
        ("sentry.user.geo.region", "user.geo.region"),
        ("http.request.method", "request.method"),
        ("url.full", "request.url"),
        ("url.query_string", "request.query_string"),
        ("http.request.cookies", "request.cookies"),
        (
            "http.request.headers.content-type",
            "request.headers.content-type",
        ),
        ("http.request.env", "request.env"),
        ("sentry.sdk.name", "sdk.name"),
        ("sentry.sdk.version", "sdk.version"),
        ("sentry.sdk.integrations", "sdk.integrations"),
        ("sentry.sdk.packages", "sdk.packages"),
    ])
});
