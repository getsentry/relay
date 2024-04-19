//! Span description scrubbing logic.
mod resource;
mod sql;
use once_cell::sync::Lazy;
use psl;
#[cfg(test)]
pub use sql::{scrub_queries, Mode};

use relay_event_schema::protocol::Span;
use std::borrow::Cow;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::path::Path;
use url::{Host, Url};

use crate::regexes::{
    DB_SQL_TRANSACTION_CORE_DATA_REGEX, DB_SUPABASE_REGEX, REDIS_COMMAND_REGEX,
    RESOURCE_NORMALIZER_REGEX,
};
use crate::span::description::resource::COMMON_PATH_SEGMENTS;
use crate::span::tag_extraction::HTTP_METHOD_EXTRACTOR_REGEX;

/// Dummy URL used to parse relative URLs.
static DUMMY_BASE_URL: Lazy<Url> = Lazy::new(|| "http://replace_me".parse().unwrap());

/// Maximum length of a resource URL segment.
///
/// Segments longer than this are treated as identifiers.
const MAX_SEGMENT_LENGTH: usize = 25;

/// Some bundlers attach characters to the end of a filename, try to catch those.
const MAX_EXTENSION_LENGTH: usize = 10;

/// Domain names that are preserved during scrubbing
const DOMAIN_ALLOW_LIST: &[&str] = &["localhost"];

/// Attempts to replace identifiers in the span description with placeholders.
///
/// Returns `None` if no scrubbing can be performed.
pub(crate) fn scrub_span_description(
    span: &Span,
) -> (Option<String>, Option<Vec<sqlparser::ast::Statement>>) {
    let Some(description) = span.description.as_str() else {
        return (None, None);
    };

    let data = span.data.value();

    let db_system = data
        .and_then(|data| data.db_system.value())
        .and_then(|system| system.as_str());
    let span_origin = span.origin.as_str();

    let mut parsed_sql = None;
    let scrubbed_description = span
        .op
        .as_str()
        .map(|op| op.split_once('.').unwrap_or((op, "")))
        .and_then(|(op, sub)| match (op, sub) {
            ("http", _) => scrub_http(description),
            ("cache", _) | ("db", "redis") => scrub_redis_keys(description),
            ("db", sub) => {
                if sub.contains("clickhouse")
                    || sub.contains("mongodb")
                    || sub.contains("redis")
                    || is_legacy_activerecord(sub, db_system)
                    || is_sql_mongodb(description, db_system)
                {
                    None
                // spans coming from CoreData need to be scrubbed differently.
                } else if span_origin == Some("auto.db.core_data") {
                    scrub_core_data(description)
                } else if sub.contains("prisma") {
                    // We're not able to extract the exact query ran.
                    // The description will only contain the entity queried and
                    // the query type ("User find" for example).
                    Some(description.to_owned())
                } else if span_origin == Some("auto.db.supabase")
                    && description.starts_with("from(")
                {
                    // The description only contains the table name, e.g. `"from(users)`.
                    // In the future, we might want to parse `data.query` as well.
                    // See https://github.com/supabase-community/sentry-integration-js/blob/master/index.js#L259
                    scrub_supabase(description)
                } else {
                    let (scrubbed, mode) = sql::scrub_queries(db_system, description);
                    if let sql::Mode::Parsed(ast) = mode {
                        parsed_sql = Some(ast);
                    }
                    scrubbed
                }
            }
            ("resource", ty) => scrub_resource(ty, description),
            ("ai", sub) if sub.starts_with("run.") || sub.starts_with("pipeline.") => {
                // ai.run.* and ai.pipeline.* are low cardinality (<100 per org) and describe
                // the names of nodes of an AI pipeline.
                Some(description.to_owned())
            }
            ("ui", "load") => {
                // `ui.load` spans contain component names like `ListAppViewController`, so
                // they _should_ be low-cardinality.
                Some(description.to_owned())
            }
            ("ui", sub) if sub.starts_with("interaction.") || sub.starts_with("react.") => data
                .and_then(|data| data.ui_component_name.value())
                .and_then(|value| value.as_str())
                .map(String::from),
            ("app", _) => {
                // `app.*` has static descriptions, like `Cold Start`
                // or `Pre Runtime Init`.
                // They are low-cardinality.
                Some(description.to_owned())
            }
            ("contentprovider", "load") => {
                // `contentprovider.load` spans contain paths of third party framework components
                // and their onCreate method such as
                // `io.sentry.android.core.SentryPerformanceProvider.onCreate`, which
                // _should_ be low-cardinality, on the order of 10s per project.
                Some(description.to_owned())
            }
            ("application", "load") => {
                // `application.load` spans contain paths of app components and their
                // onCreate method such as
                // `io.sentry.samples.android.MyApplication.onCreate`, which _should_ be
                // low-cardinality.
                Some(description.to_owned())
            }
            ("activity", "load") => {
                // `activity.load` spans contain paths of app components and their onCreate/onStart
                // method such as `io.sentry.samples.android.MainActivity.onCreate`, which
                // _should_ be low-cardinality, less than 10 per project.
                Some(description.to_owned())
            }
            ("file", _) => scrub_file(description),
            _ => None,
        });
    (scrubbed_description, parsed_sql)
}

/// A span declares `op: db.sql.query`, but contains mongodb.
fn is_sql_mongodb(description: &str, db_system: Option<&str>) -> bool {
    description.contains("\"$")
        || description.contains("({")
        || description.contains("[{")
        || description.starts_with('{')
        || db_system == Some("mongodb")
}

/// We are unable to parse active record when we do not know which database is being used.
fn is_legacy_activerecord(sub_op: &str, db_system: Option<&str>) -> bool {
    db_system.is_none() && (sub_op.contains("active_record") || sub_op.contains("activerecord"))
}

fn scrub_core_data(string: &str) -> Option<String> {
    match DB_SQL_TRANSACTION_CORE_DATA_REGEX.replace_all(string, "*") {
        Cow::Owned(scrubbed) => Some(scrubbed),
        Cow::Borrowed(_) => None,
    }
}

fn scrub_supabase(string: &str) -> Option<String> {
    Some(DB_SUPABASE_REGEX.replace_all(string, "{%s}").into())
}

fn scrub_http(string: &str) -> Option<String> {
    let (method, url) = string.split_once(' ')?;
    if !HTTP_METHOD_EXTRACTOR_REGEX.is_match(method) {
        return None;
    };

    if url.starts_with("data:image/") {
        return Some(format!("{method} data:image/*"));
    }

    let scrubbed = match Url::parse(url) {
        Ok(url) => {
            let scheme = url.scheme();
            let scrubbed_host = url.host().map(scrub_host);
            let domain = concatenate_host_and_port(scrubbed_host.as_deref(), url.port());

            format!("{method} {scheme}://{domain}")
        }
        Err(_) => {
            format!("{method} *")
        }
    };

    Some(scrubbed)
}

fn scrub_file(description: &str) -> Option<String> {
    let filename = match description.split_once(' ') {
        Some((filename, _)) => filename,
        _ => description,
    };
    match Path::new(filename).extension() {
        Some(extension) => {
            let ext = scrub_resource_file_extension(extension.to_str()?);
            if ext != "*" {
                Some(format!("*.{ext}"))
            } else {
                Some("*".to_string())
            }
        }
        _ => Some("*".to_owned()),
    }
}

/// Scrub a [`Host`] object.
///
/// Domain names are run through a scrubber. All IP addresses except well known ones are replaced with a scrubbed variant.
/// Returns the scrubbed value as a string.
///
/// # Examples
///
/// ```
/// use url::{Host, Url};
/// use std::net::{Ipv4Addr, Ipv6Addr};
/// use relay_event_normalization::span::description::scrub_host;
///
/// assert_eq!(scrub_host(Host::Domain("foo.bar.baz")), "*.bar.baz");
/// assert_eq!(scrub_host(Host::Ipv4(Ipv4Addr::LOCALHOST)), "127.0.0.1");
/// ```
pub fn scrub_host(host: Host<&str>) -> Cow<'_, str> {
    match host {
        Host::Ipv4(ip) => Cow::Borrowed(scrub_ipv4(ip)),
        Host::Ipv6(ip) => Cow::Borrowed(scrub_ipv6(ip)),
        Host::Domain(domain) => scrub_domain_name(domain),
    }
}

/// Scrub an IPv4 address.
///
/// Allow well-known IPs like loopback, and fully scrub out all other IPs.
/// Returns the scrubbed value as a string.
///
/// # Examples
///
/// ```
/// use std::net::Ipv4Addr;
/// use relay_event_normalization::span::description::{scrub_ipv4};
///
/// assert_eq!(scrub_ipv4(Ipv4Addr::LOCALHOST), "127.0.0.1");
/// assert_eq!(scrub_ipv4(Ipv4Addr::new(8, 8, 8, 8)), "*.*.*.*");
/// ```
pub fn scrub_ipv4(ip: Ipv4Addr) -> &'static str {
    match ip {
        Ipv4Addr::LOCALHOST => "127.0.0.1",
        _ => "*.*.*.*",
    }
}

/// Scrub an IPv6 address.
///
/// # Examples
///
/// ```
/// use std::net::Ipv6Addr;
/// use relay_event_normalization::span::description::{scrub_ipv6};
///
/// assert_eq!(scrub_ipv6(Ipv6Addr::LOCALHOST), "::1");
/// assert_eq!(scrub_ipv6(Ipv6Addr::new(8, 8, 8, 8, 8, 8, 8, 8)), "*:*:*:*:*:*:*:*");
/// ```
pub fn scrub_ipv6(ip: Ipv6Addr) -> &'static str {
    match ip {
        Ipv6Addr::LOCALHOST => "::1",
        _ => "*:*:*:*:*:*:*:*",
    }
}

/// Sanitize a qualified domain string.
///
/// Replace all but the last two segments with asterisks.
/// Returns a string. In cases where the string is not domain-like, returns the original string.
///
/// # Examples
///
/// ```
/// use relay_event_normalization::span::description::scrub_domain_name;
///
/// assert_eq!(scrub_domain_name("my.domain.com"), "*.domain.com");
/// assert_eq!(scrub_domain_name("data.bbc.co.uk"), "*.bbc.co.uk");
/// assert_eq!(scrub_domain_name("hello world"), "hello world");
/// ```
pub fn scrub_domain_name(domain: &str) -> Cow<'_, str> {
    if DOMAIN_ALLOW_LIST.contains(&domain) {
        return Cow::Borrowed(domain);
    }

    let parsed_domain = psl::domain(domain.as_bytes());

    let Some(parsed_domain) = parsed_domain else {
        // If parsing fails, return the original string
        return Cow::Borrowed(domain);
    };

    let suffix = parsed_domain.suffix().as_bytes();
    let Some(second_level_domain) = parsed_domain.as_bytes().strip_suffix(suffix) else {
        return Cow::Borrowed(domain);
    };

    let subdomain = domain
        .as_bytes()
        .strip_suffix(suffix)
        .and_then(|s| s.strip_suffix(second_level_domain));

    match subdomain {
        None | Some(b"") => Cow::Borrowed(domain),
        Some(_subdomain) => {
            let scrubbed = [b"*.", second_level_domain, suffix].concat();
            match String::from_utf8(scrubbed) {
                Ok(s) => Cow::Owned(s),
                Err(_) => Cow::Borrowed(domain),
            }
        }
    }
}

/// Concatenate an optional host and an optional port.
///
/// Returns either a host + port combination, or the host. Never returns just the port.
///
/// # Examples
///
/// ```
/// use relay_event_normalization::span::description::concatenate_host_and_port;
///
/// assert_eq!(concatenate_host_and_port(None, None), "");
/// assert_eq!(concatenate_host_and_port(Some("my.domain.com"), None), "my.domain.com");
/// assert_eq!(concatenate_host_and_port(Some("my.domain.com"), Some(1919)), "my.domain.com:1919");
/// ```
pub fn concatenate_host_and_port(host: Option<&str>, port: Option<u16>) -> Cow<str> {
    match (host, port) {
        (None, _) => Cow::Borrowed(""),
        (Some(host), None) => Cow::Borrowed(host),
        (Some(host), Some(port)) => Cow::Owned(format!("{host}:{port}")),
    }
}

fn scrub_redis_keys(string: &str) -> Option<String> {
    let parts = REDIS_COMMAND_REGEX
        .captures(string)
        .map(|caps| (caps.name("command"), caps.name("args")));
    let scrubbed = match parts {
        Some((Some(command), Some(_args))) => command.as_str().to_owned() + " *",
        Some((Some(command), None)) => command.as_str().into(),
        None | Some((None, _)) => "*".into(),
    };
    Some(scrubbed)
}

enum UrlType {
    /// A full URL including scheme and domain.
    Full,
    /// Missing domain, starts with `/`.
    Absolute,
    /// Missing domain, does not start with `/`.
    Relative,
}

/// Scrubber for spans with `span.op` "resource.*".
fn scrub_resource(resource_type: &str, string: &str) -> Option<String> {
    let (url, ty) = match Url::parse(string) {
        Ok(url) => (url, UrlType::Full),
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            // Try again, with base URL
            match Url::options().base_url(Some(&DUMMY_BASE_URL)).parse(string) {
                Ok(url) => (
                    url,
                    if string.starts_with('/') {
                        UrlType::Absolute
                    } else {
                        UrlType::Relative
                    },
                ),
                Err(_) => return None,
            }
        }
        Err(_) => {
            return None;
        }
    };

    let formatted = match url.scheme() {
        "data" => match url.path().split_once(';') {
            Some((ty, _data)) => format!("data:{ty}"),
            None => "data:*/*".to_owned(),
        },
        "chrome-extension" | "moz-extension" | "ms-browser-extension" => {
            return Some("browser-extension://*".to_owned());
        }
        scheme => {
            let scrubbed_host = url.host().map(scrub_host);
            let domain = concatenate_host_and_port(scrubbed_host.as_deref(), url.port());

            let segment_count = url.path_segments().map(|s| s.count()).unwrap_or_default();
            let mut output_segments = vec![];
            for (i, segment) in url.path_segments().into_iter().flatten().enumerate() {
                if i + 1 == segment_count {
                    break;
                }
                if COMMON_PATH_SEGMENTS.contains(segment) {
                    output_segments.push(segment);
                } else if !output_segments.last().is_some_and(|s| *s == "*") {
                    // only one asterisk
                    output_segments.push("*");
                }
            }

            let segments = output_segments.join("/");

            let last_segment = url
                .path_segments()
                .and_then(|s| s.last())
                .unwrap_or_default();
            let last_segment = scrub_resource_filename(resource_type, last_segment);

            if segments.is_empty() {
                format!("{scheme}://{domain}/{last_segment}")
            } else {
                format!("{scheme}://{domain}/{segments}/{last_segment}")
            }
        }
    };

    // Remove previously inserted dummy URL if necessary:
    let formatted = match ty {
        UrlType::Full => formatted,
        UrlType::Absolute => formatted.replace("http://replace_me", ""),
        UrlType::Relative => formatted.replace("http://replace_me/", ""),
    };

    Some(formatted)
}

fn scrub_resource_filename<'a>(ty: &str, path: &'a str) -> Cow<'a, str> {
    if path.is_empty() {
        return Cow::Borrowed("");
    }
    let (mut basename, mut extension) = path.rsplit_once('.').unwrap_or((path, ""));
    if extension.contains('/') {
        // Not really an extension
        basename = path;
        extension = "";
    }

    let extension = scrub_resource_file_extension(extension);

    let basename = if ty == "img" {
        Cow::Borrowed("*")
    } else {
        scrub_resource_segment(basename)
    };

    if extension.is_empty() {
        basename
    } else {
        let mut filename = basename.to_string();
        filename.push('.');
        filename.push_str(extension);
        Cow::Owned(filename)
    }
}

fn scrub_resource_segment(segment: &str) -> Cow<str> {
    let segment = RESOURCE_NORMALIZER_REGEX.replace_all(segment, "$pre*$post");

    // Crude heuristic: treat long segments as idendifiers.
    if segment.len() > MAX_SEGMENT_LENGTH {
        return Cow::Borrowed("*");
    }

    let mut all_alphabetic = true;
    let mut found_uppercase = false;

    // Do not accept segments with special characters.
    for char in segment.chars() {
        if !char.is_ascii_alphabetic() {
            all_alphabetic = false;
        }
        if char.is_ascii_uppercase() {
            found_uppercase = true;
        }
        if char.is_numeric() || "&%#=+@".contains(char) {
            return Cow::Borrowed("*");
        };
    }

    if all_alphabetic && found_uppercase {
        // Assume random string identifier.
        return Cow::Borrowed("*");
    }

    segment
}

fn scrub_resource_file_extension(mut extension: &str) -> &str {
    // Only accept short, clean file extensions.
    let mut digits = 0;
    for (i, byte) in extension.bytes().enumerate() {
        if byte.is_ascii_digit() {
            digits += 1;
        }
        if digits > 1 {
            // Allow extensions like `.mp4`
            return "*";
        }
        if !byte.is_ascii_alphanumeric() {
            extension = &extension[..i];
            break;
        }
    }

    if extension.len() > MAX_EXTENSION_LENGTH {
        extension = "*";
    }

    extension
}

#[cfg(test)]
mod tests {
    use relay_protocol::Annotated;
    use similar_asserts::assert_eq;

    use super::*;

    macro_rules! span_description_test {
        // Tests the scrubbed span description for the given op.

        // Same output and input means the input was already scrubbed.
        // An empty output `""` means the input wasn't scrubbed and Relay didn't scrub it.
        ($name:ident, $description_in:expr, $op_in:literal, $expected:literal) => {
            #[test]
            fn $name() {
                let json = format!(
                    r#"
                    {{
                        "description": "",
                        "span_id": "bd2eb23da2beb459",
                        "start_timestamp": 1597976393.4619668,
                        "timestamp": 1597976393.4718769,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "op": "{}"
                    }}
                "#,
                    $op_in
                );

                let mut span = Annotated::<Span>::from_json(&json).unwrap();
                span.value_mut()
                    .as_mut()
                    .unwrap()
                    .description
                    .set_value(Some($description_in.into()));

                let scrubbed = scrub_span_description(span.value_mut().as_mut().unwrap());

                if $expected == "" {
                    assert!(scrubbed.0.is_none());
                } else {
                    assert_eq!($expected, scrubbed.0.unwrap());
                }
            }
        };
    }

    span_description_test!(empty, "", "http.client", "");

    span_description_test!(
        only_domain,
        "GET http://service.io",
        "http.client",
        "GET http://service.io"
    );

    span_description_test!(
        only_urllike_on_http_ops,
        "GET https://www.service.io/resources/01234",
        "http.client",
        "GET https://*.service.io"
    );

    span_description_test!(
        path_ids_end,
        "GET https://www.service.io/resources/01234",
        "http.client",
        "GET https://*.service.io"
    );

    span_description_test!(
        path_ids_middle,
        "GET https://www.service.io/resources/01234/details",
        "http.client",
        "GET https://*.service.io"
    );

    span_description_test!(
        path_multiple_ids,
        "GET https://www.service.io/users/01234-qwerty/settings/98765-adfghj",
        "http.client",
        "GET https://*.service.io"
    );

    span_description_test!(
        localhost,
        "GET https://localhost/data",
        "http.client",
        "GET https://localhost"
    );

    span_description_test!(
        loopback,
        "GET https://127.0.0.1/data",
        "http.client",
        "GET https://127.0.0.1"
    );

    span_description_test!(
        ip_address,
        "GET https://8.8.8.8/data",
        "http.client",
        "GET https://*.*.*.*"
    );

    span_description_test!(
        path_md5_hashes,
        "GET /clients/563712f9722fb0996ac8f3905b40786f/project/01234",
        "http.client",
        "GET *"
    );

    span_description_test!(
        path_sha_hashes,
        "GET /clients/403926033d001b5279df37cbbe5287b7c7c267fa/project/01234",
        "http.client",
        "GET *"
    );

    span_description_test!(
        hex,
        "GET /shop/de/f43/beef/3D6/my-beef",
        "http.client",
        "GET *"
    );

    span_description_test!(
        path_uuids,
        "GET /clients/8ff81d74-606d-4c75-ac5e-cee65cbbc866/project/01234",
        "http.client",
        "GET *"
    );

    span_description_test!(
        data_images,
        "GET data:image/png;base64,drtfghaksjfdhaeh/blah/blah/blah",
        "http.client",
        "GET data:image/*"
    );

    span_description_test!(
        simple_cctld,
        "GET http://bbc.co.uk",
        "http.client",
        "GET http://bbc.co.uk"
    );

    span_description_test!(
        longer_cctld,
        "GET http://www.radio1.bbc.co.uk",
        "http.client",
        "GET http://*.bbc.co.uk"
    );

    span_description_test!(
        complicated_tld,
        "GET https://application.www.xn--85x722f.xn--55qx5d.cn",
        "http.client",
        "GET https://*.xn--85x722f.xn--55qx5d.cn"
    );

    span_description_test!(
        only_dblike_on_db_ops,
        "SELECT count() FROM table WHERE id IN (%s, %s)",
        "http.client",
        ""
    );

    span_description_test!(
        cache,
        "GET abc:12:{def}:{34}:{fg56}:EAB38:zookeeper",
        "cache.get_item",
        "GET *"
    );

    span_description_test!(redis_set, "SET mykey myvalue", "db.redis", "SET *");

    span_description_test!(
        redis_set_quoted,
        r#"SET mykey 'multi: part, value'"#,
        "db.redis",
        "SET *"
    );

    span_description_test!(redis_whitespace, " GET  asdf:123", "db.redis", "GET *");

    span_description_test!(redis_no_args, "EXEC", "db.redis", "EXEC");

    span_description_test!(redis_invalid, "What a beautiful day!", "db.redis", "*");

    span_description_test!(
        redis_long_command,
        "ACL SETUSER jane",
        "db.redis",
        "ACL SETUSER *"
    );

    span_description_test!(
        nothing_cache,
        "abc-dontscrubme-meneither:stillno:ohplsstop",
        "cache.get_item",
        "*"
    );

    span_description_test!(
        resource_script,
        "https://example.com/static/chunks/vendors-node_modules_somemodule_v1.2.3_mini-dist_index_js-client_dist-6c733292-f3cd-11ed-a05b-0242ac120003-0dc369dcf3d311eda05b0242ac120003.[hash].abcd1234.chunk.js-0242ac120003.map",
        "resource.script",
        "https://example.com/static/chunks/*.map"
    );

    span_description_test!(
        resource_script_numeric_filename,
        "https://example.com/static/chunks/09876543211234567890",
        "resource.script",
        "https://example.com/static/chunks/*"
    );

    span_description_test!(
        resource_next_chunks,
        "/_next/static/chunks/12345-abcdef0123456789.js",
        "resource.script",
        "/_next/static/chunks/*-*.js"
    );

    span_description_test!(
        resource_next_media,
        "/_next/static/media/Some_Font-Bold.0123abcd.woff2",
        "resource.css",
        "/_next/static/media/Some_Font-Bold.*.woff2"
    );

    span_description_test!(
        resource_css,
        "https://example.com/assets/dark_high_contrast-764fa7c8-f3cd-11ed-a05b-0242ac120003.css",
        "resource.css",
        "https://example.com/assets/dark_high_contrast-*.css"
    );

    span_description_test!(
        integer_in_resource,
        "https://example.com/assets/this_is-a_good_resource-123-scrub_me.js",
        "resource.css",
        "https://example.com/assets/*.js"
    );

    span_description_test!(
        resource_query_params,
        "/organization-avatar/123/?s=120",
        "resource.img",
        "/*/"
    );

    span_description_test!(
        resource_query_params2,
        "https://data.domain.com/data/guide123.gif?jzb=3f535634H467g5-2f256f&ct=1234567890&v=1.203.0_prod",
        "resource.img",
        "https://*.domain.com/data/*.gif"
    );

    span_description_test!(
        resource_query_params2_script,
        "https://data.domain.com/data/guide123.js?jzb=3f535634H467g5-2f256f&ct=1234567890&v=1.203.0_prod",
        "resource.script",
        "https://*.domain.com/data/guide*.js"
    );

    span_description_test!(
        resource_no_ids,
        "https://data.domain.com/js/guide.js",
        "resource.script",
        "https://*.domain.com/js/guide.js"
    );

    span_description_test!(
        resource_no_ids_img_known_segment,
        "https://data.domain.com/data/guide.gif",
        "resource.img",
        "https://*.domain.com/data/*.gif"
    );

    span_description_test!(
        resource_no_ids_img,
        "https://data.domain.com/something/guide.gif",
        "resource.img",
        "https://*.domain.com/*/*.gif"
    );

    span_description_test!(
        resource_webpack,
        "https://domain.com/path/to/app-1f90d5.f012d11690e188c96fe6.js",
        "resource.js",
        "https://domain.com/*/app-*.*.js"
    );

    span_description_test!(
        resource_vite,
        "webroot/assets/Profile-73f6525d.js",
        "resource.js",
        "*/assets/Profile-*.js"
    );

    span_description_test!(
        resource_vite_css,
        "webroot/assets/Shop-1aff80f7.css",
        "resource.css",
        "*/assets/Shop-*.css"
    );

    span_description_test!(
        chrome_extension,
        "chrome-extension://begnopegbbhjeeiganiajffnalhlkkjb/img/assets/icon-10k.svg",
        "resource.other",
        "browser-extension://*"
    );

    span_description_test!(
        urlencoded_path_segments,
        "https://some.domain.com/embed/%2Fembed%2Fdashboards%2F20%3FSlug%3Dsomeone%*hide_title%3Dtrue",
        "resource.iframe",
        "https://*.domain.com/*/*"
    );

    span_description_test!(
        random_string1,
        "https://static.domain.com/6gezWf_qs4Wc12Nz9rpLOx2aw2k/foo-99",
        "resource.img",
        "https://*.domain.com/*/*"
    );

    span_description_test!(
        random_string1_script,
        "https://static.domain.com/6gezWf_qs4Wc12Nz9rpLOx2aw2k/foo-99",
        "resource.script",
        "https://*.domain.com/*/foo-*"
    );

    span_description_test!(
        random_string2,
        "http://domain.com/fy2XSqBMqkEm_qZZH3RrzvBTKg4/qltdXIJWTF_cuwt3uKmcwWBc1DM/z1a--BVsUI_oyUjJR12pDBcOIn5.dom.jsonp",
        "resource.script",
        "http://domain.com/*/*.jsonp"
    );

    span_description_test!(
        random_string3,
        "jkhdkkncnoglghljlkmcimlnlhkeamab/123.css",
        "resource.link",
        "*/*.css"
    );

    span_description_test!(
        ui_load,
        "ListAppViewController",
        "ui.load",
        "ListAppViewController"
    );

    span_description_test!(
        contentprovider_load,
        "io.sentry.android.core.SentryPerformanceProvider.onCreate",
        "contentprovider.load",
        "io.sentry.android.core.SentryPerformanceProvider.onCreate"
    );

    span_description_test!(
        application_load,
        "io.sentry.samples.android.MyApplication.onCreate",
        "application.load",
        "io.sentry.samples.android.MyApplication.onCreate"
    );

    span_description_test!(
        activity_load,
        "io.sentry.samples.android.MainActivity.onCreate",
        "activity.load",
        "io.sentry.samples.android.MainActivity.onCreate"
    );

    span_description_test!(
        span_description_file_write_keep_extension_only,
        "data.data (42 KB)",
        "file.write",
        "*.data"
    );

    span_description_test!(
        span_description_file_read_keep_extension_only,
        "Info.plist",
        "file.read",
        "*.plist"
    );

    span_description_test!(
        span_description_file_with_no_extension,
        "somefilenamewithnoextension",
        "file.read",
        "*"
    );

    span_description_test!(
        span_description_file_extension_with_numbers_only,
        "backup.2024041101",
        "file.read",
        "*"
    );

    span_description_test!(
        resource_url_with_fragment,
        "https://data.domain.com/data/guide123.gif#url=someotherurl",
        "resource.img",
        "https://*.domain.com/data/*.gif"
    );

    span_description_test!(
        resource_script_with_no_extension,
        "https://www.domain.com/page?id=1234567890",
        "resource.script",
        "https://*.domain.com/page"
    );

    span_description_test!(
        resource_script_with_no_domain,
        "/page.js?action=name",
        "resource.script",
        "/page.js"
    );

    span_description_test!(
        resource_script_with_no_domain_no_extension,
        "/page?action=name",
        "resource.script",
        "/page"
    );

    span_description_test!(
        resource_script_with_long_extension,
        "/path/to/file.thisismycustomfileextension2000",
        "resource.script",
        "/*/file.*"
    );

    span_description_test!(
        resource_script_with_long_suffix,
        "/path/to/file.js~ri~some-_-1,,thing-_-words%2Fhere~ri~",
        "resource.script",
        "/*/file.js"
    );

    span_description_test!(
        resource_script_with_tilde_extension,
        "/path/to/file.~~",
        "resource.script",
        "/*/file"
    );

    span_description_test!(
        resource_img_extension,
        "http://domain.com/something.123",
        "resource.img",
        "http://domain.com/*.*"
    );

    span_description_test!(
        resource_img_embedded,
        "data:image/svg+xml;base64,PHN2ZyB4bW",
        "resource.img",
        "data:image/svg+xml"
    );

    span_description_test!(
        db_category_with_mongodb_query,
        "find({some_id:1234567890},{limit:100})",
        "db",
        ""
    );

    span_description_test!(db_category_with_not_sql, "{someField:someValue}", "db", "");

    span_description_test!(
        resource_img_semi_colon,
        "http://www.foo.com/path/to/resource;param1=test;param2=ing",
        "resource.img",
        "http://*.foo.com/*/*"
    );

    span_description_test!(
        resource_img_comma_with_extension,
        "https://example.org/p/fit=cover,width=150,height=150,format=auto,quality=90/media/photosV2/weird-stuff-123-234-456.jpg",
        "resource.img",
        "https://example.org/*/media/*/*.jpg"
    );

    span_description_test!(
        resource_script_comma_with_extension,
        "https://example.org/p/fit=cover,width=150,height=150,format=auto,quality=90/media/photosV2/weird-stuff-123-234-456.js",
        "resource.script",
        "https://example.org/*/media/*/weird-stuff-*-*-*.js"
    );

    span_description_test!(
        resource_img_path_with_comma,
        "/help/purchase-details/1,*,0&fmt=webp&qlt=*,1&fit=constrain,0&op_sharpen=0&resMode=sharp2&iccEmbed=0&printRes=*",
        "resource.img",
        "/*/*"
    );

    span_description_test!(
        resource_script_path_with_comma,
        "/help/purchase-details/1,*,0&fmt=webp&qlt=*,1&fit=constrain,0&op_sharpen=0&resMode=sharp2&iccEmbed=0&printRes=*",
        "resource.script",
        "/*/*"
    );

    span_description_test!(
        resource_script_random_path_only,
        "/ERs-sUsu3/wd4/LyMTWg/Ot1Om4m8cu3p7a/QkJWAQ/FSYL/GBlxb3kB",
        "resource.script",
        "/*/*"
    );

    span_description_test!(
        resource_script_normalize_domain,
        "https://sub.sub.sub.domain.com/resource.js",
        "resource.script",
        "https://*.domain.com/resource.js"
    );

    span_description_test!(
        resource_script_extension_in_segment,
        "https://domain.com/foo.bar/resource.js",
        "resource.script",
        "https://domain.com/*/resource.js"
    );

    span_description_test!(
        resource_script_missing_scheme,
        "domain.com/foo.bar/resource.js",
        "resource.script",
        "*/resource.js"
    );

    span_description_test!(
        resource_script_missing_scheme_integer_id,
        "domain.com/zero-length-00",
        "resource.script",
        "*/zero-length-*"
    );

    span_description_test!(db_prisma, "User find", "db.sql.prisma", "User find");

    #[test]
    fn informed_sql_parser() {
        let json = r#"
            {
                "description": "SELECT \"not an identifier\"",
                "span_id": "bd2eb23da2beb459",
                "start_timestamp": 1597976393.4619668,
                "timestamp": 1597976393.4718769,
                "trace_id": "ff62a8b040f340bda5d830223def1d81",
                "op": "db",
                "data": {"db.system": "mysql"}
            }
        "#;

        let mut span = Annotated::<Span>::from_json(json).unwrap();
        let span = span.value_mut().as_mut().unwrap();
        let scrubbed = scrub_span_description(span);
        assert_eq!(scrubbed.0.as_deref(), Some("SELECT %s"));
    }

    #[test]
    fn active_record() {
        let json = r#"{
            "description": "/*some comment `my_function'*/ SELECT `a` FROM `b`",
            "op": "db.sql.activerecord"
        }"#;

        let mut span = Annotated::<Span>::from_json(json).unwrap();

        let scrubbed = scrub_span_description(span.value_mut().as_mut().unwrap());

        // When db.system is missing, no scrubbed description (i.e. no group) is set.
        assert!(scrubbed.0.is_none());
    }

    #[test]
    fn active_record_with_db_system() {
        let json = r#"{
            "description": "/*some comment `my_function'*/ SELECT `a` FROM `b`",
            "op": "db.sql.activerecord",
            "data": {
                "db.system": "mysql"
            }
        }"#;

        let mut span = Annotated::<Span>::from_json(json).unwrap();

        let scrubbed = scrub_span_description(span.value_mut().as_mut().unwrap());

        // Can be scrubbed with db system.
        assert_eq!(scrubbed.0.as_deref(), Some("SELECT a FROM b"));
    }

    #[test]
    fn core_data() {
        let json = r#"{
            "description": "INSERTED 1 'UAEventData'",
            "op": "db.sql.transaction",
            "origin": "auto.db.core_data"
        }"#;

        let mut span = Annotated::<Span>::from_json(json).unwrap();

        let scrubbed = scrub_span_description(span.value_mut().as_mut().unwrap());

        assert_eq!(scrubbed.0.as_deref(), Some("INSERTED * 'UAEventData'"));
    }

    #[test]
    fn multiple_core_data() {
        let json = r#"{
            "description": "UPDATED 1 'QueuedRequest', DELETED 1 'QueuedRequest'",
            "op": "db.sql.transaction",
            "origin": "auto.db.core_data"
        }"#;

        let mut span = Annotated::<Span>::from_json(json).unwrap();

        let scrubbed = scrub_span_description(span.value_mut().as_mut().unwrap());

        assert_eq!(
            scrubbed.0.as_deref(),
            Some("UPDATED * 'QueuedRequest', DELETED * 'QueuedRequest'")
        );
    }

    #[test]
    fn ui_interaction_with_component_name() {
        let json = r#"{
            "description": "input.app-asdfasfg.asdfasdf[type=\"range\"][name=\"replay-timeline\"]",
            "op": "ui.interaction.click",
            "data": {
                "ui.component_name": "my-component-name"
            }
        }"#;

        let mut span = Annotated::<Span>::from_json(json).unwrap();

        let scrubbed = scrub_span_description(span.value_mut().as_mut().unwrap());

        // Can be scrubbed with db system.
        assert_eq!(scrubbed.0.as_deref(), Some("my-component-name"));
    }
}
