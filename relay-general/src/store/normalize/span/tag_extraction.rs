//! Logic for persisting items into `span.data` fields.
//! These are then used for metrics extraction.

use itertools::Itertools;
use once_cell::sync::Lazy;
use regex::Regex;
use url::Url;

use crate::macros::derive_fromstr_and_display;
use crate::protocol::{Event, Span, TraceContext};
use crate::store::utils::{
    extract_http_status_code, extract_transaction_op, get_eventuser_tag, http_status_code_from_span,
};
use crate::types::Annotated;
// use relay_filter::csp::SchemeDomainPort;
// use relay_metrics::{AggregatorConfig, Metric};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SpanTagKey {
    // Specific to a transaction
    Release,
    User,
    Environment,
    Transaction,
    TransactionMethod,
    TransactionOp,
    HttpStatusCode,

    // Specific to spans
    Description,
    Group,
    SpanOp,
    Category,
    Module,
    Action,
    Domain,
    System,
    Status,
    StatusCode,
}

impl SpanTagKey {
    /// Whether or not this tag should be added to metrics extracted from the span.
    pub fn is_metric_tag(&self) -> bool {
        !matches!(self, SpanTagKey::Release | SpanTagKey::User)
    }
}

derive_fromstr_and_display!(SpanTagKey, (), {
    SpanTagKey::Release => "release",
    SpanTagKey::User => "user",
    SpanTagKey::Environment => "environment",
    SpanTagKey::Transaction => "transaction",
    SpanTagKey::TransactionMethod => "transaction.method",
    SpanTagKey::TransactionOp => "transaction.op",
    SpanTagKey::HttpStatusCode => "http.status_code",

    SpanTagKey::Description => "span.description",
    SpanTagKey::Group => "span.group",
    SpanTagKey::SpanOp => "span.op",
    SpanTagKey::Category => "span.category",
    SpanTagKey::Module => "span.module",
    SpanTagKey::Action => "span.action",
    SpanTagKey::Domain => "span.domain",
    SpanTagKey::System => "span.system",
    SpanTagKey::Status => "span.status",
    SpanTagKey::StatusCode => "span.status_code",
});

/// Configuration for span tag extraction.
pub(crate) struct Config {
    /// The maximum allowed size of tag values in bytes. Longer values will be cropped.
    pub max_tag_value_length: usize,
}

/// Extracts tags from event and spans and materializes them into `span.data`.
pub(crate) fn extract_span_tags(event: &mut Event, config: &Config) {
    // TODO: To prevent differences between metrics and payloads, we should not extract tags here
    // when they have already been extracted by a downstream relay.
    let shared_tags = extract_shared_tags(event);

    let Some(spans) = event
        .spans
        .value_mut() else {return};

    for span in spans {
        let Some(span) = span.value_mut().as_mut() else {continue};

        let tags = extract_tags(span, config);

        let data = span.data.value_mut().get_or_insert_with(Default::default);
        data.extend(
            shared_tags
                .clone()
                .into_iter()
                .chain(tags.into_iter())
                .map(|(k, v)| (k.to_string(), Annotated::new(v.into()))),
        );
    }
}

/// Extract tags shared by every span.
fn extract_shared_tags(event: &Event) -> BTreeMap<SpanTagKey, String> {
    let mut tags = BTreeMap::new();

    if let Some(release) = event.release.as_str() {
        tags.insert(SpanTagKey::Release, release.to_owned());
    }

    if let Some(user) = event.user.value().and_then(get_eventuser_tag) {
        tags.insert(SpanTagKey::User, user);
    }

    if let Some(environment) = event.environment.as_str() {
        tags.insert(SpanTagKey::Environment, environment.to_owned());
    }

    if let Some(transaction_name) = event.transaction.value() {
        tags.insert(SpanTagKey::Transaction, transaction_name.to_owned());

        let transaction_method_from_request = event
            .request
            .value()
            .and_then(|r| r.method.value())
            .map(|m| m.to_uppercase());

        if let Some(transaction_method) = transaction_method_from_request
            .or(http_method_from_transaction_name(transaction_name).map(|m| m.to_uppercase()))
        {
            tags.insert(SpanTagKey::TransactionMethod, transaction_method);
        }
    }

    if let Some(trace_context) = event.context::<TraceContext>() {
        if let Some(op) = extract_transaction_op(trace_context) {
            tags.insert(SpanTagKey::TransactionOp, op.to_lowercase());
        }
    }

    if let Some(transaction_http_status_code) = extract_http_status_code(event) {
        tags.insert(SpanTagKey::HttpStatusCode, transaction_http_status_code);
    }

    tags
}

/// Write fields into [`Span::data`].
pub(crate) fn extract_tags(span: &Span, config: &Config) -> BTreeMap<SpanTagKey, String> {
    let mut span_tags = BTreeMap::new();
    if let Some(unsanitized_span_op) = span.op.value() {
        let span_op = unsanitized_span_op.to_owned().to_lowercase();

        span_tags.insert(SpanTagKey::SpanOp, span_op.to_owned());

        if let Some(category) = span_op_to_category(&span_op) {
            span_tags.insert(SpanTagKey::Category, category.to_owned());
        }

        let span_module = if span_op.starts_with("http") {
            Some("http")
        } else if span_op.starts_with("db") {
            Some("db")
        } else if span_op.starts_with("cache") {
            Some("cache")
        } else {
            None
        };

        if let Some(module) = span_module {
            span_tags.insert(SpanTagKey::Module, module.to_owned());
        }

        let scrubbed_description = span
            .data
            .value()
            .and_then(|data| data.get("description.scrubbed"))
            .and_then(|value| value.as_str());

        // TODO(iker): we're relying on the existance of `http.method`
        // or `db.operation`. This is not guaranteed, and we'll need to
        // parse the span description in that case.
        let action = match (span_module, span_op.as_str(), scrubbed_description) {
            (Some("http"), _, _) => span
                .data
                .value()
                // TODO(iker): some SDKs extract this as method
                .and_then(|v| v.get("http.method"))
                .and_then(|method| method.as_str())
                .map(|s| s.to_uppercase()),
            (_, "db.redis", Some(desc)) => {
                // This only works as long as redis span descriptions contain the command + " *"
                let command = desc.replace(" *", "");
                if command.is_empty() {
                    None
                } else {
                    Some(command)
                }
            }
            (Some("db"), _, _) => {
                let action_from_data = span
                    .data
                    .value()
                    .and_then(|v| v.get("db.operation"))
                    .and_then(|db_op| db_op.as_str())
                    .map(|s| s.to_uppercase());
                action_from_data.or_else(|| {
                    span.description
                        .value()
                        .and_then(|d| sql_action_from_query(d))
                        .map(|a| a.to_uppercase())
                })
            }
            _ => None,
        };

        if let Some(act) = action.clone() {
            span_tags.insert(SpanTagKey::Action, act);
        }

        let domain = if span_op == "http.client" {
            span.description
                .value()
                .and_then(|url| domain_from_http_url(url))
                .map(|d| d.to_lowercase())
        } else if span_op.starts_with("db") {
            span.description
                .value()
                .and_then(|query| sql_table_from_query(query))
                .map(|t| t.to_lowercase())
        } else {
            None
        };

        if !span_op.starts_with("db.redis") {
            if let Some(dom) = domain.clone() {
                span_tags.insert(SpanTagKey::Domain, dom);
            }
        }

        let sanitized_description = sanitized_span_description(
            scrubbed_description,
            span_module,
            action.as_deref(),
            domain.as_deref(),
        );

        if let Some(scrubbed_desc) = sanitized_description {
            // Truncating the span description's tag value is, for now,
            // a temporary solution to not get large descriptions dropped. The
            // group tag mustn't be affected by this, and still be
            // computed from the full, untruncated description.

            let mut span_group = format!("{:?}", md5::compute(&scrubbed_desc));
            span_group.truncate(16);
            span_tags.insert(SpanTagKey::Group, span_group);

            let truncated = truncate_string(scrubbed_desc, config.max_tag_value_length);
            span_tags.insert(SpanTagKey::Description, truncated);
        }
    }

    let system = span
        .data
        .value()
        .and_then(|v| v.get("db.system"))
        .and_then(|system| system.as_str());
    if let Some(sys) = system {
        span_tags.insert(SpanTagKey::System, sys.to_lowercase());
    }

    if let Some(span_status) = span.status.value() {
        span_tags.insert(SpanTagKey::Status, span_status.to_string());
    }

    if let Some(status_code) = http_status_code_from_span(span) {
        span_tags.insert(SpanTagKey::StatusCode, status_code);
    }

    span_tags
}

/// Returns the sanitized span description.
///
/// If a scrub description is provided, that's returned. If not, a new
/// description is built for `http*` modules with the following format:
/// `{action} {domain}/<unparameterized>`.
fn sanitized_span_description(
    scrubbed_description: Option<&str>,
    module: Option<&str>,
    action: Option<&str>,
    domain: Option<&str>,
) -> Option<String> {
    if let Some(scrubbed) = scrubbed_description {
        return Some(scrubbed.to_owned());
    }

    if !module?.starts_with("http") {
        return None;
    }

    let mut sanitized = String::new();

    if let Some(transaction_method) = action {
        sanitized.push_str(&format!("{transaction_method} "));
    }
    if let Some(domain) = domain {
        sanitized.push_str(&format!("{domain}/"));
    }
    sanitized.push_str("<unparameterized>");

    Some(sanitized)
}

/// Trims the given string with the given maximum bytes. Splitting only happens
/// on char boundaries.
///
/// If the string is short, it remains unchanged. If it's long, this method
/// truncates it to the maximum allowed size and sets the last character to
/// `*`.
fn truncate_string(mut string: String, max_bytes: usize) -> String {
    if string.len() <= max_bytes {
        return string;
    }

    if max_bytes == 0 {
        return String::new();
    }

    let mut cutoff = max_bytes - 1; // Leave space for `*`

    while cutoff > 0 && !string.is_char_boundary(cutoff) {
        cutoff -= 1;
    }

    string.truncate(cutoff);
    string.push('*');
    string
}

/// Regex with a capture group to extract the database action from a query.
///
/// Currently, we're only interested in `SELECT`, `INSERT`, `DELETE` and `UPDATE` statements.
static SQL_ACTION_EXTRACTOR_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?i)(?P<action>(SELECT|INSERT|DELETE|UPDATE))"#).unwrap());

fn sql_action_from_query(query: &str) -> Option<&str> {
    extract_captured_substring(query, &SQL_ACTION_EXTRACTOR_REGEX)
}

/// Regex with a capture group to extract the table from a database query,
/// based on `FROM` and `INTO` keywords.
static SQL_TABLE_EXTRACTOR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)(from|into)(\s|"|'|\()+(?P<table>(\w+(\.\w+)*))(\s|"|'|\))+"#).unwrap()
});

/// Returns the table in the SQL query, if any.
///
/// If multiple tables exist, only the first one is returned.
fn sql_table_from_query(query: &str) -> Option<&str> {
    extract_captured_substring(query, &SQL_TABLE_EXTRACTOR_REGEX)
}

/// Regex with a capture group to extract the HTTP method from a string.
static HTTP_METHOD_EXTRACTOR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)^(?P<method>(GET|HEAD|POST|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH))\s"#)
        .unwrap()
});

fn http_method_from_transaction_name(name: &str) -> Option<&str> {
    extract_captured_substring(name, &HTTP_METHOD_EXTRACTOR_REGEX)
}

/// Returns the captured substring in `string` with the capture group in `pattern`.
///
/// It assumes there's only one capture group in `pattern`, and only returns the first one.
fn extract_captured_substring<'a>(string: &'a str, pattern: &'a Lazy<Regex>) -> Option<&'a str> {
    let capture_names: Vec<_> = pattern.capture_names().flatten().collect();

    for captures in pattern.captures_iter(string) {
        for name in &capture_names {
            if let Some(capture) = captures.name(name) {
                return Some(&string[capture.start()..capture.end()]);
            }
        }
    }

    None
}

/// Returns the category of a span from its operation. The mapping is available in:
/// <https://develop.sentry.dev/sdk/performance/span-operations/>
fn span_op_to_category(op: &str) -> Option<&str> {
    let category =
        // General
        if op.starts_with("mark") {
            "mark"
        }
        //
        // Browser
        // `ui*` mapped in JS frameworks
        else if op.starts_with("pageload") {
            "pageload"
        } else if op.starts_with("navigation") {
            "navigation"
        } else if op.starts_with("resource") {
            "resource"
        } else if op.starts_with("browser") {
            "browser"
        } else if op.starts_with("measure") {
            "measure"
        } else if op.starts_with("http") {
            "http"
        } else if op.starts_with("serialize") {
            "serialize"
        }
        //
        // JS frameworks
        else if op.starts_with("ui") {
            if op.starts_with("ui.react") {
                "ui.react"
            } else if op.starts_with("ui.vue") {
                "ui.vue"
            } else if op.starts_with("ui.svelte") {
                "ui.svelte"
            } else if op.starts_with("ui.angular") {
                "ui.angular"
            } else if op.starts_with("ui.ember") {
                "ui.ember"
            } else {
                "ui"
            }
        }
        //
        // Web server
        // `http*` mapped in Browser
        // `serialize*` mapped in Browser
        else if op.starts_with("websocket") {
            "websocket"
        } else if op.starts_with("rpc") {
            "rpc"
        } else if op.starts_with("grpc") {
            "grpc"
        } else if op.starts_with("graphql") {
            "graphql"
        } else if op.starts_with("subprocess") {
            "subprocess"
        } else if op.starts_with("middleware") {
            "middleware"
        } else if op.starts_with("view") {
            "view"
        } else if op.starts_with("template") {
            "template"
        } else if op.starts_with("event") {
            "event"
        } else if op.starts_with("function") {
            if op.starts_with("function.nextjs") {
                "function.nextjs"
            } else if op.starts_with("function.remix") {
                "function.remix"
            } else if op.starts_with("function.gpc") {
                "function.grpc"
            } else if op.starts_with("function.aws") {
                "function.aws"
            } else if op.starts_with("function.azure") {
                "function.azure"
            } else {
                "function"
            }
        } else if op.starts_with("console") {
            "console"
        } else if op.starts_with("file") {
            "file"
        } else if op.starts_with("app") {
            "app"
        }
        //
        // Database
        else if op.starts_with("db") {
            "db"
        } else if op.starts_with("cache") {
            "cache"
        }
        //
        // Serverless
        // `http*` marked in Browser
        // `grpc*` marked in Web server
        // `function*` marked in Web server
        //
        // Mobile
        // `app*` marked in Web server
        // `ui*` marked in Browser
        // `navigation*` marked in Browser
        // `file*` marked in Web server
        // `serialize*` marked in Web server
        // `http*` marked in Browser

        // Desktop
        // `app*` marked in Web server
        // `ui*` marked in Browser
        // `serialize*` marked in Web server
        // `http*` marked in Browser

        // Messages / queues
        else if op.starts_with("topic") {
            "topic"
        } else if op.starts_with("queue") {
            "queue"
        }
        //
        // Unknown
        else {
            return None;
        };

    Some(category)
}

fn domain_from_http_url(url: &str) -> Option<String> {
    match url.split_once(' ') {
        Some((_method, url)) => {
            let url = dbg!(Url::parse(dbg!(url)));
            let (domain, port) = match &url {
                Ok(url) => (url.domain(), url.port()),
                Err(_) => (None, None),
            };
            match (domain, port) {
                (Some(domain), port) => normalize_domain(domain, port),
                _ => None,
            }
        }
        _ => None,
    }
}

fn normalize_domain(domain: &str, port: Option<u16>) -> Option<String> {
    if let Some(allow_listed) = normalized_domain_from_allowlist(domain, port) {
        return Some(allow_listed);
    }

    let mut tokens = domain.rsplitn(3, '.');
    let tld = tokens.next();
    let domain = tokens.next();
    let prefix = tokens.next().map(|_| "*");

    let mut replaced = prefix
        .iter()
        .chain(domain.iter())
        .chain(tld.iter())
        .join(".");

    if let Some(port) = port {
        replaced = format!("{replaced}:{port}");
    }

    if replaced.is_empty() {
        return None;
    }
    Some(replaced)
}

/// Allow list of domains to not get subdomains scrubbed.
const DOMAIN_ALLOW_LIST: &[&str] = &["127.0.0.1", "localhost"];

fn normalized_domain_from_allowlist(domain: &str, port: Option<u16>) -> Option<String> {
    if let Some(domain) = DOMAIN_ALLOW_LIST.iter().find(|allowed| **allowed == domain) {
        let with_port = port.map_or_else(|| (*domain).to_owned(), |p| format!("{}:{}", domain, p));
        return Some(with_port);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Event, Request};
    use crate::store::{self, LightNormalizationConfig};
    use crate::types::Annotated;

    #[test]
    fn test_truncate_string_no_panic() {
        let string = "ÆÆ".to_owned();

        let truncated = truncate_string(string.clone(), 0);
        assert_eq!(truncated, "");

        let truncated = truncate_string(string.clone(), 1);
        assert_eq!(truncated, "*");

        let truncated = truncate_string(string.clone(), 2);
        assert_eq!(truncated, "*");

        let truncated = truncate_string(string.clone(), 3);
        assert_eq!(truncated, "Æ*");

        let truncated = truncate_string(string.clone(), 4);
        assert_eq!(truncated, "ÆÆ");

        let truncated = truncate_string(string, 5);
        assert_eq!(truncated, "ÆÆ");
    }

    macro_rules! span_transaction_method_test {
        // Tests transaction.method is picked from the right place.
        ($name:ident, $transaction_name:literal, $request_method:literal, $expected_method:literal) => {
            #[test]
            fn $name() {
                let json = format!(
                    r#"
                    {{
                        "type": "transaction",
                        "platform": "javascript",
                        "start_timestamp": "2021-04-26T07:59:01+0100",
                        "timestamp": "2021-04-26T08:00:00+0100",
                        "transaction": "{}",
                        "contexts": {{
                            "trace": {{
                                "trace_id": "ff62a8b040f340bda5d830223def1d81",
                                "span_id": "bd429c44b67a3eb4"
                            }}
                        }},
                        "spans": [
                            {{
                                "span_id": "bd429c44b67a3eb4",
                                "start_timestamp": 1597976300.0000000,
                                "timestamp": 1597976302.0000000,
                                "trace_id": "ff62a8b040f340bda5d830223def1d81"
                            }}
                        ]
                    }}
                "#,
                    $transaction_name
                );

                let mut event = Annotated::<Event>::from_json(&json).unwrap();

                if !$request_method.is_empty() {
                    if let Some(e) = event.value_mut() {
                        e.request = Annotated::new(Request {
                            method: Annotated::new(format!("{}", $request_method)),
                            ..Default::default()
                        });
                    }
                }

                // Normalize first, to make sure that all things are correct as in the real pipeline:
                let res = store::light_normalize_event(
                    &mut event,
                    LightNormalizationConfig {
                        enrich_spans: true,
                        light_normalize_spans: true,
                        ..Default::default()
                    },
                );
                assert!(res.is_ok());

                assert_eq!(
                    $expected_method,
                    event
                        .value()
                        .and_then(|e| e.spans.value())
                        .and_then(|spans| spans[0].value())
                        .and_then(|s| s.data.value())
                        .and_then(|d| d.get("transaction.method"))
                        .and_then(|v| v.value())
                        .and_then(|v| v.as_str())
                        .unwrap()
                );
            }
        };
    }

    span_transaction_method_test!(
        test_http_method_txname,
        "get /api/:version/users/",
        "",
        "GET"
    );

    span_transaction_method_test!(
        test_http_method_context,
        "/api/:version/users/",
        "post",
        "POST"
    );

    span_transaction_method_test!(
        test_http_method_request_prioritized,
        "get /api/:version/users/",
        "post",
        "POST"
    );
}
