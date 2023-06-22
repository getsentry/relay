use crate::metrics_extraction::spans::types::{SpanMetric, SpanTagKey};
use crate::metrics_extraction::transactions::types::ExtractMetricsError;
use crate::metrics_extraction::utils::extract_http_status_code;
use crate::metrics_extraction::utils::http_status_code_from_span;
use crate::metrics_extraction::utils::{
    extract_transaction_op, get_eventuser_tag, get_trace_context,
};
use crate::metrics_extraction::IntoMetric;
use itertools::Itertools;
use once_cell::sync::Lazy;
use regex::Regex;
use relay_common::{EventType, UnixTimestamp};
use relay_filter::csp::SchemeDomainPort;
use relay_general::protocol::Event;
use relay_general::types::{Annotated, Value};
use relay_metrics::{AggregatorConfig, Metric};
use std::collections::BTreeMap;

mod types;

/// Extracts metrics from the spans of the given transaction, and sets common
/// tags for all the metrics and spans. If a span already contains a tag
/// extracted for a metric, the tag value is overwritten.
pub(crate) fn extract_span_metrics(
    aggregator_config: &AggregatorConfig,
    event: &mut Event,
    metrics: &mut Vec<Metric>, // output parameter
) -> Result<(), ExtractMetricsError> {
    // TODO(iker): measure the performance of this whole method

    if event.ty.value() != Some(&EventType::Transaction) {
        return Ok(());
    }
    let (Some(&_start), Some(&end)) = (event.start_timestamp.value(), event.timestamp.value()) else {
        relay_log::debug!("failed to extract the start and the end timestamps from the event");
        return Err(ExtractMetricsError::MissingTimestamp);
    };

    let Some(timestamp) = UnixTimestamp::from_datetime(end.into_inner()) else {
        relay_log::debug!("event timestamp is not a valid unix timestamp");
        return Err(ExtractMetricsError::InvalidTimestamp);
    };

    // Validate the transaction event against the metrics timestamp limits. If the metric is too
    // old or too new, we cannot extract the metric and also need to drop the transaction event
    // for consistency between metrics and events.
    if !aggregator_config.timestamp_range().contains(&timestamp) {
        relay_log::debug!("event timestamp is out of the valid range for metrics");
        return Err(ExtractMetricsError::InvalidTimestamp);
    }

    // Collect data for the databag that isn't metrics
    let mut databag = BTreeMap::new();

    if let Some(release) = event.release.as_str() {
        databag.insert(SpanTagKey::Release, release.to_owned());
    }

    if let Some(user) = event.user.value().and_then(get_eventuser_tag) {
        databag.insert(SpanTagKey::User, user);
    }

    // Collect the shared tags for all the metrics and spans on this transaction.
    let mut shared_tags = BTreeMap::new();

    if let Some(environment) = event.environment.as_str() {
        shared_tags.insert(SpanTagKey::Environment, environment.to_owned());
    }

    if let Some(transaction_name) = event.transaction.value() {
        shared_tags.insert(SpanTagKey::Transaction, transaction_name.to_owned());

        let transaction_method_from_request = event
            .request
            .value()
            .and_then(|r| r.method.value())
            .map(|m| m.to_uppercase());

        if let Some(transaction_method) = transaction_method_from_request
            .or(http_method_from_transaction_name(transaction_name).map(|m| m.to_uppercase()))
        {
            shared_tags.insert(SpanTagKey::TransactionMethod, transaction_method.clone());
        }
    }

    if let Some(trace_context) = get_trace_context(event) {
        if let Some(op) = extract_transaction_op(trace_context) {
            shared_tags.insert(SpanTagKey::TransactionOp, op.to_lowercase());
        }
    }

    if let Some(transaction_http_status_code) = extract_http_status_code(event) {
        shared_tags.insert(SpanTagKey::HttpStatusCode, transaction_http_status_code);
    }

    let Some(spans) = event.spans.value_mut() else { return Ok(()) };

    for annotated_span in spans {
        if let Some(span) = annotated_span.value_mut() {
            let mut span_tags = shared_tags.clone();

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

                // TODO(iker): we're relying on the existance of `http.method`
                // or `db.operation`. This is not guaranteed, and we'll need to
                // parse the span description in that case.
                let action = match span_module {
                    Some("http") => span
                        .data
                        .value()
                        // TODO(iker): some SDKs extract this as method
                        .and_then(|v| v.get("http.method"))
                        .and_then(|method| method.as_str())
                        .map(|s| s.to_uppercase()),
                    Some("db") => {
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

                if let Some(dom) = domain.clone() {
                    span_tags.insert(SpanTagKey::Domain, dom);
                }

                let scrubbed_description = span
                    .data
                    .value()
                    .and_then(|data| data.get("description.scrubbed"))
                    .and_then(|value| value.as_str());

                let sanitized_description = sanitized_span_description(
                    scrubbed_description,
                    span_module,
                    action.as_deref(),
                    domain.clone().as_deref(),
                );

                if let Some(scrubbed_desc) = sanitized_description {
                    span_tags.insert(SpanTagKey::Description, scrubbed_desc.to_owned());

                    let mut span_group = format!("{:?}", md5::compute(scrubbed_desc));
                    span_group.truncate(16);
                    span_tags.insert(SpanTagKey::Group, span_group);
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

            // Even if we emit metrics, we want this info to be duplicated in every span.
            span.data.get_or_insert_with(BTreeMap::new).extend({
                let it = span_tags
                    .clone()
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), Annotated::new(Value::String(v))));
                it.chain(
                    databag
                        .clone()
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), Annotated::new(Value::String(v)))),
                )
            });

            if let Some(user) = event.user.value() {
                if let Some(user_tag) = get_eventuser_tag(user) {
                    metrics.push(
                        SpanMetric::User {
                            value: user_tag,
                            tags: span_tags.clone(),
                        }
                        .into_metric(timestamp),
                    );
                }
            }

            if let Some(exclusive_time) = span.exclusive_time.value() {
                // NOTE(iker): this exclusive time doesn't consider all cases,
                // such as sub-transactions. We accept these limitations for
                // now.
                metrics.push(
                    SpanMetric::ExclusiveTime {
                        value: *exclusive_time,
                        tags: span_tags.clone(),
                    }
                    .into_metric(timestamp),
                );
            }

            if let (Some(&span_start), Some(&span_end)) =
                (span.start_timestamp.value(), span.timestamp.value())
            {
                // The `duration` of a span. This metric also serves as the
                // counter metric `throughput`.
                metrics.push(
                    SpanMetric::Duration {
                        value: span_end - span_start,
                        tags: span_tags.clone(),
                    }
                    .into_metric(timestamp),
                );
            };
        }
    }

    Ok(())
}

fn sanitized_span_description(
    scrubbed_description: Option<&str>,
    module: Option<&str>,
    action: Option<&str>,
    domain: Option<&str>,
) -> Option<String> {
    if let Some(scrubbed) = scrubbed_description {
        return Some(scrubbed.to_owned());
    }

    if let Some(module) = module {
        if !module.starts_with("http") {
            return None;
        }
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

/// Regex with a capture group to extract the database action from a query.
///
/// Currently, we're only interested in either `SELECT` or `INSERT` statements.
static SQL_ACTION_EXTRACTOR_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?i)(?P<action>(SELECT|INSERT))"#).unwrap());

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
            let domain_port = SchemeDomainPort::from(url);
            match (domain_port.domain, domain_port.port) {
                (Some(domain), port) => normalize_domain(&domain, port.as_ref()),
                _ => None,
            }
        }
        _ => None,
    }
}

fn normalize_domain(domain: &str, port: Option<&String>) -> Option<String> {
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
    Some(replaced)
}

/// Allow list of domains to not get subdomains scrubbed.
const DOMAIN_ALLOW_LIST: &[&str] = &["127.0.0.1", "localhost"];

fn normalized_domain_from_allowlist(domain: &str, port: Option<&String>) -> Option<String> {
    if let Some(domain) = DOMAIN_ALLOW_LIST.iter().find(|allowed| **allowed == domain) {
        let with_port = port.map_or_else(|| (*domain).to_owned(), |p| format!("{}:{}", domain, p));
        return Some(with_port);
    }
    None
}

#[cfg(test)]
mod tests {
    use relay_general::protocol::{Event, Request};
    use relay_general::store::{self, LightNormalizationConfig};
    use relay_general::types::Annotated;

    use relay_metrics::AggregatorConfig;

    use crate::metrics_extraction::spans::extract_span_metrics;

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
                        scrub_span_descriptions: true,
                        light_normalize_spans: true,
                        ..Default::default()
                    },
                );
                assert!(res.is_ok());

                let aggregator_config = AggregatorConfig {
                    max_secs_in_past: u64::MAX,
                    max_secs_in_future: u64::MAX,
                    ..Default::default()
                };

                let mut metrics = vec![];
                extract_span_metrics(
                    &aggregator_config,
                    event.value_mut().as_mut().unwrap(),
                    &mut metrics,
                )
                .unwrap();

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

                for metric in &metrics {
                    assert_eq!(
                        $expected_method,
                        metric.tags.get("transaction.method").unwrap()
                    );
                }
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

    #[test]
    fn test_extract_span_metrics() {
        let json = r#"
        {
            "type": "transaction",
            "platform": "javascript",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "timestamp": "2021-04-26T08:00:00+0100",
            "server_name": "myhost",
            "release": "1.2.3",
            "dist": "foo ",
            "environment": "fake_environment",
            "transaction": "gEt /api/:version/users/",
            "transaction_info": {"source": "custom"},
            "user": {
                "id": "user123",
                "geo": {
                    "country_code": "US"
                }
            },
            "contexts": {
                "trace": {
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "span_id": "bd429c44b67a3eb4",
                    "op": "mYOp",
                    "status": "ok"
                }
            },
            "request": {
                "method": "POST"
            },
            "spans": [
                {
                    "description": "<SomeUiRendering>",
                    "op": "UI.React.Render",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81"
                },
                {
                    "description": "POST http://127.0.0.1:1234/api/hi",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd2eb23da2beb459",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "http.method": "PoSt",
                        "status_code": "200"
                    }
                },
                {
                    "description": "POST http://sth.subdomain.domain.tld:targetport/api/hi",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd2eb23da2beb459",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "http.method": "PoSt",
                        "status_code": "200"
                    }
                },
                {
                    "description": "POST http://targetdomain.tld:targetport/api/hi",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd2eb23da2beb459",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "http.method": "POST",
                        "status_code": "200"
                    }
                },
                {
                    "description": "POST http://targetdomain:targetport/api/id/0987654321",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd2eb23da2beb459",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "http.method": "POST",
                        "status_code": "200"
                    }
                },
                {
                    "description": "POST http://sth.subdomain.domain.tld:targetport/api/hi",
                    "op": "http.client",
                    "tags": {
                        "http.status_code": "200"
                    },
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd2eb23da2beb459",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "http.method": "POST"
                    }
                },
                {
                    "description": "POST http://sth.subdomain.domain.tld:targetport/api/hi",
                    "op": "http.client",
                    "tags": {
                        "http.status_code": "200"
                    },
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd2eb23da2beb459",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "http.method": "POST",
                        "status_code": "200"
                    }
                },
                {
                    "description": "SeLeCt column FROM tAbLe WHERE id IN (1, 2, 3)",
                    "op": "db.sql.query",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "select column FROM table WHERE id IN (1, 2, 3)",
                    "op": "db",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
                },
                {
                    "description": "INSERT INTO table (col) VALUES (val)",
                    "op": "db.sql.query",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase",
                        "db.operation": "INSERT"
                    }
                },
                {
                    "description": "INSERT INTO from_date (col) VALUES (val)",
                    "op": "db.sql.query",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase",
                        "db.operation": "INSERT"
                    }
                },
                {
                    "description": "INSERT INTO table (col) VALUES (val)",
                    "op": "db",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
                },
                {
                    "description": "SELECT\n*\nFROM\ntable\nWHERE\nid\nIN\n(val)",
                    "op": "db.sql.query",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s",
                    "op": "db",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "SELECT 'TABLE'.'col' FROM 'TABLE' WHERE 'TABLE'.'col' = %s",
                    "op": "db",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "SAVEPOINT save_this_one",
                    "op": "db",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "MyDatabase"
                    }
                },
                {
                    "description": "GET cache:user:{123}",
                    "op": "cache.get_item",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "cache.hit": false
                    }
                },
                {
                    "description": "http://domain/static/myscript-v1.9.23.js",
                    "op": "resource.script",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
                }

            ]
        }
        "#;

        let mut event = Annotated::from_json(json).unwrap();

        // Normalize first, to make sure that all things are correct as in the real pipeline:
        let res = store::light_normalize_event(
            &mut event,
            LightNormalizationConfig {
                scrub_span_descriptions: true,
                light_normalize_spans: true,
                ..Default::default()
            },
        );
        assert!(res.is_ok());

        let aggregator_config = AggregatorConfig {
            max_secs_in_past: u64::MAX,
            max_secs_in_future: u64::MAX,
            ..Default::default()
        };

        let mut metrics = vec![];
        extract_span_metrics(
            &aggregator_config,
            event.value_mut().as_mut().unwrap(),
            &mut metrics,
        )
        .unwrap();

        insta::assert_debug_snapshot!(event.value().unwrap().spans);
        insta::assert_debug_snapshot!(metrics);
    }
}
