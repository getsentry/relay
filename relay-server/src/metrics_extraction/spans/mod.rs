use crate::metrics_extraction::transactions::types::ExtractMetricsError;
use crate::metrics_extraction::utils::{
    extract_transaction_op, get_eventuser_tag, get_trace_context,
};
use itertools::Itertools;
use md5;
use once_cell::sync::Lazy;
use regex::Regex;
use relay_common::{DurationUnit, EventType, MetricUnit, UnixTimestamp};
use relay_filter::csp::SchemeDomainPort;
use relay_general::protocol::Event;
use relay_general::types::{Annotated, Value};
use relay_metrics::{AggregatorConfig, Metric, MetricNamespace, MetricValue};
use std::collections::BTreeMap;

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
    let (Some(&start), Some(&end)) = (event.start_timestamp.value(), event.timestamp.value()) else {
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

    // Collect the shared tags for all the metrics and spans on this transaction.
    let mut shared_tags = BTreeMap::new();

    if let Some(environment) = event.environment.as_str() {
        shared_tags.insert("environment".to_owned(), environment.to_owned());
    }

    if let Some(transaction_name) = event.transaction.value() {
        shared_tags.insert("transaction".to_owned(), transaction_name.to_owned());

        if let Some(transaction_method) = http_method_from_transaction_name(transaction_name) {
            shared_tags.insert(
                "transaction.method".to_owned(),
                transaction_method.to_owned(),
            );
        }
    }

    if let Some(trace_context) = get_trace_context(event) {
        if let Some(op) = extract_transaction_op(trace_context) {
            shared_tags.insert("transaction.op".to_owned(), op);
        }
    }

    let Some(spans) = event.spans.value_mut() else { return Ok(())};

    for annotated_span in spans {
        if let Some(span) = annotated_span.value_mut() {
            let mut span_tags = shared_tags.clone();

            if let Some(scrubbed_description) = span
                .data
                .value()
                .and_then(|data| data.get("description.scrubbed"))
                .and_then(|value| value.as_str())
            {
                span_tags.insert(
                    "span.description".to_owned(),
                    scrubbed_description.to_owned(),
                );

                let mut span_group = format!("{:?}", md5::compute(scrubbed_description));
                span_group.truncate(16);
                span_tags.insert("span.group".to_owned(), span_group);
            }

            if let Some(span_op) = span.op.value() {
                span_tags.insert("span.op".to_owned(), span_op.to_owned());

                if let Some(category) = span_op_to_category(span_op) {
                    span_tags.insert("span.category".to_owned(), category.to_owned());
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
                    span_tags.insert("span.module".to_owned(), module.to_owned());
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
                        .and_then(|method| method.as_str()),
                    Some("db") => {
                        let action_from_data = span
                            .data
                            .value()
                            .and_then(|v| v.get("db.operation"))
                            .and_then(|db_op| db_op.as_str());
                        action_from_data.or_else(|| {
                            span.description
                                .value()
                                .and_then(|d| sql_action_from_query(d))
                        })
                    }
                    _ => None,
                };

                if let Some(act) = action {
                    span_tags.insert("span.action".to_owned(), act.to_owned());
                }

                let domain = if span_op == "http.client" {
                    span.description
                        .value()
                        .and_then(|url| domain_from_http_url(url))
                } else if span_op.starts_with("db") {
                    span.description
                        .value()
                        .and_then(|query| sql_table_from_query(query))
                        .map(|s| s.to_owned())
                } else {
                    None
                };

                if let Some(dom) = domain {
                    span_tags.insert("span.domain".to_owned(), dom.to_owned());
                }
            }

            let system = span
                .data
                .value()
                .and_then(|v| v.get("db.system"))
                .and_then(|system| system.as_str());
            if let Some(sys) = system {
                span_tags.insert("span.system".to_owned(), sys.to_owned());
            }

            if let Some(span_status) = span.status.value() {
                span_tags.insert("span.status".to_owned(), span_status.to_string());
            }

            // XXX(iker): extract status code, when its cardinality doesn't
            // represent a risk for the indexers.

            // Even if we emit metrics, we want this info to be duplicated in every span.
            span.data.get_or_insert_with(BTreeMap::new).extend(
                span_tags
                    .clone()
                    .into_iter()
                    .map(|(k, v)| (k, Annotated::new(Value::String(v)))),
            );

            if let Some(user) = event.user.value() {
                if let Some(value) = get_eventuser_tag(user) {
                    metrics.push(Metric::new_mri(
                        MetricNamespace::Transactions,
                        "span.user",
                        MetricUnit::None,
                        MetricValue::set_from_str(&value),
                        timestamp,
                        span_tags.clone(),
                    ));
                }
            }

            if let Some(exclusive_time) = span.exclusive_time.value() {
                // NOTE(iker): this exclusive time doesn't consider all cases,
                // such as sub-transactions. We accept these limitations for
                // now.
                metrics.push(Metric::new_mri(
                    MetricNamespace::Transactions,
                    "span.exclusive_time",
                    MetricUnit::Duration(DurationUnit::MilliSecond),
                    MetricValue::Distribution(*exclusive_time),
                    timestamp,
                    span_tags.clone(),
                ));
            }

            // The `duration` of a span. This metric also serves as the
            // counter metric `throughput`.
            metrics.push(Metric::new_mri(
                MetricNamespace::Transactions,
                "span.duration",
                MetricUnit::Duration(DurationUnit::MilliSecond),
                MetricValue::Distribution(relay_common::chrono_to_positive_millis(end - start)),
                timestamp,
                span_tags.clone(),
            ));
        }
    }

    Ok(())
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
