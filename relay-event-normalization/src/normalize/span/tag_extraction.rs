//! Logic for persisting items into `span.data` fields.
//! These are then used for metrics extraction.
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::ops::ControlFlow;

use once_cell::sync::Lazy;
use regex::Regex;
use relay_event_schema::protocol::{Event, Span, Timestamp, TraceContext};
use relay_protocol::Annotated;
use sqlparser::ast::Visit;
use sqlparser::ast::{ObjectName, Visitor};
use url::Url;

use crate::span::description::{parse_query, scrub_span_description};
use crate::utils::{
    extract_transaction_op, get_eventuser_tag, http_status_code_from_span, MAIN_THREAD_NAME,
    MOBILE_SDKS,
};

/// A list of supported span tags for tag extraction.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[allow(missing_docs)]
pub enum SpanTagKey {
    // Specific to a transaction
    Release,
    User,
    Environment,
    Transaction,
    TransactionMethod,
    TransactionOp,
    // `"true"` if the transaction was sent by a mobile SDK.
    Mobile,
    DeviceClass,

    // Specific to spans
    Action,
    Category,
    Description,
    Domain,
    Group,
    HttpDecodedResponseContentLength,
    HttpResponseContentLength,
    HttpResponseTransferSize,
    ResourceRenderBlockingStatus,
    SpanOp,
    StatusCode,
    System,
    /// Contributes to Time-To-Initial-Display.
    TimeToInitialDisplay,
    /// Contributes to Time-To-Full-Display.
    TimeToFullDisplay,
    /// File extension for resource spans.
    FileExtension,
    /// Span started on main thread.
    MainTread,
}

impl SpanTagKey {
    /// The key used to write this tag into `span.sentry_keys`.
    ///
    /// This key corresponds to the tag key in the snuba span dataset.
    pub fn sentry_tag_key(&self) -> &str {
        match self {
            SpanTagKey::Release => "release",
            SpanTagKey::User => "user",
            SpanTagKey::Environment => "environment",
            SpanTagKey::Transaction => "transaction",
            SpanTagKey::TransactionMethod => "transaction.method",
            SpanTagKey::TransactionOp => "transaction.op",
            SpanTagKey::Mobile => "mobile",
            SpanTagKey::DeviceClass => "device.class",

            SpanTagKey::Action => "action",
            SpanTagKey::Category => "category",
            SpanTagKey::Description => "description",
            SpanTagKey::Domain => "domain",
            SpanTagKey::Group => "group",
            SpanTagKey::HttpDecodedResponseContentLength => "http.decoded_response_content_length",
            SpanTagKey::HttpResponseContentLength => "http.response_content_length",
            SpanTagKey::HttpResponseTransferSize => "http.response_transfer_size",
            SpanTagKey::ResourceRenderBlockingStatus => "resource.render_blocking_status",
            SpanTagKey::SpanOp => "op",
            SpanTagKey::StatusCode => "status_code",
            SpanTagKey::System => "system",
            SpanTagKey::TimeToFullDisplay => "ttfd",
            SpanTagKey::TimeToInitialDisplay => "ttid",
            SpanTagKey::FileExtension => "file_extension",
            SpanTagKey::MainTread => "main_thread",
        }
    }
}

/// Render-blocking resources are static files, such as fonts, CSS, and JavaScript that block or
/// delay the browser from rendering page content to the screen.
///
/// See <https://developer.mozilla.org/en-US/docs/Web/API/PerformanceResourceTiming/renderBlockingStatus>.
enum RenderBlockingStatus {
    Blocking,
    NonBlocking,
}

impl<'a> TryFrom<&'a str> for RenderBlockingStatus {
    type Error = &'a str;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        Ok(match value {
            "blocking" => Self::Blocking,
            "non-blocking" => Self::NonBlocking,
            other => return Err(other),
        })
    }
}

impl std::fmt::Display for RenderBlockingStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Blocking => "blocking",
            Self::NonBlocking => "non-blocking",
        })
    }
}

/// Configuration for span tag extraction.
pub(crate) struct Config {
    /// The maximum allowed size of tag values in bytes. Longer values will be cropped.
    pub max_tag_value_size: usize,
}

/// Extracts tags from event and spans and materializes them into `span.data`.
pub(crate) fn extract_span_tags(event: &mut Event, config: &Config) {
    // TODO: To prevent differences between metrics and payloads, we should not extract tags here
    // when they have already been extracted by a downstream relay.
    let shared_tags = extract_shared_tags(event);
    let is_mobile = shared_tags.get(&SpanTagKey::Mobile);

    let Some(spans) = event.spans.value_mut() else {
        return;
    };

    let ttid = timestamp_by_op(spans, "ui.load.initial_display");
    let ttfd = timestamp_by_op(spans, "ui.load.full_display");

    for span in spans {
        let Some(span) = span.value_mut().as_mut() else {
            continue;
        };

        let tags = extract_tags(span, config, ttid, ttfd, is_mobile);

        span.sentry_tags = Annotated::new(
            shared_tags
                .clone()
                .into_iter()
                .chain(tags.clone())
                .map(|(k, v)| (k.sentry_tag_key().to_owned(), Annotated::new(v)))
                .collect(),
        );
    }
}

/// Extracts tags shared by every span.
pub fn extract_shared_tags(event: &Event) -> BTreeMap<SpanTagKey, String> {
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
        tags.insert(SpanTagKey::Transaction, transaction_name.clone());

        let transaction_method_from_request = event
            .request
            .value()
            .and_then(|r| r.method.value())
            .map(|m| m.to_uppercase());

        if let Some(transaction_method) = transaction_method_from_request.or_else(|| {
            http_method_from_transaction_name(transaction_name).map(|m| m.to_uppercase())
        }) {
            tags.insert(SpanTagKey::TransactionMethod, transaction_method);
        }
    }

    if let Some(trace_context) = event.context::<TraceContext>() {
        if let Some(op) = extract_transaction_op(trace_context) {
            tags.insert(SpanTagKey::TransactionOp, op.to_lowercase().to_owned());
        }
    }

    if MOBILE_SDKS.contains(&event.sdk_name()) {
        tags.insert(SpanTagKey::Mobile, "true".to_owned());
    }

    if let Some(device_class) = event.tag_value("device.class") {
        tags.insert(SpanTagKey::DeviceClass, device_class.into());
    }

    tags
}

/// Writes fields into [`Span::data`].
///
/// Generating new span data fields is based on a combination of looking at
/// [span operations](https://develop.sentry.dev/sdk/performance/span-operations/) and
/// existing [span data](https://develop.sentry.dev/sdk/performance/span-data-conventions/) fields,
/// and rely on Sentry conventions and heuristics.
pub(crate) fn extract_tags(
    span: &Span,
    config: &Config,
    initial_display: Option<Timestamp>,
    full_display: Option<Timestamp>,
    is_mobile: Option<&String>,
) -> BTreeMap<SpanTagKey, String> {
    let mut span_tags: BTreeMap<SpanTagKey, String> = BTreeMap::new();

    let system = span
        .data
        .value()
        .and_then(|v| v.get("db.system"))
        .and_then(|system| system.as_str());
    if let Some(sys) = system {
        span_tags.insert(SpanTagKey::System, sys.to_lowercase());
    }

    if let Some(unsanitized_span_op) = span.op.value() {
        let span_op = unsanitized_span_op.to_owned().to_lowercase();

        span_tags.insert(SpanTagKey::SpanOp, span_op.to_owned());

        let category = span_op_to_category(&span_op);
        if let Some(category) = category {
            span_tags.insert(SpanTagKey::Category, category.to_owned());
        }

        let scrubbed_description = scrub_span_description(span);

        let action = match (category, span_op.as_str(), &scrubbed_description) {
            (Some("http"), _, _) => span
                .data
                .value()
                .and_then(|v| {
                    v.get("http.request.method")
                        .or(v.get("http.method"))
                        .or(v.get("method"))
                })
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

        if let Some(act) = action {
            span_tags.insert(SpanTagKey::Action, act);
        }

        let domain = if span_op == "http.client" || span_op.starts_with("resource.") {
            // HACK: Parse the normalized description to get the normalized domain.
            if let Some(scrubbed) = scrubbed_description.as_deref() {
                let url = if let Some((_, url)) = scrubbed.split_once(' ') {
                    url
                } else {
                    scrubbed
                };
                Url::parse(url).ok().and_then(|url| {
                    url.domain().map(|d| {
                        let mut domain = d.to_lowercase();
                        if let Some(port) = url.port() {
                            domain = format!("{domain}:{port}");
                        }
                        domain
                    })
                })
            } else {
                None
            }
        } else if span_op.starts_with("db") {
            span.description
                .value()
                .and_then(|query| sql_tables_from_query(system, query))
        } else {
            None
        };

        if !span_op.starts_with("db.redis") {
            if let Some(dom) = domain {
                span_tags.insert(SpanTagKey::Domain, dom);
            }
        }

        if let Some(scrubbed_desc) = scrubbed_description {
            // Truncating the span description's tag value is, for now,
            // a temporary solution to not get large descriptions dropped. The
            // group tag mustn't be affected by this, and still be
            // computed from the full, untruncated description.

            let mut span_group = format!("{:?}", md5::compute(&scrubbed_desc));
            span_group.truncate(16);
            span_tags.insert(SpanTagKey::Group, span_group);

            let truncated = truncate_string(scrubbed_desc, config.max_tag_value_size);
            if span_op.starts_with("resource.") {
                if let Some(ext) = truncated
                    .rsplit('/')
                    .next()
                    .and_then(|last_segment| last_segment.rsplit_once('.'))
                    .map(|(_, extension)| extension)
                {
                    span_tags.insert(SpanTagKey::FileExtension, ext.to_lowercase());
                }
            }

            span_tags.insert(SpanTagKey::Description, truncated);
        }

        if span_op.starts_with("resource.") {
            if let Some(data) = span.data.value() {
                if let Some(value) = data
                    .get("http.response_content_length")
                    .and_then(Annotated::value)
                    .and_then(|v| String::try_from(v).ok())
                {
                    span_tags.insert(SpanTagKey::HttpResponseContentLength, value);
                }

                if let Some(value) = data
                    .get("http.decoded_response_content_length")
                    .and_then(Annotated::value)
                    .and_then(|v| String::try_from(v).ok())
                {
                    span_tags.insert(SpanTagKey::HttpDecodedResponseContentLength, value);
                }

                if let Some(value) = data
                    .get("http.response_transfer_size")
                    .and_then(Annotated::value)
                    .and_then(|v| String::try_from(v).ok())
                {
                    span_tags.insert(SpanTagKey::HttpResponseTransferSize, value);
                }
            }

            if let Some(resource_render_blocking_status) = span
                .data
                .value()
                .and_then(|data| data.get("resource.render_blocking_status"))
                .and_then(|value| value.as_str())
            {
                // Validate that it's a valid status:
                if let Ok(status) = RenderBlockingStatus::try_from(resource_render_blocking_status)
                {
                    span_tags.insert(SpanTagKey::ResourceRenderBlockingStatus, status.to_string());
                }
            }
        }
    }

    if let Some(status_code) = http_status_code_from_span(span) {
        span_tags.insert(SpanTagKey::StatusCode, status_code);
    }

    if is_mobile.is_some_and(|v| v.as_str() == "true") {
        if let Some(thread_name) = span
            .data
            .value()
            .and_then(|data| data.get("thread.name"))
            .and_then(|value| value.as_str())
        {
            if thread_name == MAIN_THREAD_NAME {
                span_tags.insert(SpanTagKey::MainTread, "true".to_owned());
            }
        }
    }

    if let Some(end_time) = span.timestamp.value() {
        if let Some(initial_display) = initial_display {
            if end_time <= &initial_display {
                span_tags.insert(SpanTagKey::TimeToInitialDisplay, "ttid".to_owned());
            }
        }
        if let Some(full_display) = full_display {
            if end_time <= &full_display {
                span_tags.insert(SpanTagKey::TimeToFullDisplay, "ttfd".to_owned());
            }
        }
    }

    span_tags
}

/// Finds first matching span and get its timestamp.
///
/// Used to get time-to-initial/full-display times.
fn timestamp_by_op(spans: &[Annotated<Span>], op: &str) -> Option<Timestamp> {
    spans
        .iter()
        .filter_map(Annotated::value)
        .find(|span| span.op.as_str() == Some(op))
        .and_then(|span| span.timestamp.value().copied())
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
/// Currently we have an explicit allow-list of database actions considered important.
static SQL_ACTION_EXTRACTOR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)(?P<action>(SELECT|INSERT|DELETE|UPDATE|SET|SAVEPOINT|RELEASE SAVEPOINT|ROLLBACK TO SAVEPOINT))"#).unwrap()
});

fn sql_action_from_query(query: &str) -> Option<&str> {
    extract_captured_substring(query, &SQL_ACTION_EXTRACTOR_REGEX)
}

/// Regex with a capture group to extract the table from a database query,
/// based on `FROM`, `INTO` and `UPDATE` keywords.
static SQL_TABLE_EXTRACTOR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)(from|into|update)(\s|")+(?P<table>(\w+(\.\w+)*))(\s|")+"#).unwrap()
});

/// Returns a sorted, comma-separated list of SQL tables, if any.
///
/// HACK: When there is a single table, add comma separation so that the
/// backend can understand the difference between tables and their subsets
/// for example: table `,users,` and table `,users_config,` should be considered different
fn sql_tables_from_query(db_system: Option<&str>, query: &str) -> Option<String> {
    match parse_query(db_system, query) {
        Ok(ast) => {
            let mut visitor = SqlTableNameVisitor {
                table_names: Default::default(),
            };
            ast.visit(&mut visitor);
            let comma_size: usize = 1;
            let mut s = String::with_capacity(
                visitor
                    .table_names
                    .iter()
                    .map(|name| String::len(name) + comma_size)
                    .sum::<usize>()
                    + comma_size,
            );
            if !visitor.table_names.is_empty() {
                s.push(',');
            }
            for name in visitor.table_names.into_iter() {
                write!(&mut s, "{name},").ok();
            }
            (!s.is_empty()).then_some(s)
        }
        Err(e) => {
            relay_log::debug!("Failed to parse SQL: {e}");
            extract_captured_substring(query, &SQL_TABLE_EXTRACTOR_REGEX).map(str::to_lowercase)
        }
    }
}

/// Visitor that finds table names in parsed SQL queries.
struct SqlTableNameVisitor {
    /// maintains sorted list of unique table names.
    /// Having a defined order reduces cardinality in the resulting tag (see [`sql_tables_from_query`]).
    table_names: BTreeSet<String>,
}

impl Visitor for SqlTableNameVisitor {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        if let Some(name) = relation.0.last() {
            let last = name.value.split('.').last().unwrap_or(&name.value);
            self.table_names.insert(last.to_lowercase());
        }
        ControlFlow::Continue(())
    }
}

/// Regex with a capture group to extract the HTTP method from a string.
pub static HTTP_METHOD_EXTRACTOR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^(?P<method>(GET|HEAD|POST|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH))\b")
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
    let mut it = op.split('.'); // e.g. "ui.react.render"
    match (it.next(), it.next()) {
        // Known categories with prefixes:
        (
            Some(prefix @ "ui"),
            Some(category @ ("react" | "vue" | "svelte" | "angular" | "ember")),
        )
        | (
            Some(prefix @ "function"),
            Some(category @ ("nextjs" | "remix" | "gpc" | "aws" | "azure")),
        ) => op.get(..prefix.len() + 1 + category.len()),
        // Main categories (only keep first part):
        (
            category @ Some(
                "app" | "browser" | "cache" | "console" | "db" | "event" | "file" | "graphql"
                | "grpc" | "http" | "measure" | "middleware" | "navigation" | "pageload" | "queue"
                | "resource" | "rpc" | "serialize" | "subprocess" | "template" | "topic" | "view"
                | "websocket",
            ),
            _,
        ) => category,
        // Map everything else to unknown:
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use relay_event_schema::processor::{process_value, ProcessingState};
    use relay_event_schema::protocol::{Event, Request};
    use relay_protocol::Annotated;

    use crate::{NormalizeProcessor, NormalizeProcessorConfig};

    use super::*;

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
                let res = process_value(
                    &mut event,
                    &mut NormalizeProcessor::new(NormalizeProcessorConfig {
                        enrich_spans: true,
                        light_normalize_spans: true,
                        ..Default::default()
                    }),
                    ProcessingState::root(),
                );
                assert!(res.is_ok());

                assert_eq!(
                    $expected_method,
                    event
                        .value()
                        .and_then(|e| e.spans.value())
                        .and_then(|spans| spans[0].value())
                        .and_then(|s| s.sentry_tags.value())
                        .and_then(|d| d.get("transaction.method"))
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

    #[test]
    fn extract_table_select() {
        let query = r#"SELECT * FROM "a.b" WHERE "x" = 1"#;
        assert_eq!(
            sql_tables_from_query(Some("postgresql"), query).unwrap(),
            ",b,"
        );
    }

    #[test]
    fn extract_table_select_nested() {
        let query = r#"SELECT * FROM (SELECT * FROM "a.b") s WHERE "x" = 1"#;
        assert_eq!(sql_tables_from_query(None, query).unwrap(), ",b,");
    }

    #[test]
    fn extract_table_multiple() {
        let query = r#"SELECT * FROM a JOIN t.c ON c_id = c.id JOIN b ON b_id = b.id"#;
        assert_eq!(
            sql_tables_from_query(Some("postgresql"), query).unwrap(),
            ",a,b,c,"
        );
    }

    #[test]
    fn extract_table_multiple_mysql() {
        let query =
            r#"SELECT * FROM a JOIN `t.c` ON /* hello */ c_id = c.id JOIN b ON b_id = b.id"#;
        assert_eq!(
            sql_tables_from_query(Some("mysql"), query).unwrap(),
            ",a,b,c,"
        );
    }

    #[test]
    fn extract_table_multiple_advanced() {
        let query = r#"
SELECT "sentry_grouprelease"."id", "sentry_grouprelease"."project_id",
  "sentry_grouprelease"."group_id", "sentry_grouprelease"."release_id",
  "sentry_grouprelease"."environment", "sentry_grouprelease"."first_seen",
  "sentry_grouprelease"."last_seen"
FROM "sentry_grouprelease"
WHERE (
  "sentry_grouprelease"."group_id" = %s AND "sentry_grouprelease"."release_id" IN (
    SELECT V0."release_id"
    FROM "sentry_environmentrelease" V0
    WHERE (
      V0."organization_id" = %s AND V0."release_id" IN (
        SELECT U0."release_id"
        FROM "sentry_release_project" U0
        WHERE U0."project_id" = %s
      )
    )
    ORDER BY V0."first_seen" DESC
    LIMIT 1
  )
)
LIMIT 1
            "#;
        assert_eq!(
            sql_tables_from_query(Some("postgresql"), query).unwrap(),
            ",sentry_environmentrelease,sentry_grouprelease,sentry_release_project,"
        );
    }

    #[test]
    fn extract_table_delete() {
        let query = r#"DELETE FROM "a.b" WHERE "x" = 1"#;
        assert_eq!(sql_tables_from_query(None, query).unwrap(), ",b,");
    }

    #[test]
    fn extract_table_insert() {
        let query = r#"INSERT INTO "a" ("x", "y") VALUES (%s, %s)"#;
        assert_eq!(
            sql_tables_from_query(Some("postgresql"), query).unwrap(),
            ",a,"
        );
    }

    #[test]
    fn extract_table_update() {
        let query = r#"UPDATE "a" SET "x" = %s, "y" = %s WHERE "z" = %s"#;
        assert_eq!(
            sql_tables_from_query(Some("postgresql"), query).unwrap(),
            ",a,"
        );
    }

    #[test]
    fn extract_sql_action() {
        let test_cases = vec![
            (
                r#"SELECT "sentry_organization"."id" FROM "sentry_organization" WHERE "sentry_organization"."id" = %s"#,
                "SELECT",
            ),
            (
                r#"INSERT INTO "sentry_groupseen" ("project_id", "group_id", "user_id", "last_seen") VALUES (%s, %s, %s, %s) RETURNING "sentry_groupseen"."id"#,
                "INSERT",
            ),
            (
                r#"UPDATE sentry_release SET date_released = %s WHERE id = %s"#,
                "UPDATE",
            ),
            (
                r#"DELETE FROM "sentry_groupinbox" WHERE "sentry_groupinbox"."id" IN (%s)"#,
                "DELETE",
            ),
            (r#"SET search_path TO my_schema, public"#, "SET"),
            (r#"SAVEPOINT %s"#, "SAVEPOINT"),
            (r#"RELEASE SAVEPOINT %s"#, "RELEASE SAVEPOINT"),
            (r#"ROLLBACK TO SAVEPOINT %s"#, "ROLLBACK TO SAVEPOINT"),
        ];

        for (query, expected) in test_cases {
            assert_eq!(sql_action_from_query(query).unwrap(), expected)
        }
    }

    #[test]
    fn test_display_times() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "javascript",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "trace": {
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "span_id": "bd429c44b67a3eb4"
                    }
                },
                "spans": [
                    {
                        "op": "before_first_display",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    },
                    {
                        "op": "ui.load.initial_display",
                        "span_id": "bd429c44b67a3eb2",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976303.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    },
                    {
                        "span_id": "bd429c44b67a3eb2",
                        "start_timestamp": 1597976303.0000000,
                        "timestamp": 1597976305.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    },
                    {
                        "op": "ui.load.full_display",
                        "span_id": "bd429c44b67a3eb2",
                        "start_timestamp": 1597976304.0000000,
                        "timestamp": 1597976306.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    },
                    {
                        "op": "after_full_display",
                        "span_id": "bd429c44b67a3eb2",
                        "start_timestamp": 1597976307.0000000,
                        "timestamp": 1597976308.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        extract_span_tags(
            &mut event,
            &Config {
                max_tag_value_size: 200,
            },
        );

        let spans = event.spans.value().unwrap();

        // First two spans contribute to initial display & full display:
        for span in &spans[..2] {
            let tags = span.value().unwrap().sentry_tags.value().unwrap();
            assert_eq!(tags.get("ttid").unwrap().as_str(), Some("ttid"));
            assert_eq!(tags.get("ttfd").unwrap().as_str(), Some("ttfd"));
        }

        // First four spans contribute to full display:
        for span in &spans[2..4] {
            let tags = span.value().unwrap().sentry_tags.value().unwrap();
            assert_eq!(tags.get("ttid"), None);
            assert_eq!(tags.get("ttfd").unwrap().as_str(), Some("ttfd"));
        }

        for span in &spans[4..] {
            let tags = span.value().unwrap().sentry_tags.value().unwrap();
            assert_eq!(tags.get("ttid"), None);
            assert_eq!(tags.get("ttfd"), None);
        }
    }

    #[test]
    fn test_resource_sizes() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "javascript",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "trace": {
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "span_id": "bd429c44b67a3eb4"
                    }
                },
                "spans": [
                    {
                        "op": "resource.script",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "data": {
                            "http.response_content_length": 1,
                            "http.decoded_response_content_length": 2.0,
                            "http.response_transfer_size": 3.3
                        }
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        extract_span_tags(
            &mut event,
            &Config {
                max_tag_value_size: 200,
            },
        );

        let span = &event.spans.value().unwrap()[0];

        let tags = span.value().unwrap().sentry_tags.value().unwrap();
        assert_eq!(
            tags.get("http.response_content_length").unwrap().as_str(),
            Some("1"),
        );
        assert_eq!(
            tags.get("http.decoded_response_content_length")
                .unwrap()
                .as_str(),
            Some("2"),
        );
        assert_eq!(
            tags.get("http.response_transfer_size").unwrap().as_str(),
            Some("3.3"),
        );
    }

    #[test]
    fn test_main_thread() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "android",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "sdk": {"name": "sentry.java.android"},
                "contexts": {
                    "trace": {
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "span_id": "bd429c44b67a3eb4"
                    }
                },
                "spans": [
                    {
                        "op": "ui.load",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "data": {
                            "thread.id": 1,
                            "thread.name": "main"
                        }
                    },
                    {
                        "op": "ui.load",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "data": {
                            "thread.id": 2,
                            "thread.name": "not main"
                        }
                    },
                    {
                        "op": "file.write",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        extract_span_tags(
            &mut event,
            &Config {
                max_tag_value_size: 200,
            },
        );

        let span = &event.spans.value().unwrap()[0];

        let tags = span.value().unwrap().sentry_tags.value().unwrap();
        assert_eq!(tags.get("main_thread").unwrap().as_str(), Some("true"));

        let span = &event.spans.value().unwrap()[1];

        let tags = span.value().unwrap().sentry_tags.value().unwrap();
        assert_eq!(tags.get("main_thread"), None);

        let span = &event.spans.value().unwrap()[2];

        let tags = span.value().unwrap().sentry_tags.value().unwrap();
        assert_eq!(tags.get("main_thread"), None);
    }
}
