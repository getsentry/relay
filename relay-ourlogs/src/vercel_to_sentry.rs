//! Transforms the Vercel Log Drain format to Sentry Logs.

use std::str::FromStr;

use chrono::{TimeZone, Utc};
use relay_conventions::{ENVIRONMENT, ORIGIN, SERVER_ADDRESS};
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel, SpanId, Timestamp, TraceId};
use relay_protocol::{Annotated, Meta, Remark, RemarkType};
use serde::{Deserialize, Deserializer};

/// Vercel log structure matching their [schema](https://vercel.com/docs/drains/reference/logs).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VercelLog {
    /// Unique identifier for the log entry.
    pub id: String,
    /// Identifier for the Vercel deployment.
    pub deployment_id: String,
    /// Origin of the log.
    pub source: String,
    /// Deployment unique URL hostname.
    pub host: String,
    /// Unix timestamp in milliseconds.
    pub timestamp: i64,
    /// Identifier for the Vercel project.
    pub project_id: String,
    /// Log severity level.
    pub level: VercelLogLevel,
    /// Log message content (may be truncated if over 256 KB).
    pub message: Option<String>,
    /// Identifier for the Vercel build. Only present on build logs.
    pub build_id: Option<String>,
    /// Entrypoint for the request.
    pub entrypoint: Option<String>,
    /// Origin of the external content. Only on external logs.
    pub destination: Option<String>,
    /// Function or dynamic path of the request.
    pub path: Option<String>,
    /// Log output type.
    #[serde(rename = "type")]
    pub ty: Option<String>,
    /// HTTP status code of the request.
    pub status_code: Option<i64>,
    /// Identifier of the request.
    pub request_id: Option<String>,
    /// Deployment environment. One of production or preview.
    pub environment: Option<String>,
    /// Git branch name.
    pub branch: Option<String>,
    /// JA3 fingerprint digest.
    pub ja3_digest: Option<String>,
    /// JA4 fingerprint digest.
    pub ja4_digest: Option<String>,
    /// Type of edge runtime. One of edge-function or middleware
    pub edge_type: Option<String>,
    /// Name of the Vercel project.
    pub project_name: Option<String>,
    /// Region where the request is executed.
    pub execution_region: Option<String>,
    /// Trace identifier for distributed tracing.
    #[serde(deserialize_with = "deserialize_trace_id", flatten)]
    pub trace_id: Option<String>,
    /// Span identifier for distributed tracing.
    #[serde(deserialize_with = "deserialize_span_id", flatten)]
    pub span_id: Option<String>,
    /// Proxy information for requests. (Optional)
    pub proxy: Option<VercelProxy>,
}

/// Macro to create a deserializer that accepts two field name variants, preferring the first.
macro_rules! define_fallback_field_deserializer {
    ($fn_name:ident, $primary:literal, $fallback:literal) => {
        fn $fn_name<'d, D: Deserializer<'d>>(d: D) -> Result<Option<String>, D::Error> {
            #[derive(Deserialize)]
            struct Helper {
                #[serde(rename = $primary)]
                primary_field: Option<String>,
                #[serde(rename = $fallback)]
                fallback_field: Option<String>,
            }

            let helper = Helper::deserialize(d)?;
            Ok(helper.primary_field.or(helper.fallback_field))
        }
    };
}

define_fallback_field_deserializer!(deserialize_trace_id, "traceId", "trace.id");
define_fallback_field_deserializer!(deserialize_span_id, "spanId", "span.id");

/// Log Level for Vercel Logs
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VercelLogLevel {
    /// Info level logs.
    Info,
    /// Warning level logs.
    Warning,
    /// Error level logs.
    Error,
    /// Fatal level logs.
    Fatal,
    /// Unknown status, for forward compatibility.
    Unknown(String),
}

/// Vercel proxy information for requests.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VercelProxy {
    /// Unix timestamp when the proxy request was made.
    pub timestamp: i64,
    /// HTTP method of the request.
    pub method: String,
    /// Hostname of the request.
    pub host: String,
    /// Request path with query parameters.
    pub path: String,
    /// User agent strings of the request.
    pub user_agent: Vec<String>,
    /// Region where the request is processed.
    pub region: String,
    /// Referer of the request.
    pub referer: Option<String>,
    /// HTTP status code of the proxy request.
    pub status_code: Option<i64>,
    /// Client IP address.
    pub client_ip: Option<String>,
    /// Protocol of the request.
    pub scheme: Option<String>,
    /// Size of the response in bytes.
    pub response_byte_size: Option<i64>,
    /// Original request ID when request is served from cache.
    pub cache_id: Option<String>,
    /// How the request was served based on its path and project configuration.
    pub path_type: Option<String>,
    /// Variant of the path type.
    pub path_type_variant: Option<String>,
    /// Vercel-specific identifier.
    pub vercel_id: Option<String>,
    /// Cache status sent to the browser.
    pub vercel_cache: Option<String>,
    /// Region where lambda function executed.
    pub lambda_region: Option<String>,
    /// Action taken by firewall rules.
    pub waf_action: Option<String>,
    /// ID of the firewall rule that matched.
    pub waf_rule_id: Option<String>,
}

/// Maps Vercel log level to Sentry log level.
fn map_vercel_level_to_sentry(level: VercelLogLevel) -> OurLogLevel {
    match level {
        VercelLogLevel::Info => OurLogLevel::Info,
        VercelLogLevel::Warning => OurLogLevel::Warn,
        VercelLogLevel::Error => OurLogLevel::Error,
        VercelLogLevel::Fatal => OurLogLevel::Fatal,
        VercelLogLevel::Unknown(_) => OurLogLevel::Info,
    }
}

fn get_trace_id(trace_id: Option<&String>, request_id: Option<&String>) -> Annotated<TraceId> {
    match trace_id {
        Some(s) if !s.is_empty() => match s.parse::<TraceId>() {
            Ok(id) => Annotated::new(id),
            Err(_) => Annotated::new_with_meta(TraceId::random(), |meta| {
                meta.add_remark(Remark::new(RemarkType::Substituted, "trace_id.invalid"));
            }),
        },
        _ => request_id
            .and_then(|s| s.parse::<TraceId>().ok())
            .map(Annotated::new)
            .unwrap_or_else(|| {
                Annotated::new_with_meta(TraceId::random(), |meta| {
                    meta.add_remark(Remark::new(RemarkType::Substituted, "trace_id.missing"));
                })
            }),
    }
}

fn get_span_id(span_id: Option<String>) -> Annotated<SpanId> {
    match span_id {
        Some(s) if !s.is_empty() => {
            SpanId::from_str(&s).map_or_else(|err| Annotated::from_error(err, None), Annotated::new)
        }
        _ => Annotated::empty(),
    }
}

/// Transforms a Vercel log record to a Sentry log.
pub fn vercel_log_to_sentry_log(vercel_log: VercelLog) -> OurLog {
    let VercelLog {
        id,
        deployment_id,
        source,
        host,
        timestamp,
        project_id,
        level,
        message,
        build_id,
        entrypoint,
        destination,
        path,
        ty,
        status_code,
        request_id,
        environment,
        branch,
        ja3_digest,
        ja4_digest,
        edge_type,
        project_name,
        execution_region,
        trace_id,
        span_id,
        proxy,
    } = vercel_log;
    let trace_id = get_trace_id(trace_id.as_ref(), request_id.as_ref());

    let mut attributes: Attributes = Attributes::default();

    macro_rules! add_optional_attribute {
        ($name:expr, $value:expr) => {{
            if let Some(value) = $value {
                attributes.insert($name.to_owned(), value);
            }
        }};
    }

    macro_rules! add_attribute {
        ($name:expr, $value:expr) => {{
            let val = $value;
            attributes.insert($name.to_owned(), val);
        }};
    }

    add_attribute!(ORIGIN, "auto.log_drain.vercel".to_owned());
    add_attribute!("vercel.id", id);
    add_attribute!("vercel.deployment_id", deployment_id);
    add_attribute!("vercel.source", source);
    add_attribute!(SERVER_ADDRESS, host);
    add_attribute!("vercel.project_id", project_id);

    add_optional_attribute!("vercel.build_id", build_id);
    add_optional_attribute!("vercel.entrypoint", entrypoint);
    add_optional_attribute!("vercel.destination", destination);
    add_optional_attribute!("vercel.path", path);
    add_optional_attribute!("vercel.log_type", ty);
    add_optional_attribute!("vercel.status_code", status_code);
    add_optional_attribute!("vercel.request_id", request_id);
    add_optional_attribute!(ENVIRONMENT, environment);
    add_optional_attribute!("vercel.branch", branch);
    add_optional_attribute!("vercel.ja3_digest", ja3_digest);
    add_optional_attribute!("vercel.ja4_digest", ja4_digest);
    add_optional_attribute!("vercel.edge_type", edge_type);
    add_optional_attribute!("vercel.project_name", project_name);
    add_optional_attribute!("vercel.execution_region", execution_region);

    if let Some(proxy) = proxy {
        let VercelProxy {
            timestamp,
            method,
            host,
            path,
            user_agent,
            referer,
            region,
            status_code,
            client_ip,
            scheme,
            response_byte_size,
            cache_id,
            path_type,
            path_type_variant,
            vercel_id,
            vercel_cache,
            lambda_region,
            waf_action,
            waf_rule_id,
        } = proxy;

        add_attribute!("vercel.proxy.timestamp", timestamp);
        add_attribute!("vercel.proxy.method", method);
        add_attribute!("vercel.proxy.host", host);
        add_attribute!("vercel.proxy.path", path);
        add_attribute!("vercel.proxy.region", region);

        if let Ok(user_agent_string) = serde_json::to_string(&user_agent) {
            attributes.insert("vercel.proxy.user_agent", user_agent_string);
        }

        add_optional_attribute!("vercel.proxy.referer", referer);
        add_optional_attribute!("vercel.proxy.status_code", status_code);
        add_optional_attribute!("vercel.proxy.client_ip", client_ip);
        add_optional_attribute!("vercel.proxy.scheme", scheme);
        add_optional_attribute!("vercel.proxy.response_byte_size", response_byte_size);
        add_optional_attribute!("vercel.proxy.cache_id", cache_id);
        add_optional_attribute!("vercel.proxy.path_type", path_type);
        add_optional_attribute!("vercel.proxy.path_type_variant", path_type_variant);
        add_optional_attribute!("vercel.proxy.vercel_id", vercel_id);
        add_optional_attribute!("vercel.proxy.vercel_cache", vercel_cache);
        add_optional_attribute!("vercel.proxy.lambda_region", lambda_region);
        add_optional_attribute!("vercel.proxy.waf_action", waf_action);
        add_optional_attribute!("vercel.proxy.waf_rule_id", waf_rule_id);
    }

    let timestamp = match Utc.timestamp_millis_opt(timestamp).single() {
        Some(datetime) => Annotated::new(Timestamp(datetime)),
        None => {
            let mut meta = Meta::default();
            meta.add_remark(Remark::new(RemarkType::Substituted, "timestamp.invalid"));
            Annotated(Some(Timestamp(Utc::now())), meta)
        }
    };

    OurLog {
        timestamp,
        trace_id,
        span_id: get_span_id(span_id),
        level: Annotated::new(map_vercel_level_to_sentry(level)),
        body: Annotated::new(message.unwrap_or_default()),
        attributes: Annotated::new(attributes),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::SerializableAnnotated;

    #[test]
    fn test_vercel_log_to_sentry_log() {
        let vercel_log = VercelLog {
            id: "test-log-123".to_owned(),
            deployment_id: "dpl_233NRGRjVZX1caZrXWtz5g1TAksD".to_owned(),
            source: "lambda".to_owned(),
            host: "my-app-abc123.vercel.app".to_owned(),
            timestamp: 1573817187330, // Unix timestamp in milliseconds
            project_id: "gdufoJxB6b9b1fEqr1jUtFkyavUU".to_owned(),
            level: VercelLogLevel::Error,
            message: Some("API request errored".to_owned()),
            build_id: Some("bld_cotnkcr76".to_owned()),
            entrypoint: Some("api/index.js".to_owned()),
            destination: Some("https://api.example.com".to_owned()),
            path: Some("/api/users".to_owned()),
            ty: Some("stdout".to_owned()),
            status_code: Some(200),
            request_id: Some("643af4e3-975a-4cc7-9e7a-1eda11539d90".to_owned()),
            environment: Some("production".to_owned()),
            branch: Some("main".to_owned()),
            ja3_digest: Some(
                "769,47-53-5-10-49161-49162-49171-49172-50-56-19-4,0-10-11,23-24-25,0".to_owned(),
            ),
            ja4_digest: Some("t13d1516h2_8daaf6152771_02713d6af862".to_owned()),
            edge_type: Some("middleware".to_owned()),
            project_name: Some("my-app".to_owned()),
            execution_region: Some("sfo1".to_owned()),
            trace_id: Some("1b02cd14bb8642fd092bc23f54c7ffcd".to_owned()),
            span_id: Some("f24e8631bd11faa7".to_owned()),
            proxy: Some(VercelProxy {
                timestamp: 1573817250172,
                method: "GET".to_owned(),
                host: "my-app.vercel.app".to_owned(),
                path: "/api/users?page=1".to_owned(),
                user_agent: vec!["Mozilla/5.0...".to_owned()],
                region: "sfo1".to_owned(),
                referer: Some("https://my-app.vercel.app".to_owned()),
                status_code: Some(200),
                client_ip: Some("120.75.16.101".to_owned()),
                scheme: Some("https".to_owned()),
                response_byte_size: Some(1024),
                cache_id: Some("pdx1::v8g4b-1744143786684-93dafbc0f70d".to_owned()),
                path_type: Some("func".to_owned()),
                path_type_variant: Some("api".to_owned()),
                vercel_id: Some("sfo1::abc123".to_owned()),
                vercel_cache: Some("MISS".to_owned()),
                lambda_region: Some("sfo1".to_owned()),
                waf_action: Some("log".to_owned()),
                waf_rule_id: Some("rule_gAHz8jtSB1Gy".to_owned()),
            }),
        };

        let our_log: Annotated<OurLog> = Annotated::new(vercel_log_to_sentry_log(vercel_log));

        insta::assert_json_snapshot!(SerializableAnnotated(&our_log), @r#"
        {
          "timestamp": 1573817187.33,
          "trace_id": "1b02cd14bb8642fd092bc23f54c7ffcd",
          "span_id": "f24e8631bd11faa7",
          "level": "error",
          "body": "API request errored",
          "attributes": {
            "sentry.environment": {
              "type": "string",
              "value": "production"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.log_drain.vercel"
            },
            "server.address": {
              "type": "string",
              "value": "my-app-abc123.vercel.app"
            },
            "vercel.branch": {
              "type": "string",
              "value": "main"
            },
            "vercel.build_id": {
              "type": "string",
              "value": "bld_cotnkcr76"
            },
            "vercel.deployment_id": {
              "type": "string",
              "value": "dpl_233NRGRjVZX1caZrXWtz5g1TAksD"
            },
            "vercel.destination": {
              "type": "string",
              "value": "https://api.example.com"
            },
            "vercel.edge_type": {
              "type": "string",
              "value": "middleware"
            },
            "vercel.entrypoint": {
              "type": "string",
              "value": "api/index.js"
            },
            "vercel.execution_region": {
              "type": "string",
              "value": "sfo1"
            },
            "vercel.id": {
              "type": "string",
              "value": "test-log-123"
            },
            "vercel.ja3_digest": {
              "type": "string",
              "value": "769,47-53-5-10-49161-49162-49171-49172-50-56-19-4,0-10-11,23-24-25,0"
            },
            "vercel.ja4_digest": {
              "type": "string",
              "value": "t13d1516h2_8daaf6152771_02713d6af862"
            },
            "vercel.log_type": {
              "type": "string",
              "value": "stdout"
            },
            "vercel.path": {
              "type": "string",
              "value": "/api/users"
            },
            "vercel.project_id": {
              "type": "string",
              "value": "gdufoJxB6b9b1fEqr1jUtFkyavUU"
            },
            "vercel.project_name": {
              "type": "string",
              "value": "my-app"
            },
            "vercel.proxy.cache_id": {
              "type": "string",
              "value": "pdx1::v8g4b-1744143786684-93dafbc0f70d"
            },
            "vercel.proxy.client_ip": {
              "type": "string",
              "value": "120.75.16.101"
            },
            "vercel.proxy.host": {
              "type": "string",
              "value": "my-app.vercel.app"
            },
            "vercel.proxy.lambda_region": {
              "type": "string",
              "value": "sfo1"
            },
            "vercel.proxy.method": {
              "type": "string",
              "value": "GET"
            },
            "vercel.proxy.path": {
              "type": "string",
              "value": "/api/users?page=1"
            },
            "vercel.proxy.path_type": {
              "type": "string",
              "value": "func"
            },
            "vercel.proxy.path_type_variant": {
              "type": "string",
              "value": "api"
            },
            "vercel.proxy.referer": {
              "type": "string",
              "value": "https://my-app.vercel.app"
            },
            "vercel.proxy.region": {
              "type": "string",
              "value": "sfo1"
            },
            "vercel.proxy.response_byte_size": {
              "type": "integer",
              "value": 1024
            },
            "vercel.proxy.scheme": {
              "type": "string",
              "value": "https"
            },
            "vercel.proxy.status_code": {
              "type": "integer",
              "value": 200
            },
            "vercel.proxy.timestamp": {
              "type": "integer",
              "value": 1573817250172
            },
            "vercel.proxy.user_agent": {
              "type": "string",
              "value": "[\"Mozilla/5.0...\"]"
            },
            "vercel.proxy.vercel_cache": {
              "type": "string",
              "value": "MISS"
            },
            "vercel.proxy.vercel_id": {
              "type": "string",
              "value": "sfo1::abc123"
            },
            "vercel.proxy.waf_action": {
              "type": "string",
              "value": "log"
            },
            "vercel.proxy.waf_rule_id": {
              "type": "string",
              "value": "rule_gAHz8jtSB1Gy"
            },
            "vercel.request_id": {
              "type": "string",
              "value": "643af4e3-975a-4cc7-9e7a-1eda11539d90"
            },
            "vercel.source": {
              "type": "string",
              "value": "lambda"
            },
            "vercel.status_code": {
              "type": "integer",
              "value": 200
            }
          }
        }
        "#);
    }

    #[test]
    fn test_parse_real_vercel_log_json() {
        // This is based on the example from the Vercel documentation
        // in https://vercel.com/docs/drains/reference/logs#format
        let json = r#"{
            "id": "1573817250283254651097202070",
            "deploymentId": "dpl_233NRGRjVZX1caZrXWtz5g1TAksD",
            "source": "lambda",
            "host": "my-app-abc123.vercel.app",
            "timestamp": 1573817250283,
            "projectId": "gdufoJxB6b9b1fEqr1jUtFkyavUU",
            "level": "info",
            "message": "API request processed",
            "entrypoint": "api/index.js",
            "requestId": "643af4e3-975a-4cc7-9e7a-1eda11539d90",
            "statusCode": 200,
            "path": "/api/users",
            "executionRegion": "sfo1",
            "environment": "production",
            "traceId": "1b02cd14bb8642fd092bc23f54c7ffcd",
            "spanId": "f24e8631bd11faa7",
            "proxy": {
                "timestamp": 1573817250172,
                "method": "GET",
                "host": "my-app.vercel.app",
                "path": "/api/users?page=1",
                "userAgent": ["Mozilla/5.0..."],
                "referer": "https://my-app.vercel.app",
                "region": "sfo1",
                "statusCode": 200,
                "clientIp": "120.75.16.101",
                "scheme": "https",
                "vercelCache": "MISS"
            }
        }"#;

        let vercel_log: VercelLog = match serde_json::from_str(json) {
            Ok(log) => log,
            Err(e) => panic!("Failed to parse Vercel log JSON: {}", e),
        };

        let our_log: Annotated<OurLog> = Annotated::new(vercel_log_to_sentry_log(vercel_log));

        insta::assert_json_snapshot!(SerializableAnnotated(&our_log), @r#"
        {
          "timestamp": 1573817250.283,
          "trace_id": "1b02cd14bb8642fd092bc23f54c7ffcd",
          "span_id": "f24e8631bd11faa7",
          "level": "info",
          "body": "API request processed",
          "attributes": {
            "sentry.environment": {
              "type": "string",
              "value": "production"
            },
            "sentry.origin": {
              "type": "string",
              "value": "auto.log_drain.vercel"
            },
            "server.address": {
              "type": "string",
              "value": "my-app-abc123.vercel.app"
            },
            "vercel.deployment_id": {
              "type": "string",
              "value": "dpl_233NRGRjVZX1caZrXWtz5g1TAksD"
            },
            "vercel.entrypoint": {
              "type": "string",
              "value": "api/index.js"
            },
            "vercel.execution_region": {
              "type": "string",
              "value": "sfo1"
            },
            "vercel.id": {
              "type": "string",
              "value": "1573817250283254651097202070"
            },
            "vercel.path": {
              "type": "string",
              "value": "/api/users"
            },
            "vercel.project_id": {
              "type": "string",
              "value": "gdufoJxB6b9b1fEqr1jUtFkyavUU"
            },
            "vercel.proxy.client_ip": {
              "type": "string",
              "value": "120.75.16.101"
            },
            "vercel.proxy.host": {
              "type": "string",
              "value": "my-app.vercel.app"
            },
            "vercel.proxy.method": {
              "type": "string",
              "value": "GET"
            },
            "vercel.proxy.path": {
              "type": "string",
              "value": "/api/users?page=1"
            },
            "vercel.proxy.referer": {
              "type": "string",
              "value": "https://my-app.vercel.app"
            },
            "vercel.proxy.region": {
              "type": "string",
              "value": "sfo1"
            },
            "vercel.proxy.scheme": {
              "type": "string",
              "value": "https"
            },
            "vercel.proxy.status_code": {
              "type": "integer",
              "value": 200
            },
            "vercel.proxy.timestamp": {
              "type": "integer",
              "value": 1573817250172
            },
            "vercel.proxy.user_agent": {
              "type": "string",
              "value": "[\"Mozilla/5.0...\"]"
            },
            "vercel.proxy.vercel_cache": {
              "type": "string",
              "value": "MISS"
            },
            "vercel.request_id": {
              "type": "string",
              "value": "643af4e3-975a-4cc7-9e7a-1eda11539d90"
            },
            "vercel.source": {
              "type": "string",
              "value": "lambda"
            },
            "vercel.status_code": {
              "type": "integer",
              "value": 200
            }
          }
        }
        "#);
    }

    #[test]
    fn test_trace_span_id_precedence() {
        // Test that camelCase fields take precedence over dotted aliases
        let json_with_both = r#"{
            "id": "test-123",
            "deploymentId": "dpl_test",
            "source": "lambda",
            "host": "test.vercel.app",
            "timestamp": 1573817250283,
            "projectId": "prj_test",
            "level": "info",
            "message": "Test message",
            "traceId": "1b02cd14bb8642fd092bc23f54c7ffcd",
            "trace.id": "922dda2462ea4ac2b6a4b339bee90863",
            "spanId": "f24e8631bd11faa7",
            "span.id": "a1e13f3f06239d69"
        }"#;

        let vercel_log: VercelLog =
            serde_json::from_str(json_with_both).expect("Failed to parse JSON with both formats");
        let our_log = vercel_log_to_sentry_log(vercel_log);
        assert_eq!(
            our_log.trace_id.value().map(|t| t.to_string()),
            Some("1b02cd14bb8642fd092bc23f54c7ffcd".to_owned())
        );
        assert_eq!(
            our_log.span_id.value().map(|s| s.to_string()),
            Some("f24e8631bd11faa7".to_owned())
        );
    }

    #[test]
    fn test_trace_id_from_request_id_when_trace_id_missing() {
        let json_without_trace_id = r#"{
            "id": "test-123",
            "deploymentId": "dpl_test",
            "source": "lambda",
            "host": "test.vercel.app",
            "timestamp": 1573817250283,
            "projectId": "prj_test",
            "level": "info",
            "message": "Test message",
            "requestId": "643af4e3-975a-4cc7-9e7a-1eda11539d90"
        }"#;

        let vercel_log: VercelLog = serde_json::from_str(json_without_trace_id)
            .expect("Failed to parse JSON without trace_id");
        let our_log = vercel_log_to_sentry_log(vercel_log);

        assert_eq!(
            our_log.trace_id.value().map(|t| t.to_string()),
            Some("643af4e3975a4cc79e7a1eda11539d90".to_owned())
        );
    }
}
