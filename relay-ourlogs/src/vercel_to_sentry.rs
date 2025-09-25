//! Transforms Vercel logs to Sentry logs.
//!
//! This module provides functionality to convert Vercel log records
//! into Sentry's internal log format (`OurLog`).

use chrono::{TimeZone, Utc};
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel, SpanId, Timestamp, TraceId};
use relay_protocol::{Annotated, Meta, Object, Remark, RemarkType};
use serde::{Deserialize, Serialize};

/// Vercel log level enumeration.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

/// Vercel proxy information for requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VercelProxy {
    /// Timestamp when the proxy request was made. Required when proxy object is present.
    pub timestamp: u64,
    /// HTTP method of the request. Required when proxy object is present.
    pub method: String,
    /// Host header of the request. Required when proxy object is present.
    pub host: String,
    /// Path of the request. Required when proxy object is present.
    pub path: String,
    /// Region where the request is processed. Required when proxy object is present.
    pub region: String,
    /// User agent header(s).
    #[serde(rename = "userAgent")]
    pub user_agent: Option<Vec<String>>,
    /// Referer header.
    pub referer: Option<String>,
    /// HTTP status code of the proxy request.
    #[serde(rename = "statusCode")]
    pub status_code: Option<i32>,
    /// Client IP address.
    #[serde(rename = "clientIp")]
    pub client_ip: Option<String>,
    /// Protocol of the request.
    pub scheme: Option<String>,
    /// Size of the response in bytes.
    #[serde(rename = "responseByteSize")]
    pub response_byte_size: Option<u64>,
    /// Original request ID when request is served from cache.
    #[serde(rename = "cacheId")]
    pub cache_id: Option<String>,
    /// How the request was served based on its path and project configuration.
    #[serde(rename = "pathType")]
    pub path_type: Option<String>,
    /// Variant of the path type.
    #[serde(rename = "pathTypeVariant")]
    pub path_type_variant: Option<String>,
    /// Vercel-specific identifier.
    #[serde(rename = "vercelId")]
    pub vercel_id: Option<String>,
    /// Cache status sent to the browser.
    #[serde(rename = "vercelCache")]
    pub vercel_cache: Option<String>,
    /// Region where lambda function executed.
    #[serde(rename = "lambdaRegion")]
    pub lambda_region: Option<String>,
    /// Action taken by firewall rules.
    #[serde(rename = "wafAction")]
    pub waf_action: Option<String>,
    /// ID of the firewall rule that matched.
    #[serde(rename = "wafRuleId")]
    pub waf_rule_id: Option<String>,
}

/// Vercel log structure matching their schema.
/// Based on: https://vercel.com/docs/drains/reference/logs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VercelLog {
    /// Unique identifier for the log entry. (Required)
    pub id: String,
    /// Unix timestamp in milliseconds. (Required)
    pub timestamp: u64,
    /// Log level. (Required)
    pub level: VercelLogLevel,
    /// The log message. (Required)
    pub message: String,
    /// Source of the log (build, lambda, edge, static, external, firewall). (Required)
    pub source: String,
    /// Deployment identifier. (Optional)
    #[serde(rename = "deploymentId")]
    pub deployment_id: Option<String>,
    /// Host/domain name. (Optional)
    pub host: Option<String>,
    /// Project identifier. (Optional)
    #[serde(rename = "projectId")]
    pub project_id: Option<String>,
    /// Project name. (Optional)
    #[serde(rename = "projectName")]
    pub project_name: Option<String>,
    /// Build identifier (for build logs). (Optional)
    #[serde(rename = "buildId")]
    pub build_id: Option<String>,
    /// Type like "stdout", "stderr". (Optional)
    #[serde(rename = "type")]
    pub log_type: Option<String>,
    /// Function entrypoint (for lambda logs). (Optional)
    pub entrypoint: Option<String>,
    /// Request identifier. (Optional)
    #[serde(rename = "requestId")]
    pub request_id: Option<String>,
    /// HTTP status code. (Optional)
    #[serde(rename = "statusCode")]
    pub status_code: Option<i32>,
    /// Request path. (Optional)
    pub path: Option<String>,
    /// Region where function executed. (Optional)
    #[serde(rename = "executionRegion")]
    pub execution_region: Option<String>,
    /// Environment (production, preview). (Optional)
    pub environment: Option<String>,
    /// Trace identifier for OpenTelemetry. (Optional)
    #[serde(rename = "traceId")]
    pub trace_id: Option<String>,
    /// Span identifier for OpenTelemetry. (Optional)
    #[serde(rename = "spanId")]
    pub span_id: Option<String>,
    /// Alternative trace ID field. (Optional)
    #[serde(rename = "trace.id")]
    pub trace_dot_id: Option<String>,
    /// Alternative span ID field. (Optional)
    #[serde(rename = "span.id")]
    pub span_dot_id: Option<String>,
    /// Proxy information for requests. (Optional)
    pub proxy: Option<VercelProxy>,
}

/// Maps Vercel log level to Sentry log level.
fn map_vercel_level_to_sentry(level: &VercelLogLevel) -> OurLogLevel {
    match level {
        VercelLogLevel::Info => OurLogLevel::Info,
        VercelLogLevel::Warning => OurLogLevel::Warn,
        VercelLogLevel::Error => OurLogLevel::Error,
        VercelLogLevel::Fatal => OurLogLevel::Fatal,
    }
}

/// Transforms a Vercel log record to a Sentry log.
pub fn vercel_log_to_sentry_log(vercel_log: VercelLog) -> OurLog {
    let VercelLog {
        id,
        timestamp,
        level,
        message,
        source,
        deployment_id,
        host,
        project_id,
        project_name,
        build_id,
        log_type,
        entrypoint,
        request_id,
        status_code,
        path,
        execution_region,
        environment,
        trace_id,
        span_id,
        trace_dot_id,
        span_dot_id,
        proxy,
    } = vercel_log;

    // Convert timestamp from milliseconds to seconds
    let timestamp_seconds = (timestamp as f64) / 1000.0;
    let datetime = Utc
        .timestamp_opt(
            timestamp_seconds as i64,
            (timestamp_seconds.fract() * 1_000_000_000.0) as u32,
        )
        .single()
        .unwrap_or_else(Utc::now);

    // Map the Vercel level to Sentry level
    let sentry_level = map_vercel_level_to_sentry(&level);

    // Handle trace ID - prefer the regular field over the dotted one
    let trace_id = {
        let trace_id_str = trace_id.or(trace_dot_id);
        match trace_id_str.as_deref() {
            Some(s) if !s.is_empty() => {
                // Try to parse as hex string first, then as bytes
                if let Ok(hex_bytes) = hex::decode(s) {
                    match TraceId::try_from(hex_bytes.as_slice()) {
                        Ok(id) => Annotated::new(id),
                        Err(_) => {
                            let mut meta = Meta::default();
                            meta.add_remark(Remark::new(
                                RemarkType::Substituted,
                                "trace_id.invalid",
                            ));
                            Annotated(Some(TraceId::random()), meta)
                        }
                    }
                } else {
                    let mut meta = Meta::default();
                    meta.add_remark(Remark::new(RemarkType::Substituted, "trace_id.invalid"));
                    Annotated(Some(TraceId::random()), meta)
                }
            }
            _ => {
                let mut meta = Meta::default();
                meta.add_remark(Remark::new(RemarkType::Substituted, "trace_id.missing"));
                Annotated(Some(TraceId::random()), meta)
            }
        }
    };

    // Handle span ID - prefer the regular field over the dotted one
    let span_id = {
        let span_id_str = span_id.or(span_dot_id);
        match span_id_str.as_deref() {
            Some(s) if !s.is_empty() => {
                // Try to parse as hex string first, then as bytes
                if let Ok(hex_bytes) = hex::decode(s) {
                    SpanId::try_from(hex_bytes.as_slice())
                        .map_or_else(|err| Annotated::from_error(err, None), Annotated::new)
                } else {
                    Annotated::empty()
                }
            }
            _ => Annotated::empty(),
        }
    };

    // Build attributes from all the Vercel log fields
    let mut attributes = Attributes::default();
    attributes.insert("sentry.origin".to_owned(), "auto.vercel".to_owned());
    attributes.insert("vercel.id".to_owned(), id);
    attributes.insert("vercel.source".to_owned(), source);

    if let Some(deployment_id) = deployment_id {
        attributes.insert("vercel.deployment_id".to_owned(), deployment_id);
    }
    if let Some(host) = host {
        attributes.insert("vercel.host".to_owned(), host);
    }
    if let Some(project_id) = project_id {
        attributes.insert("vercel.project_id".to_owned(), project_id);
    }
    if let Some(project_name) = project_name {
        attributes.insert("vercel.project_name".to_owned(), project_name);
    }
    if let Some(build_id) = build_id {
        attributes.insert("vercel.build_id".to_owned(), build_id);
    }
    if let Some(log_type) = log_type {
        attributes.insert("vercel.type".to_owned(), log_type);
    }
    if let Some(entrypoint) = entrypoint {
        attributes.insert("vercel.entrypoint".to_owned(), entrypoint);
    }
    if let Some(request_id) = request_id {
        attributes.insert("vercel.request_id".to_owned(), request_id);
    }
    if let Some(status_code) = status_code {
        attributes.insert("vercel.status_code".to_owned(), status_code.to_string());
    }
    if let Some(path) = path {
        attributes.insert("vercel.path".to_owned(), path);
    }
    if let Some(execution_region) = execution_region {
        attributes.insert("vercel.execution_region".to_owned(), execution_region);
    }
    if let Some(environment) = environment {
        attributes.insert("sentry.environment".to_owned(), environment);
    }

    // Add proxy information if present
    if let Some(proxy) = proxy {
        // Required proxy fields when proxy object is present
        attributes.insert("vercel.proxy.method".to_owned(), proxy.method);
        attributes.insert("vercel.proxy.host".to_owned(), proxy.host);
        attributes.insert("vercel.proxy.path".to_owned(), proxy.path);
        attributes.insert("vercel.proxy.region".to_owned(), proxy.region);
        attributes.insert(
            "vercel.proxy.timestamp".to_owned(),
            proxy.timestamp.to_string(),
        );
        if let Some(client_ip) = proxy.client_ip {
            attributes.insert("vercel.proxy.client_ip".to_owned(), client_ip);
        }
        if let Some(scheme) = proxy.scheme {
            attributes.insert("vercel.proxy.scheme".to_owned(), scheme);
        }
        if let Some(cache_status) = proxy.vercel_cache {
            attributes.insert("vercel.proxy.cache_status".to_owned(), cache_status);
        }
        if let Some(path_type) = proxy.path_type {
            attributes.insert("vercel.proxy.path_type".to_owned(), path_type);
        }
        if let Some(user_agent) = proxy.user_agent
            && !user_agent.is_empty()
        {
            attributes.insert("vercel.proxy.user_agent".to_owned(), user_agent.join(", "));
        }
    }

    OurLog {
        timestamp: Annotated::new(Timestamp(datetime)),
        trace_id,
        span_id,
        level: Annotated::new(sentry_level),
        body: Annotated::new(message),
        attributes: Annotated::new(attributes),
        other: Object::default(),
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
            timestamp: 1573817187330, // Unix timestamp in milliseconds
            level: VercelLogLevel::Info,
            message: "API request processed".to_owned(),
            source: "lambda".to_owned(),
            deployment_id: Some("dpl_233NRGRjVZX1caZrXWtz5g1TAksD".to_owned()),
            host: Some("my-app-abc123.vercel.app".to_owned()),
            project_id: Some("gdufoJxB6b9b1fEqr1jUtFkyavUU".to_owned()),
            project_name: Some("my-app".to_owned()),
            build_id: None,
            log_type: None,
            entrypoint: Some("api/index.js".to_owned()),
            request_id: Some("643af4e3-975a-4cc7-9e7a-1eda11539d90".to_owned()),
            status_code: Some(200),
            path: Some("/api/users".to_owned()),
            execution_region: Some("sfo1".to_owned()),
            environment: Some("production".to_owned()),
            trace_id: Some("1b02cd14bb8642fd092bc23f54c7ffcd".to_owned()),
            span_id: Some("f24e8631bd11faa7".to_owned()),
            trace_dot_id: None,
            span_dot_id: None,
            proxy: Some(VercelProxy {
                timestamp: 1573817250172,
                method: "GET".to_owned(),
                host: "my-app.vercel.app".to_owned(),
                path: "/api/users?page=1".to_owned(),
                region: "sfo1".to_owned(),
                user_agent: Some(vec!["Mozilla/5.0...".to_owned()]),
                referer: Some("https://my-app.vercel.app".to_owned()),
                status_code: Some(200),
                client_ip: Some("120.75.16.101".to_owned()),
                scheme: Some("https".to_owned()),
                response_byte_size: None,
                cache_id: None,
                path_type: None,
                path_type_variant: None,
                vercel_id: None,
                vercel_cache: Some("MISS".to_owned()),
                lambda_region: None,
                waf_action: None,
                waf_rule_id: None,
            }),
        };

        let our_log: Annotated<OurLog> = Annotated::new(vercel_log_to_sentry_log(vercel_log));

        // Snapshot test for comprehensive validation
        insta::assert_json_snapshot!(SerializableAnnotated(&our_log), @r###"
        {
          "timestamp": 1573817187.33,
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
              "value": "auto.vercel"
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
            "vercel.host": {
              "type": "string",
              "value": "my-app-abc123.vercel.app"
            },
            "vercel.id": {
              "type": "string",
              "value": "test-log-123"
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
            "vercel.proxy.region": {
              "type": "string",
              "value": "sfo1"
            },
            "vercel.proxy.scheme": {
              "type": "string",
              "value": "https"
            },
            "vercel.proxy.timestamp": {
              "type": "string",
              "value": "1573817250172"
            },
            "vercel.proxy.user_agent": {
              "type": "string",
              "value": "Mozilla/5.0..."
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
              "type": "string",
              "value": "200"
            }
          }
        }
        "###);
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

        // Transform to Sentry log
        let our_log: Annotated<OurLog> = Annotated::new(vercel_log_to_sentry_log(vercel_log));

        // Snapshot test with real Vercel log example
        insta::assert_json_snapshot!(SerializableAnnotated(&our_log), @r###"
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
              "value": "auto.vercel"
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
            "vercel.host": {
              "type": "string",
              "value": "my-app-abc123.vercel.app"
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
            "vercel.proxy.cache_status": {
              "type": "string",
              "value": "MISS"
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
            "vercel.proxy.region": {
              "type": "string",
              "value": "sfo1"
            },
            "vercel.proxy.scheme": {
              "type": "string",
              "value": "https"
            },
            "vercel.proxy.timestamp": {
              "type": "string",
              "value": "1573817250172"
            },
            "vercel.proxy.user_agent": {
              "type": "string",
              "value": "Mozilla/5.0..."
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
              "type": "string",
              "value": "200"
            }
          }
        }
        "###);
    }
}
