//! Transforms Vercel logs to Sentry logs.
//!
//! This module provides functionality to convert Vercel log records
//! into Sentry's internal log format (`OurLog`).

use chrono::{TimeZone, Utc};
use relay_event_schema::protocol::{
    Attributes, OurLog, OurLogLevel, SpanId, Timestamp, TraceId, VercelLog, VercelLogLevel,
};
use relay_protocol::{Annotated, Object};

/// Maps Vercel log level to Sentry log level.
fn map_vercel_level_to_sentry(level: &VercelLogLevel) -> OurLogLevel {
    match level {
        VercelLogLevel::Info => OurLogLevel::Info,
        VercelLogLevel::Warning => OurLogLevel::Warn,
        VercelLogLevel::Error => OurLogLevel::Error,
        VercelLogLevel::Fatal => OurLogLevel::Fatal,
        VercelLogLevel::Unknown(_) => OurLogLevel::Info, // Default fallback
    }
}

/// Transforms a Vercel log to a Sentry log.
pub fn vercel_to_sentry_log(vercel_log: VercelLog) -> OurLog {
    let VercelLog {
        id: _,
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
        log_type,
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
        trace_dot_id,
        span_dot_id,
        proxy,
        other,
    } = vercel_log;

    // Convert timestamp from Unix timestamp in milliseconds to chrono DateTime
    let datetime = timestamp.value().map(|&ts| {
        Utc.timestamp_millis_opt(ts as i64)
            .single()
            .unwrap_or_else(Utc::now)
    });

    // Extract trace_id and span_id, preferring the dot notation versions if available
    let final_trace_id = if let Some(id) = trace_dot_id.value() {
        id.parse::<TraceId>()
            .ok()
            .map(Annotated::new)
            .unwrap_or(Annotated::empty())
    } else if let Some(id) = trace_id.value() {
        id.parse::<TraceId>()
            .ok()
            .map(Annotated::new)
            .unwrap_or(Annotated::empty())
    } else {
        Annotated::empty()
    };

    let final_span_id = if let Some(id) = span_dot_id.value() {
        id.parse::<SpanId>()
            .ok()
            .map(Annotated::new)
            .unwrap_or(Annotated::empty())
    } else if let Some(id) = span_id.value() {
        id.parse::<SpanId>()
            .ok()
            .map(Annotated::new)
            .unwrap_or(Annotated::empty())
    } else {
        Annotated::empty()
    };

    let sentry_level = level
        .value()
        .map(map_vercel_level_to_sentry)
        .unwrap_or(OurLogLevel::Info);

    let mut attribute_data = Attributes::default();

    // Add deployment information
    if let Some(deployment_id) = deployment_id.value() {
        attribute_data.insert("vercel.deployment_id".to_owned(), deployment_id.clone());
    }

    if let Some(source) = source.value() {
        attribute_data.insert("vercel.source".to_owned(), source.to_string());
    }

    if let Some(host) = host.value() {
        attribute_data.insert("vercel.host".to_owned(), host.clone());
    }

    if let Some(project_id) = project_id.value() {
        attribute_data.insert("vercel.project_id".to_owned(), project_id.clone());
    }

    // Add optional fields
    if let Some(build_id) = build_id.value() {
        attribute_data.insert("vercel.build_id".to_owned(), build_id.clone());
    }

    if let Some(entrypoint) = entrypoint.value() {
        attribute_data.insert("vercel.entrypoint".to_owned(), entrypoint.clone());
    }

    if let Some(destination) = destination.value() {
        attribute_data.insert("vercel.destination".to_owned(), destination.clone());
    }

    if let Some(path) = path.value() {
        attribute_data.insert("vercel.path".to_owned(), path.clone());
    }

    if let Some(log_type) = log_type.value() {
        attribute_data.insert("vercel.log_type".to_owned(), log_type.clone());
    }

    if let Some(status_code) = status_code.value() {
        attribute_data.insert("vercel.status_code".to_owned(), *status_code);
    }

    if let Some(request_id) = request_id.value() {
        attribute_data.insert("vercel.request_id".to_owned(), request_id.clone());
    }

    if let Some(environment) = environment.value() {
        attribute_data.insert("vercel.environment".to_owned(), environment.to_string());
    }

    if let Some(branch) = branch.value() {
        attribute_data.insert("vercel.branch".to_owned(), branch.clone());
    }

    if let Some(ja3_digest) = ja3_digest.value() {
        attribute_data.insert("vercel.ja3_digest".to_owned(), ja3_digest.clone());
    }

    if let Some(ja4_digest) = ja4_digest.value() {
        attribute_data.insert("vercel.ja4_digest".to_owned(), ja4_digest.clone());
    }

    if let Some(edge_type) = edge_type.value() {
        attribute_data.insert("vercel.edge_type".to_owned(), edge_type.to_string());
    }

    if let Some(project_name) = project_name.value() {
        attribute_data.insert("vercel.project_name".to_owned(), project_name.clone());
    }

    if let Some(execution_region) = execution_region.value() {
        attribute_data.insert(
            "vercel.execution_region".to_owned(),
            execution_region.clone(),
        );
    }

    // Add proxy information if present
    if let Some(proxy) = proxy.value() {
        if let Some(proxy_timestamp) = proxy.timestamp.value() {
            attribute_data.insert("vercel.proxy.timestamp".to_owned(), *proxy_timestamp as i64);
        }
        if let Some(method) = proxy.method.value() {
            attribute_data.insert("vercel.proxy.method".to_owned(), method.clone());
        }
        if let Some(proxy_host) = proxy.host.value() {
            attribute_data.insert("vercel.proxy.host".to_owned(), proxy_host.clone());
        }
        if let Some(proxy_path) = proxy.path.value() {
            attribute_data.insert("vercel.proxy.path".to_owned(), proxy_path.clone());
        }
        if let Some(user_agent) = proxy.user_agent.value() {
            let user_agent_strings: Vec<String> = user_agent
                .iter()
                .filter_map(|ua| ua.value().cloned())
                .collect();
            if let Ok(user_agent_json) = serde_json::to_string(&user_agent_strings) {
                attribute_data.insert("vercel.proxy.user_agent".to_owned(), user_agent_json);
            }
        }
        if let Some(referer) = proxy.referer.value() {
            attribute_data.insert("vercel.proxy.referer".to_owned(), referer.clone());
        }
        if let Some(region) = proxy.region.value() {
            attribute_data.insert("vercel.proxy.region".to_owned(), region.clone());
        }
        if let Some(proxy_status_code) = proxy.status_code.value() {
            attribute_data.insert("vercel.proxy.status_code".to_owned(), *proxy_status_code);
        }
        if let Some(client_ip) = proxy.client_ip.value() {
            attribute_data.insert("vercel.proxy.client_ip".to_owned(), client_ip.clone());
        }
        if let Some(scheme) = proxy.scheme.value() {
            attribute_data.insert("vercel.proxy.scheme".to_owned(), scheme.clone());
        }
        if let Some(response_byte_size) = proxy.response_byte_size.value() {
            attribute_data.insert(
                "vercel.proxy.response_byte_size".to_owned(),
                *response_byte_size as i64,
            );
        }
        if let Some(cache_id) = proxy.cache_id.value() {
            attribute_data.insert("vercel.proxy.cache_id".to_owned(), cache_id.clone());
        }
        if let Some(path_type) = proxy.path_type.value() {
            attribute_data.insert("vercel.proxy.path_type".to_owned(), path_type.to_string());
        }
        if let Some(path_type_variant) = proxy.path_type_variant.value() {
            attribute_data.insert(
                "vercel.proxy.path_type_variant".to_owned(),
                path_type_variant.clone(),
            );
        }
        if let Some(vercel_id) = proxy.vercel_id.value() {
            attribute_data.insert("vercel.proxy.vercel_id".to_owned(), vercel_id.clone());
        }
        if let Some(vercel_cache) = proxy.vercel_cache.value() {
            attribute_data.insert(
                "vercel.proxy.vercel_cache".to_owned(),
                vercel_cache.to_string(),
            );
        }
        if let Some(lambda_region) = proxy.lambda_region.value() {
            attribute_data.insert(
                "vercel.proxy.lambda_region".to_owned(),
                lambda_region.clone(),
            );
        }
        if let Some(waf_action) = proxy.waf_action.value() {
            attribute_data.insert("vercel.proxy.waf_action".to_owned(), waf_action.to_string());
        }
        if let Some(waf_rule_id) = proxy.waf_rule_id.value() {
            attribute_data.insert("vercel.proxy.waf_rule_id".to_owned(), waf_rule_id.clone());
        }

        // Add any additional proxy fields from the 'other' object
        for (key, value) in &proxy.other {
            if let Some(value_str) = value.as_str() {
                attribute_data.insert(format!("vercel.proxy.{}", key), value_str.to_owned());
            }
        }
    }

    // Add any additional fields from the 'other' object at the top level
    for (key, value) in &other {
        if let Some(value_str) = value.as_str() {
            attribute_data.insert(format!("vercel.{}", key), value_str.to_owned());
        }
    }

    OurLog {
        timestamp: datetime.map(Timestamp).into(),
        trace_id: final_trace_id,
        span_id: final_span_id,
        level: Annotated::new(sentry_level),
        body: message,
        attributes: Annotated::new(attribute_data),
        other: Object::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::{FromValue, SerializableAnnotated, Value, get_path};

    #[test]
    fn test_vercel_to_sentry_log_basic() {
        let json = r#"{
            "id": "log123",
            "deployment_id": "dpl_abc123",
            "source": "lambda",
            "host": "example-app.vercel.app",
            "timestamp": 1544712660300,
            "project_id": "prj_abc123",
            "level": "info",
            "message": "Request processed successfully",
            "request_id": "req_abc123",
            "environment": "production",
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174"
        }"#;

        let value: Value = serde_json::from_str(json).unwrap();
        let vercel_log = VercelLog::from_value(Annotated::new(value))
            .into_value()
            .unwrap();
        let our_log = vercel_to_sentry_log(vercel_log);
        let annotated_log: Annotated<OurLog> = Annotated::new(our_log);

        assert_eq!(
            get_path!(annotated_log.body),
            Some(&Annotated::new("Request processed successfully".into()))
        );

        assert_eq!(
            annotated_log.value().unwrap().level.value().unwrap(),
            &OurLogLevel::Info
        );

        // Snapshot test for comprehensive validation
        insta::assert_json_snapshot!(SerializableAnnotated(&annotated_log), @r#"
        {
          "timestamp": 1544712660.3,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "Request processed successfully",
          "attributes": {
            "vercel.deployment_id": {
              "type": "string",
              "value": "dpl_abc123"
            },
            "vercel.environment": {
              "type": "string",
              "value": "production"
            },
            "vercel.host": {
              "type": "string",
              "value": "example-app.vercel.app"
            },
            "vercel.project_id": {
              "type": "string",
              "value": "prj_abc123"
            },
            "vercel.request_id": {
              "type": "string",
              "value": "req_abc123"
            },
            "vercel.source": {
              "type": "string",
              "value": "lambda"
            }
          }
        }
        "#);
    }

    #[test]
    fn test_vercel_to_sentry_log_with_proxy() {
        let json = r#"{
            "id": "log456",
            "deployment_id": "dpl_def456",
            "source": "edge",
            "host": "example-app.vercel.app",
            "timestamp": 1544712660300,
            "project_id": "prj_def456",
            "level": "warning",
            "message": "High response time detected",
            "proxy": {
                "timestamp": 1544712660000,
                "method": "GET",
                "host": "example-app.vercel.app",
                "path": "/api/users",
                "user_agent": ["Mozilla/5.0"],
                "referer": "https://example.com",
                "region": "iad1",
                "status_code": 200,
                "client_ip": "192.168.1.1",
                "scheme": "https",
                "response_byte_size": 1024
            }
        }"#;

        let value: Value = serde_json::from_str(json).unwrap();
        let vercel_log = VercelLog::from_value(Annotated::new(value))
            .into_value()
            .unwrap();
        let our_log = vercel_to_sentry_log(vercel_log);
        let annotated_log: Annotated<OurLog> = Annotated::new(our_log);

        assert_eq!(
            get_path!(annotated_log.body),
            Some(&Annotated::new("High response time detected".into()))
        );

        assert_eq!(
            annotated_log.value().unwrap().level.value().unwrap(),
            &OurLogLevel::Warn
        );

        // Check that proxy attributes are properly set
        let attributes = annotated_log.value().unwrap().attributes.value().unwrap();
        assert!(attributes.get_value("vercel.proxy.method").is_some());
        assert!(attributes.get_value("vercel.proxy.path").is_some());
        assert!(attributes.get_value("vercel.proxy.status_code").is_some());
    }

    #[test]
    fn test_vercel_level_mapping() {
        let test_cases = vec![
            ("info", OurLogLevel::Info),
            ("warning", OurLogLevel::Warn),
            ("error", OurLogLevel::Error),
            ("fatal", OurLogLevel::Fatal),
        ];

        for (vercel_level_str, expected_sentry_level) in test_cases {
            let json = format!(
                r#"{{
                "id": "test",
                "deployment_id": "dpl_test",
                "source": "lambda",
                "host": "test.vercel.app",
                "timestamp": 1544712660300,
                "project_id": "prj_test",
                "level": "{}",
                "message": "Test message"
            }}"#,
                vercel_level_str
            );

            let value: Value = serde_json::from_str(&json).unwrap();
            let vercel_log = VercelLog::from_value(Annotated::new(value))
                .into_value()
                .unwrap();
            let our_log = vercel_to_sentry_log(vercel_log);

            assert_eq!(
                our_log.level.value().unwrap(),
                &expected_sentry_level,
                "Vercel level '{}' should map to {:?}",
                vercel_level_str,
                expected_sentry_level
            );
        }
    }
}
