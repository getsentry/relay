use relay_common::time::UnixTimestamp;
use relay_dynamic_config::MetricExtractionConfig;
use relay_event_schema::protocol::{Event, Span};
use relay_metrics::Bucket;
use relay_quotas::DataCategory;

use crate::metrics_extraction::generic::{self, Extractable};
use crate::statsd::RelayTimers;

impl Extractable for Event {
    fn category(&self) -> DataCategory {
        // Obtain the event's data category, but treat default events as error events for the
        // purpose of metric tagging.
        match DataCategory::from(self.ty.value().copied().unwrap_or_default()) {
            DataCategory::Default => DataCategory::Error,
            category => category,
        }
    }

    fn timestamp(&self) -> Option<UnixTimestamp> {
        self.timestamp
            .value()
            .and_then(|ts| UnixTimestamp::from_datetime(ts.0))
    }
}

impl Extractable for Span {
    fn category(&self) -> DataCategory {
        DataCategory::Span
    }

    fn timestamp(&self) -> Option<UnixTimestamp> {
        self.timestamp
            .value()
            .and_then(|ts| UnixTimestamp::from_datetime(ts.0))
    }
}

/// Extracts metrics from an [`Event`].
///
/// The event must have a valid timestamp; if the timestamp is missing or invalid, no metrics are
/// extracted. Timestamp and clock drift correction should occur before metrics extraction to ensure
/// valid timestamps.
///
/// If this is a transaction event with spans, metrics will also be extracted from the spans.
pub fn extract_metrics(event: &Event, config: &MetricExtractionConfig) -> Vec<Bucket> {
    let mut metrics = generic::extract_metrics(event, config);

    relay_statsd::metric!(timer(RelayTimers::EventProcessingSpanMetricsExtraction), {
        if let Some(spans) = event.spans.value() {
            for annotated_span in spans {
                if let Some(span) = annotated_span.value() {
                    metrics.extend(generic::extract_metrics(span, config));
                }
            }
        }
    });

    metrics
}

#[cfg(test)]
mod tests {
    use relay_dynamic_config::{Feature, FeatureSet, ProjectConfig};
    use relay_event_normalization::LightNormalizationConfig;
    use relay_protocol::Annotated;
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn test_extract_span_metrics_all_modules() {
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
            "tags": {
                "http.status_code": 500
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
                    "description": "GET http://domain.tld/hi",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "http.method": "GET"
                    }
                },
                {
                    "description": "POST http://domain.tld/hi",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "http.request.method": "POST"
                    }
                },
                {
                    "description": "PUT http://domain.tld/hi",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "method": "PUT"
                    }
                },
                {
                    "description": "GET /hi/this/is/just/the/path",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "http.method": "GET"
                    }
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
                    "description": "POST http://sth.subdomain.domain.tld:1234/api/hi",
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
                    "description": "POST http://targetdomain.tld:1234/api/hi",
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
                    "description": "POST http://targetdomain:1234/api/id/0987654321",
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
                    "description": "POST http://sth.subdomain.domain.tld:1234/api/hi",
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
                    "description": "POST http://sth.subdomain.domain.tld:1234/api/hi",
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
                        "db.system": "postgresql",
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
                        "db.system": "postgresql",
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
                        "db.system": "postgresql",
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
                        "db.system": "postgresql",
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
                        "db.system": "postgresql",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "DELETE FROM table WHERE conditions",
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
                    "description": "UPDATE table WHERE conditions",
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
                    "description": "GET test:123:def",
                    "op": "db.redis",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {}
                },
                {
                    "description": "GET lkjasdlkasjdlasjdlkasjdlkasjd",
                    "op": "db.redis",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {}
                },
                {
                    "description": "SET 'aaa:bbb:123:zzz' '{\"from json\": \"no\"}'",
                    "op": "db.redis",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {}
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
        let features = FeatureSet(BTreeSet::from([
            Feature::SpanMetricsExtraction,
            Feature::SpanMetricsExtractionAllModules,
        ]));

        // Normalize first, to make sure that all things are correct as in the real pipeline:
        relay_event_normalization::light_normalize_event(
            &mut event,
            LightNormalizationConfig {
                enrich_spans: true,
                light_normalize_spans: true,
                ..Default::default()
            },
        )
        .unwrap();

        // Create a project config with the relevant feature flag. Sanitize to fill defaults.
        let mut project = ProjectConfig {
            features,
            ..ProjectConfig::default()
        };
        project.sanitize();

        let config = project.metric_extraction.ok().unwrap();
        let metrics = extract_metrics(event.value().unwrap(), &config);
        insta::assert_debug_snapshot!(metrics);
    }

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
            "tags": {
                "http.status_code": 500
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
                    "description": "GET http://domain.tld/hi",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "http.method": "GET"
                    }
                },
                {
                    "description": "POST http://domain.tld/hi",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "http.request.method": "POST"
                    }
                },
                {
                    "description": "PUT http://domain.tld/hi",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "method": "PUT"
                    }
                },
                {
                    "description": "GET /hi/this/is/just/the/path",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "http.method": "GET"
                    }
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
                    "description": "POST http://sth.subdomain.domain.tld:1234/api/hi",
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
                    "description": "POST http://targetdomain.tld:1234/api/hi",
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
                    "description": "POST http://targetdomain:1234/api/id/0987654321",
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
                    "description": "POST http://sth.subdomain.domain.tld:1234/api/hi",
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
                    "description": "POST http://sth.subdomain.domain.tld:1234/api/hi",
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
                        "db.system": "postgresql",
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
                        "db.system": "postgresql",
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
                        "db.system": "postgresql",
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
                        "db.system": "postgresql",
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
                        "db.system": "postgresql",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "DELETE FROM table WHERE conditions",
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
                    "description": "UPDATE table WHERE conditions",
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
                    "description": "GET test:123:def",
                    "op": "db.redis",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {}
                },
                {
                    "description": "GET lkjasdlkasjdlasjdlkasjdlkasjd",
                    "op": "db.redis",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {}
                },
                {
                    "description": "SET 'aaa:bbb:123:zzz' '{\"from json\": \"no\"}'",
                    "op": "db.redis",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {}
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
                },
                {
                    "description": "things.count({\"$and\":[{\"services\":{\"$exists\":true}},{\"test_id\":38}]})",
                    "op": "db.sql.query",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "mongodb",
                        "db.operation": "count"
                    }
                },
                {
                    "description": "DELETE FROM table WHERE conditions",
                    "op": "db.sql.query",
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
                    "description": "things.count({\"$and\":[{\"services\":{\"$exists\":true}},{\"test_id\":38}]})",
                    "op": "db.mongodb.find",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {}
                },
                {
                    "description": "DELETE FROM table WHERE conditions",
                    "op": "db.sql.activerecord",
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
                    "description": "SAVEPOINT save_this_one",
                    "op": "db.redis.command",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system": "redis"
                    }
                }
            ]
        }
        "#;

        let mut event = Annotated::from_json(json).unwrap();
        let features = FeatureSet(BTreeSet::from([Feature::SpanMetricsExtraction]));

        // Normalize first, to make sure that all things are correct as in the real pipeline:
        relay_event_normalization::light_normalize_event(
            &mut event,
            LightNormalizationConfig {
                enrich_spans: true,
                light_normalize_spans: true,
                ..Default::default()
            },
        )
        .unwrap();

        // Create a project config with the relevant feature flag. Sanitize to fill defaults.
        let mut project = ProjectConfig {
            features,
            ..ProjectConfig::default()
        };
        project.sanitize();

        let config = project.metric_extraction.ok().unwrap();
        let metrics = extract_metrics(event.value().unwrap(), &config);
        insta::assert_debug_snapshot!(metrics);
    }

    #[test]
    fn test_extract_span_metrics_mobile() {
        let json = r#"
        {
            "type": "transaction",
            "sdk": {"name": "sentry.cocoa"},
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "timestamp": "2021-04-26T08:00:00+0100",
            "release": "1.2.3",
            "transaction": "gEt /api/:version/users/",
            "transaction_info": {"source": "custom"},
            "contexts": {
                "trace": {
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "span_id": "bd429c44b67a3eb4"
                },
                "device": {
                    "family": "iOS",
                    "model": "iPhone1,1"
                }
            },
            "spans": [
                {
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81"
                }
            ]
        }
        "#;

        let mut event = Annotated::from_json(json).unwrap();

        // Normalize first, to make sure that all things are correct as in the real pipeline:
        relay_event_normalization::light_normalize_event(
            &mut event,
            LightNormalizationConfig {
                enrich_spans: true,
                light_normalize_spans: true,
                device_class_synthesis_config: true,
                ..Default::default()
            },
        )
        .unwrap();

        // Create a project config with the relevant feature flag. Sanitize to fill defaults.
        let mut project = ProjectConfig {
            features: [
                Feature::SpanMetricsExtraction,
                Feature::SpanMetricsExtractionAllModules,
            ]
            .into_iter()
            .collect(),
            ..ProjectConfig::default()
        };
        project.sanitize();

        let config = project.metric_extraction.ok().unwrap();
        let metrics = extract_metrics(event.value().unwrap(), &config);
        insta::assert_debug_snapshot!((&event.value().unwrap().spans, metrics));
    }
}
