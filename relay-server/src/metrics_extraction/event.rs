use std::collections::BTreeMap;

use relay_common::time::UnixTimestamp;
use relay_event_schema::protocol::{Event, Span};
use relay_metrics::{Bucket, BucketMetadata, BucketValue};

use crate::metrics_extraction::ExtractedMetrics;
use crate::processing::transactions::extraction::extract_segment_span;
use crate::statsd::RelayTimers;

/// Configuration for [`extract_metrics`].
#[derive(Debug, Copy, Clone)]
pub struct ExtractMetricsConfig {
    pub max_tag_value_size: usize,
    pub extract_spans: bool,
}

/// Extracts usage metrics from a transaction and its spans.
///
/// Spans with missing or invalid end timestamps are skipped. Timestamp and clock drift correction
/// should occur before metrics extraction to ensure valid timestamps.
pub fn extract_metrics(event: &mut Event, config: ExtractMetricsConfig) -> ExtractedMetrics {
    let mut metrics = ExtractedMetrics::default();

    if config.extract_spans {
        extract_span_metrics_for_event(event, config, &mut metrics);
    }

    metrics
}

fn extract_span_metrics_for_event(
    event: &mut Event,
    config: ExtractMetricsConfig,
    output: &mut ExtractedMetrics,
) {
    relay_statsd::metric!(timer(RelayTimers::EventProcessingSpanMetricsExtraction), {
        if let Some(transaction_span) = extract_segment_span(event, config.max_tag_value_size, &[])
        {
            output.0.extend(extract_span_usage(&transaction_span));
        }

        if let Some(spans) = event.spans.value_mut() {
            for annotated_span in spans {
                if let Some(span) = annotated_span.value_mut() {
                    output.0.extend(extract_span_usage(span));
                }
            }
        }
    });
}

/// Creates the usage counter for a span, preserving its end timestamp and segment tags.
fn extract_span_usage(span: &Span) -> Option<Bucket> {
    let Some(timestamp) = span
        .timestamp
        .value()
        .and_then(|ts| UnixTimestamp::from_datetime(ts.0))
    else {
        relay_log::error!("invalid span timestamp for metric extraction");
        return None;
    };

    let is_segment = span.is_segment.value() == Some(&true);
    let mut tags = BTreeMap::from([("is_segment".to_owned(), is_segment.to_string())]);
    if is_segment && span.was_transaction.value() == Some(&true) {
        tags.insert("was_transaction".to_owned(), "true".to_owned());
    }

    // The received time is the moment of extraction, rather than the span's timestamp.
    let received_at = if cfg!(not(test)) {
        UnixTimestamp::now()
    } else {
        UnixTimestamp::from_secs(0)
    };

    Some(Bucket {
        name: "c:spans/usage@none".into(),
        width: 0,
        value: BucketValue::counter(1.into()),
        timestamp,
        tags,
        metadata: BucketMetadata::new(received_at),
    })
}

#[cfg(test)]
mod tests {
    use relay_event_normalization::{NormalizationConfig, normalize_event};
    use relay_event_schema::protocol::Timestamp;
    use relay_protocol::Annotated;

    use super::*;

    #[test]
    fn span_usage_tags() {
        for is_segment in [None, Some(false), Some(true)] {
            for was_transaction in [None, Some(false), Some(true)] {
                let span = Span {
                    timestamp: Timestamp(UnixTimestamp::from_secs(123).as_datetime().unwrap())
                        .into(),
                    is_segment: is_segment.into(),
                    was_transaction: was_transaction.into(),
                    ..Default::default()
                };
                let metric = extract_span_usage(&span).unwrap();

                assert_eq!(&metric.name, "c:spans/usage@none");
                assert_eq!(metric.value, BucketValue::counter(1.into()));
                assert_eq!(metric.timestamp, UnixTimestamp::from_secs(123));
                assert_eq!(metric.width, 0);
                assert_eq!(
                    metric.metadata,
                    BucketMetadata::new(UnixTimestamp::from_secs(0))
                );
                assert_eq!(
                    metric.tags["is_segment"],
                    (is_segment == Some(true)).to_string()
                );
                if is_segment == Some(true) && was_transaction == Some(true) {
                    assert_eq!(metric.tags.len(), 2);
                    assert_eq!(metric.tags["was_transaction"], "true");
                } else {
                    assert_eq!(metric.tags.len(), 1);
                }
            }
        }
    }

    #[test]
    fn span_usage_requires_valid_timestamp() {
        for json in [
            r#"{}"#,
            r#"{"timestamp": -1}"#,
            r#"{"timestamp": "invalid"}"#,
        ] {
            let span = Annotated::<Span>::from_json(json).unwrap();
            assert!(extract_span_usage(span.value().unwrap()).is_none());
        }
    }

    #[test]
    fn disabled_span_extraction() {
        let mut event = Event {
            timestamp: Timestamp(UnixTimestamp::from_secs(123).as_datetime().unwrap()).into(),
            ..Default::default()
        };
        let metrics = extract_metrics(
            &mut event,
            ExtractMetricsConfig {
                max_tag_value_size: 200,
                extract_spans: false,
            },
        );
        assert!(metrics.0.is_empty());
    }

    fn extract_span_metrics() -> ExtractedMetrics {
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
                    "country_code": "FR"
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
                },
                "browser": {
                    "name": "Chrome",
                    "version": "120.1.1",
                    "type": "browser"
                }
            },
            "request": {
                "method": "POST"
            },
            "spans": [
                {
                    "description": "<SomeUiRendering>",
                    "op": "UI.React.Render",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81"
                },
                {
                    "description": "GET http://domain.tld/hi",
                    "op": "http.client",
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "http.method": "GET"
                    }
                },
                {
                    "description": "GET /hi/this/is/just/the/path",
                    "op": "http.client",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "http.method": "GET",
                        "status_code": "500"
                    }
                },
                {
                    "description": "POST http://127.0.0.1:1234/api/hi",
                    "op": "http.client",
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "postgresql",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "select column FROM table WHERE id IN (1, 2, 3)",
                    "op": "db",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
                },
                {
                    "description": "INSERT INTO table (col) VALUES (val)",
                    "op": "db.sql.query",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "postgresql",
                        "db.operation": "INSERT"
                    }
                },
                {
                    "description": "INSERT INTO from_date (col) VALUES (val)",
                    "op": "db.sql.query",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "postgresql",
                        "db.operation": "INSERT"
                    }
                },
                {
                    "description": "INSERT INTO table (col) VALUES (val)",
                    "op": "db",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
                },
                {
                    "description": "SELECT\n*\nFROM\ntable\nWHERE\nid\nIN\n(val)",
                    "op": "db.sql.query",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "postgresql",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s",
                    "op": "db",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "postgresql",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "DELETE FROM table WHERE conditions",
                    "op": "db",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "MyDatabase"
                    }
                },
                {
                    "description": "UPDATE table WHERE conditions",
                    "op": "db",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "MyDatabase"
                    }
                },
                {
                    "description": "SAVEPOINT save_this_one",
                    "op": "db",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "MyDatabase"
                    }
                },
                {
                    "description": "GET cache:user:{123}",
                    "op": "cache.get_item",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "cache.hit": false,
                        "cache.item_size": 8
                    }
                },
                {
                    "description": "GET cache:user:{123}",
                    "op": "cache.get",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "cache.hit": false,
                        "cache.item_size": 8
                    }
                },
                {
                    "description": "GET test:123:def",
                    "op": "db.redis",
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
                },
                {
                    "description": "things.count({\"$and\":[{\"services\":{\"$exists\":true}},{\"test_id\":38}]})",
                    "op": "db.sql.query",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "mongodb",
                        "db.operation": "count"
                    }
                },
                {
                    "description": "DELETE FROM table WHERE conditions",
                    "op": "db.sql.query",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "MyDatabase"
                    }
                },
                {
                    "description": "things.count({\"$and\":[{\"services\":{\"$exists\":true}},{\"test_id\":38}]})",
                    "op": "db.mongodb.find",
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "MyDatabase"
                    }
                },
                {
                    "description": "SAVEPOINT save_this_one",
                    "op": "db.redis.command",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "redis"
                    }
                },
                {
                    "data": {
                        "device.class": "2",
                        "environment": "production",
                        "http.response.status_code": "200",
                        "mobile": true,
                        "release": "sentrydemos.ios.EmpowerPlant@0.0.8+1",
                        "span.op": "ui.load",
                        "span.status": "ok",
                        "transaction": "EmpowerPlantViewController",
                        "transaction.op": "ui.load",
                        "user": "id:FADC011D-28AA-40B7-8CA8-839A2AD05168"
                    },
                    "description": "viewDidLoad",
                    "exclusive_time": 15101.696732,
                    "op": "ui.load",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "8cfaf7f29ac345b8",
                    "start_timestamp": 1695255136.239635,
                    "status": "ok",
                    "timestamp": 1695255152.073167,
                    "trace_id": "2dc90ee797b94299ba5ad82b816fc9f8"
                },
                {
                    "data": {
                        "device.class": "2",
                        "environment": "production",
                        "http.response.status_code": "200",
                        "mobile": true,
                        "release": "sentrydemos.ios.EmpowerPlant@0.0.8+1",
                        "span.op": "app.start.cold",
                        "span.status": "ok",
                        "transaction": "EmpowerPlantViewController",
                        "transaction.op": "ui.load",
                        "user": "id:FADC011D-28AA-40B7-8CA8-839A2AD05168"
                    },
                    "description": "Cold Start",
                    "exclusive_time": 0.0,
                    "op": "app.start.cold",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "0e989cd370034c76",
                    "start_timestamp": 1695255134.469436,
                    "timestamp": 1695255136.137952,
                    "trace_id": "2dc90ee797b94299ba5ad82b816fc9f8"
                },
                {
                    "timestamp": 1694732408.3145,
                    "start_timestamp": 1694732407.8367,
                    "exclusive_time": 477.800131,
                    "description": "https://cdn.domain.com/path/to/file-hk2YHeW7Eo2XLCiE38F1Fz22KuljsgCAD6hyWCyOYZM.css",
                    "op": "resource.css",
                    "span_id": "97c0ef9770a02f9d",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "trace_id": "77aeb1c16bb544a4a39b8d42944947a3",
                    "data": {
                        "http.decoded_response_content_length": 128950,
                        "http.response_content_length": 36170,
                        "http.response_transfer_size": 36470,
                        "resource.render_blocking_status": "blocking"
                    },
                    "hash": "e2fae740cccd3789"
                },
                {
                    "timestamp": 1694732408.3145,
                    "start_timestamp": 1694732407.8367,
                    "exclusive_time": 477.800131,
                    "description": "/static/myscript-v1.9.23.js",
                    "op": "resource.script",
                    "span_id": "97c0ef9770a02f9d",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "trace_id": "77aeb1c16bb544a4a39b8d42944947a3",
                    "data": {
                        "http.decoded_response_content_length": 128950,
                        "http.response_content_length": 36170,
                        "http.response_transfer_size": 36470,
                        "resource.render_blocking_status": "blocking",
                        "server.address": "subdomain.example.com:5688",
                        "url.same_origin": true,
                        "url.scheme": "https"
                    },
                    "hash": "e2fae740cccd3789"
                },
                {
                    "description": "<SomeUiRendering>",
                    "op": "UI.React.Render",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81"
                },
                {
                    "description": "GET http://domain.tld/hi",
                    "op": "http.client",
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "postgresql",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "select column FROM table WHERE id IN (1, 2, 3)",
                    "op": "db",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
                },
                {
                    "description": "INSERT INTO table (col) VALUES (val)",
                    "op": "db.sql.query",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "postgresql",
                        "db.operation": "INSERT"
                    }
                },
                {
                    "description": "INSERT INTO from_date (col) VALUES (val)",
                    "op": "db.sql.query",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "postgresql",
                        "db.operation": "INSERT"
                    }
                },
                {
                    "description": "INSERT INTO table (col) VALUES (val)",
                    "op": "db",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
                },
                {
                    "description": "SELECT\n*\nFROM\ntable\nWHERE\nid\nIN\n(val)",
                    "op": "db.sql.query",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "postgresql",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "SELECT \"table\".\"col\" FROM \"table\" WHERE \"table\".\"col\" = %s",
                    "op": "db",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "postgresql",
                        "db.operation": "SELECT"
                    }
                },
                {
                    "description": "DELETE FROM table WHERE conditions",
                    "op": "db",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "MyDatabase"
                    }
                },
                {
                    "description": "UPDATE table WHERE conditions",
                    "op": "db",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "MyDatabase"
                    }
                },
                {
                    "description": "SAVEPOINT save_this_one",
                    "op": "db",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "db.system.name": "MyDatabase"
                    }
                },
                {
                    "description": "GET cache:user:{123}",
                    "op": "cache.get_item",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "cache.hit": false,
                        "cache.item_size": 10
                    }
                },
                {
                    "description": "GET cache:user:{456}",
                    "op": "cache.get_item",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {
                        "cache.hit": true
                    }
                },
                {
                    "description": "GET test:123:def",
                    "op": "db.redis",
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
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
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok",
                    "data": {}
                },
                {
                    "description": "chrome-extension://begnopegbbhjeeiganiajffnalhlkkjb/img/assets/icon-10k.svg",
                    "op": "resource.script",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
                },
                {
                    "description": "http://domain/static/myscript-v1.9.23.js",
                    "op": "resource.script",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
                },
                {
                    "timestamp": 1694732408.3145,
                    "start_timestamp": 1694732407.8367,
                    "exclusive_time": 477.800131,
                    "description": "https://cdn.domain.com/path/to/file-hk2YHeW7Eo2XLCiE38F1Fz22KuljsgCAD6hyWCyOYZM.CSS",
                    "op": "resource.css",
                    "span_id": "97c0ef9770a02f9d",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "trace_id": "77aeb1c16bb544a4a39b8d42944947a3",
                    "data": {
                        "http.decoded_response_content_length": 128950,
                        "http.response_content_length": 36170,
                        "http.response_transfer_size": 36470,
                        "resource.render_blocking_status": "blocking"
                    },
                    "hash": "e2fae740cccd3789"
                },
                {
                    "timestamp": 1694732408.3145,
                    "start_timestamp": 1694732407.8367,
                    "span_id": "97c0ef9770a02f9d",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "trace_id": "77aeb1c16bb544a4a39b8d42944947a3",
                    "op": "resource.script",
                    "description": "domain.com/zero-length-00",
                    "data": {
                        "http.decoded_response_content_length": 0,
                        "http.response_content_length": 0,
                        "http.response_transfer_size": 0
                    }
                },
                {
                    "timestamp": 1702474613.0495,
                    "start_timestamp": 1702474613.0175,
                    "description": "input.app-123.adfasf456[type=\"range\"][name=\"replay-timeline\"]",
                    "op": "ui.interaction.click",
                    "span_id": "9b01bd820a083e63",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
                    "data": {
                        "ui.component_name": "my-component-name"
                    }
                },
                {
                    "timestamp": 1702474613.0495,
                    "start_timestamp": 1702474613.0175,
                    "description": "sentry.tasks.post_process.post_process_group",
                    "op": "queue.task.celery",
                    "span_id": "9b01bd820a083e63",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
                    "data": {
                        "messaging.destination.name": "default",
                        "messaging.message.receive.latency": 100,
                        "messaging.message.retry.count": 2,
                        "messaging.message.body.size": 1000,
                        "messaging.message.id": "abc123",
                        "messaging.operation.name": "publish",
                        "messaging.operation.type": "create"
                    }
                },
                {
                    "timestamp": 1702474613.0495,
                    "start_timestamp": 1702474613.0175,
                    "description": "sentry.tasks.post_process.post_process_group",
                    "op": "queue.submit.celery",
                    "span_id": "9b01bd820a083e63",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
                    "data": {
                        "messaging.destination.name": "default",
                        "messaging.message.receive.latency": 100,
                        "messaging.message.retry.count": 2,
                        "messaging.message.body.size": 1000,
                        "messaging.message.id": "abc123",
                        "messaging.operation.name": "publish",
                        "messaging.operation.type": "create"
                    }
                },
                {
                    "timestamp": 1702474613.0495,
                    "start_timestamp": 1702474613.0175,
                    "description": "sentry.tasks.post_process.post_process_group",
                    "op": "queue.publish",
                    "span_id": "9b01bd820a083e63",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
                    "data": {
                        "messaging.destination.name": "default",
                        "messaging.message.receive.latency": 100,
                        "messaging.message.retry.count": 2,
                        "messaging.message.body.size": 1000,
                        "messaging.message.id": "abc123",
                        "messaging.operation.name": "publish",
                        "messaging.operation.type": "create"
                    }
                },
                {
                    "timestamp": 1702474613.0495,
                    "start_timestamp": 1702474613.0175,
                    "description": "sentry.tasks.post_process.post_process_group",
                    "op": "queue.process",
                    "span_id": "9b01bd820a083e63",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
                    "data": {
                        "messaging.destination.name": "default",
                        "messaging.message.receive.latency": 100,
                        "messaging.message.retry.count": 2,
                        "messaging.message.body.size": 1000,
                        "messaging.message.id": "abc123",
                        "messaging.operation.name": "publish",
                        "messaging.operation.type": "create"
                    }
                },
                {
                    "timestamp": 1702474613.0495,
                    "start_timestamp": 1702474613.0175,
                    "description": "ConcurrentStream",
                    "op": "ai.run.langchain",
                    "origin": "auto.langchain",
                    "span_id": "9c01bd820a083e63",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
                    "data": {
                        "gen_ai.usage.total_tokens": 20
                    }
                },
                {
                    "timestamp": 1702474613.0495,
                    "start_timestamp": 1702474613.0175,
                    "description": "Autofix Pipeline",
                    "op": "ai.pipeline",
                    "origin": "auto.langchain",
                    "span_id": "9c01bd820a083e63",
                    "parent_span_id": "bd429c44b67a3eb4",
                    "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
                    "data": {
                        "gen_ai.usage.total_tokens": 30
                    }
                }
            ]
        }
        "#;

        let mut event = Annotated::from_json(json).unwrap();

        // Normalize first, to make sure that all things are correct as in the real pipeline:
        normalize_event(
            &mut event,
            &NormalizationConfig {
                enrich_spans: true,
                ..Default::default()
            },
        );

        extract_metrics(
            event.value_mut().as_mut().unwrap(),
            ExtractMetricsConfig {
                max_tag_value_size: 200,
                extract_spans: true,
            },
        )
    }

    #[test]
    fn span_usage() {
        let metrics = extract_span_metrics();

        assert_eq!(metrics.0.len(), 75);
        assert_eq!(metrics.0[0].tags["is_segment"], "true");
        assert_eq!(metrics.0[0].tags["was_transaction"], "true");
        assert!(
            metrics.0[1..]
                .iter()
                .all(|metric| { metric.tags.len() == 1 && metric.tags["is_segment"] == "false" })
        );
        assert!(
            metrics
                .0
                .into_iter()
                .all(|x| &x.name == "c:spans/usage@none")
        );
    }
}
