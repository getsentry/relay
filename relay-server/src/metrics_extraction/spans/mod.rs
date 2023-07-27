use std::collections::BTreeMap;
use std::str::FromStr;

use relay_common::{EventType, UnixTimestamp};
use relay_general::protocol::Event;
use relay_general::store::span::tag_extraction::SpanTagKey;
use relay_general::types::{Annotated, Value};
use relay_metrics::{AggregatorConfig, Metric};

use crate::metrics_extraction::spans::types::SpanMetric;

use crate::metrics_extraction::transactions::types::ExtractMetricsError;
use crate::metrics_extraction::IntoMetric;

mod types;

/// Extracts metrics from the spans of the given transaction, and sets common
/// tags for all the metrics and spans. If a span already contains a tag
/// extracted for a metric, the tag value is overwritten.
pub(crate) fn extract_span_metrics(
    aggregator_config: &AggregatorConfig,
    event: &Event,
) -> Result<Vec<Metric>, ExtractMetricsError> {
    // TODO(iker): measure the performance of this whole method
    let mut metrics = Vec::new();

    if event.ty.value() != Some(&EventType::Transaction) {
        return Ok(metrics);
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

    let Some(spans) = event.spans.value() else { return Ok(metrics) };

    for annotated_span in spans {
        let Some(span) = annotated_span.value() else { continue };

        // Get parts of `span.data` that are designated as metrics
        let span_tags: BTreeMap<_, _> = span
            .data
            .value()
            .iter()
            .flat_map(|x| x.iter())
            .filter_map(
                |(key, value)| match (SpanTagKey::from_str(key.as_str()), value) {
                    (Ok(key), Annotated(Some(Value::String(s)), _)) if key.is_metric_tag() => {
                        Some((key, s.clone()))
                    }
                    _ => None,
                },
            )
            .collect();

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

            let mut reduced_tags = span_tags.clone();
            reduced_tags.remove(&SpanTagKey::Transaction);
            metrics.push(
                SpanMetric::ExclusiveTimeLight {
                    value: *exclusive_time,
                    tags: reduced_tags,
                }
                .into_metric(timestamp),
            );
        }
    }

    Ok(metrics)
}

#[cfg(test)]
mod tests {
    use relay_general::store::{self, LightNormalizationConfig};
    use relay_general::types::Annotated;

    use relay_metrics::AggregatorConfig;

    use crate::metrics_extraction::spans::extract_span_metrics;

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

        let aggregator_config = AggregatorConfig {
            max_secs_in_past: u64::MAX,
            max_secs_in_future: u64::MAX,
            ..Default::default()
        };

        let metrics =
            extract_span_metrics(&aggregator_config, event.value_mut().as_mut().unwrap()).unwrap();

        insta::assert_debug_snapshot!(event.value().unwrap().spans);
        insta::assert_debug_snapshot!(metrics);
    }
}
