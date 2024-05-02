use relay_common::time::UnixTimestamp;
use relay_dynamic_config::CombinedMetricExtractionConfig;
use relay_event_schema::protocol::{Event, Span};
use relay_metrics::Bucket;
use relay_quotas::DataCategory;

use crate::metrics_extraction::generic::{self, Extractable};
use crate::services::processor::extract_transaction_span;
use crate::statsd::RelayTimers;
use crate::utils::sample;

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
pub fn extract_metrics(
    event: &Event,
    spans_extracted: bool,
    config: &CombinedMetricExtractionConfig<'_>,
    max_tag_value_size: usize,
    span_extraction_sample_rate: Option<f32>,
) -> Vec<Bucket> {
    let mut metrics = generic::extract_metrics(event, config);

    // If spans were already extracted for an event,
    // we rely on span processing to extract metrics.
    if !spans_extracted && sample(span_extraction_sample_rate.unwrap_or(1.0)) {
        extract_span_metrics_for_event(event, config, max_tag_value_size, &mut metrics);
    }

    metrics
}

fn extract_span_metrics_for_event(
    event: &Event,
    config: &CombinedMetricExtractionConfig<'_>,
    max_tag_value_size: usize,
    output: &mut Vec<Bucket>,
) {
    relay_statsd::metric!(timer(RelayTimers::EventProcessingSpanMetricsExtraction), {
        if let Some(transaction_span) = extract_transaction_span(event, max_tag_value_size) {
            output.extend(generic::extract_metrics(&transaction_span, config));
        }

        if let Some(spans) = event.spans.value() {
            for annotated_span in spans {
                if let Some(span) = annotated_span.value() {
                    output.extend(generic::extract_metrics(span, config));
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::{DateTime, Utc};
    use insta::assert_debug_snapshot;
    use once_cell::sync::Lazy;
    use relay_dynamic_config::{
        Feature, FeatureSet, GlobalConfig, MetricExtractionConfig, MetricExtractionGroups,
        ProjectConfig,
    };
    use relay_event_normalization::{normalize_event, NormalizationConfig};
    use relay_event_schema::protocol::Timestamp;
    use relay_protocol::Annotated;

    use super::*;

    static GLOBAL_CONFIG: Lazy<MetricExtractionGroups> = Lazy::new(|| {
        let mut global = GlobalConfig::default();
        global.normalize(); // defines metrics extraction rules
        global.metric_extraction.ok().unwrap()
    });

    static PROJECT_CONFIG: Lazy<MetricExtractionConfig> = Lazy::new(|| {
        let features = FeatureSet(BTreeSet::from([
            Feature::ExtractSpansAndSpanMetricsFromEvent,
        ]));

        let mut project = ProjectConfig {
            features,
            ..ProjectConfig::default()
        };
        project.sanitize(); // enables metrics extraction rules
        project.metric_extraction.ok().unwrap()
    });

    fn combined_config() -> CombinedMetricExtractionConfig<'static> {
        CombinedMetricExtractionConfig::new(&GLOBAL_CONFIG, &PROJECT_CONFIG)
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
                    "description": "GET /hi/this/is/just/the/path",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
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
                        "cache.hit": false,
                        "cache.item_size": 8
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
                    "parent_span_id": "6ebd1cdbb9424b88",
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
                    "parent_span_id": "6ebd1cdbb9424b88",
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
                    "parent_span_id": "9756d8d7b2b364ff",
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
                    "parent_span_id": "9756d8d7b2b364ff",
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
                        "cache.hit": false,
                        "cache.item_size": 10
                    }
                },
                {
                    "description": "GET cache:user:{456}",
                    "op": "cache.get_item",
                    "parent_span_id": "8f5a2b8768cafb4e",
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
                    "description": "chrome-extension://begnopegbbhjeeiganiajffnalhlkkjb/img/assets/icon-10k.svg",
                    "op": "resource.script",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bb7af8b99e95af5f",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "status": "ok"
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
                    "timestamp": 1694732408.3145,
                    "start_timestamp": 1694732407.8367,
                    "exclusive_time": 477.800131,
                    "description": "https://cdn.domain.com/path/to/file-hk2YHeW7Eo2XLCiE38F1Fz22KuljsgCAD6hyWCyOYZM.CSS",
                    "op": "resource.css",
                    "span_id": "97c0ef9770a02f9d",
                    "parent_span_id": "9756d8d7b2b364ff",
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
                    "parent_span_id": "9756d8d7b2b364ff",
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
                    "parent_span_id": "a1e13f3f06239d69",
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
                    "parent_span_id": "a1e13f3f06239d69",
                    "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
                    "data": {
                        "messaging.destination.name": "default",
                        "messaging.message.receive.latency": 100,
                        "messaging.message.retry.count": 2,
                        "messaging.message.body.size": 1000
                    }
                },
                {
                    "timestamp": 1702474613.0495,
                    "start_timestamp": 1702474613.0175,
                    "description": "sentry.tasks.post_process.post_process_group",
                    "op": "queue.submit.celery",
                    "span_id": "9b01bd820a083e63",
                    "parent_span_id": "a1e13f3f06239d69",
                    "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
                    "data": {
                        "messaging.destination.name": "default",
                        "messaging.message.receive.latency": 100,
                        "messaging.message.retry.count": 2,
                        "messaging.message.body.size": 1000
                    }
                },
                {
                    "timestamp": 1702474613.0495,
                    "start_timestamp": 1702474613.0175,
                    "description": "sentry.tasks.post_process.post_process_group",
                    "op": "queue.publish",
                    "span_id": "9b01bd820a083e63",
                    "parent_span_id": "a1e13f3f06239d69",
                    "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
                    "data": {
                        "messaging.destination.name": "default",
                        "messaging.message.receive.latency": 100,
                        "messaging.message.retry.count": 2,
                        "messaging.message.body.size": 1000
                    }
                },
                {
                    "timestamp": 1702474613.0495,
                    "start_timestamp": 1702474613.0175,
                    "description": "sentry.tasks.post_process.post_process_group",
                    "op": "queue.process",
                    "span_id": "9b01bd820a083e63",
                    "parent_span_id": "a1e13f3f06239d69",
                    "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
                    "data": {
                        "messaging.destination.name": "default",
                        "messaging.message.receive.latency": 100,
                        "messaging.message.retry.count": 2,
                        "messaging.message.body.size": 1000
                    }
                },
                {
                    "timestamp": 1702474613.0495,
                    "start_timestamp": 1702474613.0175,
                    "description": "ConcurrentStream",
                    "op": "ai.run.langchain",
                    "origin": "auto.langchain",
                    "span_id": "9c01bd820a083e63",
                    "parent_span_id": "a1e13f3f06239d69",
                    "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
                    "measurements": {
                        "ai_total_tokens_used": {
                            "value": 20
                        }
                    },
                    "data": {
                        "ai.pipeline.name": "Autofix Pipeline"
                    }
                },
                {
                    "timestamp": 1702474613.0495,
                    "start_timestamp": 1702474613.0175,
                    "description": "Autofix Pipeline",
                    "op": "ai.pipeline.langchain",
                    "origin": "auto.langchain",
                    "span_id": "9c01bd820a083e63",
                    "parent_span_id": "a1e13f3f06239d69",
                    "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
                    "measurements": {
                        "ai_total_tokens_used": {
                            "value": 30
                        }
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

        let metrics = extract_metrics(event.value().unwrap(), false, &combined_config(), 200, None);
        insta::assert_debug_snapshot!(metrics);
    }

    const MOBILE_EVENT: &str = r#"
        {
            "type": "transaction",
            "sdk": {"name": "sentry.javascript.react-native"},
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "timestamp": "2021-04-26T08:00:00+0100",
            "release": "1.2.3",
            "transaction": "gEt /api/:version/users/",
            "transaction_info": {"source": "custom"},
            "platform": "cocoa",
            "contexts": {
                "trace": {
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "span_id": "bd429c44b67a3eb4",
                    "op": "ui.load"
                },
                "device": {
                    "family": "iOS",
                    "model": "iPhone1,1"
                },
                "app": {
                    "app_identifier": "org.reactjs.native.example.RnDiffApp",
                    "app_name": "RnDiffApp"
                },
                "os": {
                    "name": "iOS",
                    "version": "16.2"
                }
            },
            "measurements": {
                "app_start_warm": {
                    "value": 1.0,
                    "unit": "millisecond"
                }
            },
            "spans": [
                {
                    "op": "app.start.cold",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81"
                },
                {
                    "op": "ui.load.initial_display",
                    "span_id": "bd429c44b67a3eb2",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976303.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "frames.slow": 1,
                        "frames.frozen": 2,
                        "frames.total": 9,
                        "frames.delay": 0.1
                    }
                },
                {
                    "op": "app.start.cold",
                    "description": "Cold Start",
                    "span_id": "bd429c44b67a3eb2",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976303.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81"
                },
                {
                    "op": "custom.op",
                    "description": "Custom Op",
                    "span_id": "bd429c44b67a3eb2",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976303.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81"
                },
                {
                    "op": "contentprovider.load",
                    "description": "io.sentry.android.core.SentryPerformanceProvider.onCreate",
                    "span_id": "bd429c44b67a3eb2",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976303.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81"
                },
                {
                    "op": "application.load",
                    "description": "io.sentry.samples.android.MyApplication.onCreate",
                    "span_id": "bd429c44b67a3eb2",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976303.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81"
                },
                {
                    "op": "activity.load",
                    "description": "io.sentry.samples.android.MainActivity.onCreate",
                    "span_id": "bd429c44b67a3eb2",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976303.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "app_start_type": "cold"
                    }
                },
                {
                    "op": "process.load",
                    "description": "Process Initialization",
                    "span_id": "bd429c44b67a3eb2",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976303.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "app_start_type": "cold"
                    }
                },
                {
                    "op": "file.read",
                    "description": "somebackup.212321",
                    "span_id": "bd429c44b67a3eb2",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976303.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81"
                },
                {
                    "op": "http.client",
                    "description": "www.example.com",
                    "span_id": "bd429c44b67a3eb2",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976303.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "http.response.status_code": "200"
                    }
                },
                {
                    "op": "http.client",
                    "description": "www.example.com",
                    "span_id": "bd429c44b67a3eb2",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976303.0000000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "http.response.status_code": 200
                    }
                }
            ]
        }
        "#;

    #[test]
    fn test_extract_span_metrics_mobile() {
        let mut event = Annotated::from_json(MOBILE_EVENT).unwrap();

        // Normalize first, to make sure that all things are correct as in the real pipeline:
        normalize_event(
            &mut event,
            &NormalizationConfig {
                enrich_spans: true,
                device_class_synthesis_config: true,
                ..Default::default()
            },
        );

        let metrics = extract_metrics(event.value().unwrap(), false, &combined_config(), 200, None);
        insta::assert_debug_snapshot!((&event.value().unwrap().spans, metrics));
    }

    #[test]
    fn test_extract_span_metrics_mobile_screen() {
        let json = r#"
        {
            "type": "transaction",
            "sdk": {"name": "sentry.javascript.react-native"},
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "timestamp": "2021-04-26T08:00:00+0100",
            "transaction": "gEt /api/:version/users/",
            "contexts": {
                "trace": {
                    "op": "ui.load",
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "span_id": "bd429c44b67a3eb4"
                }
            },
            "spans": [
                {
                    "description": "GET http://domain.tld/hi",
                    "op": "http.client",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1597976300.0000000,
                    "timestamp": 1597976302.0000000,
                    "exclusive_time": 2000.0,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    "data": {
                        "http.method": "GET"
                    },
                    "sentry_tags": {
                        "action": "GET",
                        "category": "http",
                        "description": "GET http://domain.tld",
                        "domain": "domain.tld",
                        "group": "d9dc18637d441612",
                        "mobile": "true",
                        "op": "http.client",
                        "transaction": "gEt /api/:version/users/",
                        "transaction.method": "GET",
                        "transaction.op": "ui.load"
                    }
                }
            ]
        }
        "#;
        let event = Annotated::from_json(json).unwrap();

        let metrics = extract_metrics(event.value().unwrap(), false, &combined_config(), 200, None);

        // When transaction.op:ui.load and mobile:true, HTTP spans still get both
        // exclusive_time metrics:
        assert!(metrics
            .iter()
            .any(|b| &*b.name == "d:spans/exclusive_time@millisecond"));
        assert!(metrics
            .iter()
            .any(|b| &*b.name == "d:spans/exclusive_time_light@millisecond"));
    }

    #[test]
    fn test_extract_span_metrics_usage() {
        let mut event = Annotated::from_json(MOBILE_EVENT).unwrap();

        // Normalize first, to make sure that all things are correct as in the real pipeline:
        normalize_event(
            &mut event,
            &NormalizationConfig {
                enrich_spans: true,
                device_class_synthesis_config: true,
                ..Default::default()
            },
        );

        let metrics = extract_metrics(event.value().unwrap(), false, &combined_config(), 200, None);

        let usage_metrics = metrics
            .into_iter()
            .filter(|b| &*b.name == "c:spans/usage@none")
            .collect::<Vec<_>>();

        let expected_usage = 12; // We count all spans received by Relay, plus one for the transaction
        assert_eq!(usage_metrics.len(), expected_usage);
        for m in usage_metrics {
            assert!(m.tags.is_empty());
        }
    }

    /// Helper function for span metric extraction tests.
    fn extract_span_metrics_mobile(span_op: &str, duration_millis: f64) -> Vec<Bucket> {
        let mut span = Span::default();
        span.sentry_tags
            .get_or_insert_with(Default::default)
            .insert("mobile".to_owned(), "true".to_owned().into());
        span.timestamp
            .set_value(Some(Timestamp::from(DateTime::<Utc>::MAX_UTC))); // whatever
        span.op.set_value(Some(span_op.into()));
        span.exclusive_time.set_value(Some(duration_millis));

        generic::extract_metrics(&span, &combined_config())
    }

    #[test]
    fn test_app_start_cold_inlier() {
        let metrics = extract_span_metrics_mobile("app.start.cold", 180000.0);
        assert_eq!(
            metrics.iter().map(|m| &*m.name).collect::<Vec<_>>(),
            vec![
                "c:spans/usage@none",
                "d:spans/exclusive_time@millisecond",
                "d:spans/exclusive_time_light@millisecond",
            ]
        );
    }

    #[test]
    fn test_app_start_cold_outlier() {
        let metrics = extract_span_metrics_mobile("app.start.cold", 181000.0);
        assert_eq!(
            metrics.iter().map(|m| &*m.name).collect::<Vec<_>>(),
            vec!["c:spans/usage@none", "d:spans/exclusive_time@millisecond",]
        );
    }

    #[test]
    fn test_app_start_warm_inlier() {
        let metrics = extract_span_metrics_mobile("app.start.warm", 180000.0);
        assert_eq!(
            metrics.iter().map(|m| &*m.name).collect::<Vec<_>>(),
            vec![
                "c:spans/usage@none",
                "d:spans/exclusive_time@millisecond",
                "d:spans/exclusive_time_light@millisecond",
            ]
        );
    }

    #[test]
    fn test_app_start_warm_outlier() {
        let metrics = extract_span_metrics_mobile("app.start.warm", 181000.0);
        assert_eq!(
            metrics.iter().map(|m| &*m.name).collect::<Vec<_>>(),
            vec!["c:spans/usage@none", "d:spans/exclusive_time@millisecond",]
        );
    }

    #[test]
    fn test_ui_load_initial_display_inlier() {
        let metrics = extract_span_metrics_mobile("ui.load.initial_display", 180000.0);
        assert_eq!(
            metrics.iter().map(|m| &*m.name).collect::<Vec<_>>(),
            vec![
                "c:spans/usage@none",
                "d:spans/exclusive_time@millisecond",
                "d:spans/exclusive_time_light@millisecond",
            ]
        );
    }

    #[test]
    fn test_ui_load_initial_display_outlier() {
        let metrics = extract_span_metrics_mobile("ui.load.initial_display", 181000.0);
        assert_eq!(
            metrics.iter().map(|m| &*m.name).collect::<Vec<_>>(),
            vec!["c:spans/usage@none", "d:spans/exclusive_time@millisecond",]
        );
    }

    #[test]
    fn test_ui_load_full_display_inlier() {
        let metrics = extract_span_metrics_mobile("ui.load.full_display", 180000.0);
        assert_eq!(
            metrics.iter().map(|m| &*m.name).collect::<Vec<_>>(),
            vec![
                "c:spans/usage@none",
                "d:spans/exclusive_time@millisecond",
                "d:spans/exclusive_time_light@millisecond",
            ]
        );
    }

    #[test]
    fn test_ui_load_full_display_outlier() {
        let metrics = extract_span_metrics_mobile("ui.load.full_display", 181000.0);
        assert_eq!(
            metrics.iter().map(|m| &*m.name).collect::<Vec<_>>(),
            vec!["c:spans/usage@none", "d:spans/exclusive_time@millisecond",]
        );
    }

    #[test]
    fn test_display_times_extracted() {
        let span = r#"{
            "op": "ui.load",
            "span_id": "bd429c44b67a3eb4",
            "start_timestamp": 1597976300.0000000,
            "timestamp": 1597976302.0000000,
            "exclusive_time": 100,
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
            "sentry_tags": {
                "mobile": "true",
                "ttid": "ttid",
                "ttfd": "ttfd"
            }
        }"#;
        let span = Annotated::<Span>::from_json(span).unwrap().into_value().unwrap();
        let metrics = generic::extract_metrics(&span, &combined_config());

        assert!(!metrics.is_empty());
        for metric in metrics {
            if &*metric.name == "d:spans/exclusive_time@millisecond" {
                assert_eq!(metric.tag("ttid"), Some("ttid"));
                assert_eq!(metric.tag("ttfd"), Some("ttfd"));
            } else {
                assert!(!metric.tags.contains_key("ttid"));
                assert!(!metric.tags.contains_key("ttfd"));
            }
        }
    }

    #[test]
    fn test_extract_span_metrics_performance_score() {
        let json = r#"
            {
                "op": "ui.interaction.click",
                "parent_span_id": "8f5a2b8768cafb4e",
                "span_id": "bd429c44b67a3eb4",
                "start_timestamp": 1597976300.0000000,
                "timestamp": 1597976302.0000000,
                "exclusive_time": 2000.0,
                "trace_id": "ff62a8b040f340bda5d830223def1d81",
                "sentry_tags": {
                    "browser.name": "Chrome",
                    "op": "ui.interaction.click"
                },
                "measurements": {
                    "score.total": {"value": 1.0},
                    "score.inp": {"value": 1.0},
                    "score.weight.inp": {"value": 1.0},
                    "inp": {"value": 1.0}
                }
            }
        "#;
        let span = Annotated::<Span>::from_json(json).unwrap();
        let metrics = generic::extract_metrics(span.value().unwrap(), &combined_config());

        for mri in [
            "d:spans/webvital.inp@millisecond",
            "d:spans/webvital.score.inp@ratio",
            "d:spans/webvital.score.total@ratio",
            "d:spans/webvital.score.weight.inp@ratio",
        ] {
            assert!(metrics.iter().any(|b| &*b.name == mri));
            assert!(metrics.iter().any(|b| b.tags.contains_key("browser.name")));
            assert!(metrics.iter().any(|b| b.tags.contains_key("span.op")));
        }
    }

    #[test]
    fn extracts_span_metrics_from_transaction() {
        let event = r#"
            {
                "type": "transaction",
                "timestamp": "2021-04-26T08:00:05+0100",
                "start_timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "my_transaction",
                "contexts": {
                    "trace": {
                        "exclusive_time": 5000.0,
                        "op": "db.query",
                        "status": "ok"
                    }
                }
            }
            "#;
        let event = Annotated::<Event>::from_json(event).unwrap();

        let metrics = extract_metrics(event.value().unwrap(), false, &combined_config(), 200, None);

        assert_eq!(metrics.len(), 4);

        assert_eq!(&*metrics[0].name, "c:spans/usage@none");

        assert_eq!(&*metrics[1].name, "d:spans/exclusive_time@millisecond");
        assert_debug_snapshot!(metrics[1].tags, @r###"
        {
            "span.category": "db",
            "span.op": "db.query",
            "transaction": "my_transaction",
            "transaction.op": "db.query",
        }
        "###);
        assert_eq!(
            &*metrics[2].name,
            "d:spans/exclusive_time_light@millisecond"
        );

        assert_eq!(&*metrics[3].name, "d:spans/duration@millisecond");
    }
}
