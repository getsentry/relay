//! This module defines bidirectional field mappings between spans and transactions.

use crate::protocol::{BrowserContext, Event, ProfileContext, Span, SpanData, TraceContext};

impl From<&Event> for Span {
    fn from(event: &Event) -> Self {
        let Event {
            transaction,

            platform,
            timestamp,
            start_timestamp,
            received,
            release,
            environment,
            tags,

            measurements,
            _metrics,
            _metrics_summary,
            ..
        } = event;

        let trace = event.context::<TraceContext>();

        // Fill data from trace context:
        let mut data = trace
            .map(|c| c.data.clone().map_value(SpanData::from))
            .unwrap_or_default();

        // Overwrite specific fields:
        let span_data = data.get_or_insert_with(Default::default);
        span_data.segment_name = transaction.clone();
        span_data.release = release.clone();
        span_data.environment = environment.clone();
        if let Some(browser) = event.context::<BrowserContext>() {
            span_data.browser_name = browser.name.clone();
        }
        if let Some(client_sdk) = event.client_sdk.value() {
            span_data.sdk_name = client_sdk.name.clone();
            span_data.sdk_version = client_sdk.version.clone();
        }

        Self {
            timestamp: timestamp.clone(),
            start_timestamp: start_timestamp.clone(),
            exclusive_time: trace.map(|c| c.exclusive_time.clone()).unwrap_or_default(),
            op: trace.map(|c| c.op.clone()).unwrap_or_default(),
            span_id: trace.map(|c| c.span_id.clone()).unwrap_or_default(),
            parent_span_id: trace.map(|c| c.parent_span_id.clone()).unwrap_or_default(),
            trace_id: trace.map(|c| c.trace_id.clone()).unwrap_or_default(),
            segment_id: trace.map(|c| c.span_id.clone()).unwrap_or_default(),
            is_segment: true.into(),
            status: trace.map(|c| c.status.clone()).unwrap_or_default(),
            description: transaction.clone(),
            tags: tags.clone().map_value(|t| t.into()),
            origin: trace.map(|c| c.origin.clone()).unwrap_or_default(),
            profile_id: event
                .context::<ProfileContext>()
                .map(|c| c.profile_id.clone())
                .unwrap_or_default(),
            data,
            sentry_tags: Default::default(),
            received: received.clone(),
            measurements: measurements.clone(),
            _metrics_summary: _metrics_summary.clone(),
            platform: platform.clone(),
            was_transaction: true.into(),
            other: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::Annotated;

    use super::*;

    #[test]
    fn convert() {
        let event = Annotated::<Event>::from_json(
            r#"{
                "type": "transaction",
                "platform": "php",
                "sdk": {"name": "sentry.php", "version": "1.2.3"},
                "release": "myapp@1.0.0",
                "environment": "prod",
                "transaction": "my 1st transaction",
                "contexts": {
                    "browser": {"name": "Chrome"},
                    "profile": {"profile_id": "a0aaaaaaaaaaaaaaaaaaaaaaaaaaaaab"},
                    "trace": {
                        "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                        "span_id": "FA90FDEAD5F74052",
                        "type": "trace",
                        "origin": "manual",
                        "op": "myop",
                        "status": "ok",
                        "exclusive_time": 123.4,
                        "parent_span_id": "FA90FDEAD5F74051",
                        "data": {
                            "custom_attribute": 42
                        }
                    }
                },
                "_metrics_summary": {
                    "some_metric": [
                        {
                            "min": 1.0,
                            "max": 2.0,
                            "sum": 3.0,
                            "count": 2,
                            "tags": {
                                "environment": "test"
                            }
                        }
                    ]
                },
                "measurements": {
                    "memory": {
                        "value": 9001.0,
                        "unit": "byte"
                    }
                }
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        let span_from_event = Span::from(&event);
        insta::assert_debug_snapshot!(span_from_event, @r###"
        Span {
            timestamp: ~,
            start_timestamp: ~,
            exclusive_time: 123.4,
            op: "myop",
            span_id: SpanId(
                "fa90fdead5f74052",
            ),
            parent_span_id: SpanId(
                "fa90fdead5f74051",
            ),
            trace_id: TraceId(
                "4c79f60c11214eb38604f4ae0781bfb2",
            ),
            segment_id: SpanId(
                "fa90fdead5f74052",
            ),
            is_segment: true,
            status: Ok,
            description: "my 1st transaction",
            tags: ~,
            origin: "manual",
            profile_id: EventId(
                a0aaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaab,
            ),
            data: SpanData {
                app_start_type: ~,
                ai_total_tokens_used: ~,
                ai_prompt_tokens_used: ~,
                ai_completion_tokens_used: ~,
                browser_name: "Chrome",
                code_filepath: ~,
                code_lineno: ~,
                code_function: ~,
                code_namespace: ~,
                db_operation: ~,
                db_system: ~,
                db_collection_name: ~,
                environment: "prod",
                release: LenientString(
                    "myapp@1.0.0",
                ),
                http_decoded_response_content_length: ~,
                http_request_method: ~,
                http_response_content_length: ~,
                http_response_transfer_size: ~,
                resource_render_blocking_status: ~,
                server_address: ~,
                cache_hit: ~,
                cache_key: ~,
                cache_item_size: ~,
                http_response_status_code: ~,
                ai_pipeline_name: ~,
                ai_model_id: ~,
                ai_input_messages: ~,
                ai_responses: ~,
                thread_name: ~,
                thread_id: ~,
                segment_name: "my 1st transaction",
                ui_component_name: ~,
                url_scheme: ~,
                user: ~,
                user_email: ~,
                user_full_name: ~,
                user_geo_country_code: ~,
                user_geo_city: ~,
                user_geo_subdivision: ~,
                user_geo_region: ~,
                user_hash: ~,
                user_id: ~,
                user_name: ~,
                user_roles: ~,
                replay_id: ~,
                sdk_name: "sentry.php",
                sdk_version: "1.2.3",
                frames_slow: ~,
                frames_frozen: ~,
                frames_total: ~,
                frames_delay: ~,
                messaging_destination_name: ~,
                messaging_message_retry_count: ~,
                messaging_message_receive_latency: ~,
                messaging_message_body_size: ~,
                messaging_message_id: ~,
                user_agent_original: ~,
                url_full: ~,
                client_address: ~,
                route: ~,
                previous_route: ~,
                lcp_element: ~,
                lcp_size: ~,
                lcp_id: ~,
                lcp_url: ~,
                other: {
                    "custom_attribute": I64(
                        42,
                    ),
                },
            },
            sentry_tags: ~,
            received: ~,
            measurements: Measurements(
                {
                    "memory": Measurement {
                        value: 9001.0,
                        unit: Information(
                            Byte,
                        ),
                    },
                },
            ),
            _metrics_summary: MetricsSummary(
                {
                    "some_metric": [
                        MetricSummary {
                            min: 1.0,
                            max: 2.0,
                            sum: 3.0,
                            count: 2,
                            tags: {
                                "environment": "test",
                            },
                        },
                    ],
                },
            ),
            platform: "php",
            was_transaction: true,
            other: {},
        }
        "###);
    }
}
