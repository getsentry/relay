//! Contains the processing-only functionality.

use crate::services::processor::ProcessingError;
use chrono::{DateTime, Utc};
use relay_event_normalization::span::ai::enrich_ai_span;
use relay_event_normalization::{
    BorrowedSpanOpDefaults, ClientHints, CombinedMeasurementsConfig, FromUserAgentInfo,
    GeoIpLookup, ModelMetadata, PerformanceScoreConfig, RawUserAgentInfo, SchemaProcessor,
    TimestampProcessor, TransactionNameRule, TransactionsProcessor, TrimmingProcessor,
    normalize_measurements, normalize_performance_score, normalize_transaction_name,
    span::tag_extraction, validate_span,
};
use relay_event_schema::processor::{ProcessingState, process_value};
use relay_event_schema::protocol::{BrowserContext, EventId, IpAddr, Span, SpanData};
use relay_metrics::UnixTimestamp;
use relay_protocol::{Annotated, Empty, Value};

/// Config needed to normalize a standalone span.
#[derive(Clone, Debug)]
pub struct NormalizeSpanConfig<'a> {
    /// The time at which the event was received in this Relay.
    pub received_at: DateTime<Utc>,
    /// Allowed time range for spans.
    pub timestamp_range: std::ops::Range<UnixTimestamp>,
    /// The maximum allowed size of tag values in bytes. Longer values will be cropped.
    pub max_tag_value_size: usize,
    /// Configuration for generating performance score measurements for web vitals
    pub performance_score: Option<&'a PerformanceScoreConfig>,
    /// Configuration for measurement normalization in transaction events.
    ///
    /// Has an optional [`relay_event_normalization::MeasurementsConfig`] from both the project and the global level.
    /// If at least one is provided, then normalization will truncate custom measurements
    /// and add units of known built-in measurements.
    pub measurements: Option<CombinedMeasurementsConfig<'a>>,
    /// Metadata for AI models including costs and context size.
    pub ai_model_metadata: Option<&'a ModelMetadata>,
    /// The maximum length for names of custom measurements.
    ///
    /// Measurements with longer names are removed from the transaction event and replaced with a
    /// metadata entry.
    pub max_name_and_unit_len: usize,
    /// Transaction name normalization rules.
    pub tx_name_rules: &'a [TransactionNameRule],
    /// The user agent parsed from the request.
    pub user_agent: Option<String>,
    /// Client hints parsed from the request.
    pub client_hints: ClientHints<String>,
    /// Hosts that are not replaced by "*" in HTTP span grouping.
    pub allowed_hosts: &'a [String],
    /// The IP address of the SDK that sent the event.
    ///
    /// When `{{auto}}` is specified and there is no other IP address in the payload, such as in the
    /// `request` context, this IP address gets added to `span.data.client_address`.
    pub client_ip: Option<IpAddr>,
    /// An initialized GeoIP lookup.
    pub geo_lookup: &'a GeoIpLookup,
    pub span_op_defaults: BorrowedSpanOpDefaults<'a>,
}

fn set_segment_attributes(span: &mut Annotated<Span>) {
    let Some(span) = span.value_mut() else { return };

    // Identify INP spans or other WebVital spans and make sure they are not wrapped in a segment.
    if let Some(span_op) = span.op.value()
        && (span_op.starts_with("ui.interaction.") || span_op.starts_with("ui.webvital."))
    {
        span.is_segment = None.into();
        span.parent_span_id = None.into();
        span.segment_id = None.into();
        return;
    }

    let Some(span_id) = span.span_id.value() else {
        return;
    };

    if let Some(segment_id) = span.segment_id.value() {
        // The span is a segment if and only if the segment_id matches the span_id.
        span.is_segment = (segment_id == span_id).into();
    } else if span.parent_span_id.is_empty() {
        // If the span has no parent, it is automatically a segment:
        span.is_segment = true.into();
    }

    // If the span is a segment, always set the segment_id to the current span_id:
    if span.is_segment.value() == Some(&true) {
        span.segment_id = span.span_id.clone();
    }
}

/// Normalizes a standalone span.
pub fn normalize(
    annotated_span: &mut Annotated<Span>,
    config: NormalizeSpanConfig,
) -> Result<(), ProcessingError> {
    let NormalizeSpanConfig {
        received_at,
        timestamp_range,
        max_tag_value_size,
        performance_score,
        measurements,
        ai_model_metadata,
        max_name_and_unit_len,
        tx_name_rules,
        user_agent,
        client_hints,
        allowed_hosts,
        client_ip,
        geo_lookup,
        span_op_defaults,
    } = config;

    set_segment_attributes(annotated_span);

    // This follows the steps of `event::normalize`.

    process_value(
        annotated_span,
        &mut SchemaProcessor::new(),
        ProcessingState::root(),
    )?;

    process_value(
        annotated_span,
        &mut TimestampProcessor,
        ProcessingState::root(),
    )?;

    if let Some(span) = annotated_span.value() {
        validate_span(span, Some(&timestamp_range))?;
    }
    process_value(
        annotated_span,
        &mut TransactionsProcessor::new(Default::default(), span_op_defaults),
        ProcessingState::root(),
    )?;

    let Some(span) = annotated_span.value_mut() else {
        return Err(ProcessingError::NoEventPayload);
    };

    // Replace missing / {{auto}} IPs:
    // Transaction and error events require an explicit `{{auto}}` to derive the IP, but
    // for spans we derive it by default:
    if let Some(client_ip) = client_ip.as_ref() {
        let ip = span.data.value().and_then(|d| d.client_address.value());
        if ip.is_none_or(|ip| ip.is_auto()) {
            span.data
                .get_or_insert_with(Default::default)
                .client_address = Annotated::new(client_ip.clone());
        }
    }

    // Derive geo ip:
    let data = span.data.get_or_insert_with(Default::default);
    if let Some(ip) = data
        .client_address
        .value()
        .and_then(|ip| ip.as_str().parse().ok())
        && let Some(geo) = geo_lookup.lookup(ip)
    {
        data.user_geo_city = geo.city;
        data.user_geo_country_code = geo.country_code;
        data.user_geo_region = geo.region;
        data.user_geo_subdivision = geo.subdivision;
    }

    populate_ua_fields(span, user_agent.as_deref(), client_hints.as_deref());

    promote_span_data_fields(span);

    if let Annotated(Some(ref mut measurement_values), ref mut meta) = span.measurements {
        normalize_measurements(
            measurement_values,
            meta,
            measurements,
            Some(max_name_and_unit_len),
            span.start_timestamp.0,
            span.timestamp.0,
        );
    }

    span.received = Annotated::new(received_at.into());

    if let Some(transaction) = span
        .data
        .value_mut()
        .as_mut()
        .map(|data| &mut data.segment_name)
    {
        normalize_transaction_name(transaction, tx_name_rules);
    }

    // Tag extraction:
    let is_mobile = false; // TODO: find a way to determine is_mobile from a standalone span.
    let tags = tag_extraction::extract_tags(
        span,
        max_tag_value_size,
        None,
        None,
        is_mobile,
        None,
        allowed_hosts,
        geo_lookup,
    );
    span.sentry_tags = Annotated::new(tags);

    normalize_performance_score(span, performance_score);

    enrich_ai_span(span, ai_model_metadata);

    tag_extraction::extract_measurements(span, is_mobile);

    process_value(
        annotated_span,
        &mut TrimmingProcessor::new(),
        ProcessingState::root(),
    )?;

    Ok(())
}

fn populate_ua_fields(
    span: &mut Span,
    request_user_agent: Option<&str>,
    mut client_hints: ClientHints<&str>,
) {
    let data = span.data.value_mut().get_or_insert_with(SpanData::default);

    let user_agent = data.user_agent_original.value_mut();
    if user_agent.is_none() {
        *user_agent = request_user_agent.map(String::from);
    } else {
        // User agent in span payload should take precendence over request
        // client hints.
        client_hints = ClientHints::default();
    }

    if data.browser_name.value().is_none()
        && let Some(context) = BrowserContext::from_hints_or_ua(&RawUserAgentInfo {
            user_agent: user_agent.as_deref(),
            client_hints,
        })
    {
        data.browser_name = context.name;
    }
}

/// Promotes some fields from span.data as there are predefined places for certain fields.
fn promote_span_data_fields(span: &mut Span) {
    // INP spans sets some top level span attributes inside span.data so make sure to pull
    // them out to the top level before further processing.
    if let Some(data) = span.data.value_mut() {
        if let Some(exclusive_time) = match data.exclusive_time.value() {
            Some(Value::I64(exclusive_time)) => Some(*exclusive_time as f64),
            Some(Value::U64(exclusive_time)) => Some(*exclusive_time as f64),
            Some(Value::F64(exclusive_time)) => Some(*exclusive_time),
            _ => None,
        } {
            span.exclusive_time = exclusive_time.into();
            data.exclusive_time.set_value(None);
        }

        if let Some(profile_id) = match data.profile_id.value() {
            Some(Value::String(profile_id)) => profile_id.parse().map(EventId).ok(),
            _ => None,
        } {
            span.profile_id = profile_id.into();
            data.profile_id.set_value(None);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use relay_event_schema::protocol::EventId;
    use relay_event_schema::protocol::Span;
    use relay_protocol::get_value;

    use super::*;

    #[test]
    fn segment_no_overwrite() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
            "is_segment": true,
            "span_id": "fa90fdead5f74052",
            "parent_span_id": "fa90fdead5f74051"
        }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment!), &true);
        assert_eq!(get_value!(span.segment_id!).to_string(), "fa90fdead5f74052");
    }

    #[test]
    fn segment_overwrite_because_of_segment_id() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
         "is_segment": false,
         "span_id": "fa90fdead5f74052",
         "segment_id": "fa90fdead5f74052",
         "parent_span_id": "fa90fdead5f74051"
     }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment!), &true);
    }

    #[test]
    fn segment_overwrite_because_of_missing_parent() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
         "is_segment": false,
         "span_id": "fa90fdead5f74052"
     }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment!), &true);
        assert_eq!(get_value!(span.segment_id!).to_string(), "fa90fdead5f74052");
    }

    #[test]
    fn segment_no_parent_but_segment() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
         "span_id": "fa90fdead5f74052",
         "segment_id": "ea90fdead5f74051"
     }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment!), &false);
        assert_eq!(get_value!(span.segment_id!).to_string(), "ea90fdead5f74051");
    }

    #[test]
    fn segment_only_parent() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
         "parent_span_id": "fa90fdead5f74051"
     }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment), None);
        assert_eq!(get_value!(span.segment_id), None);
    }

    #[test]
    fn not_segment_but_inp_span() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
         "op": "ui.interaction.click",
         "is_segment": false,
         "parent_span_id": "fa90fdead5f74051"
     }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment), None);
        assert_eq!(get_value!(span.segment_id), None);
    }

    #[test]
    fn segment_but_inp_span() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
         "op": "ui.interaction.click",
         "segment_id": "fa90fdead5f74051",
         "is_segment": true,
         "parent_span_id": "fa90fdead5f74051"
     }"#,
        )
        .unwrap();
        set_segment_attributes(&mut span);
        assert_eq!(get_value!(span.is_segment), None);
        assert_eq!(get_value!(span.segment_id), None);
    }

    #[test]
    fn keep_browser_name() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "browser.name": "foo"
                }
            }"#,
        )
        .unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            None,
            ClientHints::default(),
        );
        assert_eq!(get_value!(span.data.browser_name!), "foo");
    }

    #[test]
    fn keep_browser_name_when_ua_present() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "browser.name": "foo",
                    "user_agent.original": "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
                }
            }"#,
        )
            .unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            None,
            ClientHints::default(),
        );
        assert_eq!(get_value!(span.data.browser_name!), "foo");
    }

    #[test]
    fn derive_browser_name() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "user_agent.original": "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
                }
            }"#,
        )
            .unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            None,
            ClientHints::default(),
        );
        assert_eq!(
            get_value!(span.data.user_agent_original!),
            "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
        );
        assert_eq!(get_value!(span.data.browser_name!), "Chrome Mobile");
    }

    #[test]
    fn keep_user_agent_when_meta_is_present() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "user_agent.original": "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
                }
            }"#,
        )
            .unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            Some(
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; ONS Internet Explorer 6.1; .NET CLR 1.1.4322)",
            ),
            ClientHints::default(),
        );
        assert_eq!(
            get_value!(span.data.user_agent_original!),
            "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
        );
        assert_eq!(get_value!(span.data.browser_name!), "Chrome Mobile");
    }

    #[test]
    fn derive_user_agent() {
        let mut span: Annotated<Span> = Annotated::from_json(r#"{}"#).unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            Some(
                "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; ONS Internet Explorer 6.1; .NET CLR 1.1.4322)",
            ),
            ClientHints::default(),
        );
        assert_eq!(
            get_value!(span.data.user_agent_original!),
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; ONS Internet Explorer 6.1; .NET CLR 1.1.4322)"
        );
        assert_eq!(get_value!(span.data.browser_name!), "IE");
    }

    #[test]
    fn keep_user_agent_when_client_hints_are_present() {
        let mut span: Annotated<Span> = Annotated::from_json(
            r#"{
                "data": {
                    "user_agent.original": "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
                }
            }"#,
        )
            .unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            None,
            ClientHints {
                sec_ch_ua: Some(r#""Chromium";v="108", "Opera";v="94", "Not)A;Brand";v="99""#),
                ..Default::default()
            },
        );
        assert_eq!(
            get_value!(span.data.user_agent_original!),
            "Mozilla/5.0 (-; -; -) - Chrome/18.0.1025.133 Mobile Safari/535.19"
        );
        assert_eq!(get_value!(span.data.browser_name!), "Chrome Mobile");
    }

    #[test]
    fn derive_client_hints() {
        let mut span: Annotated<Span> = Annotated::from_json(r#"{}"#).unwrap();
        populate_ua_fields(
            span.value_mut().as_mut().unwrap(),
            None,
            ClientHints {
                sec_ch_ua: Some(r#""Chromium";v="108", "Opera";v="94", "Not)A;Brand";v="99""#),
                ..Default::default()
            },
        );
        assert_eq!(get_value!(span.data.user_agent_original), None);
        assert_eq!(get_value!(span.data.browser_name!), "Opera");
    }

    static GEO_LOOKUP: LazyLock<GeoIpLookup> = LazyLock::new(|| {
        GeoIpLookup::open("../relay-event-normalization/tests/fixtures/GeoIP2-Enterprise-Test.mmdb")
            .unwrap()
    });

    fn normalize_config() -> NormalizeSpanConfig<'static> {
        NormalizeSpanConfig {
            received_at: DateTime::from_timestamp_nanos(0),
            timestamp_range: UnixTimestamp::from_datetime(
                DateTime::<Utc>::from_timestamp_millis(1000).unwrap(),
            )
            .unwrap()
                ..UnixTimestamp::from_datetime(DateTime::<Utc>::MAX_UTC).unwrap(),
            max_tag_value_size: 200,
            performance_score: None,
            measurements: None,
            ai_model_metadata: None,
            max_name_and_unit_len: 200,
            tx_name_rules: &[],
            user_agent: None,
            client_hints: ClientHints::default(),
            allowed_hosts: &[],
            client_ip: Some(IpAddr("2.125.160.216".to_owned())),
            geo_lookup: &GEO_LOOKUP,
            span_op_defaults: Default::default(),
        }
    }

    #[test]
    fn user_ip_from_client_ip_without_auto() {
        let mut span = Annotated::from_json(
            r#"{
            "start_timestamp": 0,
            "timestamp": 1,
            "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
            "span_id": "922dda2462ea4ac2",
            "data": {
                "client.address": "2.125.160.216"
            }
        }"#,
        )
        .unwrap();

        normalize(&mut span, normalize_config()).unwrap();

        assert_eq!(
            get_value!(span.data.client_address!).as_str(),
            "2.125.160.216"
        );
        assert_eq!(get_value!(span.data.user_geo_city!), "Boxford");
    }

    #[test]
    fn user_ip_from_client_ip_with_auto() {
        let mut span = Annotated::from_json(
            r#"{
            "start_timestamp": 0,
            "timestamp": 1,
            "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
            "span_id": "922dda2462ea4ac2",
            "data": {
                "client.address": "{{auto}}"
            }
        }"#,
        )
        .unwrap();

        normalize(&mut span, normalize_config()).unwrap();

        assert_eq!(
            get_value!(span.data.client_address!).as_str(),
            "2.125.160.216"
        );
        assert_eq!(get_value!(span.data.user_geo_city!), "Boxford");
    }

    #[test]
    fn user_ip_from_client_ip_with_missing() {
        let mut span = Annotated::from_json(
            r#"{
            "start_timestamp": 0,
            "timestamp": 1,
            "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
            "span_id": "922dda2462ea4ac2"
        }"#,
        )
        .unwrap();

        normalize(&mut span, normalize_config()).unwrap();

        assert_eq!(
            get_value!(span.data.client_address!).as_str(),
            "2.125.160.216"
        );
        assert_eq!(get_value!(span.data.user_geo_city!), "Boxford");
    }

    #[test]
    fn exclusive_time_inside_span_data_i64() {
        let mut span = Annotated::from_json(
            r#"{
            "start_timestamp": 0,
            "timestamp": 1,
            "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
            "span_id": "922dda2462ea4ac2",
            "data": {
                "sentry.exclusive_time": 123
            }
        }"#,
        )
        .unwrap();

        normalize(&mut span, normalize_config()).unwrap();

        let data = get_value!(span.data!);
        assert_eq!(data.exclusive_time, Annotated::empty());
        assert_eq!(*get_value!(span.exclusive_time!), 123.0);
    }

    #[test]
    fn exclusive_time_inside_span_data_f64() {
        let mut span = Annotated::from_json(
            r#"{
            "start_timestamp": 0,
            "timestamp": 1,
            "trace_id": "922dda2462ea4ac2b6a4b339bee90863",
            "span_id": "922dda2462ea4ac2",
            "data": {
                "sentry.exclusive_time": 123.0
            }
        }"#,
        )
        .unwrap();

        normalize(&mut span, normalize_config()).unwrap();

        let data = get_value!(span.data!);
        assert_eq!(data.exclusive_time, Annotated::empty());
        assert_eq!(*get_value!(span.exclusive_time!), 123.0);
    }

    #[test]
    fn normalize_inp_spans() {
        let mut span = Annotated::from_json(
            r#"{
              "data": {
                "sentry.origin": "auto.http.browser.inp",
                "sentry.op": "ui.interaction.click",
                "release": "frontend@0735d75a05afe8d34bb0950f17c332eb32988862",
                "environment": "prod",
                "profile_id": "480ffcc911174ade9106b40ffbd822f5",
                "replay_id": "f39c5eb6539f4e49b9ad2b95226bc120",
                "transaction": "/replays",
                "user_agent.original": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "sentry.exclusive_time": 128.0
              },
              "description": "div.app-3diuwe.e88zkai6 > span.app-ksj0rb.e88zkai4",
              "op": "ui.interaction.click",
              "parent_span_id": "88457c3c28f4c0c6",
              "span_id": "be0e95480798a2a9",
              "start_timestamp": 1732635523.5048,
              "timestamp": 1732635523.6328,
              "trace_id": "bdaf4823d1c74068af238879e31e1be9",
              "origin": "auto.http.browser.inp",
              "exclusive_time": 128,
              "measurements": {
                "inp": {
                  "value": 128,
                  "unit": "millisecond"
                }
              },
              "segment_id": "88457c3c28f4c0c6"
        }"#,
        )
            .unwrap();

        normalize(&mut span, normalize_config()).unwrap();

        let data = get_value!(span.data!);

        assert_eq!(data.exclusive_time, Annotated::empty());
        assert_eq!(*get_value!(span.exclusive_time!), 128.0);

        assert_eq!(data.profile_id, Annotated::empty());
        assert_eq!(
            get_value!(span.profile_id!),
            &EventId("480ffcc911174ade9106b40ffbd822f5".parse().unwrap())
        );
    }
}
