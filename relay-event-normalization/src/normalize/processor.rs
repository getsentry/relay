//! Event normalization processor.
//!
//! This processor is work in progress. The intention is to have a single
//! processor to deal with all event normalization. Currently, the normalization
//! logic is split across several processing steps running at different times
//! and under different conditions, like light normalization and store
//! processing. Having a single processor will make things simpler.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeSet;

use std::hash::{Hash, Hasher};
use std::mem;
use std::ops::Range;

use chrono::{DateTime, Duration, Utc};
use relay_base_schema::metrics::{is_valid_metric_name, DurationUnit, FractionUnit, MetricUnit};
use relay_common::time::UnixTimestamp;
use relay_event_schema::processor::{
    self, MaxChars, ProcessingAction, ProcessingResult, ProcessingState, Processor,
};
use relay_event_schema::protocol::{
    AsPair, Context, ContextInner, Contexts, DeviceClass, Event, EventType, Exception, Headers,
    IpAddr, LogEntry, Measurement, Measurements, NelContext, Request, SpanAttribute, SpanStatus,
    Tags, TraceContext, User,
};
use relay_protocol::{Annotated, Empty, Error, ErrorKind, Meta, Object, Value};
use smallvec::SmallVec;

use crate::normalize::{mechanism, stacktrace};
use crate::span::tag_extraction::{self, extract_span_tags};
use crate::timestamp::TimestampProcessor;
use crate::utils::{self, MAX_DURATION_MOBILE_MS};
use crate::{
    breakdowns, schema, span, transactions, trimming, user_agent, BreakdownsConfig,
    ClockDriftProcessor, DynamicMeasurementsConfig, GeoIpLookup, PerformanceScoreConfig,
    RawUserAgentInfo, SpanDescriptionRule, TransactionNameConfig,
};

/// Configuration for [`NormalizeProcessor`].
#[derive(Clone, Debug)]
pub struct NormalizeProcessorConfig<'a> {
    /// The IP address of the SDK that sent the event.
    ///
    /// When `{{auto}}` is specified and there is no other IP address in the payload, such as in the
    /// `request` context, this IP address gets added to the `user` context.
    pub client_ip: Option<&'a IpAddr>,

    /// The user-agent and client hints obtained from the submission request headers.
    ///
    /// Client hints are the preferred way to infer device, operating system, and browser
    /// information should the event payload contain no such data. If no client hints are present,
    /// normalization falls back to the user agent.
    pub user_agent: RawUserAgentInfo<&'a str>,

    /// The time at which the event was received in this Relay.
    ///
    /// This timestamp is persisted into the event payload.
    pub received_at: Option<DateTime<Utc>>,

    /// The maximum amount of seconds an event can be dated in the past.
    ///
    /// If the event's timestamp is older, the received timestamp is assumed.
    pub max_secs_in_past: Option<i64>,

    /// The maximum amount of seconds an event can be predated into the future.
    ///
    /// If the event's timestamp lies further into the future, the received timestamp is assumed.
    pub max_secs_in_future: Option<i64>,

    /// Timestamp range in which a transaction must end.
    ///
    /// Transactions that finish outside of this range are considered invalid.
    /// This check is skipped if no range is provided.
    pub transaction_range: Option<Range<UnixTimestamp>>,

    /// The maximum length for names of custom measurements.
    ///
    /// Measurements with longer names are removed from the transaction event and replaced with a
    /// metadata entry.
    pub max_name_and_unit_len: Option<usize>,

    /// Configuration for measurement normalization in transaction events.
    ///
    /// Has an optional [`crate::MeasurementsConfig`] from both the project and the global level.
    /// If at least one is provided, then normalization will truncate custom measurements
    /// and add units of known built-in measurements.
    pub measurements: Option<DynamicMeasurementsConfig<'a>>,

    /// Emit breakdowns based on given configuration.
    pub breakdowns_config: Option<&'a BreakdownsConfig>,

    /// When `Some(true)`, context information is extracted from the user agent.
    pub normalize_user_agent: Option<bool>,

    /// Configuration to apply to transaction names, especially around sanitizing.
    pub transaction_name_config: TransactionNameConfig<'a>,

    /// When `Some(true)`, it is assumed that the event has been normalized before.
    ///
    /// This disables certain normalizations, especially all that are not idempotent. The
    /// renormalize mode is intended for the use in the processing pipeline, so an event modified
    /// during ingestion can be validated against the schema and large data can be trimmed. However,
    /// advanced normalizations such as inferring contexts or clock drift correction are disabled.
    ///
    /// `None` equals to `false`.
    pub is_renormalize: bool,

    /// When `true`, infers the device class from CPU and model.
    pub device_class_synthesis_config: bool,

    /// When `true`, extracts tags from event and spans and materializes them into `span.data`.
    pub enrich_spans: bool,

    /// When `true`, computes and materializes attributes in spans based on the given configuration.
    pub light_normalize_spans: bool,

    /// The maximum allowed size of tag values in bytes. Longer values will be cropped.
    pub max_tag_value_length: usize, // TODO: move span related fields into separate config.

    /// Configuration for replacing identifiers in the span description with placeholders.
    ///
    /// This is similar to `transaction_name_config`, but applies to span descriptions.
    pub span_description_rules: Option<&'a Vec<SpanDescriptionRule>>,

    /// Configuration for generating performance score measurements for web vitals
    pub performance_score: Option<&'a PerformanceScoreConfig>,

    /// An initialized GeoIP lookup.
    pub geoip_lookup: Option<&'a GeoIpLookup>,

    /// When `Some(true)`, individual parts of the event payload is trimmed to a maximum size.
    ///
    /// See the event schema for size declarations.
    pub enable_trimming: bool,
}

impl<'a> Default for NormalizeProcessorConfig<'a> {
    fn default() -> Self {
        Self {
            client_ip: Default::default(),
            user_agent: Default::default(),
            received_at: Default::default(),
            max_secs_in_past: Default::default(),
            max_secs_in_future: Default::default(),
            transaction_range: Default::default(),
            max_name_and_unit_len: Default::default(),
            breakdowns_config: Default::default(),
            normalize_user_agent: Default::default(),
            transaction_name_config: Default::default(),
            is_renormalize: Default::default(),
            device_class_synthesis_config: Default::default(),
            enrich_spans: Default::default(),
            light_normalize_spans: Default::default(),
            max_tag_value_length: usize::MAX,
            span_description_rules: Default::default(),
            performance_score: Default::default(),
            geoip_lookup: Default::default(),
            enable_trimming: false,
            measurements: None,
        }
    }
}

/// Remove measurements that do not conform to the given config.
///
/// Built-in measurements are accepted if their unit is correct, dropped otherwise.
/// Custom measurements are accepted up to a limit.
///
/// Note that [`Measurements`] is a BTreeMap, which means its keys are sorted.
/// This ensures that for two events with the same measurement keys, the same set of custom
/// measurements is retained.
fn remove_invalid_measurements(
    measurements: &mut Measurements,
    meta: &mut Meta,
    measurements_config: DynamicMeasurementsConfig,
    max_name_and_unit_len: Option<usize>,
) {
    let max_custom_measurements = measurements_config.max_custom_measurements().unwrap_or(0);

    let mut custom_measurements_count = 0;
    let mut removed_measurements = Object::new();

    measurements.retain(|name, value| {
        let measurement = match value.value_mut() {
            Some(m) => m,
            None => return false,
        };

        if !is_valid_metric_name(name) {
            meta.add_error(Error::invalid(format!(
                "Metric name contains invalid characters: \"{name}\""
            )));
            removed_measurements.insert(name.clone(), Annotated::new(std::mem::take(measurement)));
            return false;
        }

        // TODO(jjbayer): Should we actually normalize the unit into the event?
        let unit = measurement.unit.value().unwrap_or(&MetricUnit::None);

        if let Some(max_name_and_unit_len) = max_name_and_unit_len {
            let max_name_len = max_name_and_unit_len - unit.to_string().len();

            if name.len() > max_name_len {
                meta.add_error(Error::invalid(format!(
                    "Metric name too long {}/{max_name_len}: \"{name}\"",
                    name.len(),
                )));
                removed_measurements
                    .insert(name.clone(), Annotated::new(std::mem::take(measurement)));
                return false;
            }
        }

        // Check if this is a builtin measurement:
        for builtin_measurement in measurements_config.builtin_measurement_keys() {
            if &builtin_measurement.name == name {
                // If the unit matches a built-in measurement, we allow it.
                // If the name matches but the unit is wrong, we do not even accept it as a custom measurement,
                // and just drop it instead.
                return &builtin_measurement.unit == unit;
            }
        }

        // For custom measurements, check the budget:
        if custom_measurements_count < max_custom_measurements {
            custom_measurements_count += 1;
            return true;
        }

        meta.add_error(Error::invalid(format!("Too many measurements: {name}")));
        removed_measurements.insert(name.clone(), Annotated::new(std::mem::take(measurement)));

        false
    });

    if !removed_measurements.is_empty() {
        meta.set_original_value(Some(removed_measurements));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use insta::assert_debug_snapshot;
    use relay_base_schema::metrics::{DurationUnit, MetricUnit};
    use relay_event_schema::protocol::{
        Contexts, Csp, DeviceContext, Event, Headers, IpAddr, Measurement, Measurements, Request,
        Tags,
    };
    use relay_protocol::{Annotated, Meta, Object, SerializableAnnotated};
    use serde_json::json;

    use crate::normalize::processor::{
        filter_mobile_outliers, normalize_app_start_measurements, normalize_device_class,
        normalize_dist, normalize_measurements, normalize_performance_score,
        normalize_security_report, normalize_units, remove_invalid_measurements,
    };
    use crate::{
        ClientHints, DynamicMeasurementsConfig, MeasurementsConfig, PerformanceScoreConfig,
        RawUserAgentInfo,
    };

    #[test]
    fn test_normalize_dist_none() {
        let mut dist = Annotated::default();
        normalize_dist(&mut dist).unwrap();
        assert_eq!(dist.value(), None);
    }

    #[test]
    fn test_normalize_dist_empty() {
        let mut dist = Annotated::new("".to_string());
        normalize_dist(&mut dist).unwrap();
        assert_eq!(dist.value(), None);
    }

    #[test]
    fn test_normalize_dist_trim() {
        let mut dist = Annotated::new(" foo  ".to_string());
        normalize_dist(&mut dist).unwrap();
        assert_eq!(dist.value(), Some(&"foo".to_string()));
    }

    #[test]
    fn test_normalize_dist_whitespace() {
        let mut dist = Annotated::new(" ".to_owned());
        normalize_dist(&mut dist).unwrap();
        assert_eq!(dist.value(), None);
    }

    #[test]
    fn test_computed_measurements() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "frames_slow": {"value": 1},
                "frames_frozen": {"value": 2},
                "frames_total": {"value": 4},
                "stall_total_time": {"value": 4000, "unit": "millisecond"}
            }
        }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        normalize_measurements(&mut event, None, None);

        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r#"
        {
          "type": "transaction",
          "timestamp": 1619420405.0,
          "start_timestamp": 1619420400.0,
          "measurements": {
            "frames_frozen": {
              "value": 2.0,
              "unit": "none",
            },
            "frames_frozen_rate": {
              "value": 0.5,
              "unit": "ratio",
            },
            "frames_slow": {
              "value": 1.0,
              "unit": "none",
            },
            "frames_slow_rate": {
              "value": 0.25,
              "unit": "ratio",
            },
            "frames_total": {
              "value": 4.0,
              "unit": "none",
            },
            "stall_percentage": {
              "value": 0.8,
              "unit": "ratio",
            },
            "stall_total_time": {
              "value": 4000.0,
              "unit": "millisecond",
            },
          },
        }
        "#);
    }
    #[test]
    fn test_filter_custom_measurements() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "my_custom_measurement_1": {"value": 123},
                "frames_frozen": {"value": 666, "unit": "invalid_unit"},
                "frames_slow": {"value": 1},
                "my_custom_measurement_3": {"value": 456},
                "my_custom_measurement_2": {"value": 789}
            }
        }
        "#;
        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        let project_measurement_config: MeasurementsConfig = serde_json::from_value(json!({
            "builtinMeasurements": [
                {"name": "frames_frozen", "unit": "none"},
                {"name": "frames_slow", "unit": "none"}
            ],
            "maxCustomMeasurements": 2,
            "stray_key": "zzz"
        }))
        .unwrap();

        let dynamic_measurement_config = DynamicMeasurementsConfig {
            project: Some(&project_measurement_config),
            global: None,
        };

        normalize_measurements(&mut event, Some(dynamic_measurement_config), None);

        // Only two custom measurements are retained, in alphabetic order (1 and 2)
        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r#"
        {
          "type": "transaction",
          "timestamp": 1619420405.0,
          "start_timestamp": 1619420400.0,
          "measurements": {
            "frames_slow": {
              "value": 1.0,
              "unit": "none",
            },
            "my_custom_measurement_1": {
              "value": 123.0,
              "unit": "none",
            },
            "my_custom_measurement_2": {
              "value": 789.0,
              "unit": "none",
            },
          },
          "_meta": {
            "measurements": {
              "": Meta(Some(MetaInner(
                err: [
                  [
                    "invalid_data",
                    {
                      "reason": "Too many measurements: my_custom_measurement_3",
                    },
                  ],
                ],
                val: Some({
                  "my_custom_measurement_3": {
                    "unit": "none",
                    "value": 456.0,
                  },
                }),
              ))),
            },
          },
        }
        "#);
    }

    #[test]
    fn test_normalize_units() {
        let mut measurements = Annotated::<Measurements>::from_json(
            r#"{
                "fcp": {"value": 1.1},
                "stall_count": {"value": 3.3},
                "foo": {"value": 8.8}
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();
        insta::assert_debug_snapshot!(measurements, @r#"
        Measurements(
            {
                "fcp": Measurement {
                    value: 1.1,
                    unit: ~,
                },
                "foo": Measurement {
                    value: 8.8,
                    unit: ~,
                },
                "stall_count": Measurement {
                    value: 3.3,
                    unit: ~,
                },
            },
        )
        "#);
        normalize_units(&mut measurements);
        insta::assert_debug_snapshot!(measurements, @r#"
        Measurements(
            {
                "fcp": Measurement {
                    value: 1.1,
                    unit: Duration(
                        MilliSecond,
                    ),
                },
                "foo": Measurement {
                    value: 8.8,
                    unit: None,
                },
                "stall_count": Measurement {
                    value: 3.3,
                    unit: None,
                },
            },
        )
        "#);
    }

    #[test]
    fn test_normalize_security_report() {
        let mut event = Event {
            csp: Annotated::from(Csp::default()),
            ..Default::default()
        };
        let ipaddr = IpAddr("213.164.1.114".to_string());

        let client_ip = Some(&ipaddr);
        let user_agent = RawUserAgentInfo::new_test_dummy();

        // This call should fill the event headers with info from the user_agent which is
        // tested below.
        normalize_security_report(&mut event, client_ip, &user_agent);

        let headers = event
            .request
            .value_mut()
            .get_or_insert_with(Request::default)
            .headers
            .value_mut()
            .get_or_insert_with(Headers::default);

        assert_eq!(
            event.user.value().unwrap().ip_address,
            Annotated::from(ipaddr)
        );
        assert_eq!(
            headers.get_header(RawUserAgentInfo::USER_AGENT),
            user_agent.user_agent
        );
        assert_eq!(
            headers.get_header(ClientHints::SEC_CH_UA),
            user_agent.client_hints.sec_ch_ua,
        );
        assert_eq!(
            headers.get_header(ClientHints::SEC_CH_UA_MODEL),
            user_agent.client_hints.sec_ch_ua_model,
        );
        assert_eq!(
            headers.get_header(ClientHints::SEC_CH_UA_PLATFORM),
            user_agent.client_hints.sec_ch_ua_platform,
        );
        assert_eq!(
            headers.get_header(ClientHints::SEC_CH_UA_PLATFORM_VERSION),
            user_agent.client_hints.sec_ch_ua_platform_version,
        );

        assert!(
            std::mem::size_of_val(&ClientHints::<&str>::default()) == 64,
            "If you add new fields, update the test accordingly"
        );
    }

    #[test]
    fn test_no_device_class() {
        let mut event = Event {
            ..Default::default()
        };
        normalize_device_class(&mut event);
        let tags = &event.tags.value_mut().get_or_insert_with(Tags::default).0;
        assert_eq!(None, tags.get("device_class"));
    }

    #[test]
    fn test_apple_low_device_class() {
        let mut event = Event {
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(DeviceContext {
                    family: "iPhone".to_string().into(),
                    model: "iPhone8,4".to_string().into(),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        };
        normalize_device_class(&mut event);
        assert_debug_snapshot!(event.tags, @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "device.class",
                        "1",
                    ),
                ],
            ),
        )
        "#);
    }

    #[test]
    fn test_apple_medium_device_class() {
        let mut event = Event {
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(DeviceContext {
                    family: "iPhone".to_string().into(),
                    model: "iPhone12,8".to_string().into(),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        };
        normalize_device_class(&mut event);
        assert_debug_snapshot!(event.tags, @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "device.class",
                        "2",
                    ),
                ],
            ),
        )
        "#);
    }

    #[test]
    fn test_android_low_device_class() {
        let mut event = Event {
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(DeviceContext {
                    family: "android".to_string().into(),
                    processor_frequency: 1000.into(),
                    processor_count: 6.into(),
                    memory_size: (2 * 1024 * 1024 * 1024).into(),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        };
        normalize_device_class(&mut event);
        assert_debug_snapshot!(event.tags, @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "device.class",
                        "1",
                    ),
                ],
            ),
        )
        "#);
    }

    #[test]
    fn test_android_medium_device_class() {
        let mut event = Event {
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(DeviceContext {
                    family: "android".to_string().into(),
                    processor_frequency: 2000.into(),
                    processor_count: 8.into(),
                    memory_size: (6 * 1024 * 1024 * 1024).into(),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        };
        normalize_device_class(&mut event);
        assert_debug_snapshot!(event.tags, @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "device.class",
                        "2",
                    ),
                ],
            ),
        )
        "#);
    }

    #[test]
    fn test_android_high_device_class() {
        let mut event = Event {
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(DeviceContext {
                    family: "android".to_string().into(),
                    processor_frequency: 2500.into(),
                    processor_count: 8.into(),
                    memory_size: (6 * 1024 * 1024 * 1024).into(),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        };
        normalize_device_class(&mut event);
        assert_debug_snapshot!(event.tags, @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "device.class",
                        "3",
                    ),
                ],
            ),
        )
        "#);
    }

    #[test]
    fn test_keeps_valid_measurement() {
        let name = "lcp";
        let measurement = Measurement {
            value: Annotated::new(420.69),
            unit: Annotated::new(MetricUnit::Duration(DurationUnit::MilliSecond)),
        };

        assert!(!is_measurement_dropped(name, measurement));
    }

    #[test]
    fn test_drops_too_long_measurement_names() {
        let name = "lcpppppppppppppppppppppppppppp";
        let measurement = Measurement {
            value: Annotated::new(420.69),
            unit: Annotated::new(MetricUnit::Duration(DurationUnit::MilliSecond)),
        };

        assert!(is_measurement_dropped(name, measurement));
    }

    #[test]
    fn test_drops_measurements_with_invalid_characters() {
        let name = "i æm frøm nørwåy";
        let measurement = Measurement {
            value: Annotated::new(420.69),
            unit: Annotated::new(MetricUnit::Duration(DurationUnit::MilliSecond)),
        };

        assert!(is_measurement_dropped(name, measurement));
    }

    fn is_measurement_dropped(name: &str, measurement: Measurement) -> bool {
        let max_name_and_unit_len = Some(30);

        let mut measurements: BTreeMap<String, Annotated<Measurement>> = Object::new();
        measurements.insert(name.to_string(), Annotated::new(measurement));

        let mut measurements = Measurements(measurements);
        let mut meta = Meta::default();
        let measurements_config = MeasurementsConfig {
            max_custom_measurements: 1,
            ..Default::default()
        };

        let dynamic_config = DynamicMeasurementsConfig {
            project: Some(&measurements_config),
            global: None,
        };

        // Just for clarity.
        // Checks that there is 1 measurement before processing.
        assert_eq!(measurements.len(), 1);

        remove_invalid_measurements(
            &mut measurements,
            &mut meta,
            dynamic_config,
            max_name_and_unit_len,
        );

        // Checks whether the measurement is dropped.
        measurements.len() == 0
    }

    #[test]
    fn test_normalize_app_start_measurements_does_not_add_measurements() {
        let mut measurements = Annotated::<Measurements>::from_json(r###"{}"###)
            .unwrap()
            .into_value()
            .unwrap();
        insta::assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {},
        )
        "###);
        normalize_app_start_measurements(&mut measurements);
        insta::assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {},
        )
        "###);
    }

    #[test]
    fn test_normalize_app_start_cold_measurements() {
        let mut measurements =
            Annotated::<Measurements>::from_json(r#"{"app.start.cold": {"value": 1.1}}"#)
                .unwrap()
                .into_value()
                .unwrap();
        insta::assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {
                "app.start.cold": Measurement {
                    value: 1.1,
                    unit: ~,
                },
            },
        )
        "###);
        normalize_app_start_measurements(&mut measurements);
        insta::assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {
                "app_start_cold": Measurement {
                    value: 1.1,
                    unit: ~,
                },
            },
        )
        "###);
    }

    #[test]
    fn test_normalize_app_start_warm_measurements() {
        let mut measurements =
            Annotated::<Measurements>::from_json(r#"{"app.start.warm": {"value": 1.1}}"#)
                .unwrap()
                .into_value()
                .unwrap();
        insta::assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {
                "app.start.warm": Measurement {
                    value: 1.1,
                    unit: ~,
                },
            },
        )
        "###);
        normalize_app_start_measurements(&mut measurements);
        insta::assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {
                "app_start_warm": Measurement {
                    value: 1.1,
                    unit: ~,
                },
            },
        )
        "###);
    }

    #[test]
    fn test_apple_high_device_class() {
        let mut event = Event {
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(DeviceContext {
                    family: "iPhone".to_string().into(),
                    model: "iPhone15,3".to_string().into(),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        };
        normalize_device_class(&mut event);
        assert_debug_snapshot!(event.tags, @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "device.class",
                        "3",
                    ),
                ],
            ),
        )
        "#);
    }

    #[test]
    fn test_filter_mobile_outliers() {
        let mut measurements =
            Annotated::<Measurements>::from_json(r#"{"app_start_warm": {"value": 180001}}"#)
                .unwrap()
                .into_value()
                .unwrap();
        assert_eq!(measurements.len(), 1);
        filter_mobile_outliers(&mut measurements);
        assert_eq!(measurements.len(), 0);
    }

    #[test]
    fn test_computed_performance_score() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "fid": {"value": 213, "unit": "millisecond"},
                "fcp": {"value": 1237, "unit": "millisecond"},
                "lcp": {"value": 6596, "unit": "millisecond"},
                "cls": {"value": 0.11}
            },
            "contexts": {
                "browser": {
                    "name": "Chrome",
                    "version": "120.1.1",
                    "type": "browser"
                }
            }
        }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        let performance_score: PerformanceScoreConfig = serde_json::from_value(json!({
            "profiles": [
                {
                    "name": "Desktop",
                    "scoreComponents": [
                        {
                            "measurement": "fcp",
                            "weight": 0.15,
                            "p10": 900,
                            "p50": 1600
                        },
                        {
                            "measurement": "lcp",
                            "weight": 0.30,
                            "p10": 1200,
                            "p50": 2400
                        },
                        {
                            "measurement": "fid",
                            "weight": 0.30,
                            "p10": 100,
                            "p50": 300
                        },
                        {
                            "measurement": "cls",
                            "weight": 0.25,
                            "p10": 0.1,
                            "p50": 0.25
                        },
                        {
                            "measurement": "ttfb",
                            "weight": 0.0,
                            "p10": 0.2,
                            "p50": 0.4
                        },
                    ],
                    "condition": {
                        "op":"eq",
                        "name": "event.contexts.browser.name",
                        "value": "Chrome"
                    }
                }
            ]
        }))
        .unwrap();

        normalize_performance_score(&mut event, Some(&performance_score));

        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r###"
        {
          "type": "transaction",
          "timestamp": 1619420405.0,
          "start_timestamp": 1619420400.0,
          "contexts": {
            "browser": {
              "name": "Chrome",
              "version": "120.1.1",
              "type": "browser",
            },
          },
          "measurements": {
            "cls": {
              "value": 0.11,
            },
            "fcp": {
              "value": 1237.0,
              "unit": "millisecond",
            },
            "fid": {
              "value": 213.0,
              "unit": "millisecond",
            },
            "lcp": {
              "value": 6596.0,
              "unit": "millisecond",
            },
            "score.cls": {
              "value": 0.21864170607444863,
              "unit": "ratio",
            },
            "score.fcp": {
              "value": 0.10750855443790831,
              "unit": "ratio",
            },
            "score.fid": {
              "value": 0.19657361348282545,
              "unit": "ratio",
            },
            "score.lcp": {
              "value": 0.009238896571386584,
              "unit": "ratio",
            },
            "score.total": {
              "value": 0.531962770566569,
              "unit": "ratio",
            },
            "score.weight.cls": {
              "value": 0.25,
              "unit": "ratio",
            },
            "score.weight.fcp": {
              "value": 0.15,
              "unit": "ratio",
            },
            "score.weight.fid": {
              "value": 0.3,
              "unit": "ratio",
            },
            "score.weight.lcp": {
              "value": 0.3,
              "unit": "ratio",
            },
            "score.weight.ttfb": {
              "value": 0.0,
              "unit": "ratio",
            },
          },
        }
        "###);
    }

    // Test performance score is calculated correctly when the sum of weights is under 1.
    // The expected result should normalize the weights to a sum of 1 and scale the weight measurements accordingly.
    #[test]
    fn test_computed_performance_score_with_under_normalized_weights() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "fid": {"value": 213, "unit": "millisecond"},
                "fcp": {"value": 1237, "unit": "millisecond"},
                "lcp": {"value": 6596, "unit": "millisecond"},
                "cls": {"value": 0.11}
            },
            "contexts": {
                "browser": {
                    "name": "Chrome",
                    "version": "120.1.1",
                    "type": "browser"
                }
            }
        }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        let performance_score: PerformanceScoreConfig = serde_json::from_value(json!({
            "profiles": [
                {
                    "name": "Desktop",
                    "scoreComponents": [
                        {
                            "measurement": "fcp",
                            "weight": 0.03,
                            "p10": 900,
                            "p50": 1600
                        },
                        {
                            "measurement": "lcp",
                            "weight": 0.06,
                            "p10": 1200,
                            "p50": 2400
                        },
                        {
                            "measurement": "fid",
                            "weight": 0.06,
                            "p10": 100,
                            "p50": 300
                        },
                        {
                            "measurement": "cls",
                            "weight": 0.05,
                            "p10": 0.1,
                            "p50": 0.25
                        },
                        {
                            "measurement": "ttfb",
                            "weight": 0.0,
                            "p10": 0.2,
                            "p50": 0.4
                        },
                    ],
                    "condition": {
                        "op":"eq",
                        "name": "event.contexts.browser.name",
                        "value": "Chrome"
                    }
                }
            ]
        }))
        .unwrap();

        normalize_performance_score(&mut event, Some(&performance_score));

        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r###"
        {
          "type": "transaction",
          "timestamp": 1619420405.0,
          "start_timestamp": 1619420400.0,
          "contexts": {
            "browser": {
              "name": "Chrome",
              "version": "120.1.1",
              "type": "browser",
            },
          },
          "measurements": {
            "cls": {
              "value": 0.11,
            },
            "fcp": {
              "value": 1237.0,
              "unit": "millisecond",
            },
            "fid": {
              "value": 213.0,
              "unit": "millisecond",
            },
            "lcp": {
              "value": 6596.0,
              "unit": "millisecond",
            },
            "score.cls": {
              "value": 0.21864170607444863,
              "unit": "ratio",
            },
            "score.fcp": {
              "value": 0.10750855443790831,
              "unit": "ratio",
            },
            "score.fid": {
              "value": 0.19657361348282545,
              "unit": "ratio",
            },
            "score.lcp": {
              "value": 0.009238896571386584,
              "unit": "ratio",
            },
            "score.total": {
              "value": 0.531962770566569,
              "unit": "ratio",
            },
            "score.weight.cls": {
              "value": 0.25,
              "unit": "ratio",
            },
            "score.weight.fcp": {
              "value": 0.15,
              "unit": "ratio",
            },
            "score.weight.fid": {
              "value": 0.3,
              "unit": "ratio",
            },
            "score.weight.lcp": {
              "value": 0.3,
              "unit": "ratio",
            },
            "score.weight.ttfb": {
              "value": 0.0,
              "unit": "ratio",
            },
          },
        }
        "###);
    }

    // Test performance score is calculated correctly when the sum of weights is over 1.
    // The expected result should normalize the weights to a sum of 1 and scale the weight measurements accordingly.
    #[test]
    fn test_computed_performance_score_with_over_normalized_weights() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "fid": {"value": 213, "unit": "millisecond"},
                "fcp": {"value": 1237, "unit": "millisecond"},
                "lcp": {"value": 6596, "unit": "millisecond"},
                "cls": {"value": 0.11}
            },
            "contexts": {
                "browser": {
                    "name": "Chrome",
                    "version": "120.1.1",
                    "type": "browser"
                }
            }
        }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        let performance_score: PerformanceScoreConfig = serde_json::from_value(json!({
            "profiles": [
                {
                    "name": "Desktop",
                    "scoreComponents": [
                        {
                            "measurement": "fcp",
                            "weight": 0.30,
                            "p10": 900,
                            "p50": 1600
                        },
                        {
                            "measurement": "lcp",
                            "weight": 0.60,
                            "p10": 1200,
                            "p50": 2400
                        },
                        {
                            "measurement": "fid",
                            "weight": 0.60,
                            "p10": 100,
                            "p50": 300
                        },
                        {
                            "measurement": "cls",
                            "weight": 0.50,
                            "p10": 0.1,
                            "p50": 0.25
                        },
                        {
                            "measurement": "ttfb",
                            "weight": 0.0,
                            "p10": 0.2,
                            "p50": 0.4
                        },
                    ],
                    "condition": {
                        "op":"eq",
                        "name": "event.contexts.browser.name",
                        "value": "Chrome"
                    }
                }
            ]
        }))
        .unwrap();

        normalize_performance_score(&mut event, Some(&performance_score));

        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r###"
        {
          "type": "transaction",
          "timestamp": 1619420405.0,
          "start_timestamp": 1619420400.0,
          "contexts": {
            "browser": {
              "name": "Chrome",
              "version": "120.1.1",
              "type": "browser",
            },
          },
          "measurements": {
            "cls": {
              "value": 0.11,
            },
            "fcp": {
              "value": 1237.0,
              "unit": "millisecond",
            },
            "fid": {
              "value": 213.0,
              "unit": "millisecond",
            },
            "lcp": {
              "value": 6596.0,
              "unit": "millisecond",
            },
            "score.cls": {
              "value": 0.21864170607444863,
              "unit": "ratio",
            },
            "score.fcp": {
              "value": 0.10750855443790831,
              "unit": "ratio",
            },
            "score.fid": {
              "value": 0.19657361348282545,
              "unit": "ratio",
            },
            "score.lcp": {
              "value": 0.009238896571386584,
              "unit": "ratio",
            },
            "score.total": {
              "value": 0.531962770566569,
              "unit": "ratio",
            },
            "score.weight.cls": {
              "value": 0.25,
              "unit": "ratio",
            },
            "score.weight.fcp": {
              "value": 0.15,
              "unit": "ratio",
            },
            "score.weight.fid": {
              "value": 0.3,
              "unit": "ratio",
            },
            "score.weight.lcp": {
              "value": 0.3,
              "unit": "ratio",
            },
            "score.weight.ttfb": {
              "value": 0.0,
              "unit": "ratio",
            },
          },
        }
        "###);
    }

    #[test]
    fn test_computed_performance_score_missing_measurement() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "a": {"value": 213, "unit": "millisecond"}
            },
            "contexts": {
                "browser": {
                    "name": "Chrome",
                    "version": "120.1.1",
                    "type": "browser"
                }
            }
        }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        let performance_score: PerformanceScoreConfig = serde_json::from_value(json!({
            "profiles": [
                {
                    "name": "Desktop",
                    "scoreComponents": [
                        {
                            "measurement": "a",
                            "weight": 0.15,
                            "p10": 900,
                            "p50": 1600
                        },
                        {
                            "measurement": "b",
                            "weight": 0.30,
                            "p10": 1200,
                            "p50": 2400
                        },
                    ],
                    "condition": {
                        "op":"eq",
                        "name": "event.contexts.browser.name",
                        "value": "Chrome"
                    }
                }
            ]
        }))
        .unwrap();

        normalize_performance_score(&mut event, Some(&performance_score));

        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r###"
        {
          "type": "transaction",
          "timestamp": 1619420405.0,
          "start_timestamp": 1619420400.0,
          "contexts": {
            "browser": {
              "name": "Chrome",
              "version": "120.1.1",
              "type": "browser",
            },
          },
          "measurements": {
            "a": {
              "value": 213.0,
              "unit": "millisecond",
            },
          },
        }
        "###);
    }
}
