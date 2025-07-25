//! Validation and normalization of [`Replay`] events.

use std::net::IpAddr as StdIpAddr;

use relay_event_schema::processor::{self, ProcessingState, Processor};
use relay_event_schema::protocol::{Contexts, IpAddr, Replay};
use relay_protocol::Annotated;

use crate::event::normalize_user_geoinfo;
use crate::normalize::user_agent;
use crate::user_agent::RawUserAgentInfo;
use crate::{GeoIpLookup, trimming};

/// Replay validation or normalization error.
///
/// This error is returned from [`validate`] and [`normalize`].
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    /// The Replay event could not be parsed from JSON.
    #[error("invalid json")]
    CouldNotParse(#[from] serde_json::Error),

    /// The Replay event was parsed but did not match the schema.
    #[error("no data found")]
    NoContent,

    /// The Replay contains invalid data or is missing a required field.
    ///
    /// This is returned from [`validate`].
    #[error("invalid payload {0}")]
    InvalidPayload(String),

    /// An error occurred during PII scrubbing of the Replay.
    ///
    /// This erorr is usually returned when the PII configuration fails to parse.
    #[error("failed to scrub PII: {0}")]
    CouldNotScrub(String),
}

/// Checks if the Replay event is structurally valid.
///
/// Returns `Ok(())`, if the Replay is valid and can be normalized. Otherwise, returns
/// `Err(ReplayError::InvalidPayload)` describing the missing or invalid data.
pub fn validate(replay: &Replay) -> Result<(), ReplayError> {
    replay
        .replay_id
        .value()
        .ok_or_else(|| ReplayError::InvalidPayload("missing replay_id".to_owned()))?;

    let segment_id = *replay
        .segment_id
        .value()
        .ok_or_else(|| ReplayError::InvalidPayload("missing segment_id".to_owned()))?;

    if segment_id > u16::MAX as u64 {
        return Err(ReplayError::InvalidPayload(
            "segment_id exceeded u16 limit".to_owned(),
        ));
    }

    if replay
        .error_ids
        .value()
        .into_iter()
        .flat_map(|v| v.iter())
        .any(|v| v.meta().has_errors())
    {
        return Err(ReplayError::InvalidPayload(
            "Invalid error-id specified.".to_owned(),
        ));
    }

    if replay
        .trace_ids
        .value()
        .into_iter()
        .flat_map(|v| v.iter())
        .any(|v| v.meta().has_errors())
    {
        return Err(ReplayError::InvalidPayload(
            "Invalid trace-id specified.".to_owned(),
        ));
    }

    Ok(())
}

/// Adds default fields and normalizes all values in to their standard representation.
pub fn normalize(
    replay: &mut Annotated<Replay>,
    client_ip: Option<StdIpAddr>,
    user_agent: RawUserAgentInfo<&str>,
    geoip_lookup: Option<&GeoIpLookup>,
) {
    let _ = processor::apply(replay, |replay_value, meta| {
        normalize_platform(replay_value);
        normalize_ip_address(replay_value, client_ip);
        if let Some(geoip_lookup) = geoip_lookup {
            normalize_user_geoinfo(
                geoip_lookup,
                &mut replay_value.user,
                client_ip.map(|ip| IpAddr(ip.to_string())).as_ref(),
            );
        }
        normalize_user_agent(replay_value, user_agent);
        normalize_type(replay_value);
        normalize_array_fields(replay_value);
        let _ = trimming::TrimmingProcessor::new().process_replay(
            replay_value,
            meta,
            ProcessingState::root(),
        );
        Ok(())
    });
}

fn normalize_array_fields(replay: &mut Replay) {
    // TODO: This should be replaced by the TrimmingProcessor.
    // https://github.com/getsentry/relay/pull/1910#pullrequestreview-1337188206
    if let Some(items) = replay.error_ids.value_mut() {
        items.truncate(100);
    }

    if let Some(items) = replay.trace_ids.value_mut() {
        items.truncate(100);
    }

    if let Some(items) = replay.urls.value_mut() {
        items.truncate(100);
    }
}

fn normalize_ip_address(replay: &mut Replay, ip_address: Option<StdIpAddr>) {
    crate::event::normalize_ip_addresses(
        &mut replay.request,
        &mut replay.user,
        replay.platform.as_str(),
        ip_address.map(|ip| IpAddr(ip.to_string())).as_ref(),
        replay.sdk.value(),
    );
}

fn normalize_user_agent(replay: &mut Replay, default_user_agent: RawUserAgentInfo<&str>) {
    let headers = match replay
        .request
        .value()
        .and_then(|request| request.headers.value())
    {
        Some(headers) => headers,
        None => return,
    };

    let user_agent_info = RawUserAgentInfo::from_headers(headers);

    let user_agent_info = if user_agent_info.is_empty() {
        default_user_agent
    } else {
        user_agent_info
    };

    let contexts = replay.contexts.get_or_insert_with(Contexts::new);
    user_agent::normalize_user_agent_info_generic(contexts, &replay.platform, &user_agent_info);
}

fn normalize_platform(replay: &mut Replay) {
    // Null platforms are permitted but must be defaulted before continuing.
    let platform = replay.platform.get_or_insert_with(|| "other".to_owned());

    // Normalize bad platforms to "other" type.
    if !crate::is_valid_platform(platform) {
        replay.platform = Annotated::from("other".to_owned());
    }
}

fn normalize_type(replay: &mut Replay) {
    replay.ty = Annotated::from("replay_event".to_owned());
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use chrono::{TimeZone, Utc};
    use insta::assert_json_snapshot;
    use relay_protocol::{SerializableAnnotated, assert_annotated_snapshot, get_value};
    use uuid::Uuid;

    use relay_event_schema::protocol::{
        BrowserContext, Context, DeviceContext, EventId, OsContext, TagEntry, Tags,
    };

    use super::*;

    #[test]
    fn test_event_roundtrip() {
        // NOTE: Interfaces will be tested separately.
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "replay_id": "52df9022835246eeb317dbd739ccd059",
  "segment_id": 0,
  "replay_type": "session",
  "error_sample_rate": 0.5,
  "session_sample_rate": 0.5,
  "timestamp": 946684800.0,
  "replay_start_timestamp": 946684800.0,
  "urls": ["localhost:9000"],
  "error_ids": ["52df9022835246eeb317dbd739ccd059"],
  "trace_ids": ["52df9022835246eeb317dbd739ccd059"],
  "platform": "myplatform",
  "release": "myrelease",
  "dist": "mydist",
  "environment": "myenv",
  "tags": [
    [
      "tag",
      "value"
    ]
  ]
}"#;

        let replay = Annotated::new(Replay {
            event_id: Annotated::new(EventId("52df9022835246eeb317dbd739ccd059".parse().unwrap())),
            replay_id: Annotated::new(EventId("52df9022835246eeb317dbd739ccd059".parse().unwrap())),
            replay_type: Annotated::new("session".to_owned()),
            segment_id: Annotated::new(0),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            replay_start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            urls: Annotated::new(vec![Annotated::new("localhost:9000".to_owned())]),
            error_ids: Annotated::new(vec![Annotated::new(
                Uuid::parse_str("52df9022835246eeb317dbd739ccd059").unwrap(),
            )]),
            trace_ids: Annotated::new(vec![Annotated::new(
                Uuid::parse_str("52df9022835246eeb317dbd739ccd059").unwrap(),
            )]),
            platform: Annotated::new("myplatform".to_owned()),
            release: Annotated::new("myrelease".to_owned().into()),
            dist: Annotated::new("mydist".to_owned()),
            environment: Annotated::new("myenv".to_owned()),
            tags: {
                let items = vec![Annotated::new(TagEntry(
                    Annotated::new("tag".to_owned()),
                    Annotated::new("value".to_owned()),
                ))];
                Annotated::new(Tags(items.into()))
            },
            ..Default::default()
        });

        assert_eq!(replay, Annotated::from_json(json).unwrap());
    }

    #[test]
    fn test_lenient_release() {
        let input = r#"{"release":42}"#;
        let output = r#"{"release":"42"}"#;
        let event = Annotated::new(Replay {
            release: Annotated::new("42".to_owned().into()),
            ..Default::default()
        });

        assert_eq!(event, Annotated::from_json(input).unwrap());
        assert_eq!(output, event.to_json().unwrap());
    }

    #[test]
    fn test_set_user_agent_meta() {
        // Parse user input.
        let payload = include_str!("../../tests/fixtures/replay.json");

        let mut replay: Annotated<Replay> = Annotated::from_json(payload).unwrap();
        normalize(&mut replay, None, RawUserAgentInfo::default(), None);

        let contexts = get_value!(replay.contexts!);
        assert_eq!(
            contexts.get::<BrowserContext>(),
            Some(&BrowserContext {
                name: Annotated::new("Safari".to_owned()),
                version: Annotated::new("15.5".to_owned()),
                ..Default::default()
            })
        );
        assert_eq!(
            contexts.get_key("client_os"),
            Some(&Context::Os(Box::new(OsContext {
                name: Annotated::new("Mac OS X".to_owned()),
                version: Annotated::new(">=10.15.7".to_owned()),
                ..Default::default()
            })))
        );
        assert_eq!(
            contexts.get::<DeviceContext>(),
            Some(&DeviceContext {
                family: Annotated::new("Mac".to_owned()),
                brand: Annotated::new("Apple".to_owned()),
                model: Annotated::new("Mac".to_owned()),
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_missing_user() {
        let payload = include_str!("../../tests/fixtures/replay_missing_user.json");

        let mut replay: Annotated<Replay> = Annotated::from_json(payload).unwrap();

        // No user object and no ip-address was provided.
        normalize(&mut replay, None, RawUserAgentInfo::default(), None);
        assert_eq!(get_value!(replay.user), None);

        // No user object but an ip-address was provided.
        let ip_address = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        normalize(
            &mut replay,
            Some(ip_address),
            RawUserAgentInfo::default(),
            None,
        );

        let ipaddr = get_value!(replay.user!).ip_address.as_str();
        assert_eq!(Some("127.0.0.1"), ipaddr);
    }

    #[test]
    fn test_set_ip_address_missing_user_ip_address_and_geo() {
        let lookup = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();
        let ip_address = IpAddr::V4(Ipv4Addr::new(2, 125, 160, 216));

        // IP-Address set.
        let payload = include_str!("../../tests/fixtures/replay_missing_user_ip_address.json");

        let mut replay: Annotated<Replay> = Annotated::from_json(payload).unwrap();
        normalize(
            &mut replay,
            Some(ip_address),
            RawUserAgentInfo::default(),
            Some(&lookup),
        );

        let user = &replay.value().unwrap().user;
        assert_json_snapshot!(SerializableAnnotated(user), @r###"
        {
          "id": "123",
          "email": "user@site.com",
          "ip_address": "2.125.160.216",
          "username": "user",
          "geo": {
            "country_code": "GB",
            "city": "Boxford",
            "subdivision": "England",
            "region": "United Kingdom"
          }
        }
        "###);
    }

    #[test]
    fn test_loose_type_requirements() {
        let payload = include_str!("../../tests/fixtures/replay_failure_22_08_31.json");

        let mut replay: Annotated<Replay> = Annotated::from_json(payload).unwrap();
        normalize(&mut replay, None, RawUserAgentInfo::default(), None);

        let user = get_value!(replay.user!);
        assert_eq!(user.ip_address.as_str(), Some("127.1.1.1"));
        assert_eq!(user.username.value(), None);
        assert_eq!(user.email.as_str(), Some("email@sentry.io"));
        assert_eq!(user.id.as_str(), Some("1"));
    }

    #[test]
    fn test_capped_values() {
        let urls: Vec<Annotated<String>> = (0..101)
            .map(|_| Annotated::new("localhost:9000".to_owned()))
            .collect();

        let error_ids: Vec<Annotated<Uuid>> = (0..101)
            .map(|_| Annotated::new(Uuid::parse_str("52df9022835246eeb317dbd739ccd059").unwrap()))
            .collect();

        let trace_ids: Vec<Annotated<Uuid>> = (0..101)
            .map(|_| Annotated::new(Uuid::parse_str("52df9022835246eeb317dbd739ccd059").unwrap()))
            .collect();

        let mut replay = Annotated::new(Replay {
            urls: Annotated::new(urls),
            error_ids: Annotated::new(error_ids),
            trace_ids: Annotated::new(trace_ids),
            ..Default::default()
        });

        let replay_value = replay.value_mut().as_mut().unwrap();
        normalize_array_fields(replay_value);

        assert!(replay_value.error_ids.value().unwrap().len() == 100);
        assert!(replay_value.trace_ids.value().unwrap().len() == 100);
        assert!(replay_value.urls.value().unwrap().len() == 100);
    }

    #[test]
    fn test_truncated_list_less_than_limit() {
        let mut replay = Annotated::new(Replay {
            urls: Annotated::new(Vec::new()),
            error_ids: Annotated::new(Vec::new()),
            trace_ids: Annotated::new(Vec::new()),
            ..Default::default()
        });

        let replay_value = replay.value_mut().as_mut().unwrap();
        normalize_array_fields(replay_value);

        assert!(replay_value.error_ids.value().unwrap().is_empty());
        assert!(replay_value.trace_ids.value().unwrap().is_empty());
        assert!(replay_value.urls.value().unwrap().is_empty());
    }

    #[test]
    fn test_error_id_validation() {
        // NOTE: Interfaces will be tested separately.
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "replay_id": "52df9022835246eeb317dbd739ccd059",
  "segment_id": 0,
  "replay_type": "session",
  "error_sample_rate": 0.5,
  "session_sample_rate": 0.5,
  "timestamp": 946684800.0,
  "replay_start_timestamp": 946684800.0,
  "urls": ["localhost:9000"],
  "error_ids": ["test"],
  "trace_ids": [],
  "platform": "myplatform",
  "release": "myrelease",
  "dist": "mydist",
  "environment": "myenv",
  "tags": [
    [
      "tag",
      "value"
    ]
  ]
}"#;

        let mut replay = Annotated::<Replay>::from_json(json).unwrap();
        let validation_result = validate(replay.value_mut().as_mut().unwrap());
        assert!(validation_result.is_err());
    }

    #[test]
    fn test_trace_id_validation() {
        // NOTE: Interfaces will be tested separately.
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "replay_id": "52df9022835246eeb317dbd739ccd059",
  "segment_id": 0,
  "replay_type": "session",
  "error_sample_rate": 0.5,
  "session_sample_rate": 0.5,
  "timestamp": 946684800.0,
  "replay_start_timestamp": 946684800.0,
  "urls": ["localhost:9000"],
  "error_ids": [],
  "trace_ids": ["123"],
  "platform": "myplatform",
  "release": "myrelease",
  "dist": "mydist",
  "environment": "myenv",
  "tags": [
    [
      "tag",
      "value"
    ]
  ]
}"#;

        let mut replay = Annotated::<Replay>::from_json(json).unwrap();
        let validation_result = validate(replay.value_mut().as_mut().unwrap());
        assert!(validation_result.is_err());
    }

    #[test]
    fn test_maxchars_trimming() {
        let json = format!(r#"{{"dist": "{}"}}"#, "0".repeat(100));
        let mut replay = Annotated::<Replay>::from_json(json.as_str()).unwrap();

        normalize(&mut replay, None, RawUserAgentInfo::default(), None);
        assert_annotated_snapshot!(replay, @r###"
        {
          "platform": "other",
          "dist": "0000000000000000000000000000000000000000000000000000000000000...",
          "type": "replay_event",
          "_meta": {
            "dist": {
              "": {
                "rem": [
                  [
                    "!limit",
                    "s",
                    61,
                    64
                  ]
                ],
                "len": 100
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_validate_u16_segment_id() {
        // Does not fit within a u16.
        let replay_id =
            Annotated::new(EventId("52df9022835246eeb317dbd739ccd059".parse().unwrap()));
        let segment_id: Annotated<u64> = Annotated::new(u16::MAX as u64 + 1);
        let mut replay = Annotated::new(Replay {
            replay_id,
            segment_id,
            ..Default::default()
        });
        assert!(validate(replay.value_mut().as_mut().unwrap()).is_err());

        // Fits within a u16.
        let replay_id =
            Annotated::new(EventId("52df9022835246eeb317dbd739ccd059".parse().unwrap()));
        let segment_id: Annotated<u64> = Annotated::new(u16::MAX as u64);
        let mut replay = Annotated::new(Replay {
            replay_id,
            segment_id,
            ..Default::default()
        });
        assert!(validate(replay.value_mut().as_mut().unwrap()).is_ok());
    }
}
