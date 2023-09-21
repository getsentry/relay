//! Contextual information stored on traces.
//!
//! [`DynamicSamplingContext`] (DSC) contains properties associated to traces that are shared
//! between individual data submissions to Sentry. These properties are supposed to not change
//! during the lifetime of the trace.
//!
//! The information in the DSC is used to compute a deterministic sampling decision without access
//! to all individual data in the trace.

use std::collections::BTreeMap;
use std::fmt;

use relay_base_schema::events::EventType;
use relay_base_schema::project::ProjectKey;
use relay_event_schema::protocol::{Event, TraceContext};
use relay_protocol::{Getter, Val};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// DynamicSamplingContext created by the first Sentry SDK in the call chain.
///
/// Because SDKs need to funnel this data through the baggage header, this needs to be
/// representable as `HashMap<String, String>`, meaning no nested dictionaries/objects, arrays or
/// other non-string values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicSamplingContext {
    /// ID created by clients to represent the current call flow.
    pub trace_id: Uuid,
    /// The project key.
    pub public_key: ProjectKey,
    /// The release.
    #[serde(default)]
    pub release: Option<String>,
    /// The environment.
    #[serde(default)]
    pub environment: Option<String>,
    /// The name of the transaction extracted from the `transaction` field in the starting
    /// transaction.
    ///
    /// Set on transaction start, or via `scope.transaction`.
    #[serde(default)]
    pub transaction: Option<String>,
    /// The sample rate with which this trace was sampled in the client. This is a float between
    /// `0.0` and `1.0`.
    #[serde(
        default,
        with = "sample_rate_as_string",
        skip_serializing_if = "Option::is_none"
    )]
    pub sample_rate: Option<f64>,
    /// The user specific identifier (e.g. a user segment, or similar created by the SDK from the
    /// user object).
    #[serde(flatten, default)]
    pub user: TraceUserContext,
    /// If the event occurred during a session replay, the associated replay_id is added to the DSC.
    pub replay_id: Option<Uuid>,
    /// Set to true if the transaction starting the trace has been kept by client side sampling.
    #[serde(
        default,
        deserialize_with = "deserialize_bool_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub sampled: Option<bool>,
    /// Additional arbitrary fields for forwards compatibility.
    #[serde(flatten, default)]
    pub other: BTreeMap<String, Value>,
}

impl DynamicSamplingContext {
    /// Computes a dynamic sampling context from a transaction event.
    ///
    /// Returns `None` if the passed event is not a transaction event, or if it does not contain a
    /// trace ID in its trace context. All optional fields in the dynamic sampling context are
    /// populated with the corresponding attributes from the event payload if they are available.
    ///
    /// Since sampling information is not available in the event payload, the `sample_rate` field
    /// cannot be set when computing the dynamic sampling context from a transaction event.
    pub fn from_transaction(public_key: ProjectKey, event: &Event) -> Option<Self> {
        if event.ty.value() != Some(&EventType::Transaction) {
            return None;
        }

        let Some(trace) = event.context::<TraceContext>() else {
            return None;
        };
        let trace_id = trace.trace_id.value()?.0.parse().ok()?;
        let user = event.user.value();

        Some(Self {
            trace_id,
            public_key,
            release: event.release.as_str().map(str::to_owned),
            environment: event.environment.value().cloned(),
            transaction: event.transaction.value().cloned(),
            replay_id: None,
            sample_rate: None,
            user: TraceUserContext {
                user_segment: user
                    .and_then(|u| u.segment.value().cloned())
                    .unwrap_or_default(),
                user_id: user
                    .and_then(|u| u.id.as_str())
                    .unwrap_or_default()
                    .to_owned(),
            },
            sampled: None,
            other: Default::default(),
        })
    }
}

impl Getter for DynamicSamplingContext {
    fn get_value(&self, path: &str) -> Option<Val<'_>> {
        Some(match path.strip_prefix("trace.")? {
            "release" => self.release.as_deref()?.into(),
            "environment" => self.environment.as_deref()?.into(),
            "user.id" => or_none(&self.user.user_id)?.into(),
            "user.segment" => or_none(&self.user.user_segment)?.into(),
            "transaction" => self.transaction.as_deref()?.into(),
            "replay_id" => self.replay_id?.into(),
            _ => return None,
        })
    }
}

fn or_none(string: &impl AsRef<str>) -> Option<&str> {
    match string.as_ref() {
        "" => None,
        other => Some(other),
    }
}

/// User-related information in a [`DynamicSamplingContext`].
#[derive(Debug, Clone, Serialize, Default)]
pub struct TraceUserContext {
    /// The value of the `user.segment` property.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user_segment: String,

    /// The value of the `user.id` property.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user_id: String,
}

impl<'de> Deserialize<'de> for TraceUserContext {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize, Default)]
        struct Nested {
            #[serde(default)]
            pub segment: String,
            #[serde(default)]
            pub id: String,
        }

        #[derive(Deserialize)]
        struct Helper {
            // Nested implements default, but we used to accept user=null (not sure if any SDK
            // sends this though)
            #[serde(default)]
            user: Option<Nested>,
            #[serde(default)]
            user_segment: String,
            #[serde(default)]
            user_id: String,
        }

        let helper = Helper::deserialize(deserializer)?;

        if helper.user_id.is_empty() && helper.user_segment.is_empty() {
            let user = helper.user.unwrap_or_default();
            Ok(TraceUserContext {
                user_segment: user.segment,
                user_id: user.id,
            })
        } else {
            Ok(TraceUserContext {
                user_segment: helper.user_segment,
                user_id: helper.user_id,
            })
        }
    }
}

mod sample_rate_as_string {
    use std::borrow::Cow;

    use serde::{Deserialize, Serialize};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = match Option::<Cow<'_, str>>::deserialize(deserializer)? {
            Some(value) => value,
            None => return Ok(None),
        };

        let parsed_value =
            serde_json::from_str(&value).map_err(|e| serde::de::Error::custom(e.to_string()))?;

        if parsed_value < 0.0 {
            return Err(serde::de::Error::custom("sample rate cannot be negative"));
        }

        Ok(Some(parsed_value))
    }

    pub fn serialize<S>(value: &Option<f64>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match value {
            Some(float) => serde_json::to_string(float)
                .map_err(|e| serde::ser::Error::custom(e.to_string()))?
                .serialize(serializer),
            None => value.serialize(serializer),
        }
    }
}

struct BoolOptionVisitor;

impl<'de> serde::de::Visitor<'de> for BoolOptionVisitor {
    type Value = Option<bool>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("`true` or `false` as boolean or string")
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Some(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(match v {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        })
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(None)
    }
}

fn deserialize_bool_option<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserializer.deserialize_any(BoolOptionVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    /*
    #[test]
    fn test_adjust_sample_rate() {
        assert_eq!(adjusted_sample_rate(0.0, 0.5), 0.5);
        assert_eq!(adjusted_sample_rate(1.0, 0.5), 0.5);
        assert_eq!(adjusted_sample_rate(0.1, 0.5), 1.0);
        assert_eq!(adjusted_sample_rate(0.5, 0.1), 0.2);
        assert_eq!(adjusted_sample_rate(-0.5, 0.5), 0.5);
    }
    */

    #[test]
    fn parse_full() {
        let json = include_str!("../tests/fixtures/dynamic_sampling_context.json");
        serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
    }

    #[test]
    /// Test parse user
    fn parse_user() {
        let jsons = [
            r#"{
                "trace_id": "00000000-0000-0000-0000-000000000000",
                "public_key": "abd0f232775f45feab79864e580d160b",
                "user": {
                    "id": "some-id",
                    "segment": "all"
                }
            }"#,
            r#"{
                "trace_id": "00000000-0000-0000-0000-000000000000",
                "public_key": "abd0f232775f45feab79864e580d160b",
                "user_id": "some-id",
                "user_segment": "all"
            }"#,
            // testing some edgecases to see whether they behave as expected, but we don't actually
            // rely on this behavior anywhere (ignoring Hyrum's law). it would be fine for them to
            // change, we just have to be conscious about it.
            r#"{
                "trace_id": "00000000-0000-0000-0000-000000000000",
                "public_key": "abd0f232775f45feab79864e580d160b",
                "user_id": "",
                "user_segment": "",
                "user": {
                    "id": "some-id",
                    "segment": "all"
                }
            }"#,
            r#"{
                "trace_id": "00000000-0000-0000-0000-000000000000",
                "public_key": "abd0f232775f45feab79864e580d160b",
                "user_id": "some-id",
                "user_segment": "all",
                "user": {
                    "id": "bogus-id",
                    "segment": "nothing"
                }
            }"#,
            r#"{
                "trace_id": "00000000-0000-0000-0000-000000000000",
                "public_key": "abd0f232775f45feab79864e580d160b",
                "user_id": "some-id",
                "user_segment": "all",
                "user": null
            }"#,
        ];

        for json in jsons {
            let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
            assert_eq!(dsc.user.user_id, "some-id");
            assert_eq!(dsc.user.user_segment, "all");
        }
    }

    #[test]
    fn test_parse_user_partial() {
        // in that case we might have two different sdks merging data and we at least shouldn't mix
        // data together
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "user": {
                "segment": "all"
            }
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        insta::assert_ron_snapshot!(dsc, @r#"
        {
          "trace_id": "00000000-0000-0000-0000-000000000000",
          "public_key": "abd0f232775f45feab79864e580d160b",
          "release": None,
          "environment": None,
          "transaction": None,
          "user_id": "hello",
          "replay_id": None,
        }
        "#);
    }

    #[test]
    fn test_parse_sample_rate() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sample_rate": "0.5"
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        insta::assert_ron_snapshot!(dsc, @r#"
        {
          "trace_id": "00000000-0000-0000-0000-000000000000",
          "public_key": "abd0f232775f45feab79864e580d160b",
          "release": None,
          "environment": None,
          "transaction": None,
          "sample_rate": "0.5",
          "user_id": "hello",
          "replay_id": None,
        }
        "#);
    }

    #[test]
    fn test_parse_sample_rate_scientific_notation() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sample_rate": "1e-5"
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        insta::assert_ron_snapshot!(dsc, @r#"
        {
          "trace_id": "00000000-0000-0000-0000-000000000000",
          "public_key": "abd0f232775f45feab79864e580d160b",
          "release": None,
          "environment": None,
          "transaction": None,
          "sample_rate": "0.00001",
          "user_id": "hello",
          "replay_id": None,
        }
        "#);
    }

    #[test]
    fn test_parse_sample_rate_bogus() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sample_rate": "bogus"
        }
        "#;
        serde_json::from_str::<DynamicSamplingContext>(json).unwrap_err();
    }

    #[test]
    fn test_parse_sample_rate_number() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sample_rate": 0.1
        }
        "#;
        serde_json::from_str::<DynamicSamplingContext>(json).unwrap_err();
    }

    #[test]
    fn test_parse_sample_rate_negative() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sample_rate": "-0.1"
        }
        "#;
        serde_json::from_str::<DynamicSamplingContext>(json).unwrap_err();
    }

    #[test]
    fn test_parse_sampled_with_incoming_boolean() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sampled": true
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        let dsc_as_json = serde_json::to_string_pretty(&dsc).unwrap();
        let expected_json = r#"{
  "trace_id": "00000000-0000-0000-0000-000000000000",
  "public_key": "abd0f232775f45feab79864e580d160b",
  "release": null,
  "environment": null,
  "transaction": null,
  "user_id": "hello",
  "replay_id": null,
  "sampled": true
}"#;

        assert_eq!(dsc_as_json, expected_json);
    }

    #[test]
    fn test_parse_sampled_with_incoming_boolean_as_string() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sampled": "false"
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        let dsc_as_json = serde_json::to_string_pretty(&dsc).unwrap();
        let expected_json = r#"{
  "trace_id": "00000000-0000-0000-0000-000000000000",
  "public_key": "abd0f232775f45feab79864e580d160b",
  "release": null,
  "environment": null,
  "transaction": null,
  "user_id": "hello",
  "replay_id": null,
  "sampled": false
}"#;

        assert_eq!(dsc_as_json, expected_json);
    }

    #[test]
    fn test_parse_sampled_with_incoming_invalid_boolean_as_string() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sampled": "tru"
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        let dsc_as_json = serde_json::to_string_pretty(&dsc).unwrap();
        let expected_json = r#"{
  "trace_id": "00000000-0000-0000-0000-000000000000",
  "public_key": "abd0f232775f45feab79864e580d160b",
  "release": null,
  "environment": null,
  "transaction": null,
  "user_id": "hello",
  "replay_id": null
}"#;

        assert_eq!(dsc_as_json, expected_json);
    }

    #[test]
    fn test_parse_sampled_with_incoming_null_value() {
        let json = r#"
        {
            "trace_id": "00000000-0000-0000-0000-000000000000",
            "public_key": "abd0f232775f45feab79864e580d160b",
            "user_id": "hello",
            "sampled": null
        }
        "#;
        let dsc = serde_json::from_str::<DynamicSamplingContext>(json).unwrap();
        let dsc_as_json = serde_json::to_string_pretty(&dsc).unwrap();
        let expected_json = r#"{
  "trace_id": "00000000-0000-0000-0000-000000000000",
  "public_key": "abd0f232775f45feab79864e580d160b",
  "release": null,
  "environment": null,
  "transaction": null,
  "user_id": "hello",
  "replay_id": null
}"#;

        assert_eq!(dsc_as_json, expected_json);
    }

    #[test]
    fn getter_filled() {
        let replay_id = Uuid::new_v4();
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".into()),
            user: TraceUserContext {
                user_segment: "user-seg".into(),
                user_id: "user-id".into(),
            },
            environment: Some("prod".into()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            replay_id: Some(replay_id),
            sampled: None,
            other: BTreeMap::new(),
        };

        assert_eq!(Some(Val::String("1.1.1")), dsc.get_value("trace.release"));
        assert_eq!(
            Some(Val::String("prod")),
            dsc.get_value("trace.environment")
        );
        assert_eq!(Some(Val::String("user-id")), dsc.get_value("trace.user.id"));
        assert_eq!(
            Some(Val::String("user-seg")),
            dsc.get_value("trace.user.segment")
        );
        assert_eq!(
            Some(Val::String("transaction1")),
            dsc.get_value("trace.transaction")
        );
        assert_eq!(Some(Val::Uuid(replay_id)), dsc.get_value("trace.replay_id"));
    }

    #[test]
    fn getter_empty() {
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: TraceUserContext::default(),
            environment: None,
            transaction: None,
            sample_rate: None,
            replay_id: None,
            sampled: None,
            other: BTreeMap::new(),
        };
        assert_eq!(None, dsc.get_value("trace.release"));
        assert_eq!(None, dsc.get_value("trace.environment"));
        assert_eq!(None, dsc.get_value("trace.user.id"));
        assert_eq!(None, dsc.get_value("trace.user.segment"));
        assert_eq!(None, dsc.get_value("trace.user.transaction"));
        assert_eq!(None, dsc.get_value("trace.replay_id"));

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: TraceUserContext::default(),
            environment: None,
            transaction: None,
            sample_rate: None,
            replay_id: None,
            sampled: None,
            other: BTreeMap::new(),
        };
        assert_eq!(None, dsc.get_value("trace.user.id"));
        assert_eq!(None, dsc.get_value("trace.user.segment"));
    }
}
