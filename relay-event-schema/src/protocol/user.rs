#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;
use crate::protocol::{IpAddr, LenientString};

/// Geographical location of the end user or device.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_geo")]
pub struct Geo {
    /// Two-letter country code (ISO 3166-1 alpha-2).
    #[metastructure(pii = "true", max_chars = 102, max_chars_allowance = 1004)]
    pub country_code: Annotated<String>,

    /// Human readable city name.
    #[metastructure(pii = "true", max_chars = 1024, max_chars_allowance = 100)]
    pub city: Annotated<String>,

    /// Human readable subdivision name.
    #[metastructure(pii = "true", max_chars = 1024, max_chars_allowance = 100)]
    pub subdivision: Annotated<String>,

    /// Human readable region name or code.
    #[metastructure(pii = "true", max_chars = 1024, max_chars_allowance = 100)]
    pub region: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Information about the user who triggered an event.
///
/// ```json
/// {
///   "user": {
///     "id": "unique_id",
///     "username": "my_user",
///     "email": "foo@example.com",
///     "ip_address": "127.0.0.1",
///     "subscription": "basic"
///   }
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_user", value_type = "User")]
pub struct User {
    /// Unique identifier of the user.
    #[metastructure(pii = "true", max_chars = 128, skip_serialization = "empty")]
    pub id: Annotated<LenientString>,

    /// Email address of the user.
    #[metastructure(pii = "true", max_chars = 75, skip_serialization = "empty")]
    pub email: Annotated<String>,

    /// Remote IP address of the user. Defaults to "{{auto}}".
    #[metastructure(pii = "true", skip_serialization = "empty")]
    pub ip_address: Annotated<IpAddr>,

    /// Username of the user.
    #[metastructure(pii = "true", max_chars = 128, skip_serialization = "empty")]
    pub username: Annotated<String>,

    /// Human readable name of the user.
    #[metastructure(pii = "true", max_chars = 128, skip_serialization = "empty")]
    pub name: Annotated<String>,

    /// Approximate geographical location of the end user or device.
    #[metastructure(skip_serialization = "empty")]
    pub geo: Annotated<Geo>,

    /// The user segment, for apps that divide users in user segments.
    #[metastructure(skip_serialization = "empty")]
    pub segment: Annotated<String>,

    /// The user string representation as handled in Sentry.
    ///
    /// This field is computed by concatenating the name of specific fields of the `User`
    /// struct with their value. For example, if `id` is set, `sentry_user` will be equal to
    /// `"id:id-of-the-user".
    #[metastructure(skip_serialization = "empty")]
    pub sentry_user: Annotated<String>,

    /// Additional arbitrary fields, as stored in the database (and sometimes as sent by clients).
    /// All data from `self.other` should end up here after store normalization.
    #[metastructure(pii = "true", skip_serialization = "empty")]
    pub data: Annotated<Object<Value>>,

    /// Additional arbitrary fields, as sent by clients.
    #[metastructure(additional_properties, pii = "true")]
    pub other: Object<Value>,
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use super::*;
    use relay_protocol::{Error, Map};

    #[test]
    fn test_geo_roundtrip() {
        let json = r#"{
  "country_code": "US",
  "city": "San Francisco",
  "subdivision": "California",
  "region": "CA",
  "other": "value"
}"#;
        let geo = Annotated::new(Geo {
            country_code: Annotated::new("US".to_string()),
            city: Annotated::new("San Francisco".to_string()),
            subdivision: Annotated::new("California".to_string()),
            region: Annotated::new("CA".to_string()),
            other: {
                let mut map = Map::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        });

        assert_eq!(geo, Annotated::from_json(json).unwrap());
        assert_eq!(json, geo.to_json_pretty().unwrap());
    }

    #[test]
    fn test_geo_default_values() {
        let json = "{}";
        let geo = Annotated::new(Geo {
            country_code: Annotated::empty(),
            city: Annotated::empty(),
            subdivision: Annotated::empty(),
            region: Annotated::empty(),
            other: Object::default(),
        });

        assert_eq!(geo, Annotated::from_json(json).unwrap());
        assert_eq!(json, geo.to_json_pretty().unwrap());
    }

    #[test]
    fn test_user_roundtrip() {
        let json = r#"{
  "id": "e4e24881-8238-4539-a32b-d3c3ecd40568",
  "email": "mail@example.org",
  "ip_address": "{{auto}}",
  "username": "john_doe",
  "name": "John Doe",
  "segment": "vip",
  "data": {
    "data": "value"
  },
  "other": "value"
}"#;
        let user = Annotated::new(User {
            id: Annotated::new("e4e24881-8238-4539-a32b-d3c3ecd40568".to_string().into()),
            email: Annotated::new("mail@example.org".to_string()),
            ip_address: Annotated::new(IpAddr::auto()),
            name: Annotated::new("John Doe".to_string()),
            username: Annotated::new("john_doe".to_string()),
            geo: Annotated::empty(),
            segment: Annotated::new("vip".to_string()),
            data: {
                let mut map = Object::new();
                map.insert(
                    "data".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                Annotated::new(map)
            },
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
            ..Default::default()
        });

        assert_eq!(user, Annotated::from_json(json).unwrap());
        assert_eq!(json, user.to_json_pretty().unwrap());
    }

    #[test]
    fn test_user_lenient_id() {
        let input = r#"{"id":42}"#;
        let output = r#"{"id":"42"}"#;
        let user = Annotated::new(User {
            id: Annotated::new("42".to_string().into()),
            ..User::default()
        });

        assert_eq!(user, Annotated::from_json(input).unwrap());
        assert_eq!(output, user.to_json().unwrap());
    }

    #[test]
    fn test_user_invalid_id() {
        let json = r#"{"id":[]}"#;
        let user = Annotated::new(User {
            id: Annotated::from_error(
                Error::expected("a primitive value"),
                Some(Value::Array(vec![])),
            ),
            ..User::default()
        });

        assert_eq!(user, Annotated::from_json(json).unwrap());
    }

    #[test]
    fn test_explicit_none() {
        let json = r#"{
  "id": null
}"#;

        let user = Annotated::new(User::default());

        assert_eq!(user, Annotated::from_json(json).unwrap());
        assert_eq!("{}", user.to_json_pretty().unwrap());
    }
}
