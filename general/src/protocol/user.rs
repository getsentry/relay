use crate::protocol::{IpAddr, LenientString};
use crate::types::{Annotated, Object, Value};

/// Geographical location of the end user or device.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_geo")]
pub struct Geo {
    /// Two-letter country code (ISO 3166-1 alpha-2).
    #[metastructure(pii = "true", max_chars = "summary")]
    pub country_code: Annotated<String>,

    /// Human readable city name.
    #[metastructure(pii = "true", max_chars = "summary")]
    pub city: Annotated<String>,

    /// Human readable region name or code.
    #[metastructure(pii = "true", max_chars = "summary")]
    pub region: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// Information about the user who triggered an event.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_user", value_type = "User")]
pub struct User {
    /// Unique identifier of the user.
    #[metastructure(pii = "true", max_chars = "enumlike", skip_serialization = "empty")]
    pub id: Annotated<LenientString>,

    /// Email address of the user.
    #[metastructure(pii = "true", max_chars = "email", skip_serialization = "empty")]
    pub email: Annotated<String>,

    /// Remote IP address of the user. Defaults to "{{auto}}".
    #[metastructure(pii = "true", skip_serialization = "empty")]
    pub ip_address: Annotated<IpAddr>,

    /// Username of the user.
    #[metastructure(pii = "true", max_chars = "enumlike", skip_serialization = "empty")]
    pub username: Annotated<String>,

    /// Human readable name of the user.
    #[metastructure(pii = "true", max_chars = "enumlike", skip_serialization = "empty")]
    pub name: Annotated<String>,

    /// Approximate geographical location of the end user or device.
    #[metastructure(skip_serialization = "empty")]
    pub geo: Annotated<Geo>,

    /// Additional arbitrary fields, as stored in the database (and sometimes as sent by clients).
    /// All data from `self.other` should end up here after store normalization.
    #[metastructure(skip_serialization = "empty")]
    pub data: Annotated<Object<Value>>,

    /// Additional arbitrary fields, as sent by clients.
    #[metastructure(additional_properties, pii = "true")]
    pub other: Object<Value>,
}

#[test]
fn test_geo_roundtrip() {
    use crate::types::Map;

    let json = r#"{
  "country_code": "US",
  "city": "San Francisco",
  "region": "CA",
  "other": "value"
}"#;
    let geo = Annotated::new(Geo {
        country_code: Annotated::new("US".to_string()),
        city: Annotated::new("San Francisco".to_string()),
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

    assert_eq_dbg!(geo, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, geo.to_json_pretty().unwrap());
}

#[test]
fn test_geo_default_values() {
    let json = "{}";
    let geo = Annotated::new(Geo {
        country_code: Annotated::empty(),
        city: Annotated::empty(),
        region: Annotated::empty(),
        other: Default::default(),
    });

    assert_eq_dbg!(geo, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, geo.to_json_pretty().unwrap());
}

#[test]
fn test_user_roundtrip() {
    let json = r#"{
  "id": "e4e24881-8238-4539-a32b-d3c3ecd40568",
  "email": "mail@example.org",
  "ip_address": "{{auto}}",
  "username": "john_doe",
  "name": "John Doe",
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
    });

    assert_eq_dbg!(user, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, user.to_json_pretty().unwrap());
}

#[test]
fn test_user_lenient_id() {
    let input = r#"{"id":42}"#;
    let output = r#"{"id":"42"}"#;
    let user = Annotated::new(User {
        id: Annotated::new("42".to_string().into()),
        ..User::default()
    });

    assert_eq_dbg!(user, Annotated::from_json(input).unwrap());
    assert_eq_str!(output, user.to_json().unwrap());
}

#[test]
fn test_user_invalid_id() {
    use crate::types::Error;
    let json = r#"{"id":[]}"#;
    let user = Annotated::new(User {
        id: Annotated::from_error(
            Error::expected("a primitive value"),
            Some(Value::Array(vec![])),
        ),
        ..User::default()
    });

    assert_eq_dbg!(user, Annotated::from_json(json).unwrap());
}

#[test]
fn test_explicit_none() {
    let json = r#"{
  "id": null
}"#;

    let user = Annotated::new(User::default());

    assert_eq_dbg!(user, Annotated::from_json(json).unwrap());
    assert_eq_str!("{}", user.to_json_pretty().unwrap());
}
