use crate::protocol::IpAddr;
use crate::types::{Annotated, Array, Object, Value};

/// An installed and loaded package as part of the Sentry SDK.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Empty,
    FromValue,
    ToValue,
    ProcessValue,
    PiiAttributes,
    SchemaAttributes,
)]
pub struct ClientSdkPackage {
    /// Name of the package.
    pub name: Annotated<String>,
    /// Version of the package.
    pub version: Annotated<String>,
}

/// Information about the Sentry SDK.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Empty,
    FromValue,
    ToValue,
    ProcessValue,
    SchemaAttributes,
    PiiAttributes,
)]
#[metastructure(process_func = "process_client_sdk_info", value_type = "ClientSdkInfo")]
pub struct ClientSdkInfo {
    /// Unique SDK name.
    #[metastructure(max_chars = "symbol")]
    #[required]
    pub name: Annotated<String>,

    /// SDK version.
    #[metastructure(max_chars = "symbol")]
    #[required]
    pub version: Annotated<String>,

    /// List of integrations that are enabled in the SDK.
    #[metastructure(skip_serialization = "empty_deep")]
    pub integrations: Annotated<Array<String>>,

    /// List of installed and loaded SDK packages.
    #[metastructure(skip_serialization = "empty_deep")]
    pub packages: Annotated<Array<ClientSdkPackage>>,

    /// IP Address of sender??? Seems unused.
    #[metastructure(skip_serialization = "empty")]
    #[should_strip_pii = true]
    pub client_ip: Annotated<IpAddr>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[test]
fn test_client_sdk_roundtrip() {
    use crate::types::Map;

    let json = r#"{
  "name": "sentry.rust",
  "version": "1.0.0",
  "integrations": [
    "actix"
  ],
  "packages": [
    {
      "name": "cargo:sentry",
      "version": "0.10.0"
    },
    {
      "name": "cargo:sentry-actix",
      "version": "0.10.0"
    }
  ],
  "client_ip": "127.0.0.1",
  "other": "value"
}"#;
    let sdk = Annotated::new(ClientSdkInfo {
        name: Annotated::new("sentry.rust".to_string()),
        version: Annotated::new("1.0.0".to_string()),
        integrations: Annotated::new(vec![Annotated::new("actix".to_string())]),
        packages: Annotated::new(vec![
            Annotated::new(ClientSdkPackage {
                name: Annotated::new("cargo:sentry".to_string()),
                version: Annotated::new("0.10.0".to_string()),
            }),
            Annotated::new(ClientSdkPackage {
                name: Annotated::new("cargo:sentry-actix".to_string()),
                version: Annotated::new("0.10.0".to_string()),
            }),
        ]),
        client_ip: Annotated::new(IpAddr("127.0.0.1".to_owned())),
        other: {
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(sdk, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, sdk.to_json_pretty().unwrap());
}

#[test]
fn test_client_sdk_default_values() {
    let json = r#"{
  "name": "sentry.rust",
  "version": "1.0.0",
  "client_ip": "127.0.0.1"
}"#;
    let sdk = Annotated::new(ClientSdkInfo {
        name: Annotated::new("sentry.rust".to_string()),
        version: Annotated::new("1.0.0".to_string()),
        integrations: Annotated::empty(),
        packages: Annotated::empty(),
        client_ip: Annotated::new(IpAddr("127.0.0.1".to_owned())),
        other: Default::default(),
    });

    assert_eq_dbg!(sdk, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, sdk.to_json_pretty().unwrap());
}
