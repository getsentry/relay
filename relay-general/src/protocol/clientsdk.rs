use crate::protocol::IpAddr;
use crate::types::{Annotated, Array, Object, Value};

/// An installed and loaded package as part of the Sentry SDK.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct ClientSdkPackage {
    /// Name of the package.
    pub name: Annotated<String>,
    /// Version of the package.
    pub version: Annotated<String>,
}

/// The SDK Interface describes the Sentry SDK and its configuration used to capture and transmit an event.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_client_sdk_info", value_type = "ClientSdkInfo")]
pub struct ClientSdkInfo {
    /// Unique SDK name. _Required._
    ///
    /// The name of the SDK. The format is `entity.ecosystem[.flavor]` where entity identifies the
    /// developer of the SDK, ecosystem refers to the programming language or platform where the
    /// SDK is to be used and the optional flavor is used to identify standalone SDKs that are part
    /// of a major ecosystem.
    ///
    /// Official Sentry SDKs use the entity `sentry`, as in `sentry.python` or
    /// `sentry.javascript.react-native`. Please use a different entity for your own SDKs.
    #[metastructure(required = "true", max_chars = "symbol")]
    pub name: Annotated<String>,

    /// The version of the SDK. _Required._
    ///
    /// It should have the [Semantic Versioning](https://semver.org/) format `MAJOR.MINOR.PATCH`,
    /// without any prefix (no `v` or anything else in front of the major version number).
    ///
    /// Examples: `0.1.0`, `1.0.0`, `4.3.12`
    #[metastructure(required = "true", max_chars = "symbol")]
    pub version: Annotated<String>,

    /// List of integrations that are enabled in the SDK. _Optional._
    ///
    /// The list should have all enabled integrations, including default integrations. Default
    /// integrations are included because different SDK releases may contain different default
    /// integrations.
    #[metastructure(skip_serialization = "empty_deep")]
    pub integrations: Annotated<Array<String>>,

    /// List of installed and loaded SDK packages. _Optional._
    ///
    /// A list of packages that were installed as part of this SDK or the activated integrations.
    /// Each package consists of a name in the format `source:identifier` and `version`. If the
    /// source is a Git repository, the `source` should be `git`, the identifier should be a
    /// checkout link and the version should be a Git reference (branch, tag or SHA).
    #[metastructure(skip_serialization = "empty_deep")]
    pub packages: Annotated<Array<ClientSdkPackage>>,

    /// IP Address of sender??? Seems unused. Do not send, this only leads to surprises wrt PII, as
    /// the value appears nowhere in the UI.
    #[metastructure(pii = "true", skip_serialization = "empty", omit_from_schema)]
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
