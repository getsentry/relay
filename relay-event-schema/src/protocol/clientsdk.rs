use crate::processor::ProcessValue;
use crate::protocol::IpAddr;
use relay_protocol::{
    Annotated, Array, Empty, ErrorKind, FromValue, IntoValue, Object, SkipSerialization, Value,
};
use serde::{Serialize, Serializer};
use std::str::FromStr;
use thiserror::Error;

/// An installed and loaded package as part of the Sentry SDK.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct ClientSdkPackage {
    /// Name of the package.
    pub name: Annotated<String>,
    /// Version of the package.
    pub version: Annotated<String>,
}

/// An error returned when parsing setting values fail.
#[derive(Debug, Clone, Error)]
pub enum ParseSettingError {
    #[error("Invalid value for 'infer_ip'.")]
    InferIp,
}

/// A collection of settings that are used to control behaviour in relay through flags.
///
/// The settings aim to replace magic values in fields which need special treatment,
/// for example `{{auto}}` in the user.ip_address. The SDK would instead send `infer_ip`
/// to toggle the behaviour.
#[derive(Debug, Clone, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct ClientSdkSettings {
    infer_ip: Annotated<AutoInferSetting>,
}

impl ClientSdkSettings {
    /// Returns the current [`AutoInferSetting`] setting.
    ///
    /// **NOTE**: For forwards compatibility, this method has two defaults:
    /// * If `settings.infer_ip` is missing entirely, it will default
    ///   to [`AutoInferSetting::Legacy`].
    /// * If `settings.infer_ip` contains an invalid value, it will default
    ///   to [`AutoInferSetting::Never`].
    ///
    /// The reason behind this is that we don't want to fall back to the legacy behaviour
    /// if we add a new value to [`AutoInferSetting`] and a relay is running an old version
    /// which does not have the new value yet.
    pub fn infer_ip(&self) -> AutoInferSetting {
        if self.infer_ip.meta().has_errors() {
            AutoInferSetting::Never
        } else {
            self.infer_ip.value().copied().unwrap_or_default()
        }
    }
}

/// Used to control the IP inference setting in relay. This is used as an alternative to magic
/// values like {{auto}} in the user.ip_address field.
#[derive(Debug, Copy, Clone, PartialEq, Default, ProcessValue)]
pub enum AutoInferSetting {
    /// Derive the IP address from the connection information.
    Auto,

    /// Do not derive the IP address, keep what was being sent by the client.
    Never,

    /// Enables the legacy IP inference behaviour.
    ///
    /// The legacy behavior works mainly by inspecting the content of `user.ip_address` and
    /// decides based on the value.
    /// Unfortunately, not all platforms are treated equals so there are exceptions for
    /// `javascript`, `cocoa` and `objc`.
    ///
    /// If the value in `ip_address` is `{{auto}}`, it will work the
    /// same as [`AutoInferSetting::Auto`]. This is true for all platforms.
    ///
    /// If the value in `ip_address` is [`None`], it will only infer the IP address if a
    /// `REMOTE_ADDR` header is sent in the request payload of the event.
    ///
    /// **NOTE**: Setting `ip_address` to [`None`] will behave the same as setting it to `{{auto}}`
    ///           for `javascript`, `cocoa` and `objc`.
    #[default]
    Legacy,
}

impl Empty for AutoInferSetting {
    fn is_empty(&self) -> bool {
        matches!(self, AutoInferSetting::Legacy)
    }
}

impl AutoInferSetting {
    /// Returns a string representation for [`AutoInferSetting`].
    pub fn as_str(&self) -> &'static str {
        match self {
            AutoInferSetting::Auto => "auto",
            AutoInferSetting::Never => "never",
            AutoInferSetting::Legacy => "legacy",
        }
    }
}

impl FromStr for AutoInferSetting {
    type Err = ParseSettingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "auto" => Ok(AutoInferSetting::Auto),
            "never" => Ok(AutoInferSetting::Never),
            "legacy" => Ok(AutoInferSetting::Legacy),
            _ => Err(ParseSettingError::InferIp),
        }
    }
}

impl FromValue for AutoInferSetting {
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        match String::from_value(value) {
            Annotated(Some(value), mut meta) => match value.parse() {
                Ok(infer_ip) => Annotated(Some(infer_ip), meta),
                Err(_) => {
                    meta.add_error(ErrorKind::InvalidData);
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for AutoInferSetting {
    fn into_value(self) -> Value {
        Value::String(self.as_str().to_string())
    }

    fn serialize_payload<S>(&self, s: S, _: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(self.as_str(), s)
    }
}

/// The SDK Interface describes the Sentry SDK and its configuration used to capture and transmit an event.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
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
    #[metastructure(required = true, max_chars = 256, max_chars_allowance = 20)]
    pub name: Annotated<String>,

    /// The version of the SDK. _Required._
    ///
    /// It should have the [Semantic Versioning](https://semver.org/) format `MAJOR.MINOR.PATCH`,
    /// without any prefix (no `v` or anything else in front of the major version number).
    ///
    /// Examples: `0.1.0`, `1.0.0`, `4.3.12`
    #[metastructure(required = true, max_chars = 256, max_chars_allowance = 20)]
    pub version: Annotated<String>,

    /// List of integrations that are enabled in the SDK. _Optional._
    ///
    /// The list should have all enabled integrations, including default integrations. Default
    /// integrations are included because different SDK releases may contain different default
    /// integrations.
    #[metastructure(skip_serialization = "empty_deep")]
    pub integrations: Annotated<Array<String>>,

    /// List of features that are enabled in the SDK. _Optional._
    ///
    /// A list of feature names identifying enabled SDK features. This list
    /// should contain all enabled SDK features. On some SDKs, enabling a feature in the
    /// options also adds an integration. We encourage tracking such features with either
    /// integrations or features but not both to reduce the payload size.
    #[metastructure(skip_serialization = "empty_deep")]
    pub features: Annotated<Array<String>>,

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

    /// Settings that are used to control behaviour of relay.
    #[metastructure(skip_serialization = "empty")]
    pub settings: Annotated<ClientSdkSettings>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

impl ClientSdkInfo {
    pub fn has_integration<T: AsRef<str>>(&self, integration: T) -> bool {
        if let Some(integrations) = self.integrations.value() {
            for x in integrations {
                if x.as_str().unwrap_or_default() == integration.as_ref() {
                    return true;
                }
            }
        };
        false
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::Map;
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_client_sdk_roundtrip() {
        let json = r#"{
  "name": "sentry.rust",
  "version": "1.0.0",
  "integrations": [
    "actix"
  ],
  "features": [
    "feature1"
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
            features: Annotated::new(vec![Annotated::new("feature1".to_string())]),
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
            settings: Annotated::empty(),
            other: {
                let mut map = Map::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        });

        assert_eq!(sdk, Annotated::from_json(json).unwrap());
        assert_eq!(json, sdk.to_json_pretty().unwrap());
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
            features: Annotated::empty(),
            packages: Annotated::empty(),
            client_ip: Annotated::new(IpAddr("127.0.0.1".to_owned())),
            settings: Annotated::empty(),
            other: Default::default(),
        });

        assert_eq!(sdk, Annotated::from_json(json).unwrap());
        assert_eq!(json, sdk.to_json_pretty().unwrap());
    }

    #[test]
    fn test_sdk_settings_auto() {
        let json = r#"{
  "settings": {
    "infer_ip": "auto"
  }
}"#;
        let sdk = Annotated::new(ClientSdkInfo {
            settings: Annotated::new(ClientSdkSettings {
                infer_ip: Annotated::new(AutoInferSetting::Auto),
            }),
            ..Default::default()
        });

        assert_eq!(sdk, Annotated::from_json(json).unwrap());
        assert_eq!(json, sdk.to_json_pretty().unwrap());
    }

    #[test]
    fn test_sdk_settings_never() {
        let json = r#"{
  "settings": {
    "infer_ip": "never"
  }
}"#;
        let sdk = Annotated::new(ClientSdkInfo {
            settings: Annotated::new(ClientSdkSettings {
                infer_ip: Annotated::new(AutoInferSetting::Never),
            }),
            ..Default::default()
        });

        assert_eq!(sdk, Annotated::from_json(json).unwrap());
        assert_eq!(json, sdk.to_json_pretty().unwrap());
    }

    #[test]
    fn test_sdk_settings_default() {
        let sdk = Annotated::new(ClientSdkInfo {
            settings: Annotated::new(ClientSdkSettings {
                infer_ip: Annotated::empty(),
            }),
            ..Default::default()
        });

        assert_eq!(
            sdk.value().unwrap().settings.value().unwrap().infer_ip(),
            AutoInferSetting::Legacy
        )
    }

    #[test]
    fn test_infer_ip_invalid() {
        let json = r#"{
            "settings": {
                "infer_ip": "invalid"
            }
        }"#;
        let sdk: Annotated<ClientSdkInfo> = Annotated::from_json(json).unwrap();
        assert_eq!(
            sdk.value().unwrap().settings.value().unwrap().infer_ip(),
            AutoInferSetting::Never
        );
    }
}
