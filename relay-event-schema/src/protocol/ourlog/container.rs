use serde::{Deserialize, Serialize};

use crate::protocol::utils;

/// Metadata supplied with an item container containing logs.
///
/// See also: <https://develop.sentry.dev/sdk/telemetry/logs/#version-and-ingest_settings-properties>.
///
/// # Versions
///
/// ## Behaviour
///
/// Different versions influence how logs are normalized and processed:
///
/// | Version |      client ip     | user agent |
/// |---------|--------------------|------------|
/// | none    | {{auto}}           | always     |
/// | 2       | {{auto}}, settings | settings   |
///
/// For example in version 2, the client ip is inferred if either the client address attribute is
/// set to `auto` or the ingest settings specify `infer_ip` to `autp`.
/// The user agent is only considered if ingest settings specify `infer_user_agent` as `auto`.
///
/// ## Schema Changelog
///
/// | Version | Changes                         |
/// |---------|---------------------------------|
/// | 2       | Adds `ingest_settings`          |
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ContainerMetadata {
    /// The metadata version
    ///
    /// The version influences how logs are processed during ingestion.
    ///
    /// Currently supported versions:
    /// - `none`: no processing changes
    /// - `2`: adds support for `ingest_settings`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u16>,
    /// Settings controlling parts of the ingestion of individual logs.
    ///
    /// Supported since version `2`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingest_settings: Option<IngestSettings>,
}

/// Span ingestion settings.
///
/// See also: [`ContainerMetadata::ingest_settings`].
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IngestSettings {
    /// Controls how and if the client IP address should be inferred.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "utils::none_on_error"
    )]
    pub infer_ip: Option<InferIp>,
    /// Controls how and if the user agent should be inferred.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "utils::none_on_error"
    )]
    pub infer_user_agent: Option<InferUserAgent>,
}

/// Settings controlling how client IP addresses should be inferred.
///
/// Specifically frontend SDKs may want to have the IP address automatically inferred.
#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
pub enum InferIp {
    /// The IP address is inferred by from the client connection.
    #[serde(rename = "auto")]
    Auto,
    /// The client IP address is never inferred.
    #[serde(rename = "never")]
    Never,
}

impl InferIp {
    /// Returns `true` if `self` is [`Self::Auto`].
    pub fn is_auto(&self) -> bool {
        matches!(self, Self::Auto)
    }
}

/// Settings controlling how the user agent should be inferred.
///
/// Specifically frontend SDKs may want to have the user agent and browser information automatically inferred
/// from the HTTP header provided user agent.
#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
pub enum InferUserAgent {
    /// The user agent should be inferred from the http request submitting the logs.
    #[serde(rename = "auto")]
    Auto,
    /// The user agent should never be inferred.
    #[serde(rename = "never")]
    Never,
}

impl InferUserAgent {
    /// Returns `true` if `self` is [`Self::Auto`].
    pub fn is_auto(&self) -> bool {
        matches!(self, Self::Auto)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_round_trip() {
        let m: ContainerMetadata = serde_json::from_str(
            r#"{
            "version": 123, 
            "ingest_settings": {
                "infer_ip": "auto",
                "infer_user_agent": "never"
            }
        }"#,
        )
        .unwrap();

        insta::assert_json_snapshot!(&m, @r#"
        {
          "version": 123,
          "ingest_settings": {
            "infer_ip": "auto",
            "infer_user_agent": "never"
          }
        }
        "#);
    }

    #[test]
    fn test_ingest_settings_invalid() {
        let m: ContainerMetadata = serde_json::from_str(
            r#"{"ingest_settings": {
            "infer_ip": "unknown",
            "infer_user_agent": 123
        }}"#,
        )
        .unwrap();

        insta::assert_json_snapshot!(&m, @r#"
        {
          "ingest_settings": {}
        }
        "#);
    }

    #[test]
    fn test_version_invalid_fails() {
        let _ = serde_json::from_str::<ContainerMetadata>(r#"{"version": {}}"#).unwrap_err();
    }
}
