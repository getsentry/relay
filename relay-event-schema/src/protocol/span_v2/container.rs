use serde::{Deserialize, Serialize};

/// Metadata supplied with an item container containing spans.
///
/// See also: <https://develop.sentry.dev/sdk/telemetry/spans/span-protocol/#version-and-ingest_settings-properties>.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ContainerMetadata {
    /// The metadata version
    ///
    /// The version influences how spans are processed during ingestion.
    ///
    /// Currently supported versions:
    /// - `missing`: no behaviour change
    /// - `2`: adds support for `ingest_settings`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u16>,
    /// Settings controlling parts of the ingestion of individual spans.
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
        deserialize_with = "none_on_error"
    )]
    pub infer_ip: Option<InferIp>,
    /// Controls how and if the user agent should be inferred.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "none_on_error"
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
    /// The user agent should be inferred from the http request submitting the spans.
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

fn none_on_error<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::de::Deserializer<'de>,
    T: serde::de::DeserializeOwned + std::fmt::Debug,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Try<T> {
        T(T),
        Err(serde::de::IgnoredAny),
    }

    Ok(match Try::<T>::deserialize(deserializer)? {
        Try::T(t) => Some(t),
        Try::Err(_) => None,
    })
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
