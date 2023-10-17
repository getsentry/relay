//! Contains definitions for the Network Error Logging (NEL) interface.
//!
//! NEL is a browser feature that allows reporting of failed network requests from the client side.
//! W3C Editor's Draft: <https://w3c.github.io/network-error-logging/>
//! MDN: <https://developer.mozilla.org/en-US/docs/Web/HTTP/Network_Error_Logging>

#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};
use thiserror::Error;

/// The NEL parsing errors.
#[derive(Debug, Error)]
pub enum NetworkReportError {
    /// Unexpected format.
    #[error("unexpected format")]
    InvalidNel,
    /// Incoming Json is unparsable.
    #[error("incoming json is unparsable")]
    InvalidJson(#[from] serde_json::Error),
}

/// Generated network error report (NEL).
#[derive(Debug, Default, Clone, PartialEq, FromValue, IntoValue, Empty)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct BodyRaw {
    /// The time between the start of the resource fetch and when it was completed or aborted.
    pub elapsed_time: Annotated<u64>,
    /// HTTP method.
    pub method: Annotated<String>,
    /// If request failed, the phase of its network error. If request succeeded, "application".
    pub phase: Annotated<String>,
    /// The HTTP protocol and version.
    pub protocol: Annotated<String>,
    /// Request's referrer, as determined by the referrer policy associated with its client.
    pub referrer: Annotated<String>,
    /// The sampling rate.
    pub sampling_fraction: Annotated<f64>,
    /// The IP address of the server where the site is hosted.
    pub server_ip: Annotated<String>,
    /// HTTP status code.
    pub status_code: Annotated<i64>,
    /// If request failed, the type of its network error. If request succeeded, "ok".
    #[metastructure(field = "type")]
    pub ty: Annotated<String>,
    /// For forward compatibility.
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}

/// Models the content of a NEL report.
///
/// See <https://w3c.github.io/network-error-logging/>
#[derive(Debug, Default, Clone, PartialEq, FromValue, IntoValue, Empty)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct NetworkReportRaw {
    /// The age of the report since it got collected and before it got sent.
    pub age: Annotated<i64>,
    /// The type of the report.
    #[metastructure(field = "type")]
    pub ty: Annotated<String>,
    /// The URL of the document in which the error occurred.
    #[metastructure(pii = "true")]
    pub url: Annotated<String>,
    /// The User-Agent HTTP header.
    pub user_agent: Annotated<String>,
    /// The body of the NEL report.
    pub body: Annotated<BodyRaw>,
    /// For forward compatibility.
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}

impl NetworkReportRaw {
    pub fn try_annotated_from(value: &[u8]) -> Result<Annotated<Self>, NetworkReportError> {
        Annotated::from_json_bytes(value).map_err(NetworkReportError::InvalidJson)
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::assert_annotated_snapshot;

    use crate::protocol::NetworkReportRaw;

    #[test]
    fn test_nel_raw_basic() {
        let json = r#"{
            "age": 31042,
            "body": {
                "elapsed_time": 0,
                "method": "GET",
                "phase": "connection",
                "protocol": "http/1.1",
                "referrer": "",
                "sampling_fraction": 1.0,
                "server_ip": "127.0.0.1",
                "status_code": 0,
                "type": "tcp.refused"
            },
            "type": "network-error",
            "url": "http://example.com/",
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        }"#;

        let report = NetworkReportRaw::try_annotated_from(json.as_bytes()).unwrap();

        assert_annotated_snapshot!(report, @r###"
        {
          "age": 31042,
          "type": "network-error",
          "url": "http://example.com/",
          "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
          "body": {
            "elapsed_time": 0,
            "method": "GET",
            "phase": "connection",
            "protocol": "http/1.1",
            "referrer": "",
            "sampling_fraction": 1.0,
            "server_ip": "127.0.0.1",
            "status_code": 0,
            "type": "tcp.refused"
          }
        }
        "###);
    }
}
