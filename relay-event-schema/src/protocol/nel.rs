//! Contains definitions for the Network Error Logging (NEL) interface.
//!
//! NEL is a browser feature that allows reporting of failed network requests from the client side.
//! W3C Editor's Draft: <https://w3c.github.io/network-error-logging/>
//! MDN: <https://developer.mozilla.org/en-US/docs/Web/HTTP/Network_Error_Logging>
use std::ops::Sub;

use chrono::{DateTime, Duration, Utc};

use crate::processor::ProcessValue;
use crate::protocol::{
    AsPair, Event, HeaderName, HeaderValue, Headers, LogEntry, PairList, Request, TagEntry, Tags,
    Timestamp,
};
#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue};
use thiserror::Error;

/// The NEL parsing errors.
#[derive(Debug, Error)]
pub enum NelError {
    /// Unexpected format.
    #[error("unexpected format")]
    InvalidNel,
    /// Incoming Json is unparsable.
    #[error("incoming json is unparsable")]
    InvalidJson(#[from] serde_json::Error),
}

/// Generated network error report (NEL).
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct NelBody {
    /// The elapsed number of milliseconds between the start of the resource
    /// fetch and when it was completed or aborted by the user agent.
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
}

/// Models the content of a NEL report.
///
/// NOTE: This is the structure used inside the Event (serialization is based on Annotated
/// infrastructure). We also use a version of this structure to deserialize from raw JSON
/// via serde.
///
/// See <https://w3c.github.io/network-error-logging/>
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Nel {
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
    pub body: Annotated<NelBody>,
}

impl Nel {
    fn get_request(&self) -> Request {
        let headers = match self.user_agent.value() {
            Some(ref user_agent) if !user_agent.is_empty() => {
                Annotated::new(Headers(PairList(vec![Annotated::new((
                    Annotated::new(HeaderName::new("User-Agent")),
                    Annotated::new(HeaderValue::new(user_agent)),
                ))])))
            }
            Some(_) | None => Annotated::empty(),
        };
        Request {
            url: self.url.clone(),
            headers,
            ..Request::default()
        }
    }
    pub fn apply_to_event(data: &[u8], event: &mut Event) -> Result<(), NelError> {
        let mut nel: Annotated<Nel> =
            Annotated::from_json_bytes(data).map_err(NelError::InvalidJson)?;

        if let Some(nel) = nel.value_mut() {
            if let Some(body) = nel.body.value() {
                event.logentry = Annotated::new(LogEntry::from({
                    if nel.ty.value().map_or("", |v| v.as_str()) == "http.error" {
                        format!(
                            "{} / {} ({})",
                            body.phase.as_str().unwrap_or(""),
                            body.ty.as_str().unwrap_or(""),
                            body.status_code.value().unwrap_or(&0)
                        )
                    } else {
                        format!(
                            "{} / {}",
                            body.phase.as_str().unwrap_or(""),
                            body.ty.as_str().unwrap_or(""),
                        )
                    }
                }));

                event.request = Annotated::new(nel.get_request());
                event.logger = Annotated::from("nel".to_string());

                // Exrtact common tags.
                let tags = event.tags.get_or_insert_with(Tags::default);
                if let Some(ref method) = body.method.value() {
                    tags.push(Annotated::new(TagEntry::from_pair((
                        Annotated::new("method".to_string()),
                        Annotated::new(method.to_string()),
                    ))));
                }
                if let Some(ref protocol) = body.protocol.value() {
                    tags.push(Annotated::new(TagEntry::from_pair((
                        Annotated::new("protocol".to_string()),
                        Annotated::new(protocol.to_string()),
                    ))));
                }
                if let Some(ref status_code) = body.status_code.value() {
                    tags.push(Annotated::new(TagEntry::from_pair((
                        Annotated::new("status_code".to_string()),
                        Annotated::new(status_code.to_string()),
                    ))));
                }
                if let Some(ref server_ip) = body.server_ip.value() {
                    tags.push(Annotated::new(TagEntry::from_pair((
                        Annotated::new("server_ip".to_string()),
                        Annotated::new(server_ip.to_string()),
                    ))));
                }
            }

            // Set the timestamp on the event when it actually occured.
            let now: DateTime<Utc> = Utc::now();
            let event_time = now.sub(Duration::milliseconds(*nel.age.value().unwrap_or(&0)));
            event.timestamp = Annotated::new(Timestamp::from(event_time));
        }
        event.nel = nel;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use relay_protocol::assert_annotated_snapshot;

    #[test]
    fn test_nel_basic() {
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

        let mut event = Event::default();
        Nel::apply_to_event(json.as_bytes(), &mut event).unwrap();

        // mock timestamp because it is actually dynamic and depend on current time and "age" field
        event.timestamp =
            Annotated::new(Utc.with_ymd_and_hms(2023, 10, 5, 0, 0, 0).unwrap().into());

        assert_annotated_snapshot!(Annotated::new(event), @r###"
        {
          "logentry": {
            "formatted": "connection / tcp.refused"
          },
          "logger": "nel",
          "timestamp": 1696464000.0,
          "request": {
            "url": "http://example.com/",
            "headers": [
              [
                "User-Agent",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
              ]
            ]
          },
          "tags": [
            [
              "method",
              "GET"
            ],
            [
              "protocol",
              "http/1.1"
            ],
            [
              "status_code",
              "0"
            ],
            [
              "server_ip",
              "127.0.0.1"
            ]
          ],
          "nel": {
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
        }
        "###);
    }
}
