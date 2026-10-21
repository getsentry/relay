use std::{fmt::Display, str::FromStr};

use relay_protocol::{Annotated, Empty, Error, FromValue, IntoValue, Object, Value};
use serde::Serialize;
use thiserror::Error;

use crate::protocol::nel;

/// Errors that can occur when parsing a [`BrowserReportType`].
#[derive(Debug, Clone, Copy, Error)]
pub enum ParseBrowserReportTypeError {
    #[error("unknown browser report type")]
    Unknown,
}

/// Supported browser report types.
#[derive(Debug, Clone, PartialEq, Eq, Empty)]
pub enum BrowserReportType {
    NetworkError,
}

impl BrowserReportType {
    /// Returns the report type as a `&str`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NetworkError => "network-error",
        }
    }
}

impl FromStr for BrowserReportType {
    type Err = ParseBrowserReportTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "network-error" => Ok(Self::NetworkError),
            _ => Err(ParseBrowserReportTypeError::Unknown),
        }
    }
}

impl Display for BrowserReportType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromValue for BrowserReportType {
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        match value {
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(Value::String(s)), mut meta) => match s.parse() {
                Ok(report_type) => Annotated(Some(report_type), meta),
                Err(error) => {
                    meta.add_error(relay_protocol::Error::invalid(error));
                    meta.set_original_value(Some(s));
                    Annotated(None, meta)
                }
            },
            Annotated(Some(v), mut meta) => {
                meta.add_error(Error::expected("browser report type"));
                meta.set_original_value(Some(v));
                Annotated(None, meta)
            }
        }
    }
}

impl IntoValue for BrowserReportType {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(
        &self,
        s: S,
        _behavior: relay_protocol::SkipSerialization,
    ) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        Serialize::serialize(self.as_str(), s)
    }
}

/// The contents of the `body` field of a [`BrowserReport`].
///
/// This contains a variant for each supported report type.
#[derive(Debug, Clone, PartialEq, Empty)]
pub enum BrowserReportBody {
    #[cfg_attr(not(test), expect(unused))]
    Nel(Box<nel::BodyRaw>),
}

impl IntoValue for BrowserReportBody {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        match self {
            Self::Nel(nel) => nel.into_value(),
        }
    }

    fn serialize_payload<S>(
        &self,
        s: S,
        behavior: relay_protocol::SkipSerialization,
    ) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        match self {
            Self::Nel(nel) => nel.serialize_payload(s, behavior),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Empty, IntoValue)]
#[cfg_attr(not(test), expect(unused))]
struct BrowserReport {
    /// The age of the report since it got collected and before it got sent.
    pub age: Annotated<i64>,
    /// The type of the report.
    #[metastructure(field = "type")]
    pub ty: Annotated<BrowserReportType>,
    /// The URL of the document in which the error occurred.
    pub url: Annotated<String>,
    /// The User-Agent HTTP header.
    pub user_agent: Annotated<String>,
    /// The body of the report.
    pub body: Annotated<BrowserReportBody>,
}

impl FromValue for BrowserReport {
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        #[derive(Debug, FromValue)]
        struct BrowserReportRaw {
            age: Annotated<i64>,
            #[metastructure(field = "type")]
            ty: Annotated<BrowserReportType>,
            url: Annotated<String>,
            user_agent: Annotated<String>,
            body: Annotated<Object<Value>>,
        }

        let raw = BrowserReportRaw::from_value(value);

        match raw {
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(raw), meta) => {
                let mut out = Self {
                    age: raw.age,
                    ty: raw.ty,
                    url: raw.url,
                    user_agent: raw.user_agent,
                    body: Annotated::empty(),
                };

                match &mut out.ty {
                    Annotated(Some(BrowserReportType::NetworkError), _) => {
                        out.body = nel::BodyRaw::from_value(raw.body.map_value(Value::Object))
                            .map_value(|body| BrowserReportBody::Nel(Box::new(body)));
                    }
                    Annotated(None, ty_meta) => {
                        if !ty_meta.has_errors() {
                            ty_meta.add_error(relay_protocol::Error::nonempty());
                        }
                        let body_meta = out.body.meta_mut();
                        body_meta.add_error(relay_protocol::Error::invalid(
                            "omitted because of invalid report type",
                        ));
                        body_meta.set_original_value(raw.body.value().cloned().map(Value::Object));
                    }
                }

                if let Annotated(None, body_meta) = &mut out.body
                    && !body_meta.has_errors()
                {
                    body_meta.add_error(relay_protocol::Error::nonempty());
                }

                Annotated(Some(out), meta)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::assert_annotated_snapshot;

    use super::*;

    #[test]
    fn parse_nel() {
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

        let report: Annotated<BrowserReport> = Annotated::from_json_bytes(json.as_bytes()).unwrap();

        assert_annotated_snapshot!(report, @r#"
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
        "#);
    }

    #[test]
    fn parse_missing_report_type() {
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
            "url": "http://example.com/",
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        }"#;

        let report: Annotated<BrowserReport> = Annotated::from_json_bytes(json.as_bytes()).unwrap();

        assert_annotated_snapshot!(report, @r#"
        {
          "age": 31042,
          "type": null,
          "url": "http://example.com/",
          "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
          "body": null,
          "_meta": {
            "body": {
              "": {
                "err": [
                  [
                    "invalid_data",
                    {
                      "reason": "omitted because of invalid report type"
                    }
                  ]
                ],
                "val": {
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
            },
            "type": {
              "": {
                "err": [
                  [
                    "invalid_data",
                    {
                      "reason": "expected a non-empty value"
                    }
                  ]
                ]
              }
            }
          }
        }
        "#);
    }

    #[test]
    fn parse_unknown_report_type() {
        let json = r#"{
            "age": 31042,
            "body": {
                "frobnicated": true,
                "mana_used": 477
            },
            "type": "future-magic",
            "url": "http://example.com/",
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        }"#;

        let report: Annotated<BrowserReport> = Annotated::from_json_bytes(json.as_bytes()).unwrap();

        assert_annotated_snapshot!(report, @r#"
        {
          "age": 31042,
          "type": null,
          "url": "http://example.com/",
          "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
          "body": null,
          "_meta": {
            "body": {
              "": {
                "err": [
                  [
                    "invalid_data",
                    {
                      "reason": "omitted because of invalid report type"
                    }
                  ]
                ],
                "val": {
                  "frobnicated": true,
                  "mana_used": 477
                }
              }
            },
            "type": {
              "": {
                "err": [
                  [
                    "invalid_data",
                    {
                      "reason": "unknown browser report type"
                    }
                  ]
                ],
                "val": "future-magic"
              }
            }
          }
        }
        "#);
    }
}
