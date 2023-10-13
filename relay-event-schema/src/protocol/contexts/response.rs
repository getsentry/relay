#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;
use crate::protocol::{Cookies, Headers};

/// Response interface that contains information on a HTTP response related to the event.
///
/// The data variable should only contain the response body. It can either be
/// a dictionary (for standard HTTP responses) or a raw response body.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct ResponseContext {
    /// The cookie values.
    ///
    /// Can be given unparsed as string, as dictionary, or as a list of tuples.
    #[metastructure(pii = "true", bag_size = "medium")]
    #[metastructure(skip_serialization = "empty")]
    pub cookies: Annotated<Cookies>,

    /// A dictionary of submitted headers.
    ///
    /// If a header appears multiple times it, needs to be merged according to the HTTP standard
    /// for header merging. Header names are treated case-insensitively by Sentry.
    #[metastructure(pii = "true", bag_size = "large")]
    #[metastructure(skip_serialization = "empty")]
    pub headers: Annotated<Headers>,

    /// HTTP status code.
    pub status_code: Annotated<u64>,

    /// HTTP response body size.
    pub body_size: Annotated<u64>,

    /// Response data in any format that makes sense.
    ///
    /// SDKs should discard large and binary bodies by default. Can be given as a string or
    /// structural data of any format.
    #[metastructure(pii = "true", bag_size = "large")]
    pub data: Annotated<Value>,

    /// The inferred content type of the response payload.
    #[metastructure(skip_serialization = "empty")]
    pub inferred_content_type: Annotated<String>,

    /// Origin address.
    ///
    /// The adddress of the origin server, where this response is coming from.
    pub origin: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    /// These fields are retained (`retain = "true"`) to keep supporting the format that the Dio integration sends:
    /// <https://github.com/getsentry/sentry-dart/blob/7011abe27ac69bd160bdc6ecf3314974b8340b97/dart/lib/src/protocol/sentry_response.dart#L4-L8>
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for ResponseContext {
    fn default_key() -> &'static str {
        "response"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Response(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Response(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Response(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Response(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::Annotated;

    use super::*;
    use crate::protocol::{Context, PairList};

    #[test]
    fn test_response_context_roundtrip() {
        let json = r#"{
  "cookies": [
    [
      "PHPSESSID",
      "298zf09hf012fh2"
    ],
    [
      "csrftoken",
      "u32t4o3tb3gg43"
    ],
    [
      "_gat",
      "1"
    ]
  ],
  "headers": [
    [
      "Content-Type",
      "text/html"
    ]
  ],
  "status_code": 500,
  "body_size": 1000,
  "data": {
    "some": 1
  },
  "inferred_content_type": "application/json",
  "arbitrary_field": "arbitrary",
  "type": "response"
}"#;

        let cookies =
            Cookies::parse("PHPSESSID=298zf09hf012fh2; csrftoken=u32t4o3tb3gg43; _gat=1;").unwrap();
        let headers = vec![Annotated::new((
            Annotated::new("content-type".to_string().into()),
            Annotated::new("text/html".to_string().into()),
        ))];
        let context = Annotated::new(Context::Response(Box::new(ResponseContext {
            cookies: Annotated::new(cookies),
            headers: Annotated::new(Headers(PairList(headers))),
            status_code: Annotated::new(500),
            body_size: Annotated::new(1000),
            data: {
                let mut map = Object::new();
                map.insert("some".to_string(), Annotated::new(Value::I64(1)));
                Annotated::new(Value::Object(map))
            },
            inferred_content_type: Annotated::new("application/json".to_string()),
            origin: Annotated::empty(),
            other: {
                let mut map = Object::new();
                map.insert(
                    "arbitrary_field".to_string(),
                    Annotated::new(Value::String("arbitrary".to_string())),
                );
                map
            },
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
