use crate::protocol::{Cookies, Headers};
use crate::types::{Annotated, Object, Value};

/// Response interface that contains information on a HTTP response related to the event.
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

    /// Additional arbitrary fields for forwards compatibility.
    /// These fields are retained (`retain = "true"`) to keep supporting the format that the Dio integration sends:
    /// https://github.com/getsentry/sentry-dart/blob/7011abe27ac69bd160bdc6ecf3314974b8340b97/dart/lib/src/protocol/sentry_response.dart#L4-L8
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl ResponseContext {
    /// The key under which a runtime context is generally stored (in `Contexts`).
    pub fn default_key() -> &'static str {
        "response"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        protocol::{Context, PairList},
        types::Annotated,
    };
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
