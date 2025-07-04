use cookie::Cookie;
use relay_protocol::{Annotated, Empty, Error, FromValue, IntoValue, Object, Value};
use url::form_urlencoded;

use crate::processor::ProcessValue;
use crate::protocol::{JsonLenientString, LenientString, PairList};

type CookieEntry = Annotated<(Annotated<String>, Annotated<String>)>;

/// A map holding cookies.
#[derive(Clone, Debug, Default, PartialEq, Empty, IntoValue, ProcessValue)]
pub struct Cookies(pub PairList<(Annotated<String>, Annotated<String>)>);

impl Cookies {
    pub fn parse(string: &str) -> Result<Self, Error> {
        let pairs: Result<_, _> = Self::iter_cookies(string).collect();
        pairs.map(Cookies)
    }

    fn iter_cookies(string: &str) -> impl Iterator<Item = Result<CookieEntry, Error>> + '_ {
        string
            .split(';')
            .filter(|cookie| !cookie.trim().is_empty())
            .map(Cookies::parse_cookie)
    }

    fn parse_cookie(string: &str) -> Result<CookieEntry, Error> {
        match Cookie::parse_encoded(string) {
            Ok(cookie) => Ok(Annotated::from((
                cookie.name().to_owned().into(),
                cookie.value().to_owned().into(),
            ))),
            Err(error) => Err(Error::invalid(error)),
        }
    }
}

impl std::ops::Deref for Cookies {
    type Target = PairList<(Annotated<String>, Annotated<String>)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Cookies {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromValue for Cookies {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => {
                let mut cookies = Vec::new();
                for result in Cookies::iter_cookies(&value) {
                    match result {
                        Ok(cookie) => cookies.push(cookie),
                        Err(error) => meta.add_error(error),
                    }
                }

                if meta.has_errors() && meta.original_value().is_none() {
                    meta.set_original_value(Some(value));
                }

                Annotated(Some(Cookies(PairList(cookies))), meta)
            }
            annotated @ Annotated(Some(Value::Object(_)), _)
            | annotated @ Annotated(Some(Value::Array(_)), _) => {
                PairList::from_value(annotated).map_value(Cookies)
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("cookies"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

/// A "into-string" type that normalizes header names.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Empty, IntoValue, ProcessValue)]
#[metastructure(process_func = "process_header_name")]
pub struct HeaderName(String);

impl HeaderName {
    /// Creates a normalized header name.
    pub fn new<S: AsRef<str>>(name: S) -> Self {
        let name = name.as_ref();
        let mut normalized = String::with_capacity(name.len());

        name.chars().fold(true, |uppercase, c| {
            if uppercase {
                normalized.extend(c.to_uppercase());
            } else {
                normalized.push(c); // does not lowercase on purpose
            }
            c == '-'
        });

        HeaderName(normalized)
    }

    /// Returns the string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Unwraps the inner raw string.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for HeaderName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for HeaderName {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for HeaderName {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<String> for HeaderName {
    fn from(value: String) -> Self {
        HeaderName::new(value)
    }
}

impl From<&'_ str> for HeaderName {
    fn from(value: &str) -> Self {
        HeaderName::new(value)
    }
}

impl FromValue for HeaderName {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        String::from_value(value).map_value(HeaderName::new)
    }
}

/// A "into-string" type that normalizes header values.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Empty, IntoValue, ProcessValue)]
pub struct HeaderValue(String);

impl HeaderValue {
    pub fn new<S: AsRef<str>>(value: S) -> Self {
        HeaderValue(value.as_ref().to_owned())
    }
}

impl AsRef<str> for HeaderValue {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for HeaderValue {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for HeaderValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<String> for HeaderValue {
    fn from(value: String) -> Self {
        HeaderValue::new(value)
    }
}

impl From<&'_ str> for HeaderValue {
    fn from(value: &str) -> Self {
        HeaderValue::new(value)
    }
}

impl FromValue for HeaderValue {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Array(array)), mut meta) => {
                let mut header_value = String::new();
                for array_value in array {
                    let array_value = LenientString::from_value(array_value);
                    for error in array_value.meta().iter_errors() {
                        meta.add_error(error.clone());
                    }

                    if let Some(string) = array_value.value() {
                        if !header_value.is_empty() {
                            header_value.push(',');
                        }

                        header_value.push_str(string);
                    }
                }

                Annotated(Some(HeaderValue::new(header_value)), meta)
            }
            annotated => LenientString::from_value(annotated).map_value(HeaderValue::new),
        }
    }
}

/// A map holding headers.
#[derive(Clone, Debug, Default, PartialEq, Empty, IntoValue, ProcessValue)]
pub struct Headers(pub PairList<(Annotated<HeaderName>, Annotated<HeaderValue>)>);

impl Headers {
    pub fn get_header(&self, key: &str) -> Option<&str> {
        for item in self.iter() {
            if let Some((k, v)) = item.value() {
                if k.as_str() == Some(key) {
                    return v.as_str();
                }
            }
        }

        None
    }
}

impl std::ops::Deref for Headers {
    type Target = PairList<(Annotated<HeaderName>, Annotated<HeaderValue>)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Headers {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromValue for Headers {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        // Preserve order if SDK sent headers as array
        let should_sort = matches!(value.value(), Some(Value::Object(_)));

        type HeaderTuple = (Annotated<HeaderName>, Annotated<HeaderValue>);
        PairList::<HeaderTuple>::from_value(value).map_value(|mut pair_list| {
            if should_sort {
                pair_list.sort_unstable_by(|a, b| {
                    a.value()
                        .map(|x| x.0.value())
                        .cmp(&b.value().map(|x| x.0.value()))
                });
            }

            Headers(pair_list)
        })
    }
}

/// A map holding query string pairs.
#[derive(Clone, Debug, Default, PartialEq, Empty, IntoValue, ProcessValue)]
pub struct Query(pub PairList<(Annotated<String>, Annotated<JsonLenientString>)>);

impl Query {
    pub fn parse(mut string: &str) -> Self {
        if string.starts_with('?') {
            string = &string[1..];
        }

        form_urlencoded::parse(string.as_bytes()).collect()
    }
}

impl std::ops::Deref for Query {
    type Target = PairList<(Annotated<String>, Annotated<JsonLenientString>)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Query {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K, V> FromIterator<(K, V)> for Query
where
    K: Into<String>,
    V: Into<String>,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (K, V)>,
    {
        let pairs = iter.into_iter().map(|(key, value)| {
            Annotated::new((
                Annotated::new(key.into()),
                Annotated::new(value.into().into()),
            ))
        });

        Query(pairs.collect())
    }
}

impl FromValue for Query {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(v)), meta) => Annotated(Some(Query::parse(&v)), meta),
            annotated @ Annotated(Some(Value::Object(_)), _)
            | annotated @ Annotated(Some(Value::Array(_)), _) => {
                PairList::from_value(annotated).map_value(Query)
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("a query string or map"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

/// Http request information.
///
/// The Request interface contains information on a HTTP request related to the event. In client
/// SDKs, this can be an outgoing request, or the request that rendered the current web page. On
/// server SDKs, this could be the incoming web request that is being handled.
///
/// The data variable should only contain the request body (not the query string). It can either be
/// a dictionary (for standard HTTP requests) or a raw request body.
///
/// ### Ordered Maps
///
/// In the Request interface, several attributes can either be declared as string, object, or list
/// of tuples. Sentry attempts to parse structured information from the string representation in
/// such cases.
///
/// Sometimes, keys can be declared multiple times, or the order of elements matters. In such
/// cases, use the tuple representation over a plain object.
///
/// Example of request headers as object:
///
/// ```json
/// {
///   "content-type": "application/json",
///   "accept": "application/json, application/xml"
/// }
/// ```
///
/// Example of the same headers as list of tuples:
///
/// ```json
/// [
///   ["content-type", "application/json"],
///   ["accept", "application/json"],
///   ["accept", "application/xml"]
/// ]
/// ```
///
/// Example of a fully populated request object:
///
/// ```json
/// {
///   "request": {
///     "method": "POST",
///     "url": "http://absolute.uri/foo",
///     "query_string": "query=foobar&page=2",
///     "data": {
///       "foo": "bar"
///     },
///     "cookies": "PHPSESSID=298zf09hf012fh2; csrftoken=u32t4o3tb3gg43; _gat=1;",
///     "headers": {
///       "content-type": "text/html"
///     },
///     "env": {
///       "REMOTE_ADDR": "192.168.0.1"
///     }
///   }
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[metastructure(process_func = "process_request", value_type = "Request")]
pub struct Request {
    /// The URL of the request if available.
    ///
    ///The query string can be declared either as part of the `url`, or separately in `query_string`.
    #[metastructure(max_chars = 256, max_chars_allowance = 40, pii = "maybe")]
    pub url: Annotated<String>,

    /// HTTP request method.
    pub method: Annotated<String>,

    /// HTTP protocol.
    pub protocol: Annotated<String>,

    /// Request data in any format that makes sense.
    ///
    /// SDKs should discard large and binary bodies by default. Can be given as a string or
    /// structural data of any format.
    #[metastructure(pii = "true", max_depth = 7, max_bytes = 8192)]
    pub data: Annotated<Value>,

    /// The query string component of the URL.
    ///
    /// Can be given as unparsed string, dictionary, or list of tuples.
    ///
    /// If the query string is not declared and part of the `url`, Sentry moves it to the
    /// query string.
    #[metastructure(pii = "true", max_depth = 3, max_bytes = 1024)]
    #[metastructure(skip_serialization = "empty")]
    pub query_string: Annotated<Query>,

    /// The fragment of the request URI.
    #[metastructure(pii = "true", max_chars = 1024, max_chars_allowance = 100)]
    #[metastructure(skip_serialization = "empty")]
    pub fragment: Annotated<String>,

    /// The cookie values.
    ///
    /// Can be given unparsed as string, as dictionary, or as a list of tuples.
    #[metastructure(pii = "true", max_depth = 5, max_bytes = 2048)]
    #[metastructure(skip_serialization = "empty")]
    pub cookies: Annotated<Cookies>,

    /// A dictionary of submitted headers.
    ///
    /// If a header appears multiple times it, needs to be merged according to the HTTP standard
    /// for header merging. Header names are treated case-insensitively by Sentry.
    #[metastructure(pii = "true", max_depth = 7, max_bytes = 8192)]
    #[metastructure(skip_serialization = "empty")]
    pub headers: Annotated<Headers>,

    /// HTTP request body size.
    pub body_size: Annotated<u64>,

    /// Server environment data, such as CGI/WSGI.
    ///
    /// A dictionary containing environment information passed from the server. This is where
    /// information such as CGI/WSGI/Rack keys go that are not HTTP headers.
    ///
    /// Sentry will explicitly look for `REMOTE_ADDR` to extract an IP address.
    #[metastructure(pii = "true", max_depth = 7, max_bytes = 8192)]
    #[metastructure(skip_serialization = "empty")]
    pub env: Annotated<Object<Value>>,

    /// The inferred content type of the request payload.
    #[metastructure(skip_serialization = "empty")]
    pub inferred_content_type: Annotated<String>,

    /// The API target/specification that made the request.
    ///
    /// Values can be `graphql`, `rest`, etc.
    ///
    /// The data field should contain the request and response bodies based on its target specification.
    ///
    /// This information can be used for better data scrubbing and normalization.
    pub api_target: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii = "true")]
    pub other: Object<Value>,
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_header_normalization() {
        let json = r#"{
  "-other-": "header",
  "accept": "application/json",
  "WWW-Authenticate": "basic",
  "x-sentry": "version=8"
}"#;

        let headers = vec![
            Annotated::new((
                Annotated::new("-Other-".to_owned().into()),
                Annotated::new("header".to_owned().into()),
            )),
            Annotated::new((
                Annotated::new("Accept".to_owned().into()),
                Annotated::new("application/json".to_owned().into()),
            )),
            Annotated::new((
                Annotated::new("WWW-Authenticate".to_owned().into()),
                Annotated::new("basic".to_owned().into()),
            )),
            Annotated::new((
                Annotated::new("X-Sentry".to_owned().into()),
                Annotated::new("version=8".to_owned().into()),
            )),
        ];

        let headers = Annotated::new(Headers(PairList(headers)));
        assert_eq!(headers, Annotated::from_json(json).unwrap());
    }

    #[test]
    fn test_header_from_sequence() {
        let json = r#"[
  ["accept", "application/json"]
]"#;

        let headers = vec![Annotated::new((
            Annotated::new("Accept".to_owned().into()),
            Annotated::new("application/json".to_owned().into()),
        ))];

        let headers = Annotated::new(Headers(PairList(headers)));
        assert_eq!(headers, Annotated::from_json(json).unwrap());

        let json = r#"[
  ["accept", "application/json"],
  [1, 2],
  ["a", "b", "c"],
  23
]"#;
        let headers = Annotated::<Headers>::from_json(json).unwrap();
        #[derive(Debug, Empty, IntoValue)]
        pub struct Container {
            headers: Annotated<Headers>,
        }
        assert_eq!(
            Annotated::new(Container { headers })
                .to_json_pretty()
                .unwrap(),
            r#"{
  "headers": [
    [
      "Accept",
      "application/json"
    ],
    [
      null,
      "2"
    ],
    null,
    null
  ],
  "_meta": {
    "headers": {
      "1": {
        "0": {
          "": {
            "err": [
              [
                "invalid_data",
                {
                  "reason": "expected a string"
                }
              ]
            ],
            "val": 1
          }
        }
      },
      "2": {
        "": {
          "err": [
            [
              "invalid_data",
              {
                "reason": "expected a tuple"
              }
            ]
          ],
          "val": [
            "a",
            "b",
            "c"
          ]
        }
      },
      "3": {
        "": {
          "err": [
            [
              "invalid_data",
              {
                "reason": "expected a tuple"
              }
            ]
          ],
          "val": 23
        }
      }
    }
  }
}"#
        );
    }

    #[test]
    fn test_request_roundtrip() {
        let json = r#"{
  "url": "https://google.com/search",
  "method": "GET",
  "data": {
    "some": 1
  },
  "query_string": [
    [
      "q",
      "foo"
    ]
  ],
  "fragment": "home",
  "cookies": [
    [
      "GOOGLE",
      "1"
    ]
  ],
  "headers": [
    [
      "Referer",
      "https://google.com/"
    ]
  ],
  "body_size": 1024,
  "env": {
    "REMOTE_ADDR": "213.47.147.207"
  },
  "inferred_content_type": "application/json",
  "api_target": "graphql",
  "other": "value"
}"#;

        let request = Annotated::new(Request {
            url: Annotated::new("https://google.com/search".to_owned()),
            method: Annotated::new("GET".to_owned()),
            protocol: Annotated::empty(),
            data: {
                let mut map = Object::new();
                map.insert("some".to_owned(), Annotated::new(Value::I64(1)));
                Annotated::new(Value::Object(map))
            },
            query_string: Annotated::new(Query(
                vec![Annotated::new((
                    Annotated::new("q".to_owned()),
                    Annotated::new("foo".to_owned().into()),
                ))]
                .into(),
            )),
            fragment: Annotated::new("home".to_owned()),
            cookies: Annotated::new(Cookies({
                PairList(vec![Annotated::new((
                    Annotated::new("GOOGLE".to_owned()),
                    Annotated::new("1".to_owned()),
                ))])
            })),
            headers: Annotated::new(Headers({
                let headers = vec![Annotated::new((
                    Annotated::new("Referer".to_owned().into()),
                    Annotated::new("https://google.com/".to_owned().into()),
                ))];
                PairList(headers)
            })),
            body_size: Annotated::new(1024),
            env: Annotated::new({
                let mut map = Object::new();
                map.insert(
                    "REMOTE_ADDR".to_owned(),
                    Annotated::new(Value::String("213.47.147.207".to_owned())),
                );
                map
            }),
            inferred_content_type: Annotated::new("application/json".to_owned()),
            api_target: Annotated::new("graphql".to_owned()),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_owned(),
                    Annotated::new(Value::String("value".to_owned())),
                );
                map
            },
        });

        assert_eq!(request, Annotated::from_json(json).unwrap());
        assert_eq!(json, request.to_json_pretty().unwrap());
    }

    #[test]
    fn test_query_string() {
        let query = Annotated::new(Query(
            vec![Annotated::new((
                Annotated::new("foo".to_owned()),
                Annotated::new("bar".to_owned().into()),
            ))]
            .into(),
        ));
        assert_eq!(query, Annotated::from_json("\"foo=bar\"").unwrap());
        assert_eq!(query, Annotated::from_json("\"?foo=bar\"").unwrap());

        let query = Annotated::new(Query(
            vec![
                Annotated::new((
                    Annotated::new("foo".to_owned()),
                    Annotated::new("bar".to_owned().into()),
                )),
                Annotated::new((
                    Annotated::new("baz".to_owned()),
                    Annotated::new("42".to_owned().into()),
                )),
            ]
            .into(),
        ));
        assert_eq!(query, Annotated::from_json("\"foo=bar&baz=42\"").unwrap());
    }

    #[test]
    fn test_query_string_legacy_nested() {
        // this test covers a case that previously was let through the ingest system but in a bad
        // way.  This was untyped and became a str repr() in Python.  New SDKs will no longer send
        // nested objects here but for legacy values we instead serialize it out as JSON.
        let query = Annotated::new(Query(
            vec![Annotated::new((
                Annotated::new("foo".to_owned()),
                Annotated::new("bar".to_owned().into()),
            ))]
            .into(),
        ));
        assert_eq!(query, Annotated::from_json("\"foo=bar\"").unwrap());

        let query = Annotated::new(Query(
            vec![
                Annotated::new((
                    Annotated::new("baz".to_owned()),
                    Annotated::new(r#"{"a":42}"#.to_owned().into()),
                )),
                Annotated::new((
                    Annotated::new("foo".to_owned()),
                    Annotated::new("bar".to_owned().into()),
                )),
            ]
            .into(),
        ));
        assert_eq!(
            query,
            Annotated::from_json(
                r#"
        {
            "foo": "bar",
            "baz": {"a": 42}
        }
    "#
            )
            .unwrap()
        );
    }

    #[test]
    fn test_query_invalid() {
        let query = Annotated::<Query>::from_error(
            Error::expected("a query string or map"),
            Some(Value::I64(42)),
        );
        assert_eq!(query, Annotated::from_json("42").unwrap());
    }

    #[test]
    fn test_cookies_parsing() {
        let json = "\" PHPSESSID=298zf09hf012fh2; csrftoken=u32t4o3tb3gg43; _gat=1;\"";

        let map = vec![
            Annotated::new((
                Annotated::new("PHPSESSID".to_owned()),
                Annotated::new("298zf09hf012fh2".to_owned()),
            )),
            Annotated::new((
                Annotated::new("csrftoken".to_owned()),
                Annotated::new("u32t4o3tb3gg43".to_owned()),
            )),
            Annotated::new((
                Annotated::new("_gat".to_owned()),
                Annotated::new("1".to_owned()),
            )),
        ];

        let cookies = Annotated::new(Cookies(PairList(map)));
        assert_eq!(cookies, Annotated::from_json(json).unwrap());
    }

    #[test]
    fn test_cookies_array() {
        let input = r#"{"cookies":[["foo","bar"],["invalid", 42],["none",null]]}"#;
        let output = r#"{"cookies":[["foo","bar"],["invalid",null],["none",null]],"_meta":{"cookies":{"1":{"1":{"":{"err":[["invalid_data",{"reason":"expected a string"}]],"val":42}}}}}}"#;

        let map = vec![
            Annotated::new((
                Annotated::new("foo".to_owned()),
                Annotated::new("bar".to_owned()),
            )),
            Annotated::new((
                Annotated::new("invalid".to_owned()),
                Annotated::from_error(Error::expected("a string"), Some(Value::I64(42))),
            )),
            Annotated::new((Annotated::new("none".to_owned()), Annotated::empty())),
        ];

        let cookies = Annotated::new(Cookies(PairList(map)));
        let request = Annotated::new(Request {
            cookies,
            ..Default::default()
        });
        assert_eq!(request, Annotated::from_json(input).unwrap());
        assert_eq!(request.to_json().unwrap(), output);
    }

    #[test]
    fn test_cookies_object() {
        let json = r#"{"foo":"bar", "invalid": 42}"#;

        let map = vec![
            Annotated::new((
                Annotated::new("foo".to_owned()),
                Annotated::new("bar".to_owned()),
            )),
            Annotated::new((
                Annotated::new("invalid".to_owned()),
                Annotated::from_error(Error::expected("a string"), Some(Value::I64(42))),
            )),
        ];

        let cookies = Annotated::new(Cookies(PairList(map)));
        assert_eq!(cookies, Annotated::from_json(json).unwrap());
    }

    #[test]
    fn test_cookies_invalid() {
        let cookies =
            Annotated::<Cookies>::from_error(Error::expected("cookies"), Some(Value::I64(42)));
        assert_eq!(cookies, Annotated::from_json("42").unwrap());
    }

    #[test]
    fn test_querystring_without_value() {
        let json = r#""foo=bar&baz""#;

        let query = Annotated::new(Query(
            vec![
                Annotated::new((
                    Annotated::new("foo".to_owned()),
                    Annotated::new("bar".to_owned().into()),
                )),
                Annotated::new((
                    Annotated::new("baz".to_owned()),
                    Annotated::new("".to_owned().into()),
                )),
            ]
            .into(),
        ));

        assert_eq!(query, Annotated::from_json(json).unwrap());
    }

    #[test]
    fn test_headers_lenient_value() {
        let input = r#"{
  "headers": {
    "X-Foo": "",
    "X-Bar": 42
  }
}"#;

        let output = r#"{
  "headers": [
    [
      "X-Bar",
      "42"
    ],
    [
      "X-Foo",
      ""
    ]
  ]
}"#;

        let request = Annotated::new(Request {
            headers: Annotated::new(Headers(PairList(vec![
                Annotated::new((
                    Annotated::new("X-Bar".to_owned().into()),
                    Annotated::new("42".to_owned().into()),
                )),
                Annotated::new((
                    Annotated::new("X-Foo".to_owned().into()),
                    Annotated::new("".to_owned().into()),
                )),
            ]))),
            ..Default::default()
        });

        assert_eq!(Annotated::from_json(input).unwrap(), request);
        assert_eq!(request.to_json_pretty().unwrap(), output);
    }

    #[test]
    fn test_headers_multiple_values() {
        let input = r#"{
  "headers": {
    "X-Foo": [""],
    "X-Bar": [
      42,
      "bar",
      "baz"
    ]
  }
}"#;

        let output = r#"{
  "headers": [
    [
      "X-Bar",
      "42,bar,baz"
    ],
    [
      "X-Foo",
      ""
    ]
  ]
}"#;

        let request = Annotated::new(Request {
            headers: Annotated::new(Headers(PairList(vec![
                Annotated::new((
                    Annotated::new("X-Bar".to_owned().into()),
                    Annotated::new("42,bar,baz".to_owned().into()),
                )),
                Annotated::new((
                    Annotated::new("X-Foo".to_owned().into()),
                    Annotated::new("".to_owned().into()),
                )),
            ]))),
            ..Default::default()
        });

        assert_eq!(Annotated::from_json(input).unwrap(), request);
        assert_eq!(request.to_json_pretty().unwrap(), output);
    }
}
