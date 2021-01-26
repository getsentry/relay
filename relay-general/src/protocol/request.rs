use std::iter::{FromIterator, IntoIterator};

use cookie::Cookie;
use url::form_urlencoded;

#[cfg(feature = "jsonschema")]
use schemars::gen::SchemaGenerator;
#[cfg(feature = "jsonschema")]
use schemars::schema::Schema;

use crate::protocol::{JsonLenientString, LenientString, PairList};
use crate::types::{Annotated, Error, FromValue, Object, Value};

type CookieEntry = Annotated<(Annotated<String>, Annotated<String>)>;

/// A map holding cookies.
#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
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
                cookie.name().to_string().into(),
                cookie.value().to_string().into(),
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
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
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
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
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

                        header_value.push_str(&string);
                    }
                }

                Annotated(Some(HeaderValue::new(header_value)), meta)
            }
            annotated => LenientString::from_value(annotated).map_value(HeaderValue::new),
        }
    }
}

/// A map holding headers.
#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Headers(pub PairList<(Annotated<HeaderName>, Annotated<HeaderValue>)>);

impl Headers {
    pub fn get_header(&self, key: &str) -> Option<&str> {
        for item in self.iter() {
            if let Some((ref k, ref v)) = item.value() {
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
#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
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

#[cfg(feature = "jsonschema")]
impl schemars::JsonSchema for Query {
    fn schema_name() -> String {
        "Query".to_string()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Schema {
        #[derive(schemars::JsonSchema)]
        #[schemars(untagged)]
        #[allow(unused)]
        enum Helper {
            QueryString(String),
            CanonicalQueryObject(PairList<(Annotated<String>, Annotated<String>)>),
        }

        Helper::json_schema(gen)
    }

    fn is_referenceable() -> bool {
        false
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
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_request", value_type = "Request")]
pub struct Request {
    /// The URL of the request if available.
    ///
    ///The query string can be declared either as part of the `url`, or separately in `query_string`.
    #[metastructure(max_chars = "path")]
    pub url: Annotated<String>,

    /// HTTP request method.
    pub method: Annotated<String>,

    /// Request data in any format that makes sense.
    ///
    /// SDKs should discard large and binary bodies by default. Can be given as string or
    /// structural data of any format.
    #[metastructure(pii = "true", bag_size = "large")]
    pub data: Annotated<Value>,

    /// The query string component of the URL.
    ///
    /// Can be given as unparsed string, dictionary, or list of tuples.
    ///
    /// If the query string is not declared and part of the `url`, Sentry moves it to the
    /// query string.
    #[metastructure(pii = "true", bag_size = "small")]
    #[metastructure(skip_serialization = "empty")]
    pub query_string: Annotated<Query>,

    /// The fragment of the request URL.
    #[metastructure(pii = "true", max_chars = "summary")]
    #[metastructure(skip_serialization = "empty")]
    pub fragment: Annotated<String>,

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

    /// Server environment data, such as CGI/WSGI.
    ///
    /// A dictionary containing environment information passed from the server. This is where
    /// information such as CGI/WSGI/Rack keys go that are not HTTP headers.
    ///
    /// Sentry will explicitly look for `REMOTE_ADDR` to extract an IP address.
    #[metastructure(pii = "true", bag_size = "large")]
    #[metastructure(skip_serialization = "empty")]
    pub env: Annotated<Object<Value>>,

    /// The inferred content type of the request payload.
    #[metastructure(skip_serialization = "empty")]
    pub inferred_content_type: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii = "true")]
    pub other: Object<Value>,
}

#[test]
fn test_header_normalization() {
    let json = r#"{
  "-other-": "header",
  "accept": "application/json",
  "WWW-Authenticate": "basic",
  "x-sentry": "version=8"
}"#;

    let mut headers = Vec::new();
    headers.push(Annotated::new((
        Annotated::new("-Other-".to_string().into()),
        Annotated::new("header".to_string().into()),
    )));
    headers.push(Annotated::new((
        Annotated::new("Accept".to_string().into()),
        Annotated::new("application/json".to_string().into()),
    )));
    headers.push(Annotated::new((
        Annotated::new("WWW-Authenticate".to_string().into()),
        Annotated::new("basic".to_string().into()),
    )));
    headers.push(Annotated::new((
        Annotated::new("X-Sentry".to_string().into()),
        Annotated::new("version=8".to_string().into()),
    )));

    let headers = Annotated::new(Headers(PairList(headers)));
    assert_eq_dbg!(headers, Annotated::from_json(json).unwrap());
}

#[test]
fn test_header_from_sequence() {
    let json = r#"[
  ["accept", "application/json"]
]"#;

    let mut headers = Vec::new();
    headers.push(Annotated::new((
        Annotated::new("Accept".to_string().into()),
        Annotated::new("application/json".to_string().into()),
    )));

    let headers = Annotated::new(Headers(PairList(headers)));
    assert_eq_dbg!(headers, Annotated::from_json(json).unwrap());

    let json = r#"[
  ["accept", "application/json"],
  [1, 2],
  ["a", "b", "c"],
  23
]"#;
    let headers = Annotated::<Headers>::from_json(json).unwrap();
    #[derive(Debug, Empty, ToValue)]
    pub struct Container {
        headers: Annotated<Headers>,
    }
    assert_eq_str!(
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
  "env": {
    "REMOTE_ADDR": "213.47.147.207"
  },
  "inferred_content_type": "application/json",
  "other": "value"
}"#;

    let request = Annotated::new(Request {
        url: Annotated::new("https://google.com/search".to_string()),
        method: Annotated::new("GET".to_string()),
        data: {
            let mut map = Object::new();
            map.insert("some".to_string(), Annotated::new(Value::I64(1)));
            Annotated::new(Value::Object(map))
        },
        query_string: Annotated::new(Query(
            vec![Annotated::new((
                Annotated::new("q".to_string()),
                Annotated::new("foo".to_string().into()),
            ))]
            .into(),
        )),
        fragment: Annotated::new("home".to_string()),
        cookies: Annotated::new(Cookies({
            let mut map = Vec::new();
            map.push(Annotated::new((
                Annotated::new("GOOGLE".to_string()),
                Annotated::new("1".to_string()),
            )));
            PairList(map)
        })),
        headers: Annotated::new(Headers({
            let mut headers = Vec::new();
            headers.push(Annotated::new((
                Annotated::new("Referer".to_string().into()),
                Annotated::new("https://google.com/".to_string().into()),
            )));
            PairList(headers)
        })),
        env: Annotated::new({
            let mut map = Object::new();
            map.insert(
                "REMOTE_ADDR".to_string(),
                Annotated::new(Value::String("213.47.147.207".to_string())),
            );
            map
        }),
        inferred_content_type: Annotated::new("application/json".to_string()),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(request, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, request.to_json_pretty().unwrap());
}

#[test]
fn test_query_string() {
    let query = Annotated::new(Query(
        vec![Annotated::new((
            Annotated::new("foo".to_string()),
            Annotated::new("bar".to_string().into()),
        ))]
        .into(),
    ));
    assert_eq_dbg!(query, Annotated::from_json("\"foo=bar\"").unwrap());
    assert_eq_dbg!(query, Annotated::from_json("\"?foo=bar\"").unwrap());

    let query = Annotated::new(Query(
        vec![
            Annotated::new((
                Annotated::new("foo".to_string()),
                Annotated::new("bar".to_string().into()),
            )),
            Annotated::new((
                Annotated::new("baz".to_string()),
                Annotated::new("42".to_string().into()),
            )),
        ]
        .into(),
    ));
    assert_eq_dbg!(query, Annotated::from_json("\"foo=bar&baz=42\"").unwrap());
}

#[test]
fn test_query_string_legacy_nested() {
    // this test covers a case that previously was let through the ingest system but in a bad
    // way.  This was untyped and became a str repr() in Python.  New SDKs will no longer send
    // nested objects here but for legacy values we instead serialize it out as JSON.
    let query = Annotated::new(Query(
        vec![Annotated::new((
            Annotated::new("foo".to_string()),
            Annotated::new("bar".to_string().into()),
        ))]
        .into(),
    ));
    assert_eq_dbg!(query, Annotated::from_json("\"foo=bar\"").unwrap());

    let query = Annotated::new(Query(
        vec![
            Annotated::new((
                Annotated::new("baz".to_string()),
                Annotated::new(r#"{"a":42}"#.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("foo".to_string()),
                Annotated::new("bar".to_string().into()),
            )),
        ]
        .into(),
    ));
    assert_eq_dbg!(
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
    assert_eq_dbg!(query, Annotated::from_json("42").unwrap());
}

#[test]
fn test_cookies_parsing() {
    let json = "\" PHPSESSID=298zf09hf012fh2; csrftoken=u32t4o3tb3gg43; _gat=1;\"";

    let mut map = Vec::new();
    map.push(Annotated::new((
        Annotated::new("PHPSESSID".to_string()),
        Annotated::new("298zf09hf012fh2".to_string()),
    )));
    map.push(Annotated::new((
        Annotated::new("csrftoken".to_string()),
        Annotated::new("u32t4o3tb3gg43".to_string()),
    )));
    map.push(Annotated::new((
        Annotated::new("_gat".to_string()),
        Annotated::new("1".to_string()),
    )));

    let cookies = Annotated::new(Cookies(PairList(map)));
    assert_eq_dbg!(cookies, Annotated::from_json(json).unwrap());
}

#[test]
fn test_cookies_array() {
    let input = r#"{"cookies":[["foo","bar"],["invalid", 42],["none",null]]}"#;
    let output = r#"{"cookies":[["foo","bar"],["invalid",null],["none",null]],"_meta":{"cookies":{"1":{"1":{"":{"err":[["invalid_data",{"reason":"expected a string"}]],"val":42}}}}}}"#;

    let mut map = Vec::new();
    map.push(Annotated::new((
        Annotated::new("foo".to_string()),
        Annotated::new("bar".to_string()),
    )));
    map.push(Annotated::new((
        Annotated::new("invalid".to_string()),
        Annotated::from_error(Error::expected("a string"), Some(Value::I64(42))),
    )));
    map.push(Annotated::new((
        Annotated::new("none".to_string()),
        Annotated::empty(),
    )));

    let cookies = Annotated::new(Cookies(PairList(map)));
    let request = Annotated::new(Request {
        cookies,
        ..Default::default()
    });
    assert_eq_dbg!(request, Annotated::from_json(input).unwrap());
    assert_eq_dbg!(request.to_json().unwrap(), output);
}

#[test]
fn test_cookies_object() {
    let json = r#"{"foo":"bar", "invalid": 42}"#;

    let mut map = Vec::new();
    map.push(Annotated::new((
        Annotated::new("foo".to_string()),
        Annotated::new("bar".to_string()),
    )));
    map.push(Annotated::new((
        Annotated::new("invalid".to_string()),
        Annotated::from_error(Error::expected("a string"), Some(Value::I64(42))),
    )));

    let cookies = Annotated::new(Cookies(PairList(map)));
    assert_eq_dbg!(cookies, Annotated::from_json(json).unwrap());
}

#[test]
fn test_cookies_invalid() {
    let cookies =
        Annotated::<Cookies>::from_error(Error::expected("cookies"), Some(Value::I64(42)));
    assert_eq_dbg!(cookies, Annotated::from_json("42").unwrap());
}

#[test]
fn test_querystring_without_value() {
    let json = r#""foo=bar&baz""#;

    let query = Annotated::new(Query(
        vec![
            Annotated::new((
                Annotated::new("foo".to_string()),
                Annotated::new("bar".to_string().into()),
            )),
            Annotated::new((
                Annotated::new("baz".to_string()),
                Annotated::new("".to_string().into()),
            )),
        ]
        .into(),
    ));

    assert_eq_dbg!(query, Annotated::from_json(json).unwrap());
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
                Annotated::new("X-Bar".to_string().into()),
                Annotated::new("42".to_string().into()),
            )),
            Annotated::new((
                Annotated::new("X-Foo".to_string().into()),
                Annotated::new("".to_string().into()),
            )),
        ]))),
        ..Default::default()
    });

    assert_eq_dbg!(Annotated::from_json(input).unwrap(), request);
    assert_eq_dbg!(request.to_json_pretty().unwrap(), output);
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
                Annotated::new("X-Bar".to_string().into()),
                Annotated::new("42,bar,baz".to_string().into()),
            )),
            Annotated::new((
                Annotated::new("X-Foo".to_string().into()),
                Annotated::new("".to_string().into()),
            )),
        ]))),
        ..Default::default()
    });

    assert_eq_dbg!(Annotated::from_json(input).unwrap(), request);
    assert_eq_dbg!(request.to_json_pretty().unwrap(), output);
}
