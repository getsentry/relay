use cookie::Cookie;
use url::form_urlencoded;

use crate::types::{Annotated, Array, FromValue, Map, Object, Value};

/// A map holding cookies.
#[derive(Debug, Clone, PartialEq, ToValue, ProcessValue)]
pub struct Cookies(pub Object<String>);

impl std::ops::Deref for Cookies {
    type Target = Object<String>;

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
                let mut cookies = Map::new();
                for cookie in value.split(';') {
                    if cookie.trim().is_empty() {
                        continue;
                    }
                    match Cookie::parse_encoded(cookie) {
                        Ok(cookie) => {
                            cookies.insert(
                                cookie.name().to_string(),
                                Annotated::new(cookie.value().to_string()),
                            );
                        }
                        Err(err) => {
                            meta.add_error(err.to_string());
                            meta.set_original_value(Some(Value::String(cookie.to_string())));
                        }
                    }
                }
                Annotated(Some(Cookies(cookies)), meta)
            }
            annotated @ Annotated(Some(Value::Object(_)), _) => {
                let Annotated(value, meta) = FromValue::from_value(annotated);
                Annotated(value.map(Cookies), meta)
            }
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("cookies", value);
                Annotated(None, meta)
            }
        }
    }
}

/// A map holding headers.
#[derive(Debug, Clone, PartialEq, ToValue, ProcessValue)]
pub struct Headers(pub Array<(Annotated<String>, Annotated<String>)>);

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
    type Target = Array<(Annotated<String>, Annotated<String>)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Headers {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

fn normalize_header(key: &str) -> String {
    key.split('-')
        .enumerate()
        .fold(String::new(), |mut all, (i, part)| {
            // join
            if i > 0 {
                all.push_str("-");
            }

            // capitalize the first characters
            let mut chars = part.chars();
            if let Some(c) = chars.next() {
                all.extend(c.to_uppercase());
            }

            // copy all others
            all.extend(chars);
            all
        })
}

impl FromValue for Headers {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        type HeaderTuple = (Annotated<String>, Annotated<String>);

        match value {
            Annotated(Some(Value::Array(items)), meta) => {
                let mut rv = Vec::new();
                for item in items.into_iter() {
                    rv.push(
                        HeaderTuple::from_value(item)
                            .map_value(|(k, v)| (k.and_then(|k| normalize_header(&k)), v)),
                    );
                }
                Annotated(Some(Headers(rv)), meta)
            }
            Annotated(Some(Value::Object(items)), meta) => {
                let mut rv = Vec::new();
                for (key, value) in items.into_iter() {
                    rv.push((normalize_header(&key), String::from_value(value)));
                }
                rv.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                Annotated(
                    Some(Headers(
                        rv.into_iter()
                            .map(|(k, v)| Annotated::new((Annotated::new(k), v)))
                            .collect(),
                    )),
                    meta,
                )
            }
            other => FromValue::from_value(other).map_value(Headers),
        }
    }
}

/// A map holding query string pairs.
#[derive(Debug, Clone, PartialEq, ToValue, ProcessValue)]
pub struct Query(pub Object<String>);

impl std::ops::Deref for Query {
    type Target = Object<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Query {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromValue for Query {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(v)), meta) => {
                let mut rv = Object::new();
                let qs = if v.starts_with('?') { &v[1..] } else { &v[..] };
                for (key, value) in form_urlencoded::parse(qs.as_bytes()) {
                    rv.insert(key.to_string(), Annotated::new(value.to_string()));
                }
                Annotated(Some(Query(rv)), meta)
            }
            Annotated(Some(Value::Object(items)), meta) => Annotated(
                Some(Query(
                    items
                        .into_iter()
                        .map(|(k, v)| match v {
                            v @ Annotated(Some(Value::String(_)), _)
                            | v @ Annotated(Some(Value::Null), _) => (k, FromValue::from_value(v)),
                            v => {
                                let v = match v {
                                    v @ Annotated(Some(Value::Object(_)), _)
                                    | v @ Annotated(Some(Value::Array(_)), _) => {
                                        let meta = v.1.clone();
                                        let json_val: serde_json::Value = v.into();
                                        Annotated(
                                            Some(Value::String(
                                                serde_json::to_string(&json_val).unwrap(),
                                            )),
                                            meta,
                                        )
                                    }
                                    other => other,
                                };
                                (k, FromValue::from_value(v))
                            }
                        }).collect(),
                )),
                meta,
            ),
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("query-string or map", value);
                Annotated(None, meta)
            }
        }
    }
}

/// Http request information.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_request")]
pub struct Request {
    /// URL of the request.
    #[metastructure(pii_kind = "freeform", max_chars = "path")]
    pub url: Annotated<String>,

    /// HTTP request method.
    pub method: Annotated<String>,

    /// Request data in any format that makes sense.
    // TODO: Custom logic + info
    #[metastructure(pii_kind = "databag", bag_size = "large")]
    pub data: Annotated<Value>,

    /// URL encoded HTTP query string.
    #[metastructure(pii_kind = "databag", bag_size = "small")]
    pub query_string: Annotated<Query>,

    /// The fragment of the request URL.
    #[metastructure(pii_kind = "freeform", max_chars = "summary")]
    pub fragment: Annotated<String>,

    /// URL encoded contents of the Cookie header.
    #[metastructure(pii_kind = "databag", bag_size = "medium")]
    pub cookies: Annotated<Cookies>,

    /// HTTP request headers.
    #[metastructure(pii_kind = "databag")]
    #[metastructure(pii_kind = "databag", bag_size = "large")]
    pub headers: Annotated<Headers>,

    /// Server environment data, such as CGI/WSGI.
    #[metastructure(pii_kind = "databag", bag_size = "large")]
    pub env: Annotated<Object<Value>>,

    /// The inferred content type of the request payload.
    pub inferred_content_type: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, pii_kind = "databag")]
    pub other: Object<Value>,
}

#[test]
fn test_header_normalization() {
    let json = r#"{
  "-other-": "header",
  "accept": "application/json",
  "x-sentry": "version=8"
}"#;

    let mut headers = Vec::new();
    headers.push(Annotated::new((
        Annotated::new("-Other-".to_string()),
        Annotated::new("header".to_string()),
    )));
    headers.push(Annotated::new((
        Annotated::new("Accept".to_string()),
        Annotated::new("application/json".to_string()),
    )));
    headers.push(Annotated::new((
        Annotated::new("X-Sentry".to_string()),
        Annotated::new("version=8".to_string()),
    )));

    let headers = Annotated::new(Headers(headers));
    assert_eq_dbg!(headers, Annotated::from_json(json).unwrap());
}

#[test]
fn test_header_from_sequence() {
    let json = r#"[
  ["accept", "application/json"]
]"#;

    let mut headers = Vec::new();
    headers.push(Annotated::new((
        Annotated::new("Accept".to_string()),
        Annotated::new("application/json".to_string()),
    )));

    let headers = Annotated::new(Headers(headers));
    assert_eq_dbg!(headers, Annotated::from_json(json).unwrap());

    let json = r#"[
  ["accept", "application/json"],
  ["whatever", 42],
  [1, 2],
  ["a", "b", "c"],
  23
]"#;
    let headers = Annotated::<Headers>::from_json(json).unwrap();
    #[derive(ToValue, Debug)]
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
      "Whatever",
      null
    ],
    [
      null,
      null
    ],
    null,
    null
  ],
  "_meta": {
    "headers": {
      "1": {
        "1": {
          "": {
            "err": [
              "expected a string"
            ],
            "val": 42
          }
        }
      },
      "2": {
        "0": {
          "": {
            "err": [
              "expected a string"
            ],
            "val": 1
          }
        },
        "1": {
          "": {
            "err": [
              "expected a string"
            ],
            "val": 2
          }
        }
      },
      "3": {
        "": {
          "err": [
            "expected tuple"
          ],
          "val": [
            "a",
            "b",
            "c"
          ]
        }
      },
      "4": {
        "": {
          "err": [
            "expected tuple"
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
  "query_string": {
    "q": "foo"
  },
  "fragment": "home",
  "cookies": {
    "GOOGLE": "1"
  },
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
        query_string: Annotated::new(Query({
            let mut map = Object::new();
            map.insert("q".to_string(), Annotated::new("foo".to_string()));
            map
        })),
        fragment: Annotated::new("home".to_string()),
        cookies: Annotated::new(Cookies({
            let mut map = Map::new();
            map.insert("GOOGLE".to_string(), Annotated::new("1".to_string()));
            map
        })),
        headers: Annotated::new(Headers({
            let mut headers = Vec::new();
            headers.push(Annotated::new((
                Annotated::new("Referer".to_string()),
                Annotated::new("https://google.com/".to_string()),
            )));
            headers
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
    let mut map = Object::new();
    map.insert("foo".to_string(), Annotated::new("bar".to_string()));
    let query = Annotated::new(Query(map));
    assert_eq_dbg!(query, Annotated::from_json("\"foo=bar\"").unwrap());
    assert_eq_dbg!(query, Annotated::from_json("\"?foo=bar\"").unwrap());

    let mut map = Object::new();
    map.insert("foo".to_string(), Annotated::new("bar".to_string()));
    map.insert("baz".to_string(), Annotated::new("42".to_string()));
    let query = Annotated::new(Query(map));
    assert_eq_dbg!(query, Annotated::from_json("\"foo=bar&baz=42\"").unwrap());
}

#[test]
fn test_query_string_legacy_nested() {
    // this test covers a case that previously was let through the ingest system but in a bad
    // way.  This was untyped and became a str repr() in Python.  New SDKs will no longer send
    // nested objects here but for legacy values we instead serialize it out as JSON.
    let mut map = Object::new();
    map.insert("foo".to_string(), Annotated::new("bar".to_string()));
    let query = Annotated::new(Query(map));
    assert_eq_dbg!(query, Annotated::from_json("\"foo=bar\"").unwrap());

    let mut map = Object::new();
    map.insert("foo".to_string(), Annotated::new("bar".to_string()));
    map.insert("baz".to_string(), Annotated::new(r#"{"a":42}"#.to_string()));
    let query = Annotated::new(Query(map));
    assert_eq_dbg!(
        query,
        Annotated::from_json(
            r#"
        {
            "foo": "bar",
            "baz": {"a": 42}
        }
    "#
        ).unwrap()
    );
}

#[test]
fn test_query_invalid() {
    let query =
        Annotated::<Query>::from_error("expected query-string or map", Some(Value::U64(64)));
    assert_eq_dbg!(query, Annotated::from_json("42").unwrap());
}

#[test]
fn test_cookies_parsing() {
    let json = "\" PHPSESSID=298zf09hf012fh2; csrftoken=u32t4o3tb3gg43; _gat=1;\"";

    let mut map = Map::new();
    map.insert(
        "PHPSESSID".to_string(),
        Annotated::new("298zf09hf012fh2".to_string()),
    );
    map.insert(
        "csrftoken".to_string(),
        Annotated::new("u32t4o3tb3gg43".to_string()),
    );
    map.insert("_gat".to_string(), Annotated::new("1".to_string()));

    let cookies = Annotated::new(Cookies(map));
    assert_eq_dbg!(cookies, Annotated::from_json(json).unwrap());
}

#[test]
fn test_cookies_object() {
    let json = r#"{"foo":"bar", "invalid": 42}"#;

    let mut map = Object::new();
    map.insert("foo".to_string(), Annotated::new("bar".to_string()));
    map.insert(
        "invalid".to_string(),
        Annotated::from_error("expected a string", Some(Value::U64(42))),
    );

    let cookies = Annotated::new(Cookies(map));
    assert_eq_dbg!(cookies, Annotated::from_json(json).unwrap());
}

#[test]
fn test_cookies_invalid() {
    let cookies = Annotated::<Cookies>::from_error("expected cookies", Some(Value::I64(42)));
    assert_eq_dbg!(cookies, Annotated::from_json("42").unwrap());
}
