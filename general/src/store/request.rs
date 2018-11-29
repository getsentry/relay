use lazy_static::lazy_static;
use regex::Regex;
use serde::de::IgnoredAny;
use url::Url;

use crate::protocol::{Query, Request};
use crate::types::{Annotated, Object, Value};

lazy_static! {
    static ref METHOD_RE: Regex = Regex::new(r"^[A-Z\-_]{3,32}$").unwrap();
}

fn infer_content_type(body: &Annotated<Value>) -> Option<String> {
    let body = body.value()?.as_str()?;

    if serde_json::from_str::<IgnoredAny>(body).is_ok() {
        Some("application/json".to_string())
    } else if serde_urlencoded::from_str::<IgnoredAny>(body).is_ok() {
        Some("application/x-www-form-urlencoded".to_string())
    } else {
        None
    }
}

fn normalize_url(request: &mut Request) {
    if let Annotated(Some(ref mut url_string), ref mut meta) = request.url {
        match Url::parse(&url_string) {
            Ok(mut url) => {
                // If either the query string or fragment is specified both as part of
                // the URL and as separate attribute, the attribute wins.
                request.query_string.get_or_insert_with(|| {
                    Query(
                        url.query_pairs()
                            .map(|(k, v)| (k.into(), Annotated::new(v.into())))
                            .collect(),
                    )
                });

                if let Some(fragment) = url.fragment() {
                    request
                        .fragment
                        .0
                        .get_or_insert_with(|| fragment.to_string());
                }

                // Remove the fragment and query since they have been moved to their own
                // parameters to avoid duplication or inconsistencies.
                url.set_fragment(None);
                url.set_query(None);

                // TODO: Check if this generates unwanted effects with `meta.remarks`
                // when the URL was already PII stripped.
                *url_string = url.into_string();
            }
            Err(err) => {
                meta.add_error(err.to_string(), None);
            }
        }
    }
}

fn normalize_method(method: &mut Annotated<String>) {
    if let Some(ref mut method) = method.0 {
        *method = method.to_uppercase();
    }

    if method
        .0
        .as_ref()
        .map(|x| METHOD_RE.is_match(x))
        .unwrap_or(true)
    {
        *method = Annotated::from_error("invalid http method", None);
    }
}

fn set_auto_remote_addr(env: &mut Annotated<Object<Value>>, remote_addr: &str) {
    if let Some(ref mut env) = env.0 {
        env.entry("REMOTE_ADDR".to_string()).and_modify(|value| {
            value.modify(|value| {
                value.filter_map(Annotated::is_valid, |v| match v.as_str() {
                    Some("{{auto}}") => Value::String(remote_addr.to_string()),
                    _ => v,
                })
            })
        });
    }
}

pub fn normalize_request(request: &mut Request, client_ip: Option<&str>) {
    normalize_method(&mut request.method);

    if request.url.is_valid() {
        normalize_url(request);
    }

    if let Some(ref client_ip) = client_ip {
        set_auto_remote_addr(&mut request.env, client_ip);
    }

    if !request.inferred_content_type.is_present() {
        let content_type = request
            .headers
            .value()
            .and_then(|headers| headers.get("Content-Type"))
            .and_then(|annotated| annotated.value().cloned())
            .or_else(|| infer_content_type(&request.data));

        request.inferred_content_type.set_value(content_type);
    }
}
