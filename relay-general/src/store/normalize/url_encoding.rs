use crate::types::{Annotated, Value};

/// Decodes an urlencoded body.
pub fn encoded_from_str(raw: &str) -> Option<Value> {
    // Binary strings would be decoded, but we know url-encoded bodies are ASCII.
    if !raw.is_ascii() {
        return None;
    }

    // Avoid false positives with XML and partial JSON.
    if raw.starts_with("<?xml") || raw.starts_with('{') || raw.starts_with('[') {
        return None;
    }

    // serde_urlencoded always deserializes into `Value::Object`.
    let object = match serde_urlencoded::from_str(raw) {
        Ok(Value::Object(value)) => value,
        _ => return None,
    };

    // `serde_urlencoded` can decode any string with valid characters into an object. However, we
    // need to account for false-positives in the following cases:
    //  - An empty string "" is decoded as empty object
    //  - A string "foo" is decoded as {"foo": ""} (check for single empty value)
    //  - A base64 encoded string "dGU=" also decodes with a single empty value
    //  - A base64 encoded string "dA==" decodes as {"dA": "="} (check for single =)
    let is_valid = object.len() > 1
        || object
            .values()
            .next()
            .and_then(Annotated::<Value>::as_str)
            .map_or(false, |s| !matches!(s, "" | "="));

    if is_valid {
        Some(Value::Object(object))
    } else {
        None
    }
}
