use crate::processor::{FromValue, ProcessValue, ToValue};
use crate::protocol::LenientString;
use crate::types::{Annotated, Value};

/// A fingerprint value.
#[derive(Debug, Clone, PartialEq)]
pub struct Fingerprint(Vec<String>);

impl std::ops::Deref for Fingerprint {
    type Target = Vec<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<String>> for Fingerprint {
    fn from(vec: Vec<String>) -> Fingerprint {
        Fingerprint(vec)
    }
}

impl FromValue for Fingerprint {
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        match value {
            // TODO: check error reporting here, this seems wrong
            Annotated(Some(Value::Array(array)), mut meta) => {
                let mut fingerprint = vec![];
                let mut bad_values = vec![];
                for elem in array {
                    let Annotated(value, mut elem_meta) = LenientString::from_value(elem);
                    if let (Some(value), false) = (value, elem_meta.has_errors()) {
                        fingerprint.push(value.0);
                    }
                    if let Some(bad_value) = elem_meta.take_original_value() {
                        bad_values.push(Annotated::new(bad_value));
                    }
                }

                if !bad_values.is_empty() {
                    meta.add_error("bad values in fingerprint", Some(Value::Array(bad_values)));
                }
                Annotated(
                    if fingerprint.is_empty() && meta.has_errors() {
                        None
                    } else {
                        Some(Fingerprint(fingerprint))
                    },
                    meta,
                )
            }
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("array", value);
                Annotated(None, meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl ToValue for Fingerprint {
    fn to_value(value: Annotated<Self>) -> Annotated<Value>
    where
        Self: Sized,
    {
        match value {
            Annotated(Some(value), mut meta) => match serde_json::to_value(value.0) {
                Ok(value) => Annotated(Some(value.into()), meta),
                Err(err) => {
                    meta.add_error(err.to_string(), None);
                    Annotated(None, meta)
                }
            },
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    #[inline(always)]
    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.0, s)
    }
}

// Fingerprints must not be trimmed.
impl ProcessValue for Fingerprint {}

#[test]
fn test_fingerprint_string() {
    assert_eq_dbg!(
        Annotated::new(vec!["fingerprint".to_string()].into()),
        Annotated::<Fingerprint>::from_json("[\"fingerprint\"]").unwrap()
    );
}

#[test]
fn test_fingerprint_bool() {
    assert_eq_dbg!(
        Annotated::new(vec!["True".to_string(), "False".to_string()].into()),
        Annotated::<Fingerprint>::from_json("[true, false]").unwrap()
    );
}

#[test]
fn test_fingerprint_number() {
    assert_eq_dbg!(
        Annotated::new(vec!["-22".to_string()].into()),
        Annotated::<Fingerprint>::from_json("[-22]").unwrap()
    );
}

#[test]
fn test_fingerprint_float() {
    assert_eq_dbg!(
        Annotated::new(vec!["3".to_string()].into()),
        Annotated::<Fingerprint>::from_json("[3.0]").unwrap()
    );
}

#[test]
fn test_fingerprint_float_trunc() {
    assert_eq_dbg!(
        Annotated::new(vec!["3".to_string()].into()),
        Annotated::<Fingerprint>::from_json("[3.5]").unwrap()
    );
}

#[test]
fn test_fingerprint_float_strip() {
    assert_eq_dbg!(
        Annotated::from_error(
            "bad values in fingerprint",
            Some(Value::Array(vec![Annotated::new(Value::F64(-1e100))]))
        ),
        Annotated::<Fingerprint>::from_json("[-1e100]").unwrap()
    );
}

#[test]
fn test_fingerprint_float_bounds() {
    assert_eq_dbg!(
        Annotated::from_error(
            "bad values in fingerprint",
            Some(Value::Array(vec![Annotated::new(Value::F64(
                #[cfg_attr(feature = "cargo-clippy", allow(excessive_precision))]
                1.797_693_134_862_315_7e+308
            ))]))
        ),
        Annotated::<Fingerprint>::from_json("[1.7976931348623157e+308]").unwrap()
    );
}

#[test]
fn test_fingerprint_invalid_fallback() {
    use crate::types::Meta;
    // XXX: review, this was changed after refactor
    assert_eq_dbg!(
        Annotated(
            Some(Fingerprint(vec!["a".to_string(), "d".to_string()])),
            Meta::default()
        ),
        Annotated::<Fingerprint>::from_json("[\"a\", null, \"d\"]").unwrap()
    );
}

#[test]
fn test_fingerprint_empty() {
    // XXX: review, this was changed after refactor
    assert_eq_dbg!(
        Annotated::new(vec![].into()),
        Annotated::<Fingerprint>::from_json("[]").unwrap()
    );
}
