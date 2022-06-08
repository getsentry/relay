use crate::processor::ProcessValue;
use crate::protocol::LenientString;
use crate::types::{
    Annotated, Empty, Error, ErrorKind, FromValue, IntoValue, MetaMap, SkipSerialization, Value,
};

/// A fingerprint value.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Fingerprint(Vec<String>);

impl std::ops::Deref for Fingerprint {
    type Target = Vec<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Fingerprint {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<String>> for Fingerprint {
    fn from(vec: Vec<String>) -> Fingerprint {
        Fingerprint(vec)
    }
}

impl Empty for Fingerprint {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn is_deep_empty(&self) -> bool {
        self.0.iter().all(Empty::is_deep_empty)
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
                    if meta.original_length().is_none() {
                        meta.set_original_length(Some(fingerprint.len() + bad_values.len()));
                    }

                    meta.add_error(Error::with(ErrorKind::InvalidData, |error| {
                        error.insert("value", bad_values);
                    }));
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
                meta.add_error(Error::expected("an array"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    fn attach_meta_map(&mut self, _meta_map: MetaMap) {
        // since we apparently store the fingerprint in a bare Vec<String> as opposed to
        // Vec<Annotated<_>>, we can't store child meta
    }
}

impl IntoValue for Fingerprint {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        Value::Array(
            self.0
                .into_iter()
                .map(|x| Annotated::new(Value::String(x)))
                .collect(),
        )
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
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
    use crate::types::Meta;

    let bad_values = vec![Annotated::new(Value::F64(-1e100))];

    let mut meta = Meta::from_error(Error::with(ErrorKind::InvalidData, |e| {
        e.insert("value", bad_values);
    }));
    meta.set_original_length(Some(1));

    assert_eq_dbg!(
        Annotated(None, meta),
        Annotated::<Fingerprint>::from_json("[-1e100]").unwrap()
    );
}

#[test]
fn test_fingerprint_float_bounds() {
    use crate::types::Meta;

    let bad_values = vec![Annotated::new(Value::F64(
        #[allow(clippy::excessive_precision)]
        1.797_693_134_862_315_7e+308,
    ))];

    let mut meta = Meta::from_error(Error::with(ErrorKind::InvalidData, |e| {
        e.insert("value", bad_values);
    }));
    meta.set_original_length(Some(1));

    assert_eq_dbg!(
        Annotated(None, meta),
        Annotated::<Fingerprint>::from_json("[1.7976931348623157e+308]").unwrap()
    );
}

#[test]
fn test_fingerprint_invalid_fallback() {
    // XXX: review, this was changed after refactor
    assert_eq_dbg!(
        Annotated::new(Fingerprint(vec!["a".to_string(), "d".to_string()])),
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
