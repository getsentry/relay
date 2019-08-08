//! Common types of the protocol.
use std::borrow::Cow;
use std::fmt;
use std::iter::{FromIterator, IntoIterator};
use std::net;
use std::str::FromStr;

use failure::Fail;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::processor::{process_value, ProcessValue, ProcessingState, Processor, ValueType};
use crate::types::{
    Annotated, Array, Empty, Error, ErrorKind, FromValue, Meta, Object, SkipSerialization, ToValue,
    Value, ValueAction,
};

/// A array like wrapper used in various places.
#[derive(Clone, Debug, PartialEq, Empty, ToValue, ProcessValue)]
#[metastructure(process_func = "process_values")]
pub struct Values<T> {
    /// The values of the collection.
    #[metastructure(required = "true", skip_serialization = "empty_deep")]
    pub values: Annotated<Array<T>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

impl<T> Default for Values<T> {
    fn default() -> Values<T> {
        // Default implemented manually even if <T> does not impl Default.
        Values::new(Vec::new())
    }
}

impl<T> Values<T> {
    /// Constructs a new value array from a given array
    pub fn new(values: Array<T>) -> Values<T> {
        Values {
            values: Annotated::new(values),
            other: Object::default(),
        }
    }
}

impl<T: FromValue> FromValue for Values<T> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            // Example:
            // {"threads": [foo, bar, baz]}
            Annotated(Some(Value::Array(items)), meta) => Annotated(
                Some(Values {
                    values: Annotated(
                        Some(items.into_iter().map(FromValue::from_value).collect()),
                        meta,
                    ),
                    other: Default::default(),
                }),
                Meta::default(),
            ),
            Annotated(Some(Value::Object(mut obj)), meta) => {
                if obj.is_empty() {
                    // Example:
                    // {"exception": {}}
                    // {"threads": {}}
                    Annotated(None, meta)
                } else if let Some(values) = obj.remove("values") {
                    // Example:
                    // {"exception": {"values": [foo, bar, baz]}}
                    Annotated(
                        Some(Values {
                            values: FromValue::from_value(values),
                            other: obj,
                        }),
                        meta,
                    )
                } else {
                    // Example:
                    // {"exception": {"type": "ZeroDivisonError"}
                    Annotated(
                        Some(Values {
                            values: Annotated(
                                Some(vec![FromValue::from_value(Annotated(
                                    Some(Value::Object(obj)),
                                    meta,
                                ))]),
                                Default::default(),
                            ),
                            other: Default::default(),
                        }),
                        Meta::default(),
                    )
                }
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("a list or values object"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

/// A trait to abstract over pairs.
pub trait AsPair {
    type Key: AsRef<str>;
    type Value: ProcessValue;

    /// Constructs this value from a raw tuple.
    fn from_pair(pair: (Annotated<Self::Key>, Annotated<Self::Value>)) -> Self;

    /// Converts this pair into a raw tuple.
    fn into_pair(self) -> (Annotated<Self::Key>, Annotated<Self::Value>);

    /// Extracts a key and value pair from the object.
    fn as_pair(&self) -> (&Annotated<Self::Key>, &Annotated<Self::Value>);

    /// Extracts the mutable key and value pair from the object.
    fn as_pair_mut(&mut self) -> (&mut Annotated<Self::Key>, &mut Annotated<Self::Value>);

    /// Returns a reference to the string representation of the key.
    fn key(&self) -> Option<&str> {
        self.as_pair().0.as_str()
    }

    /// Returns a reference to the value.
    fn value(&self) -> Option<&Self::Value> {
        self.as_pair().1.value()
    }
}

impl<K, V> AsPair for (Annotated<K>, Annotated<V>)
where
    K: AsRef<str>,
    V: ProcessValue,
{
    type Key = K;
    type Value = V;

    fn from_pair(pair: (Annotated<Self::Key>, Annotated<Self::Value>)) -> Self {
        pair
    }

    fn into_pair(self) -> (Annotated<Self::Key>, Annotated<Self::Value>) {
        self
    }

    fn as_pair(&self) -> (&Annotated<Self::Key>, &Annotated<Self::Value>) {
        (&self.0, &self.1)
    }

    fn as_pair_mut(&mut self) -> (&mut Annotated<Self::Key>, &mut Annotated<Self::Value>) {
        (&mut self.0, &mut self.1)
    }
}

/// A mixture of a hashmap and an array.
#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue)]
pub struct PairList<T>(pub Array<T>);

impl<T, K, V> PairList<T>
where
    K: AsRef<str>,
    V: ProcessValue,
    T: AsPair<Key = K, Value = V>,
{
    /// Searches for an entry with the given key and returns its position.
    pub fn position<Q>(&self, key: Q) -> Option<usize>
    where
        Q: AsRef<str>,
    {
        let key = key.as_ref();
        self.0
            .iter()
            .filter_map(Annotated::value)
            .position(|entry| entry.as_pair().0.as_str() == Some(key))
    }

    /// Removes an entry matching the given key and returns its value, if found.
    pub fn remove<Q>(&mut self, key: Q) -> Option<Annotated<V>>
    where
        Q: AsRef<str>,
    {
        self.position(key)
            .and_then(|index| self.0.remove(index).0)
            .map(|entry| entry.into_pair().1)
    }

    /// Inserts a value into the list and returns the old value.
    pub fn insert(&mut self, key: K, value: Annotated<V>) -> Option<Annotated<V>> {
        match self.position(key.as_ref()) {
            Some(index) => self
                .get_mut(index)
                .and_then(|annotated| annotated.value_mut().as_mut())
                .map(|pair| std::mem::replace(pair.as_pair_mut().1, value)),
            None => {
                self.push(Annotated::new(T::from_pair((Annotated::new(key), value))));
                None
            }
        }
    }
}

impl<T> std::ops::Deref for PairList<T> {
    type Target = Array<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for PairList<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<A> FromIterator<Annotated<A>> for PairList<A> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Annotated<A>>,
    {
        PairList(FromIterator::from_iter(iter))
    }
}

impl<T> From<Array<T>> for PairList<T> {
    fn from(value: Array<T>) -> Self {
        PairList(value)
    }
}

impl<T: FromValue> FromValue for PairList<T> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Array(items)), meta) => {
                let mut rv = Vec::new();
                for item in items.into_iter() {
                    rv.push(T::from_value(item));
                }
                Annotated(Some(PairList(rv)), meta)
            }
            Annotated(Some(Value::Object(items)), meta) => {
                let mut rv = Vec::new();
                for (key, value) in items.into_iter() {
                    rv.push(T::from_value(Annotated::new(Value::Array(vec![
                        Annotated::new(Value::String(key)),
                        value,
                    ]))));
                }
                Annotated(Some(PairList(rv)), meta)
            }
            other => FromValue::from_value(other).map_value(PairList),
        }
    }
}

impl<T> ProcessValue for PairList<T>
where
    T: ProcessValue + AsPair,
{
    #[inline]
    fn value_type(&self) -> Option<ValueType> {
        Some(ValueType::Object)
    }

    #[inline]
    fn process_value<P>(
        &mut self,
        meta: &mut Meta,
        processor: &mut P,
        state: &ProcessingState<'_>,
    ) -> ValueAction
    where
        P: Processor,
    {
        processor.process_pairlist(self, meta, state)
    }

    fn process_child_values<P>(&mut self, processor: &mut P, state: &ProcessingState<'_>)
    where
        P: Processor,
    {
        for (idx, pair) in self.0.iter_mut().enumerate() {
            let state = state.enter_index(idx, state.inner_attrs(), ValueType::for_field(pair));
            process_value(pair, processor, &state);
        }
    }
}

macro_rules! hex_metrastructure {
    ($type:ident, $expectation:expr) => {
        impl FromStr for $type {
            type Err = std::num::ParseIntError;

            fn from_str(s: &str) -> Result<$type, Self::Err> {
                if s.starts_with("0x") || s.starts_with("0X") {
                    u64::from_str_radix(&s[2..], 16).map($type)
                } else {
                    u64::from_str_radix(&s, 10).map($type)
                }
            }
        }

        impl fmt::Display for $type {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{:#x}", self.0)
            }
        }

        impl Empty for $type {
            #[inline]
            fn is_empty(&self) -> bool {
                false
            }
        }

        impl FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                        Ok(value) => Annotated(Some(value), meta),
                        Err(err) => {
                            meta.add_error(Error::invalid(err));
                            meta.set_original_value(Some(value));
                            Annotated(None, meta)
                        }
                    },
                    Annotated(Some(Value::U64(value)), meta) => Annotated(Some($type(value)), meta),
                    Annotated(Some(Value::I64(value)), meta) => {
                        Annotated(Some($type(value as u64)), meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(Some(value), mut meta) => {
                        meta.add_error(Error::expected($expectation));
                        meta.set_original_value(Some(value));
                        Annotated(None, meta)
                    }
                }
            }
        }

        impl ToValue for $type {
            fn to_value(self) -> Value {
                Value::String(self.to_string())
            }
            fn serialize_payload<S>(
                &self,
                s: S,
                _behavior: crate::types::SkipSerialization,
            ) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: Serializer,
            {
                Serialize::serialize(&self.to_string(), s)
            }
        }

        impl ProcessValue for $type {}
    };
}

/// Raised if a register value can't be parsed.
#[derive(Fail, Debug)]
#[fail(display = "invalid register value")]
pub struct InvalidRegVal;

/// A register value.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RegVal(pub u64);

hex_metrastructure!(RegVal, "register value");

/// An address
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Addr(pub u64);

hex_metrastructure!(Addr, "address");

/// An ip address.
#[derive(
    Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Empty, ToValue, ProcessValue, Serialize,
)]
pub struct IpAddr(pub String);

impl IpAddr {
    /// Returns the auto marker ip address.
    pub fn auto() -> IpAddr {
        IpAddr("{{auto}}".into())
    }

    /// Parses an `IpAddr` from a string
    pub fn parse<S>(value: S) -> Result<Self, S>
    where
        S: AsRef<str> + Into<String>,
    {
        if value.as_ref() == "{{auto}}" {
            return Ok(IpAddr(value.into()));
        }

        match net::IpAddr::from_str(value.as_ref()) {
            Ok(_) => Ok(IpAddr(value.into())),
            Err(_) => Err(value),
        }
    }

    /// Checks if the ip address is set to the auto marker.
    pub fn is_auto(&self) -> bool {
        self.0 == "{{auto}}"
    }

    /// Returns the string value of this ip address.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for IpAddr {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Default for IpAddr {
    fn default() -> Self {
        IpAddr::auto()
    }
}

impl From<std::net::IpAddr> for IpAddr {
    fn from(ip_addr: std::net::IpAddr) -> Self {
        Self(ip_addr.to_string())
    }
}

impl fmt::Display for IpAddr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<'de> Deserialize<'de> for IpAddr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = Cow::<'_, str>::deserialize(deserializer)?;
        IpAddr::parse(string).map_err(|_| serde::de::Error::custom("expected an ip address"))
    }
}

impl FromValue for IpAddr {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match IpAddr::parse(value) {
                Ok(addr) => Annotated(Some(addr), meta),
                Err(value) => {
                    meta.add_error(Error::expected("an ip address"));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("an ip address"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

/// An error used when parsing `Level`.
#[derive(Debug, Fail)]
#[fail(display = "invalid level")]
pub struct ParseLevelError;

/// Severity level of an event or breadcrumb.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Level {
    /// Indicates very spammy debug information.
    Debug,
    /// Informational messages.
    Info,
    /// A warning.
    Warning,
    /// An error.
    Error,
    /// Similar to error but indicates a critical event that usually causes a shutdown.
    Fatal,
}

impl Default for Level {
    fn default() -> Self {
        Level::Info
    }
}

impl Level {
    fn from_python_level(value: u64) -> Option<Level> {
        Some(match value {
            10 => Level::Debug,
            20 => Level::Info,
            30 => Level::Warning,
            40 => Level::Error,
            50 => Level::Fatal,
            _ => return None,
        })
    }
}

impl FromStr for Level {
    type Err = ParseLevelError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(match string {
            "debug" => Level::Debug,
            "info" | "log" => Level::Info,
            "warning" => Level::Warning,
            "error" => Level::Error,
            "fatal" | "critical" => Level::Fatal,
            _ => return Err(ParseLevelError),
        })
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Level::Debug => write!(f, "debug"),
            Level::Info => write!(f, "info"),
            Level::Warning => write!(f, "warning"),
            Level::Error => write!(f, "error"),
            Level::Fatal => write!(f, "fatal"),
        }
    }
}

impl FromValue for Level {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                Ok(value) => Annotated(Some(value), meta),
                Err(err) => {
                    meta.add_error(Error::invalid(err));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(Some(Value::U64(val)), mut meta) => match Level::from_python_level(val) {
                Some(value) => Annotated(Some(value), meta),
                None => {
                    meta.add_error(ErrorKind::InvalidData);
                    meta.set_original_value(Some(val));
                    Annotated(None, meta)
                }
            },
            Annotated(Some(Value::I64(val)), mut meta) => {
                match Level::from_python_level(val as u64) {
                    Some(value) => Annotated(Some(value), meta),
                    None => {
                        meta.add_error(ErrorKind::InvalidData);
                        meta.set_original_value(Some(val));
                        Annotated(None, meta)
                    }
                }
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("a level"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl ToValue for Level {
    fn to_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(&self.to_string(), s)
    }
}

impl ProcessValue for Level {}

impl Empty for Level {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

/// A "into-string" type of value. Emulates an invocation of `str(x)` in Python
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Empty, ToValue, ProcessValue)]
pub struct LenientString(pub String);

impl LenientString {
    /// Returns the string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Unwraps the inner raw string.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for LenientString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for LenientString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for LenientString {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<String> for LenientString {
    fn from(value: String) -> Self {
        LenientString(value)
    }
}

impl FromValue for LenientString {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(string)), meta) => Annotated(Some(string), meta),
            // XXX: True/False instead of true/false because of old python code
            Annotated(Some(Value::Bool(true)), meta) => Annotated(Some("True".to_string()), meta),
            Annotated(Some(Value::Bool(false)), meta) => Annotated(Some("False".to_string()), meta),
            Annotated(Some(Value::U64(num)), meta) => Annotated(Some(num.to_string()), meta),
            Annotated(Some(Value::I64(num)), meta) => Annotated(Some(num.to_string()), meta),
            Annotated(Some(Value::F64(num)), mut meta) => {
                if num.abs() < (1i64 << 53) as f64 {
                    Annotated(Some(num.trunc().to_string()), meta)
                } else {
                    meta.add_error(Error::expected("a number with JSON precision"));
                    meta.set_original_value(Some(num));
                    Annotated(None, meta)
                }
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("a primitive value"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
        .map_value(LenientString)
    }
}

/// A "into-string" type of value. All non-string values are serialized as JSON.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Empty, ToValue, ProcessValue)]
pub struct JsonLenientString(pub String);

impl JsonLenientString {
    /// Returns the string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Unwraps the inner raw string.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for JsonLenientString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for JsonLenientString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for JsonLenientString {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromValue for JsonLenientString {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(string)), meta) => Annotated(Some(string.into()), meta),
            Annotated(Some(other), meta) => {
                Annotated(Some(serde_json::to_string(&other).unwrap().into()), meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl From<String> for JsonLenientString {
    fn from(value: String) -> Self {
        JsonLenientString(value)
    }
}

#[test]
fn test_values_serialization() {
    let value = Annotated::new(Values {
        values: Annotated::new(vec![
            Annotated::new(0u64),
            Annotated::new(1u64),
            Annotated::new(2u64),
        ]),
        other: Object::default(),
    });
    assert_eq!(value.to_json().unwrap(), "{\"values\":[0,1,2]}");
}

#[test]
fn test_values_deserialization() {
    #[derive(Clone, Debug, Empty, FromValue, ToValue, PartialEq)]
    struct Exception {
        #[metastructure(field = "type")]
        ty: Annotated<String>,
        value: Annotated<String>,
    }
    let value = Annotated::<Values<Exception>>::from_json(
        r#"{"values": [{"type": "Test", "value": "aha!"}]}"#,
    )
    .unwrap();
    assert_eq!(
        value,
        Annotated::new(Values::new(vec![Annotated::new(Exception {
            ty: Annotated::new("Test".to_string()),
            value: Annotated::new("aha!".to_string()),
        })]))
    );

    let value = Annotated::<Values<Exception>>::from_json(r#"[{"type": "Test", "value": "aha!"}]"#)
        .unwrap();
    assert_eq!(
        value,
        Annotated::new(Values::new(vec![Annotated::new(Exception {
            ty: Annotated::new("Test".to_string()),
            value: Annotated::new("aha!".to_string()),
        })]))
    );

    let value =
        Annotated::<Values<Exception>>::from_json(r#"{"type": "Test", "value": "aha!"}"#).unwrap();
    assert_eq!(
        value,
        Annotated::new(Values::new(vec![Annotated::new(Exception {
            ty: Annotated::new("Test".to_string()),
            value: Annotated::new("aha!".to_string()),
        })]))
    );
}

#[test]
fn test_hex_to_string() {
    assert_eq_str!("0x0", &Addr(0).to_string());
    assert_eq_str!("0x2a", &Addr(42).to_string());
}

#[test]
fn test_hex_from_string() {
    assert_eq_dbg!(Addr(0), "0".parse().unwrap());
    assert_eq_dbg!(Addr(42), "42".parse().unwrap());
    assert_eq_dbg!(Addr(42), "0x2a".parse().unwrap());
    assert_eq_dbg!(Addr(42), "0X2A".parse().unwrap());
}

#[test]
fn test_hex_serialization() {
    let value = Value::String("0x2a".to_string());
    let addr: Annotated<Addr> = FromValue::from_value(Annotated::new(value));
    assert_eq!(addr.payload_to_json().unwrap(), "\"0x2a\"");
    let value = Value::U64(42);
    let addr: Annotated<Addr> = FromValue::from_value(Annotated::new(value));
    assert_eq!(addr.payload_to_json().unwrap(), "\"0x2a\"");
}

#[test]
fn test_hex_deserialization() {
    let addr = Annotated::<Addr>::from_json("\"0x2a\"").unwrap();
    assert_eq!(addr.payload_to_json().unwrap(), "\"0x2a\"");
    let addr = Annotated::<Addr>::from_json("42").unwrap();
    assert_eq!(addr.payload_to_json().unwrap(), "\"0x2a\"");
}

#[test]
fn test_level() {
    assert_eq_dbg!(
        Level::Info,
        Annotated::<Level>::from_json("\"log\"").unwrap().0.unwrap()
    );
    assert_eq_dbg!(
        Level::Warning,
        Annotated::<Level>::from_json("30").unwrap().0.unwrap()
    );
    assert_eq_dbg!(
        Level::Fatal,
        Annotated::<Level>::from_json("\"critical\"")
            .unwrap()
            .0
            .unwrap()
    );
}

#[test]
fn test_ip_addr() {
    assert_eq_dbg!(
        IpAddr("{{auto}}".into()),
        Annotated::<IpAddr>::from_json("\"{{auto}}\"")
            .unwrap()
            .0
            .unwrap()
    );
    assert_eq_dbg!(
        IpAddr("127.0.0.1".into()),
        Annotated::<IpAddr>::from_json("\"127.0.0.1\"")
            .unwrap()
            .0
            .unwrap()
    );
    assert_eq_dbg!(
        IpAddr("::1".into()),
        Annotated::<IpAddr>::from_json("\"::1\"")
            .unwrap()
            .0
            .unwrap()
    );
    assert_eq_dbg!(
        Annotated::from_error(
            Error::expected("an ip address"),
            Some(Value::String("clearly invalid value".into()))
        ),
        Annotated::<IpAddr>::from_json("\"clearly invalid value\"").unwrap()
    );
}
