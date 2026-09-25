use serde::de as serde_de;
use serde::de::{
    self, DeserializeSeed, Deserializer, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor,
};
use std::fmt;
use std::marker::PhantomData;

use crate::Meter;

/// Costs associated with different kinds of operations; right now, just have one cost for
/// all operations (but leave the door open for more.)
mod cost {
    pub const UNIT: usize = 1;
}

impl Meter {
    /// Wraps `deserializer`, so that everything it produces is charged to this meter.
    fn wrap<'de, D: Deserializer<'de>>(&mut self, deserializer: D) -> MeteredDeserializer<'_, D> {
        MeteredDeserializer::new(self, deserializer)
    }
}

/// An error returned by [`deserialize`].
#[derive(Debug)]
pub enum Error<E> {
    /// The value did not fit into the operation budget.
    LimitExceeded(usize),
    /// The payload could not be deserialized.
    Serde(E),
}

impl<E> Error<E> {
    /// Returns `true` if deserialization failed because it ran out of budget.
    pub fn is_limit_exceeded(&self) -> bool {
        matches!(self, Self::LimitExceeded(_))
    }

    /// Returns the contained deserialization error, if the budget was not exceeded.
    pub fn into_serde(self) -> Option<E> {
        match self {
            Self::LimitExceeded(_) => None,
            Self::Serde(error) => Some(error),
        }
    }
}

impl<E: fmt::Display> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LimitExceeded(limit) => {
                write!(f, "value exceeds the {limit} operation limit")
            }
            Self::Serde(error) => error.fmt(f),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::LimitExceeded(_) => None,
            Self::Serde(error) => Some(error),
        }
    }
}

/// Deserializes a `T` from `deserializer`, spending at most max_ops doing so.
///
/// Returns [`Error::LimitExceeded`] if the value consumes too many operations.
pub fn deserialize<'de, T, D>(deserializer: D, max_ops: usize) -> Result<T, Error<D::Error>>
where
    T: serde_de::Deserialize<'de>,
    D: Deserializer<'de>,
{
    deserialize_seed(PhantomData::<T>, deserializer, max_ops)
}

/// Like [`deserialize`], but deserializes through a [`DeserializeSeed`].
///
/// Use this for types which need to carry state into their deserialization, for example to
/// enforce a domain specific limit on top of the budget.
pub fn deserialize_seed<'de, S, D>(
    seed: S,
    deserializer: D,
    max_ops: usize,
) -> Result<S::Value, Error<D::Error>>
where
    S: DeserializeSeed<'de>,
    D: Deserializer<'de>,
{
    let mut meter = Meter::new(max_ops);

    match seed.deserialize(meter.wrap(deserializer)) {
        Ok(value) => Ok(value),
        // The budget is checked first, because the deserializer the error travelled through is
        // free to replace it with an error of its own.
        Err(_) if meter.exceeded() => Err(Error::LimitExceeded(max_ops)),
        Err(error) => Err(Error::Serde(error)),
    }
}

/// A [`Deserializer`] which charges every value it produces to a [`Meter`].
/// Deserialization fails as soon as the operation budget is exhausted.
pub struct MeteredDeserializer<'m, D> {
    meter: &'m mut Meter,
    inner: D,
}

impl<'m, 'de, D: Deserializer<'de>> MeteredDeserializer<'m, D> {
    fn new(meter: &'m mut Meter, inner: D) -> Self {
        Self { meter, inner }
    }
}

/// Forwards a [`Deserializer`] method to the wrapped deserializer, metering its visitor.
macro_rules! forward {
    ($($method:ident($($arg:ident: $ty:ty),*)),* $(,)?) => {
        $(
            fn $method<V: Visitor<'de>>(
                self,
                $($arg: $ty,)*
                visitor: V,
            ) -> Result<V::Value, Self::Error> {
                let visitor = MeteredVisitor::new(self.meter, visitor);
                self.inner.$method($($arg,)* visitor)
            }
        )*
    };
}

impl<'de, D: Deserializer<'de>> Deserializer<'de> for MeteredDeserializer<'_, D> {
    type Error = D::Error;

    forward! {
        deserialize_any(),
        deserialize_bool(),
        deserialize_i8(),
        deserialize_i16(),
        deserialize_i32(),
        deserialize_i64(),
        deserialize_i128(),
        deserialize_u8(),
        deserialize_u16(),
        deserialize_u32(),
        deserialize_u64(),
        deserialize_u128(),
        deserialize_f32(),
        deserialize_f64(),
        deserialize_char(),
        deserialize_str(),
        deserialize_string(),
        deserialize_bytes(),
        deserialize_byte_buf(),
        deserialize_option(),
        deserialize_unit(),
        deserialize_seq(),
        deserialize_map(),
        deserialize_identifier(),
        deserialize_ignored_any(),
        deserialize_unit_struct(name: &'static str),
        deserialize_newtype_struct(name: &'static str),
        deserialize_tuple(len: usize),
        deserialize_tuple_struct(name: &'static str, len: usize),
        deserialize_struct(name: &'static str, fields: &'static [&'static str]),
        deserialize_enum(name: &'static str, variants: &'static [&'static str]),
    }

    fn is_human_readable(&self) -> bool {
        self.inner.is_human_readable()
    }
}

/// A [`DeserializeSeed`] which meters the deserializer it is handed.
struct MeteredSeed<'m, S> {
    meter: &'m mut Meter,
    inner: S,
}

impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for MeteredSeed<'_, S> {
    type Value = S::Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        self.inner
            .deserialize(MeteredDeserializer::new(self.meter, deserializer))
    }
}

/// A [`Visitor`] which charges the value it is handed to a [`Meter`].
struct MeteredVisitor<'m, V> {
    meter: &'m mut Meter,
    inner: V,
}

impl<'m, V> MeteredVisitor<'m, V> {
    fn new(meter: &'m mut Meter, inner: V) -> Self {
        Self { meter, inner }
    }
}

/// Forwards a scalar [`Visitor`] method, charging the size of the value it carries.
macro_rules! visit_scalar {
    ($($method:ident($ty:ty)),* $(,)?) => {
        $(
            fn $method<E: de::Error>(self, v: $ty) -> Result<Self::Value, E> {
                self.meter.spend(cost::UNIT).map_err(E::custom)?;
                self.inner.$method(v)
            }
        )*
    };
}

impl<'de, V: Visitor<'de>> Visitor<'de> for MeteredVisitor<'_, V> {
    type Value = V::Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.expecting(f)
    }

    visit_scalar! {
        visit_bool(bool),
        visit_i8(i8),
        visit_i16(i16),
        visit_i32(i32),
        visit_i64(i64),
        visit_i128(i128),
        visit_u8(u8),
        visit_u16(u16),
        visit_u32(u32),
        visit_u64(u64),
        visit_u128(u128),
        visit_f32(f32),
        visit_f64(f64),
        visit_char(char),
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        self.meter.spend(cost::UNIT).map_err(E::custom)?;
        self.inner.visit_str(v)
    }

    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Self::Value, E> {
        self.meter.spend(cost::UNIT).map_err(E::custom)?;
        self.inner.visit_borrowed_str(v)
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
        self.meter.spend(cost::UNIT).map_err(E::custom)?;
        self.inner.visit_string(v)
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
        self.meter.spend(cost::UNIT).map_err(E::custom)?;
        self.inner.visit_bytes(v)
    }

    fn visit_borrowed_bytes<E: de::Error>(self, v: &'de [u8]) -> Result<Self::Value, E> {
        self.meter.spend(cost::UNIT).map_err(E::custom)?;
        self.inner.visit_borrowed_bytes(v)
    }

    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
        self.meter.spend(cost::UNIT).map_err(E::custom)?;
        self.inner.visit_byte_buf(v)
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        self.meter.spend(cost::UNIT).map_err(E::custom)?;
        self.inner.visit_none()
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        self.meter.spend(cost::UNIT).map_err(E::custom)?;
        self.inner.visit_unit()
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        // The payload charges itself, an `Option` only adds its discriminant on top.
        self.inner
            .visit_some(MeteredDeserializer::new(self.meter, d))
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        // A newtype is a transparent wrapper, so the value inside it is the whole cost.
        self.inner
            .visit_newtype_struct(MeteredDeserializer::new(self.meter, d))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Self::Value, A::Error> {
        self.meter
            .spend(cost::UNIT)
            .map_err(serde_de::Error::custom)?;
        self.inner.visit_seq(MeteredSeqAccess {
            meter: self.meter,
            inner: seq,
        })
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        self.meter
            .spend(cost::UNIT)
            .map_err(serde_de::Error::custom)?;

        self.inner.visit_map(MeteredMapAccess {
            meter: self.meter,
            inner: map,
        })
    }

    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_enum(MeteredEnumAccess {
            meter: self.meter,
            inner: data,
        })
    }
}

/// A [`SeqAccess`] which meters the elements it yields.
struct MeteredSeqAccess<'m, A> {
    meter: &'m mut Meter,
    inner: A,
}

impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for MeteredSeqAccess<'_, A> {
    type Error = A::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Self::Error> {
        let element = self.inner.next_element_seed(MeteredSeed {
            meter: self.meter,
            inner: seed,
        })?;

        Ok(element)
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

/// A [`MapAccess`] which meters the keys and values it yields.
struct MeteredMapAccess<'m, A> {
    meter: &'m mut Meter,
    inner: A,
}

impl<'de, A: MapAccess<'de>> MapAccess<'de> for MeteredMapAccess<'_, A> {
    type Error = A::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        let key = self.inner.next_key_seed(MeteredSeed {
            meter: self.meter,
            inner: seed,
        })?;

        Ok(key)
    }

    fn next_value_seed<Va: DeserializeSeed<'de>>(
        &mut self,
        seed: Va,
    ) -> Result<Va::Value, Self::Error> {
        self.inner.next_value_seed(MeteredSeed {
            meter: self.meter,
            inner: seed,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

/// An [`EnumAccess`] which meters the variant it yields.
struct MeteredEnumAccess<'m, A> {
    meter: &'m mut Meter,
    inner: A,
}

impl<'de, 'm, A: EnumAccess<'de>> EnumAccess<'de> for MeteredEnumAccess<'m, A> {
    type Error = A::Error;
    type Variant = MeteredVariantAccess<'m, A::Variant>;

    fn variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<(S::Value, Self::Variant), Self::Error> {
        let meter = self.meter;
        let (value, variant) = self
            .inner
            .variant_seed(MeteredSeed { meter, inner: seed })?;

        Ok((
            value,
            MeteredVariantAccess {
                meter,
                inner: variant,
            },
        ))
    }
}

/// A [`VariantAccess`] which meters the payload of the variant it yields.
struct MeteredVariantAccess<'m, A> {
    meter: &'m mut Meter,
    inner: A,
}

impl<'de, A: VariantAccess<'de>> VariantAccess<'de> for MeteredVariantAccess<'_, A> {
    type Error = A::Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        self.meter
            .spend(cost::UNIT)
            .map_err(serde_de::Error::custom)?;
        self.inner.unit_variant()
    }

    fn newtype_variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        self.inner.newtype_variant_seed(MeteredSeed {
            meter: self.meter,
            inner: seed,
        })
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner
            .tuple_variant(len, MeteredVisitor::new(self.meter, visitor))
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner
            .struct_variant(fields, MeteredVisitor::new(self.meter, visitor))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::Deserialize;

    use super::*;

    fn deserialize_return_meter<'de, T, D>(deserializer: D, max_ops: usize) -> (T, Meter)
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        let mut meter = Meter::new(max_ops);

        let metered_deserializer = meter.wrap(deserializer);
        let t = T::deserialize(metered_deserializer).unwrap();

        (t, meter)
    }

    fn json_deserializer(payload: &str) -> serde_json::Deserializer<serde_json::de::StrRead<'_>> {
        serde_json::Deserializer::from_str(payload)
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Nested {
        name: Vec<String>,
        values: Vec<u64>,
        inner: Option<Box<Nested>>,
    }

    #[test]
    fn test_deserialize_exceeds_budget() {
        let payload = format!(r#"[{}]"#, vec!["\"a\""; 4096].join(","));

        let error =
            deserialize::<Vec<String>, _>(&mut json_deserializer(&payload), 128).unwrap_err();
        assert!(error.is_limit_exceeded());
    }

    #[test]
    fn test_deserialize_exceeds_budget_when_nested() {
        let payload = format!(
            r#"{{"name": [{}], "values": [], "inner": null}}"#,
            vec!["\"a\""; 4096].join(",")
        );

        let error = deserialize::<Nested, _>(&mut json_deserializer(&payload), 256).unwrap_err();
        assert!(error.is_limit_exceeded());
    }

    #[test]
    fn test_deserialize_empty_values_are_not_free() {
        // A payload of nothing but empty objects must still exhaust a budget.
        let payload = format!("[{}]", vec!["{}"; 10_000].join(","));

        let error = deserialize::<Vec<serde_json::Map<String, serde_json::Value>>, _>(
            &mut json_deserializer(&payload),
            1 << 8,
        )
        .unwrap_err();
        assert!(error.is_limit_exceeded());
    }

    #[test]
    fn test_deserialize_invalid_payload() {
        let error =
            deserialize::<Nested, _>(&mut json_deserializer("{invalid"), 1 << 20).unwrap_err();
        assert!(!error.is_limit_exceeded());
        assert!(error.into_serde().is_some());
    }

    #[test]
    fn test_deserialize_does_not_consume_trailing_input() {
        // The wrapper does not take ownership of the deserializer, so callers remain in control of
        // checking for trailing data.
        let mut de = json_deserializer("[1] trailing");
        let value: Vec<u64> = deserialize(&mut de, 1 << 20).unwrap();
        assert_eq!(value, [1]);
        assert!(de.end().is_err());
    }

    fn prim_array_cost(num_elements: usize) -> usize {
        cost::UNIT + (cost::UNIT * num_elements)
    }

    fn str_cost() -> usize {
        cost::UNIT
    }

    fn prim_cost() -> usize {
        cost::UNIT
    }

    fn map_cost() -> usize {
        cost::UNIT
    }

    fn scalar_struct_cost(num_fields: usize) -> usize {
        map_cost() + num_fields * (str_cost() + prim_cost())
    }

    fn variant_cost(payload: usize) -> usize {
        str_cost() + payload
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct UnitStruct;

    #[derive(Debug, Deserialize, PartialEq)]
    struct NewtypeStruct(u32);

    #[derive(Debug, Deserialize, PartialEq)]
    struct TupleStruct(u8, String, bool);

    #[derive(Debug, Deserialize, PartialEq)]
    enum Variants {
        Unit,
        Newtype(i32),
        Tuple(u8, String),
        Struct { key: char, value: Option<f64> },
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Signed {
        a: i8,
        b: i16,
        c: i32,
        d: i64,
        e: i128,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Unsigned {
        a: u8,
        b: u16,
        c: u32,
        d: u64,
        e: u128,
    }

    /// A struct which exercises every path through the metered deserializer.
    #[derive(Debug, Deserialize, PartialEq)]
    struct Everything<'a> {
        flag: bool,
        signed: Signed,
        unsigned: Unsigned,
        f32_: f32,
        f64_: f64,
        letter: char,
        owned: String,
        #[serde(borrow)]
        borrowed: &'a str,
        nothing: (),
        unit_struct: UnitStruct,
        newtype: NewtypeStruct,
        tuple: (u8, String, bool),
        tuple_struct: TupleStruct,
        some: Option<u32>,
        none: Option<u32>,
        seq: Vec<Vec<u8>>,
        map: BTreeMap<String, Variants>,
        variants: Vec<Variants>,
        nested: Option<Box<Nested>>,
    }

    /// A payload which populates every field of [`Everything`], plus one field it does not declare.
    const EVERYTHING_PAYLOAD: &str = r#"{
            "flag": true,
            "signed": {"a": -8, "b": -16, "c": -32, "d": -64, "e": -128},
            "unsigned": {"a": 8, "b": 16, "c": 32, "d": 64, "e": 128},
            "f32_": 1.5,
            "f64_": -2.25,
            "letter": "x",
            "owned": "owned string",
            "borrowed": "borrowed string",
            "nothing": null,
            "unit_struct": null,
            "newtype": 7,
            "tuple": [1, "two", false],
            "tuple_struct": [2, "three", true],
            "some": 9,
            "none": null,
            "seq": [[1, 2], [], [3]],
            "map": {"unit": "Unit", "newtype": {"Newtype": -5}},
            "variants": [
                "Unit",
                {"Newtype": 5},
                {"Tuple": [1, "one"]},
                {"Struct": {"key": "k", "value": 0.5}},
                {"Struct": {"key": "l", "value": null}}
            ],
            "nested": {"name": ["a", "b"], "values": [1], "inner": null},
            "ignored": {"deeply": [{"nested": [1, 2, 3]}]}
        }"#;

    #[test]
    fn test_deserialize_all_paths() {
        let mut de = json_deserializer(EVERYTHING_PAYLOAD);
        let value: Everything<'_> = deserialize(&mut de, 1 << 20).unwrap();

        assert_eq!(
            value,
            Everything {
                flag: true,
                signed: Signed {
                    a: -8,
                    b: -16,
                    c: -32,
                    d: -64,
                    e: -128,
                },
                unsigned: Unsigned {
                    a: 8,
                    b: 16,
                    c: 32,
                    d: 64,
                    e: 128,
                },
                f32_: 1.5,
                f64_: -2.25,
                letter: 'x',
                owned: "owned string".to_owned(),
                borrowed: "borrowed string",
                nothing: (),
                unit_struct: UnitStruct,
                newtype: NewtypeStruct(7),
                tuple: (1, "two".to_owned(), false),
                tuple_struct: TupleStruct(2, "three".to_owned(), true),
                some: Some(9),
                none: None,
                seq: vec![vec![1, 2], vec![], vec![3]],
                map: BTreeMap::from([
                    ("unit".to_owned(), Variants::Unit),
                    ("newtype".to_owned(), Variants::Newtype(-5)),
                ]),
                variants: vec![
                    Variants::Unit,
                    Variants::Newtype(5),
                    Variants::Tuple(1, "one".to_owned()),
                    Variants::Struct {
                        key: 'k',
                        value: Some(0.5),
                    },
                    Variants::Struct {
                        key: 'l',
                        value: None,
                    },
                ],
                nested: Some(Box::new(Nested {
                    name: vec!["a".to_owned(), "b".to_owned()],
                    values: vec![1],
                    inner: None,
                })),
            }
        );
    }

    #[test]
    fn test_expected_costs_everything() {
        let mut de = json_deserializer(EVERYTHING_PAYLOAD);
        let (_, meter): (Everything<'_>, _) = deserialize_return_meter(&mut de, 1 << 20);

        // The cost of every field's value, in declaration order. Field names are charged
        // separately, below.
        let values = [
            // flag
            prim_cost(),
            // signed, unsigned integers
            scalar_struct_cost(5),
            scalar_struct_cost(5),
            // f32_, f64_, letter
            prim_cost(),
            prim_cost(),
            prim_cost(),
            // strings, owned and borrowed
            str_cost(),
            str_cost(),
            // nothing, unit_struct
            prim_cost(),
            prim_cost(),
            // newtype with a u32 inside
            prim_cost(),
            // tuple, tuple_struct
            prim_array_cost(3),
            prim_array_cost(3),
            // some
            prim_cost(),
            // none
            prim_cost(),
            // seq: the outer sequence plus `[1, 2]`, `[]` and `[3]`
            cost::UNIT + prim_array_cost(2) + prim_array_cost(0) + prim_array_cost(1),
            // map: two entries, each a key plus an enum variant
            map_cost()
                + (str_cost() + variant_cost(prim_cost()))
                + (str_cost() + variant_cost(prim_cost())),
            // variants: a unit, a newtype, a tuple and two struct variants
            cost::UNIT
                + variant_cost(prim_cost())
                + variant_cost(prim_cost())
                + variant_cost(prim_array_cost(2))
                + variant_cost(scalar_struct_cost(2))
                + variant_cost(scalar_struct_cost(2)),
            // nested: `Nested` with two names, one value and no inner
            map_cost()
                + (str_cost() + prim_array_cost(2))
                + (str_cost() + prim_array_cost(1))
                + (str_cost() + prim_cost()),
            // The `ignored` field (just the string field name)
            cost::UNIT,
        ];

        let expected = map_cost() + values.len() * str_cost() + values.iter().sum::<usize>();
        assert_eq!(meter.spent(), expected);
    }
}
