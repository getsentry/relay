use chrono::{DateTime, Utc};
use regex::Regex;
use uuid::Uuid;

use crate::processor::PathItem;

pub trait SchemaValidated {
    fn get_attrs(&self) -> SchemaAttrsMap {
        Default::default()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SchemaAttrsMap {
    pub required: &'static [PathItem<'static>],
    pub nonempty: &'static [PathItem<'static>],
    pub trim_whitespace: &'static [PathItem<'static>],
    pub match_regex: &'static [(PathItem<'static>, Regex)],
}

impl Default for SchemaAttrsMap {
    fn default() -> SchemaAttrsMap {
        SchemaAttrsMap {
            required: &[],
            nonempty: &[],
            trim_whitespace: &[],
            match_regex: &[],
        }
    }
}

impl SchemaAttrsMap {
    pub fn required(&self, field: &PathItem<'_>) -> bool {
        self.required.contains(&field)
    }

    pub fn nonempty(&self, field: &PathItem<'_>) -> bool {
        self.nonempty.contains(&field)
    }

    pub fn trim_whitespace(&self, field: &PathItem<'_>) -> bool {
        self.trim_whitespace.contains(&field)
    }

    pub fn match_regex(&self, field: &PathItem<'_>) -> Option<&Regex> {
        self.match_regex
            .iter()
            .filter(|(k, _)| field == k)
            .map(|(_, v)| v)
            .next()
    }
}

// protocol-independent trait impls for some Rust primitives
impl SchemaValidated for bool {}
impl SchemaValidated for DateTime<Utc> {}
impl SchemaValidated for String {}
impl SchemaValidated for u64 {}
impl SchemaValidated for i64 {}
impl SchemaValidated for f64 {}
impl SchemaValidated for Uuid {}
impl<T> SchemaValidated for Vec<T> {}
impl<K, V> SchemaValidated for std::collections::BTreeMap<K, V> {}

impl<T: SchemaValidated> SchemaValidated for Box<T> {
    fn get_attrs(&self) -> SchemaAttrsMap {
        (**self).get_attrs()
    }
}

macro_rules! tuple_schema_validated {
    () => {
        impl SchemaValidated for () {}
    };
    ($final_name:ident $(, $name:ident)*) => {
        impl <$final_name $(, $name)*> SchemaValidated for ($final_name $(, $name)*, ) {}
        tuple_schema_validated!($($name),*);
    };
}

tuple_schema_validated!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
