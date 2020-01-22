use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::processor::PathItem;

pub trait PiiStrippable {
    fn get_attrs(&self) -> PiiAttrsMap {
        Default::default()
    }
}

#[derive(Debug)]
pub struct PiiAttrsMap {
    pub whitelist: &'static [PathItem<'static>],
    pub blacklist: &'static [PathItem<'static>],
}

impl Default for PiiAttrsMap {
    fn default() -> PiiAttrsMap {
        PiiAttrsMap {
            whitelist: &[],
            blacklist: &[],
        }
    }
}

impl PiiAttrsMap {
    pub fn should_strip_pii(&self, field: &PathItem<'_>) -> Option<bool> {
        if self.blacklist.contains(field) {
            Some(false)
        } else if self.whitelist.contains(field) {
            Some(true)
        } else {
            None
        }
    }
}

// protocol-independent trait impls for some Rust primitives
impl PiiStrippable for bool {}
impl PiiStrippable for DateTime<Utc> {}
impl PiiStrippable for String {}
impl PiiStrippable for u64 {}
impl PiiStrippable for i64 {}
impl PiiStrippable for f64 {}
impl PiiStrippable for Uuid {}
impl<T> PiiStrippable for Vec<T> {}
impl<K, V> PiiStrippable for std::collections::BTreeMap<K, V> {}

impl<T: PiiStrippable> PiiStrippable for Box<T> {
    fn get_attrs(&self) -> PiiAttrsMap {
        (**self).get_attrs()
    }
}

macro_rules! tuple_pii_strippable {
    () => {
        impl PiiStrippable for () {}
    };
    ($final_name:ident $(, $name:ident)*) => {
        impl <$final_name $(, $name)*> PiiStrippable for ($final_name $(, $name)*, ) {}
        tuple_pii_strippable!($($name),*);
    };
}

tuple_pii_strippable!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
