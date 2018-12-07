use std::fmt::Debug;

use crate::types::{Annotated, MetaMap, MetaTree, Value};

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum SkipSerialization {
    /// Skip serialization of `null` values
    Null,
    /// Skip serialization of empty arrays, objects and strings
    Empty,
    /// Always serialize everything
    Never,
}

/// Implemented for all meta structures.
pub trait FromValue: Debug {
    /// Creates a meta structure from an annotated boxed value.
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized;
}

/// Implemented for all meta structures.
pub trait ToValue: Debug {
    /// Boxes the meta structure back into a value.
    fn to_value(self) -> Value
    where
        Self: Sized;

    /// Extracts children meta map out of a value.
    fn extract_child_meta(&self) -> MetaMap
    where
        Self: Sized,
    {
        Default::default()
    }

    /// Efficiently serializes the payload directly.
    fn serialize_payload<S>(&self, s: S, behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer;

    /// Extracts the meta tree out of annotated value.
    ///
    /// This should not be overridden by implementators, instead `extract_child_meta`
    /// should be provided instead.
    fn extract_meta_tree(value: &Annotated<Self>) -> MetaTree
    where
        Self: Sized,
    {
        MetaTree {
            meta: value.1.clone(),
            children: match value.0 {
                Some(ref value) => ToValue::extract_child_meta(value),
                None => Default::default(),
            },
        }
    }

    /// Whether the value should not be serialized. Should at least return true if the value would
    /// serialize to an empty array, empty object or null.
    fn skip_serialization(&self, behavior: SkipSerialization) -> bool {
        let _behavior = behavior;
        false
    }
}
