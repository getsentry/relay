use std::collections::BTreeMap;
use std::fmt::Debug;

use serde::{Deserialize, Deserializer};

use crate::types::{Annotated, MetaMap, MetaTree, Value};

/// A value that can be empty.
pub trait Empty {
    /// Returns whether this value is empty.
    fn is_empty(&self) -> bool;

    /// Returns whether this value is empty or all of its descendants are empty.
    ///
    /// This only needs to be implemented for containers by calling `Empty::is_deep_empty` on all
    /// children. The default implementation calls `Empty::is_empty`.
    ///
    /// For containers of `Annotated` elements, this must call `Annotated::skip_serialization`.
    #[inline]
    fn is_deep_empty(&self) -> bool {
        self.is_empty()
    }
}

/// Defines behavior for skipping the serialization of fields.
///
/// This behavior is configured via the `skip_serialization` attribute on fields of structs. It is
/// passed as parameter to `ToValue::skip_serialization` of the corresponding field.
///
/// The default for fields in derived structs is `SkipSerialization::Null(true)`, which will skips
/// `null` values under the field recursively. Newtype structs (`MyType(T)`) and enums pass through
/// to their inner type and variant, respectively.
///
/// ## Example
///
/// ```ignore
/// #[derive(Debug, Empty, ToValue)]
/// struct Helper {
///     #[metastructure(skip_serialization = "never")]
///     items: Annotated<Array<String>>,
/// }
/// ```
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SkipSerialization {
    /// Serialize all values. Missing values will be serialized as `null`.
    Never,

    /// Do not serialize `null` values but keep empty collections.
    ///
    /// If the `bool` flag is set to `true`, this applies to all descendants recursively; if it is
    /// set to `false`, this only applies to direct children and does not propagate down.
    Null(bool),

    /// Do not serialize empty objects as indicated by the `Empty` trait.
    ///
    /// If the `bool` flag is set to `true`, this applies to all descendants recursively; if it is
    /// set to `false`, this only applies to direct children and does not propagate down.
    Empty(bool),
}

impl SkipSerialization {
    /// Returns the serialization behavior for child elements.
    ///
    /// Shallow behaviors - `Null(false)` and `Empty(false)` - propagate as `Never`, all others
    /// remain the same. This allows empty containers to be skipped while their contents will
    /// serialize with `null` values.
    pub fn descend(self) -> Self {
        match self {
            SkipSerialization::Null(false) => SkipSerialization::Never,
            SkipSerialization::Empty(false) => SkipSerialization::Never,
            other => other,
        }
    }
}

impl Default for SkipSerialization {
    fn default() -> Self {
        SkipSerialization::Null(true)
    }
}

/// Implemented for all meta structures.
pub trait FromValue: Debug {
    fn from_deserializer<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Annotated<Self>, D::Error>
    where
        Self: Sized,
    {
        Ok(FromValue::from_value_legacy(Annotated::from(Option::<
            Value,
        >::deserialize(
            deserializer,
        )?)))
    }

    /// Legacy method for old implementors of FromValue
    fn from_value_legacy(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized;

    /// Stub method for callers of FromValue::from_value.
    ///
    /// This method should never be overridden and is scheduled to be removed.
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        let stringified = serde_json::to_string(&value.value()).unwrap();
        let mut deserializer = serde_json::Deserializer::from_str(&stringified);
        Self::from_deserializer(&mut deserializer).unwrap()
    }

    fn attach_meta_map(&mut self, meta_map: MetaMap);
}

/// Implemented for all meta structures.
pub trait IntoValue: Debug + Empty {
    /// Boxes the meta structure back into a value.
    fn into_value(self) -> Value
    where
        Self: Sized;

    /// Extracts children meta map out of a value.
    fn extract_child_meta(&self) -> MetaMap
    where
        Self: Sized,
    {
        MetaMap::new()
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
                Some(ref value) => IntoValue::extract_child_meta(value),
                None => BTreeMap::default(),
            },
        }
    }
}
