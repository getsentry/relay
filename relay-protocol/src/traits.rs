use std::collections::BTreeMap;
use std::fmt::Debug;

use uuid::Uuid;

use crate::annotated::{Annotated, MetaMap, MetaTree};
use crate::value::{Val, Value};

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
    /// Creates a meta structure from an annotated boxed value.
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized;
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

/// A type that supports field access by paths.
///
/// This is the runtime version of [`get_value!`](crate::get_value!) and additionally supports
/// indexing into [`Value`]. For typed access to static paths, use the macros instead.
///
/// # Syntax
///
/// The path identifies a value within the structure. A path consists of components separated by
/// `.`, where each of the components is the name of a field to access. Every path starts with the
/// name of the root level component, which must match for this type.
///
/// Special characters are escaped with a `\`. The two special characters are:
///  - `\.` matches a literal dot in a path component.
///  - `\\` matches a literal backslash in a path component.
///
/// # Implementation
///
/// Implementation of the `Getter` trait should follow a set of conventions to ensure the paths
/// align with expectations based on the layout of the implementing type:
///
///  1. The name of the root component should be the lowercased version of the name of the
///     structure. For example, a structure called `Event` would use `event` as the root component.
///  2. All fields of the structure are referenced by the name of the field in the containing
///     structure. This also applies to mappings such as `HashMap`, where the key should be used as
///     field name. For recursive access, this translates transitively through the hierarchy.
///  3. Newtypes and structured enumerations do not show up in paths. This especially applies to
///     `Option`, which opaque in the path: `None` is simply propagated up.
///
/// In the future, a derive for the `Getter` trait will be added to simplify implementing the
/// `Getter` trait.
///
/// # Example
///
/// ```
/// use relay_protocol::{Getter, Val};
///
/// struct Root {
///     a: u64,
///     b: Nested,
/// }
///
/// struct Nested {
///     c: u64,
/// }
///
/// impl Getter for Root {
///     fn get_value(&self, path: &str) -> Option<Val<'_>> {
///         match path.strip_prefix("root.")? {
///             "a" => Some(self.a.into()),
///             "b.c" => Some(self.b.c.into()),
///             _ => None,
///         }
///     }
/// }
///
///
/// let root = Root {
///   a: 1,
///   b: Nested {
///     c: 2,
///   }
/// };
///
/// assert_eq!(root.get_value("root.a"), Some(Val::U64(1)));
/// assert_eq!(root.get_value("root.b.c"), Some(Val::U64(2)));
/// assert_eq!(root.get_value("root.d"), None);
/// ```
pub trait Getter {
    /// Returns the serialized value of a field pointed to by a `path`.
    fn get_value(&self, path: &str) -> Option<Val<'_>>;
}
