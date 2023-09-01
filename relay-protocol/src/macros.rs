/// Returns a reference to the typed [`Annotated`] value at a given path.
///
/// The return type depends on the path expression used. By default, this macro will resolve to an
/// `Option<&Annotated<T>>`, where the option is `Some` if the path exists. Note that if the
/// annotated value at the specificed path is empty, this returns `Some` with an empty annotated.
///
/// When used with an exclamation mark after the path, this macro unwraps to an `&Annotated<T>`.
///
/// [`Annotated`]: crate::Annotated
///
/// # Syntax
///
/// A path starts with the name of a variable holding an [`Annotated`]. Access to children of this
/// type depend on the value type:
///  - To access a struct field, use a period followed by the field's name, for instance (`.field`).
///  - To access an array element, use the element's numeric index in brackets, for instance `[0]`.
///  - To access an object's element, use the element's quoted string key in brackets, for instance
///    `["key_name"]`.
///
/// Paths can be chained, so a valid path expression is `data.values["key"].field`.
///
/// To unwrap the annotated field at the destination, append an exclamation mark at the end of the
/// path, for instance: `data.field!`.
///
/// # Panics
///
/// Panics when unwrap (`!`) is used and there is an empty field along the path. Since `get_path!`
/// always returns the final `Annotated`, the final field can be empty without panicking.
///
/// # Example
///
/// ```
/// use relay_protocol::{get_path, Annotated, Object};
///
/// struct Inner {
///     value: Annotated<u64>,
/// }
///
/// struct Outer {
///     inners: Annotated<Object<Inner>>,
/// }
///
/// let outer = Annotated::new(Outer {
///     inners: Annotated::new(Object::from([(
///         "key".to_string(),
///         Annotated::new(Inner {
///             value: Annotated::new(1),
///         }),
///     )])),
/// });
///
/// assert_eq!(get_path!(outer.inners["key"].value), Some(&Annotated::new(1)));
/// assert_eq!(get_path!(outer.inners["key"].value!), &Annotated::new(1));
/// ```
#[macro_export]
macro_rules! get_path {
    (@access $root:ident,) => {};
    (@access $root:ident, !) => {
        let $root = $root.unwrap();
    };
    (@access $root:ident, . $field:ident $( $tail:tt )*) => {
        let $root = $root.and_then(|a| a.value()).map(|v| &v.$field);
        get_path!(@access $root, $($tail)*);
    };
    (@access $root:ident, [ $index:literal ] $( $tail:tt )*) => {
        let $root = $root.and_then(|a| a.value()).and_then(|v| v.get($index));
        get_path!(@access $root, $($tail)*);
    };
    ($root:ident $( $tail:tt )*) => {{
        let $root = Some(&$root);
        $crate::get_path!(@access $root, $($tail)*);
        $root
    }};
}

/// Returns a reference to the typed value at a given path in an [`Annotated`].
///
/// The return type depends on the path expression used. By default, this macro will resolve to an
/// `Option<&T>`, where the option is `Some` if the path exists and the value is present.
///
/// When used with an exclamation mark after the path, this macro unwraps to an `&T`.
///
/// [`Annotated`]: crate::Annotated
///
/// # Syntax
///
/// A path starts with the name of a variable holding an [`Annotated`]. Access to children of this
/// type depend on the value type:
///  - To access a struct field, use a period followed by the field's name, for instance (`.field`).
///  - To access an array element, use the element's numeric index in brackets, for instance `[0]`.
///  - To access an object's element, use the element's quoted string key in brackets, for instance
///    `["key_name"]`.
///
/// Paths can be chained, so a valid path expression is `data.values["key"].field`.
///
/// To unwrap the value at the destination, append an exclamation mark at the end of the path, for
/// instance: `data.field!`.
///
/// # Panics
///
/// Panics when unwrap (`!`) is used and there is an empty field along the path.
///
/// # Example
///
/// ```
/// use relay_protocol::{get_value, Annotated, Object};
///
/// struct Inner {
///     value: Annotated<u64>,
/// }
///
/// struct Outer {
///     inners: Annotated<Object<Inner>>,
/// }
///
/// let outer = Annotated::new(Outer {
///     inners: Annotated::new(Object::from([(
///         "key".to_string(),
///         Annotated::new(Inner {
///             value: Annotated::new(1),
///         }),
///     )])),
/// });
///
/// assert_eq!(get_value!(outer.inners["key"].value), Some(&1));
/// assert_eq!(get_value!(outer.inners["key"].value!), &1);
/// ```
#[macro_export]
macro_rules! get_value {
    (@access $root:ident,) => {};
    (@access $root:ident, !) => {
        let $root = $root.unwrap();
    };
    (@access $root:ident, . $field:ident $( $tail:tt )*) => {
        let $root = $root.and_then(|v| v.$field.value());
        get_value!(@access $root, $($tail)*);
    };
    (@access $root:ident, [ $index:literal ] $( $tail:tt )*) => {
        let $root = $root.and_then(|v| v.get($index)).and_then(|a| a.value());
        get_value!(@access $root, $($tail)*);
    };
    ($root:ident $( $tail:tt )*) => {{
        let $root = $root.value();
        $crate::get_value!(@access $root, $($tail)*);
        $root
    }};
}

/// Derives the [`FromValue`], [`IntoValue`], and [`Empty`] traits using the string representation.
///
/// Requires that this type implements `FromStr` and `Display`. Implements the following traits:
///
///  - [`FromValue`]: Deserializes a string, then uses [`FromStr`](std::str::FromStr) to convert
///        into the type.
///  - [`IntoValue`]: Serializes into a string using the [`Display`](std::fmt::Display) trait.
///  - [`Empty`]: This type is never empty.
///
/// [`FromValue`]: crate::FromValue
/// [`IntoValue`]: crate::IntoValue
/// [`Empty`]: crate::Empty
#[macro_export]
macro_rules! derive_string_meta_structure {
    ($type:ident, $expectation:expr) => {
        impl $crate::FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                        Ok(value) => Annotated(Some(value), meta),
                        Err(err) => {
                            meta.add_error($crate::Error::invalid(err));
                            meta.set_original_value(Some(value));
                            Annotated(None, meta)
                        }
                    },
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(Some(value), mut meta) => {
                        meta.add_error($crate::Error::expected($expectation));
                        meta.set_original_value(Some(value));
                        Annotated(None, meta)
                    }
                }
            }
        }

        impl $crate::IntoValue for $type {
            fn into_value(self) -> Value {
                Value::String(self.to_string())
            }

            fn serialize_payload<S>(
                &self,
                s: S,
                _behavior: $crate::SkipSerialization,
            ) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: serde::ser::Serializer,
            {
                s.collect_str(self)
            }
        }

        impl $crate::Empty for $type {
            fn is_empty(&self) -> bool {
                false
            }
        }
    };
}

pub use derive_string_meta_structure;

/// Asserts the snapshot of an annotated structure using `insta`.
#[cfg(feature = "test")]
#[macro_export]
macro_rules! assert_annotated_snapshot {
    ($value:expr, @$snapshot:literal) => {
        ::insta::assert_snapshot!(
            $value.to_json_pretty().unwrap(),
            stringify!($value),
            @$snapshot
        )
    };
    ($value:expr, $debug_expr:expr, @$snapshot:literal) => {
        ::insta::assert_snapshot!(
            $value.to_json_pretty().unwrap(),
            $debug_expr,
            @$snapshot
        )
    };
    ($name:expr, $value:expr) => {
        ::insta::assert_snapshot!(
            $name,
            $value.to_json_pretty().unwrap(),
            stringify!($value)
        )
    };
    ($name:expr, $value:expr, $debug_expr:expr) => {
        ::insta::assert_snapshot!(
            $name,
            $value.to_json_pretty().unwrap(),
            $debug_expr
        )
    };
    ($value:expr) => {
        ::insta::assert_snapshot!(
            None::<String>,
            $value.to_json_pretty().unwrap(),
            stringify!($value)
        )
    };
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use crate::{Annotated, Array, Object};

    #[derive(Clone, Debug, PartialEq)]
    struct Inner {
        value: Annotated<u64>,
    }

    #[derive(Clone, Debug, PartialEq)]
    struct Outer {
        inner: Annotated<Inner>,
    }

    #[test]
    fn get_path() {
        let value = Annotated::new(1);
        let inner = Annotated::new(Inner {
            value: value.clone(),
        });
        let outer = Annotated::new(Outer {
            inner: inner.clone(),
        });

        // Optional
        assert_eq!(get_path!(outer), Some(&outer));
        assert_eq!(get_path!(outer.inner), Some(&inner));
        assert_eq!(get_path!(outer.inner.value), Some(&value));

        // Unwrap
        assert_eq!(get_path!(outer!), &outer);
        assert_eq!(get_path!(outer.inner!), &inner);
        assert_eq!(get_path!(outer.inner.value!), &value);
    }

    #[test]
    fn get_path_empty() {
        let empty = Annotated::<Outer>::empty();
        let outer_empty = Annotated::new(Outer {
            inner: Annotated::empty(),
        });
        let outer = Annotated::new(Outer {
            inner: Annotated::new(Inner {
                value: Annotated::empty(),
            }),
        });

        // Empty leaf
        assert_eq!(get_path!(empty), Some(&Annotated::empty()));
        assert_eq!(get_path!(outer_empty.inner), Some(&Annotated::empty()));
        assert_eq!(get_path!(outer.inner.value), Some(&Annotated::empty()));

        // Empty in the path
        assert_eq!(get_path!(empty.inner), None);
        assert_eq!(get_path!(empty.inner.value), None);
        assert_eq!(get_path!(outer_empty.inner.value), None);

        // Empty unwraps
        assert_eq!(get_path!(empty!), &Annotated::empty());
        assert_eq!(get_path!(outer_empty.inner!), &Annotated::empty());
        assert_eq!(get_path!(outer.inner.value!), &Annotated::empty());
    }

    #[test]
    fn get_path_array() {
        let array = Annotated::new(Array::from([Annotated::new(0), Annotated::new(1)]));

        // Indexes
        assert_eq!(get_path!(array[0]), Some(&Annotated::new(0)));
        assert_eq!(get_path!(array[1]), Some(&Annotated::new(1)));
        // Out of bounds
        assert_eq!(get_path!(array[2]), None);
        // Unwrap
        assert_eq!(get_path!(array[0]!), &Annotated::new(0));
    }

    #[test]
    fn get_path_object() {
        let object = Annotated::new(Object::from([("key".to_string(), Annotated::new(1))]));

        // Exists
        assert_eq!(get_path!(object["key"]), Some(&Annotated::new(1)));
        // Unwrap
        assert_eq!(get_path!(object["key"]!), &Annotated::new(1));
        // Does not exist
        assert_eq!(get_path!(object["other"]), None);
    }

    #[test]
    fn get_path_combined() {
        struct Inner {
            value: Annotated<u64>,
        }

        struct Outer {
            inners: Annotated<Object<Inner>>,
        }

        let outer = Annotated::new(Outer {
            inners: Annotated::new(Object::from([(
                "key".to_string(),
                Annotated::new(Inner {
                    value: Annotated::new(1),
                }),
            )])),
        });

        assert_eq!(
            get_path!(outer.inners["key"].value),
            Some(&Annotated::new(1))
        );
        assert_eq!(get_path!(outer.inners["key"].value!), &Annotated::new(1));
    }

    #[test]
    fn get_value() {
        let inner = Inner {
            value: Annotated::new(1),
        };
        let outer = Outer {
            inner: Annotated::new(inner.clone()),
        };
        let annotated = Annotated::new(outer.clone());

        // Optional
        assert_eq!(get_value!(annotated), Some(&outer));
        assert_eq!(get_value!(annotated.inner), Some(&inner));
        assert_eq!(get_value!(annotated.inner.value), Some(&1));

        // Unwrap
        assert_eq!(get_value!(annotated!), &outer);
        assert_eq!(get_value!(annotated.inner!), &inner);
        assert_eq!(get_value!(annotated.inner.value!), &1);
    }

    #[test]
    fn get_value_empty() {
        let empty = Annotated::<Outer>::empty();
        let outer_empty = Annotated::new(Outer {
            inner: Annotated::empty(),
        });
        let outer = Annotated::new(Outer {
            inner: Annotated::new(Inner {
                value: Annotated::empty(),
            }),
        });

        // Empty leaf
        assert_eq!(get_value!(empty), None);
        assert_eq!(get_value!(outer_empty.inner), None);
        assert_eq!(get_value!(outer.inner.value), None);

        // Empty in the path
        assert_eq!(get_value!(empty.inner), None);
        assert_eq!(get_value!(empty.inner.value), None);
        assert_eq!(get_value!(outer_empty.inner.value), None);
    }

    #[test]
    fn get_value_array() {
        let array = Annotated::new(Array::from([Annotated::new(0), Annotated::new(1)]));

        // Existing indexes
        assert_eq!(get_value!(array[0]), Some(&0));
        assert_eq!(get_value!(array[1]), Some(&1));
        // Out of bounds
        assert_eq!(get_value!(array[2]), None);
        // Unwrap
        assert_eq!(get_value!(array[0]!), &0);
    }

    #[test]
    fn get_value_object() {
        let object = Annotated::new(Object::from([("key".to_string(), Annotated::new(1))]));

        // Exists
        assert_eq!(get_value!(object["key"]), Some(&1));
        // Unwrap
        assert_eq!(get_value!(object["key"]!), &1);
        // Does not exist
        assert_eq!(get_value!(object["other"]), None);
    }

    #[test]
    fn get_value_combined() {
        struct Inner {
            value: Annotated<u64>,
        }

        struct Outer {
            inners: Annotated<Object<Inner>>,
        }

        let outer = Annotated::new(Outer {
            inners: Annotated::new(Object::from([(
                "key".to_string(),
                Annotated::new(Inner {
                    value: Annotated::new(1),
                }),
            )])),
        });

        assert_eq!(get_value!(outer.inners["key"].value), Some(&1));
        assert_eq!(get_value!(outer.inners["key"].value!), &1);
    }
}
