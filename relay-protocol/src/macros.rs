/// Returns `&Annotated<T>` for the annotated value at the given path.
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
        get_path!(@access $root, $($tail)*);
        $root
    }};
}

/// Returns `Option<&V>` for the value at the given path.
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
        get_value!(@access $root, $($tail)*);
        $root
    }};
}

/// Derives the `FromValue`, `IntoValue`, and `Empty` traits using the string representation.
///
/// Requires that this type implements `FromStr` and `Display`. Implements the following traits:
///  - `FromValue`: Deserializes a string, then uses `FromStr` to convert into the type.
///  - `IntoValue`: Serializes into a string using the `Display` trait.
///  - `Empty`: This type is never empty.
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

/// Asserts the snapshot of an annotated structure using [`insta`].
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
