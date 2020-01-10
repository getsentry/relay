//! Test utilities for Relay and its crates.
#![warn(missing_docs)]

/// Uses `insta::assert_snapshot` to test `Annotated` elements.
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
