#[cfg(feature = "uaparser")]
use crate::protocol::{Event, Headers, PairList, Request};
#[cfg(feature = "uaparser")]
use crate::types::Annotated;

macro_rules! assert_eq_str {
    ($left:expr, $right:expr) => {
        match (&$left, &$right) {
            (left, right) => assert!(
                left == right,
                "`left == right` in line {}:\n{}\n{}",
                line!(),
                difference::Changeset::new("- left", "+ right", "\n"),
                difference::Changeset::new(&left, &right, "\n")
            ),
        }
    };
    ($left:expr, $right:expr,) => {
        assert_eq_str!($left, $right)
    };
}

pub(crate) use assert_eq_str;

macro_rules! assert_eq_bytes_str {
    ($left:expr, $right:expr) => {
        match (&$left, &$right) {
            (left, right) => assert!(
                left == right,
                "`left == right` in line {}:\n{}\n{}",
                line!(),
                pretty_hex::pretty_hex(left),
                pretty_hex::pretty_hex(right),
            ),
        }
    };
    ($left:expr, $right:expr,) => {
        assert_eq_str!($left, $right)
    };
}

pub(crate) use assert_eq_bytes_str;

macro_rules! assert_eq_dbg {
    ($left:expr, $right:expr) => {
        match (&$left, &$right) {
            (left, right) => assert!(
                left == right,
                "`left == right` in line {}:\n{}\n{}",
                line!(),
                difference::Changeset::new("- left", "+ right", "\n"),
                difference::Changeset::new(&format!("{:#?}", left), &format!("{:#?}", right), "\n")
            ),
        }
    };
    ($left:expr, $right:expr,) => {
        assert_eq_dbg!($left, $right)
    };
}

pub(crate) use assert_eq_dbg;

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

pub(crate) use assert_annotated_snapshot;

/// Returns `&Annotated<T>` for the annotated value at the given path.
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

pub(crate) use get_path;

/// Returns `Option<&V>` for the value at the given path.
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

pub(crate) use get_value;

#[cfg(feature = "uaparser")]
/// Creates an Event with the specified user agent.
pub(super) fn get_event_with_user_agent(user_agent: &str) -> Event {
    let headers = vec![
        Annotated::new((
            Annotated::new("Accept".to_string().into()),
            Annotated::new("application/json".to_string().into()),
        )),
        Annotated::new((
            Annotated::new("UsEr-AgeNT".to_string().into()),
            Annotated::new(user_agent.to_string().into()),
        )),
        Annotated::new((
            Annotated::new("WWW-Authenticate".to_string().into()),
            Annotated::new("basic".to_string().into()),
        )),
    ];

    Event {
        request: Annotated::new(Request {
            headers: Annotated::new(Headers(PairList(headers))),
            ..Request::default()
        }),
        ..Event::default()
    }
}
