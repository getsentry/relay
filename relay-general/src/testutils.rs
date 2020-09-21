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

#[cfg(feature = "uaparser")]
/// Creates an Event with the specified user agent.
pub(super) fn get_event_with_user_agent(user_agent: &str) -> Event {
    let mut headers = Vec::new();

    headers.push(Annotated::new((
        Annotated::new("Accept".to_string().into()),
        Annotated::new("application/json".to_string().into()),
    )));

    headers.push(Annotated::new((
        Annotated::new("UsEr-AgeNT".to_string().into()),
        Annotated::new(user_agent.to_string().into()),
    )));
    headers.push(Annotated::new((
        Annotated::new("WWW-Authenticate".to_string().into()),
        Annotated::new("basic".to_string().into()),
    )));

    Event {
        request: Annotated::new(Request {
            headers: Annotated::new(Headers(PairList(headers))),
            ..Request::default()
        }),
        ..Event::default()
    }
}
