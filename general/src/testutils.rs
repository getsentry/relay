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
