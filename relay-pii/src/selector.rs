use std::fmt;
use std::str::FromStr;

use pest::error::Error;
use pest::iterators::Pair;
use pest::Parser;
use relay_event_schema::processor::Path;
use smallvec::SmallVec;

use relay_event_schema::processor::{Pii, ProcessingState, ValueType};

/// Error for invalid PII selectors.
#[derive(Debug, thiserror::Error)]
pub enum InvalidSelectorError {
    /// Deep wildcard used more than once.
    #[error("deep wildcard used more than once")]
    InvalidDeepWildcard,

    /// Wildcard must be part of a path.
    #[error("wildcard must be part of a path")]
    InvalidWildcard,

    /// Invalid selector syntax.
    #[error("{0}")]
    ParseError(Box<Error<Rule>>),

    /// Invalid index.
    #[error("invalid index")]
    InvalidIndex,

    /// Unknown value.
    #[error("unknown value")]
    UnknownType,

    /// Internal parser bug: An unexpected item was consumed.
    #[error("parser bug: consumed {0} (expected {1})")]
    UnexpectedToken(String, &'static str),
}

mod parser {
    #![allow(clippy::upper_case_acronyms)]
    use pest_derive::Parser;

    #[derive(Parser)]
    #[grammar = "selector.pest"]
    pub struct SelectorParser;
}

use self::parser::{Rule, SelectorParser};

/// A path component in a composit [`SelectorSpec`].
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum SelectorPathItem {
    /// The component refers to a value type.
    Type(ValueType),
    /// The component refers to an array index.
    Index(usize),
    /// The component refers to a key in an object.
    Key(String),
    /// The component is a shallow wildcard (`*`).
    Wildcard,
    /// The component is a deep wildcard (`**`).
    DeepWildcard,
}

impl fmt::Display for SelectorPathItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SelectorPathItem::Type(ty) => write!(f, "${ty}"),
            SelectorPathItem::Index(index) => write!(f, "{index}"),
            SelectorPathItem::Key(ref key) => {
                if key_needs_quoting(key) {
                    write!(f, "'{}'", key.replace('\'', "''"))
                } else {
                    write!(f, "{key}")
                }
            }
            SelectorPathItem::Wildcard => write!(f, "*"),
            SelectorPathItem::DeepWildcard => write!(f, "**"),
        }
    }
}

impl SelectorPathItem {
    /// Determine whether a path item matches the respective processing state.
    ///
    /// `pii` is not the same as `state.attrs().pii`, but rather the PII flag of the state we're
    /// actually trying to match against. `i` is the position of the path item within the path.
    pub(super) fn matches_state(&self, pii: Pii, i: usize, state: &ProcessingState<'_>) -> bool {
        match (self, pii) {
            (_, Pii::False) => false,

            // necessary because of array indices
            (SelectorPathItem::Wildcard, _) => true,

            // a deep wildcard is too sweeping to be specific
            (SelectorPathItem::DeepWildcard, Pii::True) => true,
            (SelectorPathItem::DeepWildcard, Pii::Maybe) => false,

            (SelectorPathItem::Type(ty), Pii::True) => state.value_type().contains(*ty),
            (SelectorPathItem::Type(ty), Pii::Maybe) => {
                state.value_type().contains(*ty)
                    && match ty {
                        // Basic value types cannot be part of a specific path
                        ValueType::String
                        | ValueType::Binary
                        | ValueType::Number
                        | ValueType::Boolean
                        | ValueType::DateTime
                        | ValueType::Array
                        | ValueType::Object => false,

                        // Other schema-specific value types can be if they are on the first
                        // position. This list is explicitly typed out such that the decision
                        // to add new value types to this list has to be made consciously.
                        //
                        // It's easy to change a `false` to `true` later, but a breaking change
                        // to go the other direction. If you're not sure, return `false` for
                        // your new value type.
                        ValueType::Event
                        | ValueType::Attachments
                        | ValueType::Replay
                        | ValueType::Exception
                        | ValueType::Stacktrace
                        | ValueType::Frame
                        | ValueType::Request
                        | ValueType::User
                        | ValueType::LogEntry
                        | ValueType::Message
                        | ValueType::Thread
                        | ValueType::Breadcrumb
                        | ValueType::Span
                        | ValueType::Minidump
                        | ValueType::HeapMemory
                        | ValueType::StackMemory
                        | ValueType::ClientSdkInfo => i == 0,
                    }
            }
            (SelectorPathItem::Index(idx), _) => state.path().index() == Some(*idx),
            (SelectorPathItem::Key(ref key), _) => state
                .path()
                .key()
                .map(|k| k.to_lowercase() == key.to_lowercase())
                .unwrap_or(false),
        }
    }
}

/// A selector that can match paths of processing states.
///
/// To use a a selector, you most likely want to check whether it matches the path of a
/// [`ProcessingState`].  For this you turn the state into a [`Path`] using
/// [`ProcessingState::path`] and call [`SelectorSpec::matches_path`], which will iterate through
/// the path items in the processing state and check whether the selector matches.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum SelectorSpec {
    /// A selector that matches both of two sub-selectors.
    And(Vec<SelectorSpec>),
    /// A selector that matches either of two sub-selectors.
    Or(Vec<SelectorSpec>),
    /// A selector that matches all paths that do not match the sub-selector.
    Not(Box<SelectorSpec>),
    /// A direct path to an item.
    Path(Vec<SelectorPathItem>),
}

impl SelectorSpec {
    /// Parses a selector from a string without legacy special handling.
    pub fn parse_non_legacy(s: &str) -> Result<SelectorSpec, InvalidSelectorError> {
        handle_selector(
            SelectorParser::parse(Rule::RootSelector, s)
                .map_err(|e| InvalidSelectorError::ParseError(Box::new(e)))?
                .next()
                .unwrap()
                .into_inner()
                .next()
                .unwrap(),
        )
    }

    /// Checks if a path matches given selector.
    ///
    /// This walks both the selector and the path starting at the end and towards the root
    /// to determine if the selector matches the current path.
    pub fn matches_path(&self, path: &Path) -> bool {
        let pii = path.attrs().pii;
        if pii == Pii::False {
            return false;
        }

        match *self {
            SelectorSpec::Path(ref path_items) => {
                // fastest path: the selector is deeper than the current structure.
                if path_items.len() > path.depth() {
                    return false;
                }

                // fast path: we do not have any deep matches
                let mut state_iter = path.iter().filter(|state| state.entered_anything());
                let mut selector_iter = path_items.iter().enumerate().rev();
                let mut depth_match = false;
                for state in &mut state_iter {
                    match selector_iter.next() {
                        Some((i, path_item)) => {
                            if !path_item.matches_state(pii, i, state) {
                                return false;
                            }

                            if matches!(path_item, SelectorPathItem::DeepWildcard) {
                                depth_match = true;
                                break;
                            }
                        }
                        None => break,
                    }
                }

                if !depth_match {
                    return true;
                }

                // slow path: we collect the remaining states and skip up to the first
                // match of the selector.
                let remaining_states = state_iter.collect::<SmallVec<[&ProcessingState<'_>; 16]>>();
                let mut selector_iter = selector_iter.rev().peekable();
                let (first_selector_i, first_selector_path) = match selector_iter.next() {
                    Some(selector_path) => selector_path,
                    None => return !remaining_states.is_empty(),
                };
                let mut path_match_iterator = remaining_states.iter().rev().skip_while(|state| {
                    !first_selector_path.matches_state(pii, first_selector_i, state)
                });
                if path_match_iterator.next().is_none() {
                    return false;
                }

                // then we check all remaining items and that nothing is left of the selector
                path_match_iterator
                    .zip(&mut selector_iter)
                    .all(|(state, (i, selector_path))| selector_path.matches_state(pii, i, state))
                    && selector_iter.next().is_none()
            }
            SelectorSpec::And(ref xs) => xs.iter().all(|x| x.matches_path(path)),
            SelectorSpec::Or(ref xs) => xs.iter().any(|x| x.matches_path(path)),
            SelectorSpec::Not(ref x) => !x.matches_path(path),
        }
    }
}

impl fmt::Display for SelectorSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SelectorSpec::And(ref xs) => {
                for (idx, x) in xs.iter().enumerate() {
                    if idx > 0 {
                        write!(f, " && ")?;
                    }

                    let needs_parens = match *x {
                        SelectorSpec::And(_) => false,
                        SelectorSpec::Or(_) => true,
                        SelectorSpec::Not(_) => false,
                        SelectorSpec::Path(_) => false,
                    };

                    if needs_parens {
                        write!(f, "({x})")?;
                    } else {
                        write!(f, "{x}")?;
                    }
                }
            }
            SelectorSpec::Or(ref xs) => {
                for (idx, x) in xs.iter().enumerate() {
                    if idx > 0 {
                        write!(f, " || ")?;
                    }

                    // OR has weakest precedence, so everything else binds stronger and does not
                    // need parens

                    write!(f, "{x}")?;
                }
            }
            SelectorSpec::Not(ref x) => {
                let needs_parens = match **x {
                    SelectorSpec::And(_) => true,
                    SelectorSpec::Or(_) => true,
                    SelectorSpec::Not(_) => true,
                    SelectorSpec::Path(_) => false,
                };

                if needs_parens {
                    write!(f, "!({x})")?;
                } else {
                    write!(f, "!{x}")?;
                }
            }
            SelectorSpec::Path(ref path) => {
                for (idx, item) in path.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ".")?;
                    }
                    write!(f, "{item}")?;
                }
            }
        }
        Ok(())
    }
}

impl FromStr for SelectorSpec {
    type Err = InvalidSelectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // these are temporary legacy selectors
        match s {
            "freeform" | "email" | "sensitive" | "text" => {
                return Ok(SelectorSpec::Path(vec![SelectorPathItem::Type(
                    ValueType::String,
                )]));
            }
            "databag" | "container" => {
                return Ok(SelectorSpec::Path(vec![SelectorPathItem::Type(
                    ValueType::Object,
                )]));
            }
            _ => {}
        }

        Self::parse_non_legacy(s)
    }
}

relay_common::impl_str_serde!(SelectorSpec, "a selector");

impl From<ValueType> for SelectorSpec {
    fn from(value_type: ValueType) -> Self {
        SelectorSpec::Path(vec![SelectorPathItem::Type(value_type)])
    }
}

fn handle_selector(pair: Pair<Rule>) -> Result<SelectorSpec, InvalidSelectorError> {
    fn map_multiple_or_inner<F>(
        pair: Pair<Rule>,
        f: F,
    ) -> Result<SelectorSpec, InvalidSelectorError>
    where
        F: Fn(Vec<SelectorSpec>) -> SelectorSpec,
    {
        let mut iter = pair.into_inner().map(handle_selector).peekable();
        let first = iter.next().unwrap()?;
        if iter.peek().is_none() {
            Ok(first)
        } else {
            let mut items = vec![first];
            for item in iter {
                items.push(item?);
            }
            Ok(f(items))
        }
    }

    match pair.as_rule() {
        Rule::ParenthesisOrPath | Rule::MaybeNotSelector => {
            handle_selector(pair.into_inner().next().unwrap())
        }
        Rule::SelectorPath => {
            let mut used_deep_wildcard = false;
            let items: Vec<SelectorPathItem> = pair
                .into_inner()
                .map(|item| {
                    let rv = handle_selector_path_item(item)?;
                    if rv == SelectorPathItem::DeepWildcard {
                        if used_deep_wildcard {
                            return Err(InvalidSelectorError::InvalidDeepWildcard);
                        } else {
                            used_deep_wildcard = true;
                        }
                    }
                    Ok(rv)
                })
                .collect::<Result<_, _>>()?;

            if matches!(items.as_slice(), [SelectorPathItem::Wildcard]) {
                return Err(InvalidSelectorError::InvalidWildcard);
            }

            Ok(SelectorSpec::Path(items))
        }
        Rule::AndSelector => map_multiple_or_inner(pair, SelectorSpec::And),
        Rule::OrSelector => map_multiple_or_inner(pair, SelectorSpec::Or),
        Rule::NotSelector => Ok(SelectorSpec::Not(Box::new(handle_selector(
            pair.into_inner().next().unwrap(),
        )?))),
        rule => Err(InvalidSelectorError::UnexpectedToken(
            format!("{rule:?}"),
            "a selector",
        )),
    }
}

fn handle_selector_path_item(pair: Pair<Rule>) -> Result<SelectorPathItem, InvalidSelectorError> {
    let pair = pair.into_inner().next().unwrap();
    match pair.as_rule() {
        Rule::ObjectType => Ok(SelectorPathItem::Type(
            pair.as_str()[1..]
                .parse()
                .map_err(|_| InvalidSelectorError::UnknownType)?,
        )),
        Rule::Wildcard => Ok(SelectorPathItem::Wildcard),
        Rule::DeepWildcard => Ok(SelectorPathItem::DeepWildcard),
        Rule::Index => Ok(SelectorPathItem::Index(
            pair.as_str()
                .parse()
                .map_err(|_| InvalidSelectorError::InvalidIndex)?,
        )),
        Rule::Key => Ok(SelectorPathItem::Key(handle_key(pair)?)),
        rule => Err(InvalidSelectorError::UnexpectedToken(
            format!("{rule:?}"),
            "a selector path item",
        )),
    }
}

fn handle_key(pair: Pair<Rule>) -> Result<String, InvalidSelectorError> {
    let pair = pair.into_inner().next().unwrap();
    match pair.as_rule() {
        Rule::UnquotedKey => Ok(pair.as_str().to_owned()),
        Rule::QuotedKey => Ok({
            let mut key = String::new();
            for token in pair.into_inner() {
                key.push_str(token.as_str());
            }
            key
        }),
        rule => Err(InvalidSelectorError::UnexpectedToken(
            format!("{rule:?}"),
            "a key",
        )),
    }
}

fn key_needs_quoting(key: &str) -> bool {
    SelectorParser::parse(Rule::RootUnquotedKey, key).is_err()
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use relay_event_schema::processor::FieldAttrs;

    use super::*;

    #[test]
    fn test_roundtrip() {
        fn check_roundtrip(s: &str) {
            assert_eq!(SelectorSpec::from_str(s).unwrap().to_string(), s);
        }

        check_roundtrip("!(!a)");
        check_roundtrip("!a || !b");
        check_roundtrip("!a && !b");
        check_roundtrip("!(a && !b)");
        check_roundtrip("!(a && b)");
    }

    #[test]
    fn test_invalid() {
        assert!(matches!(
            SelectorSpec::from_str("* && foo"),
            Err(InvalidSelectorError::InvalidWildcard)
        ));
        assert!(matches!(
            SelectorSpec::from_str("$frame.**.foo.**"),
            Err(InvalidSelectorError::InvalidDeepWildcard)
        ));
    }

    macro_rules! assert_matches_raw {
        ($state:expr, $selector:expr, $expected:expr) => {{
            let selector: SelectorSpec = $selector.parse().unwrap();
            let actual = selector.matches_path(&$state.path());
            assert!(
                actual == $expected,
                "Matched {} against {}, expected {:?}, actually {:?}",
                $selector,
                $state.path(),
                $expected,
                actual
            );
        }};
    }

    macro_rules! assert_matches_pii_maybe {
        ($state:expr, $first:expr, $($selector:expr,)*) => {{
            assert_matches_pii_true!($state, $first, $($selector,)*);
            let state = &$state;
            let state = state.enter_nothing(Some(Cow::Owned(FieldAttrs::new().pii(Pii::Maybe))));

            assert_matches_raw!(state, $first, true);
            $(
                assert_matches_raw!(state, $selector, true);
            )*

            let joined = concat!($first, $(" && ", $selector,)*);
            assert_matches_raw!(state, &joined, true);

            let joined = concat!($first, $(" || ", $selector,)*);
            assert_matches_raw!(state, &joined, true);

            let joined = concat!("** || ", $first, $(" || ", $selector,)*);
            assert_matches_raw!(state, &joined, true);
        }}
    }

    macro_rules! assert_matches_pii_true {
        ($state:expr, $first:expr, $($selector:expr,)*) => {{
            let state = &$state;
            let state = state.enter_nothing(Some(Cow::Owned(FieldAttrs::new().pii(Pii::True))));

            assert_matches_raw!(state, $first, true);
            $(
                assert_matches_raw!(state, $selector, true);
            )*

            let joined = concat!($first, $(" && ", $selector,)*);
            assert_matches_raw!(state, &joined, true);

            let joined = concat!($first, $(" || ", $selector,)*);
            assert_matches_raw!(state, &joined, true);

            let joined = concat!("** || ", $first, $(" || ", $selector,)*);
            assert_matches_raw!(state, &joined, true);
        }}
    }

    macro_rules! assert_not_matches {
        ($state:expr, $($selector:expr,)*) => {{
            let state = &$state;
            $(
                assert_matches_raw!(state, $selector, false);
            )*
        }}
    }

    #[test]
    fn test_matching() {
        let event_state = ProcessingState::new_root(None, Some(ValueType::Event)); // .
        let user_state = event_state.enter_static("user", None, Some(ValueType::User)); // .user
        let extra_state = user_state.enter_static("extra", None, Some(ValueType::Object)); // .user.extra
        let foo_state = extra_state.enter_static("foo", None, Some(ValueType::Array)); // .user.extra.foo
        let zero_state = foo_state.enter_index(0, None, None); // .user.extra.foo.0

        assert_matches_pii_maybe!(
            extra_state,
            "user.extra",  // this is an exact match to the state
            "$user.extra", // this is a match below a type
            "(** || user.*) && !(foo.bar.baz || a.b.c)",
        );

        assert_matches_pii_true!(
            extra_state,
            // known limitation: double-negations *could* be specific (I'd expect this as a user), but
            // right now we don't support it
            "!(!user.extra)",
            "!(!$user.extra)",
        );

        assert_matches_pii_maybe!(
            foo_state,
            "$user.extra.*", // this is a wildcard match into a type
        );

        assert_matches_pii_maybe!(
            zero_state,
            "$user.extra.foo.*", // a wildcard match into an array
            "$user.extra.foo.0", // a direct match into an array
        );

        assert_matches_pii_true!(
            zero_state,
            // deep matches are wild
            "$user.extra.foo.**",
            "$user.extra.**",
            "$user.**",
            "$event.**",
            "$user.**.0",
            // types are anywhere
            "$user.$object.**.0",
            "(**.0 | absolutebogus)",
            "(~$object)",
            "($object.** & (~absolutebogus))",
            "($object.** & (~absolutebogus))",
        );

        assert_not_matches!(
            zero_state,
            "$user.extra.foo.1", // direct mismatch in an array
            // deep matches are wild
            "$user.extra.bar.**",
            "$user.**.1",
            "($object | absolutebogus)",
            "($object & absolutebogus)",
            "(~$object.**)",
            "($object | (**.0 & absolutebogus))",
        );

        assert_matches_pii_true!(
            foo_state,
            "($array & $object.*)",
            "(** & $object.*)",
            "**.$array",
        );

        assert_not_matches!(foo_state, "($object & $object.*)",);
    }

    #[test]
    fn test_attachments_matching() {
        let event_state = ProcessingState::new_root(None, None);
        let attachments_state = event_state.enter_static("", None, Some(ValueType::Attachments)); // .
        let txt_state = attachments_state.enter_static("file.txt", None, Some(ValueType::Binary)); // .'file.txt'
        let minidump_state =
            attachments_state.enter_static("file.dmp", None, Some(ValueType::Minidump)); // .'file.txt'
        let minidump_state_inner = minidump_state.enter_static("", None, Some(ValueType::Binary)); // .'file.txt'

        assert_matches_pii_maybe!(attachments_state, "$attachments",);
        assert_matches_pii_maybe!(txt_state, "$attachments.'file.txt'",);

        assert_matches_pii_true!(txt_state, "$binary",);
        // WAT.  All entire attachments are binary, so why not be able to select them (specific)
        // like this?  Especially since we can select them with wildcard.
        assert_matches_pii_true!(txt_state, "$attachments.$binary",);

        // WAT.  This is not problematic but rather... weird?
        assert_matches_pii_maybe!(txt_state, "$attachments.*",);
        assert_matches_pii_true!(txt_state, "$attachments.**",);

        assert_matches_pii_maybe!(minidump_state, "$minidump",);
        // WAT.  This should not behave differently from plain $minidump
        assert_matches_pii_true!(minidump_state, "$attachments.$minidump",);

        // WAT.  We have the full path to a field here.
        assert_matches_pii_true!(minidump_state_inner, "$attachments.$minidump.$binary",);
    }
}
