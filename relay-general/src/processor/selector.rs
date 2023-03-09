use std::fmt;
use std::str::FromStr;

use pest::error::Error;
use pest::iterators::Pair;
use pest::Parser;

use crate::processor::{Pii, ProcessingState, ValueType};

/// Error for invalid selectors
#[derive(Debug, thiserror::Error)]
pub enum InvalidSelectorError {
    #[error("invalid selector: deep wildcard used more than once")]
    InvalidDeepWildcard,

    #[error("invalid selector: wildcard must be part of a path")]
    InvalidWildcard,

    #[error("invalid selector: {0}")]
    ParseError(Box<Error<Rule>>),

    #[error("invalid selector: invalid index")]
    InvalidIndex,

    #[error("invalid selector: unknown value")]
    UnknownType,

    #[error("parser bug: consumed {0} (expected {1})")]
    UnexpectedToken(String, &'static str),
}

mod parser {
    #![allow(clippy::upper_case_acronyms)]
    use pest_derive::Parser;

    #[derive(Parser)]
    #[grammar = "processor/selector.pest"]
    pub struct SelectorParser;
}

use self::parser::{Rule, SelectorParser};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum SelectorPathItem {
    Type(ValueType),
    Index(usize),
    Key(String),
    Wildcard,
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

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum SelectorSpec {
    And(Vec<SelectorSpec>),
    Or(Vec<SelectorSpec>),
    Not(Box<SelectorSpec>),
    Path(Vec<SelectorPathItem>),
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
}
