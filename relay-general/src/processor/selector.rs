use std::fmt;
use std::str::FromStr;

use failure::Fail;
use pest::error::Error;
use pest::iterators::Pair;
use pest::Parser;

use crate::processor::{ProcessingState, ValueType};

/// Error for invalid selectors
#[derive(Debug, Fail)]
pub enum InvalidSelectorError {
    #[fail(display = "invalid selector: deep wildcard used more than once")]
    InvalidDeepWildcard,

    #[fail(display = "invalid selector: {}", _0)]
    ParseError(Error<Rule>),

    #[fail(display = "invalid selector: invalid index")]
    InvalidIndex,

    #[fail(display = "invalid selector: unknown value")]
    UnknownType,

    #[fail(display = "parser bug: consumed {} (expected {})", _0, _1)]
    UnexpectedToken(String, &'static str),
}

mod parser {
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
    KeySubstring(String),
    Wildcard,
    DeepWildcard,
}

impl fmt::Display for SelectorPathItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SelectorPathItem::Type(ty) => write!(f, "${}", ty),
            SelectorPathItem::Index(index) => write!(f, "{}", index),
            SelectorPathItem::Key(ref key) => {
                if key_needs_quoting(key) {
                    write!(f, "'{}'", key.replace("'", "''"))
                } else {
                    write!(f, "{}", key)
                }
            }
            SelectorPathItem::KeySubstring(ref key) => {
                if key_needs_quoting(key) {
                    write!(f, "*'{}'*", key.replace("'", "''"))
                } else {
                    write!(f, "*{}*", key)
                }
            }
            SelectorPathItem::Wildcard => write!(f, "*"),
            SelectorPathItem::DeepWildcard => write!(f, "**"),
        }
    }
}

impl SelectorPathItem {
    pub(super) fn matches_state(&self, state: &ProcessingState<'_>) -> bool {
        match *self {
            SelectorPathItem::Wildcard => true,
            SelectorPathItem::DeepWildcard => true,
            SelectorPathItem::Type(ty) => state.value_type() == Some(ty),
            SelectorPathItem::Index(idx) => state.path().index() == Some(idx),
            SelectorPathItem::KeySubstring(ref key) => state
                .path()
                .key()
                .map(|k| k.to_lowercase().contains(&key.to_lowercase()))
                .unwrap_or(false),
            SelectorPathItem::Key(ref key) => state
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

impl SelectorSpec {
    /// A selector is specific if it directly addresses a single event location by path. We use
    /// this distinction in the PII processor to decide whether pii=maybe should be scrubbed.
    pub fn is_specific(&self) -> bool {
        match *self {
            SelectorSpec::And(_) | SelectorSpec::Or(_) | SelectorSpec::Not(_) => false,
            SelectorSpec::Path(ref path) => {
                path.iter().all(|item| {
                    match *item {
                        SelectorPathItem::Type(_) => false,
                        SelectorPathItem::Index(_) => true,
                        SelectorPathItem::Key(_) => true,
                        // necessary because of array indices
                        SelectorPathItem::Wildcard => true,
                        SelectorPathItem::KeySubstring(_) => false,
                        SelectorPathItem::DeepWildcard => false,
                    }
                })
            }
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
                        write!(f, "({})", x)?;
                    } else {
                        write!(f, "{}", x)?;
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

                    write!(f, "{}", x)?;
                }
            }
            SelectorSpec::Not(ref x) => {
                let needs_parens = match **x {
                    SelectorSpec::And(_) => true,
                    SelectorSpec::Or(_) => true,
                    SelectorSpec::Not(_) => false,
                    SelectorSpec::Path(_) => false,
                };

                if needs_parens {
                    write!(f, "!({})", x)?;
                } else {
                    write!(f, "!{}", x)?;
                }
            }
            SelectorSpec::Path(ref path) => {
                for (idx, item) in path.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ".")?;
                    }
                    write!(f, "{}", item)?;
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
                .map_err(InvalidSelectorError::ParseError)?
                .next()
                .unwrap()
                .into_inner()
                .next()
                .unwrap(),
        )
    }
}

impl_str_serde!(SelectorSpec);

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
            let items = pair
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

            Ok(SelectorSpec::Path(items))
        }
        Rule::AndSelector => map_multiple_or_inner(pair, SelectorSpec::And),
        Rule::OrSelector => map_multiple_or_inner(pair, SelectorSpec::Or),
        Rule::NotSelector => Ok(SelectorSpec::Not(Box::new(handle_selector(
            pair.into_inner().next().unwrap(),
        )?))),
        rule => Err(InvalidSelectorError::UnexpectedToken(
            format!("{:?}", rule),
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
        Rule::KeySubstring => Ok(SelectorPathItem::KeySubstring(handle_key(
            pair.into_inner().next().unwrap(),
        )?)),
        Rule::Key => Ok(SelectorPathItem::Key(handle_key(pair)?)),
        rule => Err(InvalidSelectorError::UnexpectedToken(
            format!("{:?}", rule),
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
            format!("{:?}", rule),
            "a key",
        )),
    }
}

fn key_needs_quoting(key: &str) -> bool {
    SelectorParser::parse(Rule::RootUnquotedKey, key).is_err()
}
