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

#[derive(Parser)]
#[grammar = "processor/selector.pest"]
struct SelectorParser;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum SelectorPathItem {
    Type(ValueType),
    Index(usize),
    Key(String),
    ContainsKey(String),
    Wildcard,
    DeepWildcard,
}

impl fmt::Display for SelectorPathItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SelectorPathItem::Type(ty) => write!(f, "${}", ty),
            SelectorPathItem::Index(index) => write!(f, "{}", index),
            SelectorPathItem::Key(ref key) => write!(f, "{}", key),
            SelectorPathItem::ContainsKey(ref key) => write!(f, "*{}*", key),
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
            SelectorPathItem::Key(ref key) => state
                .path()
                .key()
                .map(|k| (&k.to_lowercase()) == key)
                .unwrap_or(false),
            SelectorPathItem::ContainsKey(ref key) => state
                .path()
                .key()
                .map(|k| (&k.to_lowercase()).contains(key))
                .unwrap_or(false),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum SelectorSpec {
    And(Box<SelectorSpec>, Box<SelectorSpec>),
    Or(Box<SelectorSpec>, Box<SelectorSpec>),
    Not(Box<SelectorSpec>),
    Path(Vec<SelectorPathItem>),
}

impl fmt::Display for SelectorSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SelectorSpec::And(ref a, ref b) => write!(f, "({}&{})", a, b)?,
            SelectorSpec::Or(ref a, ref b) => write!(f, "({}|{})", a, b)?,
            SelectorSpec::Not(ref x) => write!(f, "~{}", x)?,
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
    match pair.as_rule() {
        Rule::Selector => handle_selector(pair.into_inner().next().unwrap()),
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
        Rule::AndSelector => {
            let mut inner = pair.into_inner();
            let a = handle_selector(inner.next().unwrap())?;
            let b = handle_selector(inner.next().unwrap())?;
            Ok(SelectorSpec::And(Box::new(a), Box::new(b)))
        }
        Rule::OrSelector => {
            let mut inner = pair.into_inner();
            let a = handle_selector(inner.next().unwrap())?;
            let b = handle_selector(inner.next().unwrap())?;
            Ok(SelectorSpec::Or(Box::new(a), Box::new(b)))
        }
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
        Rule::Key => Ok(SelectorPathItem::Key(pair.as_str().to_owned())),
        Rule::ContainsKey => Ok(SelectorPathItem::ContainsKey(
            pair.into_inner().next().unwrap().as_str().to_owned(),
        )),
        rule => Err(InvalidSelectorError::UnexpectedToken(
            format!("{:?}", rule),
            "a selector path item",
        )),
    }
}
