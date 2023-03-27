use std::fmt;

use relay_common::MetricUnit;
use relay_metrics::{parse_name_unit, MetricType, ParseMetricError};

use crate::metrics_extraction::sessions::SessionsKind;
use crate::metrics_extraction::transactions::TransactionsKind;

mod conditional_tagging;
pub mod sessions;
pub mod transactions;
mod utils;

pub enum TypedMRI<'a> {
    Transaction(TransactionsKind<'a>),
    Session(SessionsKind),
}

impl<'a> TypedMRI<'a> {
    pub fn ty(&self) -> MetricType {
        match self {
            Self::Transaction(t) => t.ty(),
            Self::Session(s) => s.ty(),
        }
    }

    pub fn namespace(&self) -> &str {
        match self {
            Self::Session(_) => "sessions",
            Self::Transaction(_) => "transactions",
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::Transaction(t) => t.name(),
            Self::Session(s) => s.to_string(),
        }
    }

    pub fn unit(&self) -> MetricUnit {
        match self {
            Self::Transaction(t) => t.unit(),
            Self::Session(s) => s.unit(),
        }
    }

    /// Parses and validates an MRI of the form `<ty>:<ns>/<name>@<unit>`
    pub fn parse(s: &'a str) -> Result<Self, ParseMetricError> {
        // Ignoring type since at the moment, the metric type is always inferred from the name.
        // This might change in the future.
        let (_, rest) = s.split_once(':').ok_or(ParseMetricError(()))?;

        let (rest, unit) = parse_name_unit(rest).ok_or(ParseMetricError(()))?;

        let (ns, name) = rest.split_once('/').ok_or(ParseMetricError(()))?;

        Self::from_components(ns, name, unit)
    }

    pub fn from_components(
        ns: &'a str,
        name: &'a str,
        unit: MetricUnit,
    ) -> Result<Self, ParseMetricError> {
        match ns {
            "transactions" => Ok(Self::Transaction(TransactionsKind::parse(name, unit)?)),
            "sessions" => {
                let kind = match name {
                    "user" => SessionsKind::User,
                    "session" => SessionsKind::Session,
                    "error" => SessionsKind::Error,
                    _ => return Err(ParseMetricError(())),
                };
                Ok(Self::Session(kind))
            }
            _ => Err(ParseMetricError(())),
        }
    }
}

impl<'a> fmt::Display for TypedMRI<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `<ty>:<ns>/<name>@<unit>`
        write!(
            f,
            "{}:{}/{}@{}",
            self.ty(),
            self.namespace(),
            self.name(),
            self.unit()
        )
    }
}

impl<'a> From<TransactionsKind<'a>> for TypedMRI<'a> {
    fn from(value: TransactionsKind<'a>) -> Self {
        Self::Transaction(value)
    }
}

impl From<SessionsKind> for TypedMRI<'_> {
    fn from(value: SessionsKind) -> Self {
        Self::Session(value)
    }
}
