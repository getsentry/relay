use std::fmt;

use relay_common::MetricUnit;
use relay_metrics::{
    parse_name_unit, MetricNamespace, MetricResourceIdentifier, MetricType, ParseMetricError,
};

use crate::metrics_extraction::sessions::SessionsKind;
use crate::metrics_extraction::transactions::TransactionsKind;

mod conditional_tagging;
pub mod sessions;
pub mod transactions;
mod utils;
