use std::collections::BTreeMap;
use std::fmt;

use relay_common::UnixTimestamp;

#[derive(Clone, Copy, Debug)]
pub enum DurationPrecision {
    NanoSeconds,
    MilliSeconds,
    Seconds,
}

#[derive(Clone, Copy, Debug)]
pub enum MetricUnit {
    Duration(DurationPrecision),
    None,
}

#[derive(Clone, Debug)]
pub enum MetricValue {
    Float(f64),
    Integer(i64),
    // TODO: Uuid(Uuid),
    Custom(String),
}

// TODO: PartialEq for MetricValue

#[derive(Clone, Copy, Debug)]
pub enum MetricType {
    Counter,
    Histogram,
    Set,
    Gauge,
}

#[derive(Clone, Copy, Debug)]
pub struct ParseMetricError(());

impl fmt::Display for ParseMetricError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to parse metric")
    }
}

#[derive(Clone, Debug)]
pub struct Metric {
    pub name: String,
    pub unit: MetricUnit,
    pub value: MetricValue,
    pub ty: MetricType,
    pub timestamp: UnixTimestamp,
    pub tags: BTreeMap<String, String>,
}

impl Metric {
    pub fn parse(string: &str) -> Result<Self, ParseMetricError> {
        todo!()
    }

    pub fn parse_all(string: &str) -> ParseMetrics {
        todo!()
    }
}

pub struct ParseMetrics {}

impl Iterator for ParseMetrics {
    type Item = Result<Metric, ParseMetricError>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
