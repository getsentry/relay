#![allow(missing_docs)]
use std::{
    collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap},
    convert::TryFrom,
    ops::{Add, AddAssign},
};

use actix::{Actor, Context, Handler, Message};
use relay_common::UnixTimestamp;

use crate::{Metric, MetricType, MetricValue};

impl TryFrom<MetricValue> for f64 {
    type Error = ();

    fn try_from(value: MetricValue) -> Result<Self, Self::Error> {
        match value {
            MetricValue::Integer(n) => Ok(n as f64),
            MetricValue::Float(f) => Ok(f),
            MetricValue::Custom(_) => Err(()),
        }
    }
}

pub struct Bucket {}

#[derive(Clone, Copy, Debug)]
enum NumericValue {
    /// A signed integral value.
    Integer(i64),
    /// A signed floating point value.
    Float(f64),
}

impl TryFrom<MetricValue> for NumericValue {
    type Error = ();

    fn try_from(value: MetricValue) -> Result<Self, Self::Error> {
        match value {
            MetricValue::Integer(n) => Ok(Self::Integer(n)),
            MetricValue::Float(f) => Ok(Self::Float(f)),
            MetricValue::Custom(_) => Err(()),
        }
    }
}

impl Add for NumericValue {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::Integer(lhs), Self::Integer(rhs)) => Self::Integer(lhs + rhs),
            (Self::Integer(lhs), Self::Float(rhs)) => Self::Float(lhs as f64 + rhs),
            (Self::Float(lhs), Self::Integer(rhs)) => Self::Float(lhs + rhs as f64),
            (Self::Float(lhs), Self::Float(rhs)) => Self::Float(lhs + rhs),
        }
    }
}

impl AddAssign for NumericValue {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}

/// TODO
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum DiscreteValue {
    /// TODO
    Integer(i64),
    /// TODO
    Custom(String),
}

impl TryFrom<MetricValue> for DiscreteValue {
    type Error = ();

    fn try_from(value: MetricValue) -> Result<Self, Self::Error> {
        match value {
            MetricValue::Integer(n) => Ok(Self::Integer(n)),
            MetricValue::Float(_) => Err(()),
            MetricValue::Custom(s) => Ok(Self::Custom(s)),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct BucketKey {
    timestamp: UnixTimestamp,
    metric_name: String,
    tags: BTreeMap<String, String>,
}

/// TODO
enum BucketValue {
    /// TODO
    Counter(NumericValue),
    //
    Distribution(Vec<f64>),
    Gauge(MetricValue),
    /// TODO
    Set(BTreeSet<DiscreteValue>),
}

impl BucketValue {
    fn try_from_parts(ty: MetricType, value: MetricValue) -> Result<Self, ()> {
        Ok(match ty {
            MetricType::Counter => BucketValue::Counter(NumericValue::try_from(value)?),
            MetricType::Distribution => BucketValue::Distribution(vec![f64::try_from(value)?]),
            MetricType::Set => {
                BucketValue::Set(std::iter::once(DiscreteValue::try_from(value)?).collect())
            }
            MetricType::Gauge => BucketValue::Gauge(value),
        })
    }

    fn insert(&mut self, ty: MetricType, value: MetricValue) -> Result<(), ()> {
        match self {
            BucketValue::Counter(counter) if ty == MetricType::Counter => {
                *counter += NumericValue::try_from(value)?;
            }
            BucketValue::Distribution(distribution) if ty == MetricType::Distribution => {
                distribution.push(f64::try_from(value)?);
            }
            BucketValue::Gauge(gauge) if ty == MetricType::Gauge => {
                *gauge = value;
            }
            BucketValue::Set(set) if ty == MetricType::Set => {
                set.insert(DiscreteValue::try_from(value)?);
            }
            _ => {
                return Err(());
            }
        }

        Ok(())
    }
}

/// TODO
pub struct Aggregator {
    buckets: HashMap<BucketKey, BucketValue>,
}

impl Aggregator {
    pub fn new() -> Self {
        Self {
            buckets: HashMap::new(),
        }
    }

    fn insert(&mut self, metric: Metric) -> Result<(), ()> {
        let key = BucketKey {
            timestamp: metric.timestamp,
            metric_name: metric.name,
            tags: metric.tags,
        };

        match self.buckets.entry(key) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().insert(metric.ty, metric.value)?;
            }
            Entry::Vacant(entry) => {
                let value = BucketValue::try_from_parts(metric.ty, metric.value)?;
                entry.insert(value);
            }
        }

        Ok(())
    }
}

impl Actor for Aggregator {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("aggregator started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("aggregator stopped");
    }
}

#[derive(Debug)]
pub struct InsertMetric {
    metric: Metric,
}

impl InsertMetric {
    pub fn new(metric: Metric) -> Self {
        Self { metric }
    }
}

impl Message for InsertMetric {
    type Result = Result<(), ()>;
}

impl Handler<InsertMetric> for Aggregator {
    type Result = Result<(), ()>;

    fn handle(&mut self, message: InsertMetric, context: &mut Self::Context) -> Self::Result {
        let InsertMetric { metric } = message;
        self.insert(metric)
    }
}
