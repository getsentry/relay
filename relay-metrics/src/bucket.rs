use std::collections::{btree_map, BTreeMap, BTreeSet};
use std::iter::FusedIterator;
use std::{fmt, mem};

use float_ord::FloatOrd;
use relay_common::time::UnixTimestamp;
use serde::{Deserialize, Serialize};

use crate::protocol::{
    self, CounterType, DistributionType, GaugeType, MetricResourceIdentifier, MetricType,
    MetricValue, MetricsContainer, SetType,
};
use crate::ParseMetricError;

/// A snapshot of values within a [`Bucket`].
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
pub struct GaugeValue {
    /// The last value reported in the bucket.
    ///
    /// This aggregation is not commutative.
    pub last: GaugeType,
    /// The minimum value reported in the bucket.
    pub min: GaugeType,
    /// The maximum value reported in the bucket.
    pub max: GaugeType,
    /// The sum of all values reported in the bucket.
    pub sum: GaugeType,
    /// The number of times this bucket was updated with a new value.
    pub count: u64,
}

impl GaugeValue {
    /// Creates a gauge snapshot from a single value.
    pub fn single(value: GaugeType) -> Self {
        Self {
            last: value,
            min: value,
            max: value,
            sum: value,
            count: 1,
        }
    }

    /// Inserts a new value into the gauge.
    pub fn insert(&mut self, value: GaugeType) {
        self.last = value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value;
        self.count += 1;
    }

    /// Merges two gauge snapshots.
    pub fn merge(&mut self, other: Self) {
        self.last = other.last;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    /// Returns the average of all values reported in this bucket.
    pub fn avg(&self) -> GaugeType {
        if self.count > 0 {
            self.sum / (self.count as GaugeType)
        } else {
            0.0
        }
    }
}

/// Type for counting duplicates in distributions.
type Count = u32;

/// A distribution of values within a [`Bucket`].
///
/// Distributions store a histogram of values. It allows to iterate both the distribution with
/// [`iter`](Self::iter) and individual values with [`iter_values`](Self::iter_values).
///
/// Based on individual reported values, distributions allow to query the maximum, minimum, or
/// average of the reported values, as well as statistical quantiles.
///
/// # Example
///
/// ```rust
/// use relay_metrics::dist;
///
/// let mut dist = dist![1.0, 1.0, 1.0, 2.0];
/// dist.insert(5.0);
/// dist.insert_multi(3.0, 7);
/// ```
///
/// Logically, this distribution is equivalent to this visualization:
///
/// ```plain
/// value | count
/// 1.0   | ***
/// 2.0   | *
/// 3.0   | *******
/// 4.0   |
/// 5.0   | *
/// ```
///
/// # Serialization
///
/// Distributions serialize as sorted lists of floating point values. The list contains one entry
/// for each value in the distribution, including duplicates.
#[derive(Clone, Default, PartialEq)]
pub struct DistributionValue {
    values: BTreeMap<FloatOrd<DistributionType>, Count>,
    length: Count,
}

impl DistributionValue {
    /// Makes a new, empty `DistributionValue`.
    ///
    /// Does not allocate anything on its own.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of values in the map.
    ///
    /// # Examples
    ///
    /// ```
    /// use relay_metrics::DistributionValue;
    ///
    /// let mut dist = DistributionValue::new();
    /// assert_eq!(dist.len(), 0);
    /// dist.insert(1.0);
    /// dist.insert(1.0);
    /// assert_eq!(dist.len(), 2);
    /// ```
    pub fn len(&self) -> Count {
        self.length
    }

    /// Returns `true` if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Adds a value to the distribution.
    ///
    /// Returns the number this value occurs in the distribution after inserting.
    ///
    /// # Examples
    ///
    /// ```
    /// use relay_metrics::DistributionValue;
    ///
    /// let mut dist = DistributionValue::new();
    /// assert_eq!(dist.insert(1.0), 1);
    /// assert_eq!(dist.insert(1.0), 2);
    /// assert_eq!(dist.insert(2.0), 1);
    /// ```
    pub fn insert(&mut self, value: DistributionType) -> Count {
        self.insert_multi(value, 1)
    }

    /// Adds a value multiple times to the distribution.
    ///
    /// Returns the number this value occurs in the distribution after inserting.
    ///
    /// # Examples
    ///
    /// ```
    /// use relay_metrics::DistributionValue;
    ///
    /// let mut dist = DistributionValue::new();
    /// assert_eq!(dist.insert_multi(1.0, 2), 2);
    /// assert_eq!(dist.insert_multi(1.0, 3), 5);
    /// ```
    pub fn insert_multi(&mut self, value: DistributionType, count: Count) -> Count {
        self.length += count;
        if count == 0 {
            return 0;
        }

        *self
            .values
            .entry(FloatOrd(value))
            .and_modify(|c| *c += count)
            .or_insert(count)
    }

    /// Returns `true` if the set contains a value.
    ///
    /// # Examples
    ///
    /// ```
    /// use relay_metrics::dist;
    ///
    /// let dist = dist![1.0];
    ///
    /// assert_eq!(dist.contains(1.0), true);
    /// assert_eq!(dist.contains(2.0), false);
    /// ```
    pub fn contains(&self, value: impl std::borrow::Borrow<DistributionType>) -> bool {
        self.values.contains_key(&FloatOrd(*value.borrow()))
    }

    /// Returns how often the given value occurs in the distribution.
    ///
    /// # Examples
    ///
    /// ```
    /// use relay_metrics::dist;
    ///
    /// let dist = dist![1.0, 1.0];
    ///
    /// assert_eq!(dist.get(1.0), 2);
    /// assert_eq!(dist.get(2.0), 0);
    /// ```
    pub fn get(&self, value: impl std::borrow::Borrow<DistributionType>) -> Count {
        let value = &FloatOrd(*value.borrow());
        self.values.get(value).copied().unwrap_or(0)
    }

    /// Gets an iterator that visits unique values in the `DistributionValue` in ascending order.
    ///
    /// The iterator yields pairs of values and their count in the distribution.
    ///
    /// # Examples
    ///
    /// ```
    /// use relay_metrics::dist;
    ///
    /// let dist = dist![2.0, 1.0, 3.0, 2.0];
    ///
    /// let mut iter = dist.iter();
    /// assert_eq!(iter.next(), Some((1.0, 1)));
    /// assert_eq!(iter.next(), Some((2.0, 2)));
    /// assert_eq!(iter.next(), Some((3.0, 1)));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn iter(&self) -> DistributionIter<'_> {
        DistributionIter {
            inner: self.values.iter(),
        }
    }

    /// Gets an iterator that visits the values in the `DistributionValue` in ascending order.
    ///
    /// # Examples
    ///
    /// ```
    /// use relay_metrics::dist;
    ///
    /// let dist = dist![2.0, 1.0, 3.0, 2.0];
    ///
    /// let mut iter = dist.iter_values();
    /// assert_eq!(iter.next(), Some(1.0));
    /// assert_eq!(iter.next(), Some(2.0));
    /// assert_eq!(iter.next(), Some(2.0));
    /// assert_eq!(iter.next(), Some(3.0));
    /// assert_eq!(iter.next(), None);
    /// ```
    pub fn iter_values(&self) -> DistributionValuesIter<'_> {
        DistributionValuesIter {
            inner: self.iter(),
            current: 0f64,
            remaining: 0,
            total: self.length,
        }
    }
}

impl<'a> IntoIterator for &'a DistributionValue {
    type Item = (DistributionType, Count);
    type IntoIter = DistributionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl fmt::Debug for DistributionValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl Extend<f64> for DistributionValue {
    fn extend<T: IntoIterator<Item = DistributionType>>(&mut self, iter: T) {
        for value in iter.into_iter() {
            self.insert(value);
        }
    }
}

impl Extend<(DistributionType, Count)> for DistributionValue {
    fn extend<T: IntoIterator<Item = (DistributionType, Count)>>(&mut self, iter: T) {
        for (value, count) in iter.into_iter() {
            self.insert_multi(value, count);
        }
    }
}

impl FromIterator<DistributionType> for DistributionValue {
    fn from_iter<T: IntoIterator<Item = DistributionType>>(iter: T) -> Self {
        let mut value = Self::default();
        value.extend(iter);
        value
    }
}

impl FromIterator<(DistributionType, Count)> for DistributionValue {
    fn from_iter<T: IntoIterator<Item = (DistributionType, Count)>>(iter: T) -> Self {
        let mut value = Self::default();
        value.extend(iter);
        value
    }
}

impl Serialize for DistributionValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.iter_values())
    }
}

impl<'de> Deserialize<'de> for DistributionValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DistributionVisitor;

        impl<'d> serde::de::Visitor<'d> for DistributionVisitor {
            type Value = DistributionValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a list of floating point values")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'d>,
            {
                let mut distribution = DistributionValue::new();

                while let Some(value) = seq.next_element()? {
                    distribution.insert(value);
                }

                Ok(distribution)
            }
        }

        deserializer.deserialize_seq(DistributionVisitor)
    }
}

/// An iterator over distribution entries in a [`DistributionValue`].
///
/// This struct is created by the [`iter`](DistributionValue::iter) method on
/// `DistributionValue`. See its documentation for more.
#[derive(Clone)]
pub struct DistributionIter<'a> {
    inner: btree_map::Iter<'a, FloatOrd<f64>, Count>,
}

impl Iterator for DistributionIter<'_> {
    type Item = (DistributionType, Count);

    fn next(&mut self) -> Option<Self::Item> {
        let (value, count) = self.inner.next()?;
        Some((value.0, *count))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for DistributionIter<'_> {}

impl fmt::Debug for DistributionIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.clone()).finish()
    }
}

/// An iterator over all individual values in a [`DistributionValue`].
///
/// This struct is created by the [`iter_values`](DistributionValue::iter_values) method on
/// `DistributionValue`. See its documentation for more.
#[derive(Clone)]
pub struct DistributionValuesIter<'a> {
    inner: DistributionIter<'a>,
    current: DistributionType,
    remaining: Count,
    total: Count,
}

impl Iterator for DistributionValuesIter<'_> {
    type Item = DistributionType;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining > 0 {
            self.remaining -= 1;
            self.total -= 1;
            return Some(self.current);
        }

        let (value, count) = self.inner.next()?;

        self.current = value;
        self.remaining = count - 1;
        self.total -= 1;
        Some(self.current)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.total as usize;
        (len, Some(len))
    }
}

impl ExactSizeIterator for DistributionValuesIter<'_> {}

impl fmt::Debug for DistributionValuesIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.clone()).finish()
    }
}

/// Creates a [`DistributionValue`] containing the given arguments.
///
/// `dist!` allows `DistributionValue` to be defined with the same syntax as array expressions.
///
/// # Example
///
/// ```
/// let dist = relay_metrics::dist![1.0, 2.0];
/// ```
#[macro_export]
macro_rules! dist {
    () => {
        $crate::DistributionValue::new()
    };
    ($($x:expr),+ $(,)?) => {{
        let mut distribution = $crate::DistributionValue::new();
        $( distribution.insert($x); )*
        distribution
    }};
}

/// A set of unique values.
///
/// Set values can be specified as strings in the submission protocol. They are always hashed
/// into a 32-bit value and the original value is dropped. If the submission protocol contains a
/// 32-bit integer, it will be used directly, instead.
pub type SetValue = BTreeSet<SetType>;

/// The [aggregated value](Bucket::value) of a metric bucket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum BucketValue {
    /// Aggregates [`MetricValue::Counter`] values by adding them into a single value.
    ///
    /// ```text
    /// 2, 1, 3, 2 => 8
    /// ```
    ///
    /// This variant serializes to a double precision float.
    #[serde(rename = "c")]
    Counter(CounterType),
    /// Aggregates [`MetricValue::Distribution`] values by collecting their values.
    ///
    /// ```text
    /// 2, 1, 3, 2 => [1, 2, 2, 3]
    /// ```
    ///
    /// This variant serializes to a list of double precision floats, see [`DistributionValue`].
    #[serde(rename = "d")]
    Distribution(DistributionValue),
    /// Aggregates [`MetricValue::Set`] values by storing their hash values in a set.
    ///
    /// ```text
    /// 2, 1, 3, 2 => {1, 2, 3}
    /// ```
    ///
    /// This variant serializes to a list of 32-bit integers.
    #[serde(rename = "s")]
    Set(SetValue),
    /// Aggregates [`MetricValue::Gauge`] values always retaining the maximum, minimum, and last
    /// value, as well as the sum and count of all values.
    ///
    /// **Note**: The "last" component of this aggregation is not commutative.
    ///
    /// ```text
    /// 1, 2, 3, 2 => {
    ///   last: 2
    ///   min: 1,
    ///   max: 3,
    ///   sum: 8,
    ///   count: 4,
    /// }
    /// ```
    ///
    /// This variant serializes to a structure, see [`GaugeValue`].
    #[serde(rename = "g")]
    Gauge(GaugeValue),
}

impl BucketValue {
    /// Returns the type of this value.
    pub fn ty(&self) -> MetricType {
        match self {
            Self::Counter(_) => MetricType::Counter,
            Self::Distribution(_) => MetricType::Distribution,
            Self::Set(_) => MetricType::Set,
            Self::Gauge(_) => MetricType::Gauge,
        }
    }

    /// Returns the number of raw data points in this value.
    pub fn len(&self) -> usize {
        match self {
            BucketValue::Counter(_) => 1,
            BucketValue::Distribution(distribution) => distribution.len() as usize,
            BucketValue::Set(set) => set.len(),
            BucketValue::Gauge(_) => 5,
        }
    }

    /// Returns `true` if this bucket contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Estimates the number of bytes needed to encode the bucket value.
    ///
    /// Note that this does not necessarily match the exact memory footprint of the value,
    /// because data structures have a memory overhead.
    pub fn cost(&self) -> usize {
        // Beside the size of [`BucketValue`], we also need to account for the cost of values
        // allocated dynamically.
        let allocated_cost = match self {
            Self::Counter(_) => 0,
            Self::Set(s) => mem::size_of::<SetType>() * s.len(),
            Self::Gauge(_) => 0,
            Self::Distribution(m) => {
                m.values.len() * (mem::size_of::<DistributionType>() + mem::size_of::<Count>())
            }
        };

        mem::size_of::<Self>() + allocated_cost
    }
}

impl From<MetricValue> for BucketValue {
    fn from(value: MetricValue) -> Self {
        match value {
            MetricValue::Counter(value) => Self::Counter(value),
            MetricValue::Distribution(value) => Self::Distribution(dist![value]),
            MetricValue::Set(value) => Self::Set(std::iter::once(value).collect()),
            MetricValue::Gauge(value) => Self::Gauge(GaugeValue::single(value)),
        }
    }
}

/// Parses a list of counter values separated by colons and sums them up.
fn parse_counter(string: &str) -> Option<CounterType> {
    let mut sum = CounterType::default();
    for component in string.split(':') {
        sum += component.parse::<CounterType>().ok()?;
    }
    Some(sum)
}

/// Parses a distribution from a list of floating point values separated by colons.
fn parse_distribution(string: &str) -> Option<DistributionValue> {
    let mut dist = DistributionValue::default();
    for component in string.split(':') {
        dist.insert(component.parse().ok()?);
    }
    Some(dist)
}

/// Parses a set of hashed numeric values.
fn parse_set(string: &str) -> Option<SetValue> {
    let mut set = SetValue::default();
    for component in string.split(':') {
        set.insert(component.parse().ok()?);
    }
    Some(set)
}

/// Parses a gauge from a value.
///
/// The gauge can either be given as a single floating point value, or as a list of exactly five
/// values in the order of [`GaugeValue`] fields.
fn parse_gauge(string: &str) -> Option<GaugeValue> {
    let mut components = string.split(':');

    let last = components.next()?.parse().ok()?;
    Some(if let Some(min) = components.next() {
        GaugeValue {
            last,
            min: min.parse().ok()?,
            max: components.next()?.parse().ok()?,
            sum: components.next()?.parse().ok()?,
            count: components.next()?.parse().ok()?,
        }
    } else {
        GaugeValue {
            last,
            min: last,
            max: last,
            sum: last,
            count: 1,
        }
    })
}

/// Parses an MRI from a string and a separate type.
///
/// The given string must be a part of the MRI, including the following components:
///  - (optional) The namespace. If missing, it is defaulted to `"custom"`
///  - (required) The metric name.
///  - (optional) The unit. If missing, it is defaulted to "none".
///
/// The metric type is never part of this string and must be supplied separately.
fn parse_mri(string: &str, ty: MetricType) -> Option<MetricResourceIdentifier> {
    let (name_and_namespace, unit) = protocol::parse_name_unit(string)?;

    let (raw_namespace, name) = name_and_namespace
        .split_once('/')
        .unwrap_or(("custom", name_and_namespace));

    Some(MetricResourceIdentifier {
        ty,
        name,
        namespace: raw_namespace.parse().ok()?,
        unit,
    })
}

/// Parses tags in the format `tag1,tag2:value`.
///
/// Tag values are optional. For tags with missing values, an empty `""` value is assumed.
fn parse_tags(string: &str) -> Option<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();

    for pair in string.split(',') {
        let mut name_value = pair.splitn(2, ':');

        let name = name_value.next()?;
        if !protocol::is_valid_tag_key(name) {
            continue;
        }

        let mut value = name_value.next().unwrap_or_default().to_owned();
        protocol::validate_tag_value(&mut value);

        map.insert(name.to_owned(), value);
    }

    Some(map)
}

/// An aggregation of metric values.
///
/// As opposed to single metric values, bucket aggregations can carry multiple values. See
/// [`MetricType`] for a description on how values are aggregated in buckets. Values are aggregated
/// by metric name, type, time window, and all tags. Particularly, this allows metrics to have the
/// same name even if their types differ.
///
/// See the [crate documentation](crate) for general information on Metrics.
///
/// # Values
///
/// The contents of a bucket, especially their representation and serialization, depend on the
/// metric type:
///
/// - [Counters](BucketValue::Counter) store a single value, serialized as floating point.
/// - [Distributions](MetricType::Distribution) and [sets](MetricType::Set) store the full set of
///   reported values.
/// - [Gauges](BucketValue::Gauge) store a snapshot of reported values, see [`GaugeValue`].
///
/// # Submission Protocol
///
/// ```text
/// <name>[@unit]:<value>[:<value>...]|<type>|#<tag_key>:<tag_value>,<tag>
/// ```
///
/// See the field documentation on this struct for more information on the components. An example
/// submission looks like this:
///
/// ```text
#[doc = include_str!("../tests/fixtures/buckets.statsd.txt")]
/// ```
///
/// To parse a submission payload, use [`Bucket::parse_all`].
///
/// # JSON Representation
///
/// In addition to the submission protocol, metrics can be represented as structured data in JSON.
/// In addition to the field values from the submission protocol, a timestamp is added to every
/// metric (see [crate documentation](crate)).
///
/// ```json
#[doc = include_str!("../tests/fixtures/buckets.json")]
/// ```
///
/// # Submission Protocol
///
/// Buckets are always represented as JSON. The data type of the `value` field is determined by the
/// metric type.
///
/// ```json
#[doc = include_str!("../tests/fixtures/buckets.json")]
/// ```
///
/// To parse a submission payload, use [`Bucket::parse_all`].
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Bucket {
    /// The start time of the time window.
    ///
    /// If a timestamp is not supplied in the item header of the envelope, the
    /// default timestamp supplied to [`Bucket::parse`] or [`Bucket::parse_all`]
    /// is associated with the metric. It is then aligned with the aggregation window.
    pub timestamp: UnixTimestamp,
    /// The length of the time window in seconds.
    pub width: u64,
    /// The MRI (metric resource identifier).
    ///
    /// See [`Metric::name`](crate::Metric::name).
    pub name: String,
    /// The type and aggregated values of this bucket.
    ///
    /// See [`Metric::value`](crate::Metric::value) for a mapping to inbound data.
    #[serde(flatten)]
    pub value: BucketValue,
    /// A list of tags adding dimensions to the metric for filtering and aggregation.
    ///
    /// See [`Metric::tags`](crate::Metric::tags). Every combination of tags results in a different
    /// bucket.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

impl Bucket {
    /// Parse statsd-compatible payload of format
    /// ```text
    /// [<ns>/]<name>[@<unit>]:<value>|<type>[|#<tags>]`
    /// ```
    fn parse_str(string: &str, timestamp: UnixTimestamp) -> Option<Self> {
        let mut components = string.split('|');

        let (mri_str, values_str) = components.next()?.split_once(':')?;
        let ty = components.next().and_then(|s| s.parse().ok())?;

        let mri = parse_mri(mri_str, ty)?;
        let value = match ty {
            MetricType::Counter => BucketValue::Counter(parse_counter(values_str)?),
            MetricType::Distribution => BucketValue::Distribution(parse_distribution(values_str)?),
            MetricType::Set => BucketValue::Set(parse_set(values_str)?),
            MetricType::Gauge => BucketValue::Gauge(parse_gauge(values_str)?),
        };

        let mut bucket = Bucket {
            timestamp,
            width: 0,
            name: mri.to_string(),
            value,
            tags: Default::default(),
        };

        for component in components {
            if let Some('#') = component.chars().next() {
                bucket.tags = parse_tags(component.get(1..)?)?;
            }
        }

        Some(bucket)
    }

    /// Parses a single metric aggregate from the raw protocol.
    ///
    /// See the [`Bucket`] for more information on the protocol.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_metrics::{Bucket, UnixTimestamp};
    ///
    /// let bucket = Bucket::parse(b"response_time@millisecond:57|d", UnixTimestamp::now())
    ///     .expect("metric should parse");
    /// ```
    pub fn parse(slice: &[u8], timestamp: UnixTimestamp) -> Result<Self, ParseMetricError> {
        let string = std::str::from_utf8(slice).or(Err(ParseMetricError(())))?;
        Self::parse_str(string, timestamp).ok_or(ParseMetricError(()))
    }

    /// Parses a set of metric aggregates from the raw protocol.
    ///
    /// Returns a metric result for each line in `slice`, ignoring empty lines. Both UNIX newlines
    /// (`\n`) and Windows newlines (`\r\n`) are supported.
    ///
    /// It is possible to continue consuming the iterator after `Err` is yielded.
    ///
    /// See [`Bucket`] for more information on the protocol.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_metrics::{Bucket, UnixTimestamp};
    ///
    /// let data = br#"
    /// endpoint.response_time@millisecond:57|d
    /// endpoint.hits:1|c
    /// "#;
    ///
    /// for metric_result in Bucket::parse_all(data, UnixTimestamp::now()) {
    ///     let bucket = metric_result.expect("metric should parse");
    ///     println!("Metric {}: {:?}", bucket.name, bucket.value);
    /// }
    /// ```
    pub fn parse_all(slice: &[u8], timestamp: UnixTimestamp) -> ParseBuckets<'_> {
        ParseBuckets { slice, timestamp }
    }
}

impl MetricsContainer for Bucket {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn len(&self) -> usize {
        self.value.len()
    }

    fn tag(&self, name: &str) -> Option<&str> {
        self.tags.get(name).map(|s| s.as_str())
    }

    fn remove_tag(&mut self, name: &str) {
        self.tags.remove(name);
    }
}

/// Iterator over parsed metrics returned from [`Bucket::parse_all`].
#[derive(Clone, Debug)]
pub struct ParseBuckets<'a> {
    slice: &'a [u8],
    timestamp: UnixTimestamp,
}

impl Default for ParseBuckets<'_> {
    fn default() -> Self {
        Self {
            slice: &[],
            // The timestamp will never be returned.
            timestamp: UnixTimestamp::from_secs(4711),
        }
    }
}

impl Iterator for ParseBuckets<'_> {
    type Item = Result<Bucket, ParseMetricError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.slice.is_empty() {
                return None;
            }

            let mut split = self.slice.splitn(2, |&b| b == b'\n');
            let current = split.next()?;
            self.slice = split.next().unwrap_or_default();

            let string = match std::str::from_utf8(current) {
                Ok(string) => string.strip_suffix('\r').unwrap_or(string),
                Err(_) => return Some(Err(ParseMetricError(()))),
            };

            if !string.is_empty() {
                return Some(Bucket::parse_str(string, self.timestamp).ok_or(ParseMetricError(())));
            }
        }
    }
}

impl FusedIterator for ParseBuckets<'_> {}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use crate::protocol::{DurationUnit, MetricUnit};

    use super::*;

    #[test]
    fn test_distribution_insert() {
        let mut distribution = DistributionValue::new();
        assert_eq!(distribution.insert(2f64), 1);
        assert_eq!(distribution.insert(1f64), 1);
        assert_eq!(distribution.insert(2f64), 2);

        assert_eq!(distribution.len(), 3);

        assert!(!distribution.contains(0f64));
        assert!(distribution.contains(1f64));
        assert!(distribution.contains(2f64));

        assert_eq!(distribution.get(0f64), 0);
        assert_eq!(distribution.get(1f64), 1);
        assert_eq!(distribution.get(2f64), 2);
    }

    #[test]
    fn test_distribution_insert_multi() {
        let mut distribution = DistributionValue::new();
        assert_eq!(distribution.insert_multi(0f64, 0), 0);
        assert_eq!(distribution.insert_multi(2f64, 2), 2);
        assert_eq!(distribution.insert_multi(1f64, 1), 1);
        assert_eq!(distribution.insert_multi(3f64, 1), 1);
        assert_eq!(distribution.insert_multi(3f64, 2), 3);

        assert_eq!(distribution.len(), 6);

        assert!(!distribution.contains(0f64));
        assert!(distribution.contains(1f64));
        assert!(distribution.contains(2f64));
        assert!(distribution.contains(3f64));

        assert_eq!(distribution.get(0f64), 0);
        assert_eq!(distribution.get(1f64), 1);
        assert_eq!(distribution.get(2f64), 2);
        assert_eq!(distribution.get(3f64), 3);
    }

    #[test]
    fn test_distribution_iter_values() {
        let distribution = dist![2f64, 1f64, 2f64];

        let mut iter = distribution.iter_values();
        assert_eq!(iter.len(), 3);
        assert_eq!(iter.next(), Some(1f64));
        assert_eq!(iter.len(), 2);
        assert_eq!(iter.next(), Some(2f64));
        assert_eq!(iter.len(), 1);
        assert_eq!(iter.next(), Some(2f64));
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_distribution_iter_values_empty() {
        let distribution = DistributionValue::new();
        let mut iter = distribution.iter_values();
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_distribution_iter() {
        let distribution = dist![2f64, 1f64, 2f64];

        let mut iter = distribution.iter();
        assert_eq!(iter.next(), Some((1f64, 1)));
        assert_eq!(iter.next(), Some((2f64, 2)));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn test_parse_garbage() {
        let s = "x23-408j17z4232@#34d\nc3456y7^😎";
        let timestamp = UnixTimestamp::from_secs(4711);
        let result = Bucket::parse(s.as_bytes(), timestamp);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_counter() {
        let s = "transactions/foo:42|c";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        insta::assert_debug_snapshot!(metric, @r###"
        Bucket {
            timestamp: UnixTimestamp(4711),
            width: 0,
            name: "c:transactions/foo@none",
            value: Counter(
                42.0,
            ),
            tags: {},
        }
        "###);
    }

    #[test]
    fn test_parse_counter_packed() {
        let s = "transactions/foo:42:17:21|c";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(metric.value, BucketValue::Counter(80.0));
    }

    #[test]
    fn test_parse_distribution() {
        let s = "transactions/foo:17.5|d";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        insta::assert_debug_snapshot!(metric, @r###"
        Bucket {
            timestamp: UnixTimestamp(4711),
            width: 0,
            name: "d:transactions/foo@none",
            value: Distribution(
                {
                    17.5: 1,
                },
            ),
            tags: {},
        }
        "###);
    }

    #[test]
    fn test_parse_distribution_packed() {
        let s = "transactions/foo:17.5:21.9:42.7|d";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(
            metric.value,
            BucketValue::Distribution(dist![17.5, 21.9, 42.7])
        );
    }

    #[test]
    fn test_parse_histogram() {
        let s = "transactions/foo:17.5|h"; // common alias for distribution
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(metric.value, BucketValue::Distribution(dist![17.5]));
    }

    #[test]
    fn test_parse_set() {
        let s = "transactions/foo:4267882815|s";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        insta::assert_debug_snapshot!(metric, @r###"
        Bucket {
            timestamp: UnixTimestamp(4711),
            width: 0,
            name: "s:transactions/foo@none",
            value: Set(
                {
                    4267882815,
                },
            ),
            tags: {},
        }
        "###);
    }

    #[test]
    #[ignore = "set hashing not implemented"]
    fn test_parse_set_hashed() {
        let s = "transactions/foo:e2546e4c-ecd0-43ad-ae27-87960e57a658|s";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        insta::assert_debug_snapshot!(metric, @r#"
            Metric {
                name: "s:transactions/foo@none",
                value: Set(
                    4267882815,
                ),
                timestamp: UnixTimestamp(4711),
                tags: {},
            }
            "#);
    }

    #[test]
    fn test_parse_set_packed() {
        let s = "transactions/foo:3182887624:4267882815|s";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(
            metric.value,
            BucketValue::Set([3182887624, 4267882815].into())
        )
    }

    #[test]
    fn test_parse_gauge() {
        let s = "transactions/foo:42|g";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        insta::assert_debug_snapshot!(metric, @r###"
        Bucket {
            timestamp: UnixTimestamp(4711),
            width: 0,
            name: "g:transactions/foo@none",
            value: Gauge(
                GaugeValue {
                    last: 42.0,
                    min: 42.0,
                    max: 42.0,
                    sum: 42.0,
                    count: 1,
                },
            ),
            tags: {},
        }
        "###);
    }

    #[test]
    fn test_parse_gauge_packed() {
        let s = "transactions/foo:25:17:42:220:85|g";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        insta::assert_debug_snapshot!(metric, @r###"
        Bucket {
            timestamp: UnixTimestamp(4711),
            width: 0,
            name: "g:transactions/foo@none",
            value: Gauge(
                GaugeValue {
                    last: 25.0,
                    min: 17.0,
                    max: 42.0,
                    sum: 220.0,
                    count: 85,
                },
            ),
            tags: {},
        }
        "###);
    }

    #[test]
    fn test_parse_implicit_namespace() {
        let s = "foo:42|c";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        insta::assert_debug_snapshot!(metric, @r###"
        Bucket {
            timestamp: UnixTimestamp(4711),
            width: 0,
            name: "c:custom/foo@none",
            value: Counter(
                42.0,
            ),
            tags: {},
        }
        "###);
    }

    #[test]
    fn test_parse_unit() {
        let s = "transactions/foo@second:17.5|d";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        let mri = MetricResourceIdentifier::parse(&metric.name).unwrap();
        assert_eq!(mri.unit, MetricUnit::Duration(DurationUnit::Second));
    }

    #[test]
    fn test_parse_unit_regression() {
        let s = "transactions/foo@s:17.5|d";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        let mri = MetricResourceIdentifier::parse(&metric.name).unwrap();
        assert_eq!(mri.unit, MetricUnit::Duration(DurationUnit::Second));
    }

    #[test]
    fn test_parse_tags() {
        let s = "transactions/foo:17.5|d|#foo,bar:baz";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        insta::assert_debug_snapshot!(metric.tags, @r#"
            {
                "bar": "baz",
                "foo": "",
            }
            "#);
    }

    #[test]
    fn test_parse_invalid_name() {
        let s = "foo#bar:42|c";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp);
        assert!(metric.is_err());
    }

    #[test]
    fn test_parse_empty_name() {
        let s = ":42|c";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp);
        assert!(metric.is_err());
    }

    #[test]
    fn test_parse_invalid_name_with_leading_digit() {
        let s = "64bit:42|c";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp);
        assert!(metric.is_err());
    }

    #[test]
    fn test_parse_all() {
        let s = "transactions/foo:42|c\nbar:17|c";
        let timestamp = UnixTimestamp::from_secs(4711);

        let metrics: Vec<Bucket> = Bucket::parse_all(s.as_bytes(), timestamp)
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(metrics.len(), 2);
    }

    #[test]
    fn test_parse_all_crlf() {
        let s = "transactions/foo:42|c\r\nbar:17|c";
        let timestamp = UnixTimestamp::from_secs(4711);

        let metrics: Vec<Bucket> = Bucket::parse_all(s.as_bytes(), timestamp)
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(metrics.len(), 2);
    }

    #[test]
    fn test_parse_all_empty_lines() {
        let s = "transactions/foo:42|c\n\n\nbar:17|c";
        let timestamp = UnixTimestamp::from_secs(4711);

        let metric_count = Bucket::parse_all(s.as_bytes(), timestamp).count();
        assert_eq!(metric_count, 2);
    }

    #[test]
    fn test_parse_all_trailing() {
        let s = "transactions/foo:42|c\nbar:17|c\n";
        let timestamp = UnixTimestamp::from_secs(4711);

        let metric_count = Bucket::parse_all(s.as_bytes(), timestamp).count();
        assert_eq!(metric_count, 2);
    }

    #[test]
    fn test_metrics_docs() {
        let text = include_str!("../tests/fixtures/buckets.statsd.txt").trim_end();
        let json = include_str!("../tests/fixtures/buckets.json").trim_end();

        let timestamp = UnixTimestamp::from_secs(1615889449);
        let statsd_metrics = Bucket::parse_all(text.as_bytes(), timestamp)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let json_metrics: Vec<Bucket> = serde_json::from_str(json).unwrap();

        assert_eq!(statsd_metrics, json_metrics);
    }

    #[test]
    #[ignore = "set hashing not implemented"]
    fn test_set_docs() {
        let text = include_str!("../tests/fixtures/set.statsd.txt").trim_end();
        let json = include_str!("../tests/fixtures/set.json").trim_end();

        let timestamp = UnixTimestamp::from_secs(1615889449);
        let statsd_metric = Bucket::parse(text.as_bytes(), timestamp).unwrap();
        let json_metric: Bucket = serde_json::from_str(json).unwrap();

        assert_eq!(statsd_metric, json_metric);
    }

    #[test]
    fn test_parse_buckets() {
        let json = r#"[
          {
            "name": "endpoint.response_time",
            "unit": "millisecond",
            "value": [36, 49, 57, 68],
            "type": "d",
            "timestamp": 1615889440,
            "width": 10,
            "tags": {
                "route": "user_index"
            }
          }
        ]"#;

        let buckets = serde_json::from_str::<Vec<Bucket>>(json).unwrap();
        insta::assert_debug_snapshot!(buckets, @r#"
        [
            Bucket {
                timestamp: UnixTimestamp(1615889440),
                width: 10,
                name: "endpoint.response_time",
                value: Distribution(
                    {
                        36.0: 1,
                        49.0: 1,
                        57.0: 1,
                        68.0: 1,
                    },
                ),
                tags: {
                    "route": "user_index",
                },
            },
        ]
        "#);
    }

    #[test]
    fn test_parse_bucket_defaults() {
        let json = r#"[
          {
            "name": "endpoint.hits",
            "value": 4,
            "type": "c",
            "timestamp": 1615889440,
            "width": 10
          }
        ]"#;

        let buckets = serde_json::from_str::<Vec<Bucket>>(json).unwrap();
        insta::assert_debug_snapshot!(buckets, @r#"
        [
            Bucket {
                timestamp: UnixTimestamp(1615889440),
                width: 10,
                name: "endpoint.hits",
                value: Counter(
                    4.0,
                ),
                tags: {},
            },
        ]
        "#);
    }

    #[test]
    fn test_buckets_roundtrip() {
        let json = r#"[
  {
    "timestamp": 1615889440,
    "width": 10,
    "name": "endpoint.response_time",
    "type": "d",
    "value": [
      36.0,
      49.0,
      57.0,
      68.0
    ],
    "tags": {
      "route": "user_index"
    }
  },
  {
    "timestamp": 1615889440,
    "width": 10,
    "name": "endpoint.hits",
    "type": "c",
    "value": 4.0,
    "tags": {
      "route": "user_index"
    }
  },
  {
    "timestamp": 1615889440,
    "width": 10,
    "name": "endpoint.parallel_requests",
    "type": "g",
    "value": {
      "last": 25.0,
      "min": 17.0,
      "max": 42.0,
      "sum": 2210.0,
      "count": 85
    }
  },
  {
    "timestamp": 1615889440,
    "width": 10,
    "name": "endpoint.users",
    "type": "s",
    "value": [
      3182887624,
      4267882815
    ],
    "tags": {
      "route": "user_index"
    }
  }
]"#;

        let buckets = serde_json::from_str::<Vec<Bucket>>(json).unwrap();
        let serialized = serde_json::to_string_pretty(&buckets).unwrap();
        assert_eq!(json, serialized);
    }

    #[test]
    fn test_bucket_docs_roundtrip() {
        let json = include_str!("../tests/fixtures/buckets.json")
            .trim_end()
            .replace("\r\n", "\n");
        let buckets = serde_json::from_str::<Vec<Bucket>>(&json).unwrap();

        let serialized = serde_json::to_string_pretty(&buckets).unwrap();
        assert_eq!(json, serialized);
    }
}
