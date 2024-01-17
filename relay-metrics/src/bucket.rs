use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hash;
use std::iter::FusedIterator;
use std::{fmt, mem};

use hash32::{FnvHasher, Hasher as _};
use relay_cardinality::{CardinalityItem, CardinalityScope};
use relay_common::time::UnixTimestamp;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::protocol::{
    self, hash_set_value, CounterType, DistributionType, GaugeType, MetricResourceIdentifier,
    MetricType, SetType,
};
use crate::{FiniteF64, MetricNamespace, ParseMetricError};

const VALUE_SEPARATOR: char = ':';

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
        self.sum = self.sum.saturating_add(value);
        self.count += 1;
    }

    /// Merges two gauge snapshots.
    pub fn merge(&mut self, other: Self) {
        self.last = other.last;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum = self.sum.saturating_add(other.sum);
        self.count += other.count;
    }

    /// Returns the average of all values reported in this bucket.
    pub fn avg(&self) -> Option<GaugeType> {
        self.sum / FiniteF64::new(self.count as f64)?
    }
}

/// A distribution of values within a [`Bucket`].
///
/// Distributions logically store a histogram of values. Based on individual reported values,
/// distributions allow to query the maximum, minimum, or average of the reported values, as well as
/// statistical quantiles.
///
/// # Example
///
/// ```
/// use relay_metrics::dist;
///
/// let mut dist = dist![1, 1, 1, 2];
/// dist.push(5.into());
/// dist.extend(std::iter::repeat(3.into()).take(7));
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
/// Distributions serialize as lists of floating point values. The list contains one entry for each
/// value in the distribution, including duplicates.
pub type DistributionValue = SmallVec<[DistributionType; 3]>;

#[doc(hidden)]
pub use smallvec::smallvec as _smallvec;

/// Creates a [`DistributionValue`] containing the given arguments.
///
/// `dist!` allows `DistributionValue` to be defined with the same syntax as array expressions.
///
/// # Example
///
/// ```
/// let dist = relay_metrics::dist![1, 2];
/// ```
#[macro_export]
macro_rules! dist {
    ($($x:expr),*$(,)*) => {
        $crate::_smallvec!($($crate::DistributionType::from($x)),*) as $crate::DistributionValue
    };
}

/// A set of unique values.
///
/// Set values can be specified as strings in the submission protocol. They are always hashed
/// into a 32-bit value and the original value is dropped. If the submission protocol contains a
/// 32-bit integer, it will be used directly, instead.
///
/// See the [bucket docs](crate::Bucket) for more information on set hashing.
pub type SetValue = BTreeSet<SetType>;

/// The [aggregated value](Bucket::value) of a metric bucket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum BucketValue {
    /// Counts instances of an event ([`MetricType::Counter`]).
    ///
    /// Counters can be incremented and decremented. The default operation is to increment a counter
    /// by `1`, although increments by larger values are equally possible.
    ///
    /// # Statsd Format
    ///
    /// Counters are declared as `"c"`. Alternatively, `"m"` is allowed.
    ///
    /// There can be a variable number of floating point values. If more than one value is given,
    /// the values are summed into a single counter value:
    ///
    /// ```text
    /// endpoint.hits:4.5:21:17.0|c
    /// ```
    ///
    /// # Serialization
    ///
    /// This variant serializes to a double precision float.
    ///
    /// # Aggregation
    ///
    /// Counters aggregate by folding individual values into a single sum value per bucket. The sum
    /// is ingested and stored directly.
    #[serde(rename = "c")]
    Counter(CounterType),

    /// Builds a statistical distribution over values reported ([`MetricType::Distribution`]).
    ///
    /// Based on individual reported values, distributions allow to query the maximum, minimum, or
    /// average of the reported values, as well as statistical quantiles. With an increasing number
    /// of values in the distribution, its accuracy becomes approximate.
    ///
    /// # Statsd Format
    ///
    /// Distributions are declared as `"d"`. Alternatively, `"d"` and `"ms"` are allowed.
    ///
    /// There can be a variable number of floating point values. These values are collected directly
    /// in a list per bucket.
    ///
    /// ```text
    /// endpoint.response_time@millisecond:36:49:57:68|d
    /// ```
    ///
    /// # Serialization
    ///
    /// This variant serializes to a list of double precision floats, see [`DistributionValue`].
    ///
    /// # Aggregation
    ///
    /// During ingestion, all individual reported values are collected in a lossless format. In
    /// storage, these values are compressed into data sketches that allow to query quantiles.
    /// Separately, the count and sum of the reported values is stored, which makes distributions a
    /// strict superset of counters.
    #[serde(rename = "d")]
    Distribution(DistributionValue),

    /// Counts the number of unique reported values.
    ///
    /// Sets allow sending arbitrary discrete values, including strings, and store the deduplicated
    /// count. With an increasing number of unique values in the set, its accuracy becomes
    /// approximate. It is not possible to query individual values from a set.
    ///
    /// # Statsd Format
    ///
    /// Sets are declared as `"s"`. Values in the list should be deduplicated.
    ///
    ///
    /// ```text
    /// endpoint.users:3182887624:4267882815|s
    /// endpoint.users:e2546e4c-ecd0-43ad-ae27-87960e57a658|s
    /// ```
    ///
    /// # Serialization
    ///
    /// This variant serializes to a list of 32-bit integers.
    ///
    /// # Aggregation
    ///
    /// Set values are internally represented as 32-bit integer hashes of the original value. These
    /// hashes can be ingested directly as seen in the first example above. If raw strings are sent,
    /// they will be hashed on-the-fly.
    ///
    /// Internally, set metrics are stored in data sketches that expose an approximate cardinality.
    #[serde(rename = "s")]
    Set(SetValue),

    /// Stores absolute snapshots of values.
    ///
    /// In addition to plain [counters](Self::Counter), gauges store a snapshot of the maximum,
    /// minimum and sum of all values, as well as the last reported value. Note that the "last"
    /// component of this aggregation is not commutative. Which value is preserved as last value is
    /// implementation-defined.
    ///
    /// # Statsd Format
    ///
    /// Gauges are declared as `"g"`. There are two ways to ingest gauges:
    ///  1. As a single value. In this case, the provided value is assumed as the last, minimum,
    ///     maximum, and the sum.
    ///  2. As a sequence of five values in the order: `last`, `min`, `max`, `sum`, `count`.
    ///
    /// ```text
    /// endpoint.parallel_requests:25|g
    /// endpoint.parallel_requests:25:17:42:220:85|g
    /// ```
    ///
    /// # Serialization
    ///
    /// This variant serializes to a structure with named fields, see [`GaugeValue`].
    ///
    /// # Aggregation
    ///
    /// Gauges aggregate by folding each of the components based on their semantics:
    ///  - `last` assumes the newly added value
    ///  - `min` retains the smaller value
    ///  - `max` retains the larger value
    ///  - `sum` adds the new value to the existing sum
    ///  - `count` adds the count of the newly added gauge (defaulting to `1`)
    #[serde(rename = "g")]
    Gauge(GaugeValue),
}

impl BucketValue {
    /// Returns a bucket value representing a counter with the given value.
    pub fn counter(value: CounterType) -> Self {
        Self::Counter(value)
    }

    /// Returns a bucket value representing a distribution with a single given value.
    pub fn distribution(value: DistributionType) -> Self {
        Self::Distribution(dist![value])
    }

    /// Returns a bucket value representing a set with a single given hash value.
    pub fn set(value: SetType) -> Self {
        Self::Set(std::iter::once(value).collect())
    }

    /// Returns a bucket value representing a set with a single given string value.
    pub fn set_from_str(string: &str) -> Self {
        Self::set(hash_set_value(string))
    }

    /// Returns a bucket value representing a set with a single given value.
    pub fn set_from_display(display: impl fmt::Display) -> Self {
        Self::set(hash_set_value(&display.to_string()))
    }

    /// Returns a bucket value representing a gauge with a single given value.
    pub fn gauge(value: GaugeType) -> Self {
        Self::Gauge(GaugeValue::single(value))
    }

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
            BucketValue::Distribution(distribution) => distribution.len(),
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
            Self::Distribution(d) => d.len() * mem::size_of::<DistributionType>(),
        };

        mem::size_of::<Self>() + allocated_cost
    }

    /// Merges the given `bucket_value` into `self`.
    ///
    /// Returns `Ok(())` if the two bucket values can be merged. This is the case when both bucket
    /// values are of the same variant. Otherwise, this returns `Err(other)`.
    pub fn merge(&mut self, other: Self) -> Result<(), Self> {
        match (self, other) {
            (Self::Counter(slf), Self::Counter(other)) => *slf = slf.saturating_add(other),
            (Self::Distribution(slf), Self::Distribution(other)) => slf.extend_from_slice(&other),
            (Self::Set(slf), Self::Set(other)) => slf.extend(other),
            (Self::Gauge(slf), Self::Gauge(other)) => slf.merge(other),
            (_, other) => return Err(other),
        }

        Ok(())
    }
}

/// Parses a list of counter values separated by colons and sums them up.
fn parse_counter(string: &str) -> Option<CounterType> {
    let mut sum = CounterType::default();
    for component in string.split(VALUE_SEPARATOR) {
        sum = sum.saturating_add(component.parse().ok()?);
    }
    Some(sum)
}

/// Parses a distribution from a list of floating point values separated by colons.
fn parse_distribution(string: &str) -> Option<DistributionValue> {
    let mut dist = DistributionValue::default();
    for component in string.split(VALUE_SEPARATOR) {
        dist.push(component.parse().ok()?);
    }
    Some(dist)
}

/// Parses a set of hashed numeric values.
fn parse_set(string: &str) -> Option<SetValue> {
    let mut set = SetValue::default();
    for component in string.split(VALUE_SEPARATOR) {
        let hash = component
            .parse()
            .unwrap_or_else(|_| protocol::hash_set_value(component));
        set.insert(hash);
    }
    Some(set)
}

/// Parses a gauge from a value.
///
/// The gauge can either be given as a single floating point value, or as a list of exactly five
/// values in the order of [`GaugeValue`] fields.
fn parse_gauge(string: &str) -> Option<GaugeValue> {
    let mut components = string.split(VALUE_SEPARATOR);

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
        GaugeValue::single(last)
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

/// Parses a unix UTC timestamp.
fn parse_timestamp(string: &str) -> Option<UnixTimestamp> {
    string.parse().ok().map(UnixTimestamp::from_secs)
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
/// <name>[@unit]:<value>[:<value>...]|<type>|#<tag_key>:<tag_value>,<tag>|T<timestamp>
/// ```
///
/// See the [field documentation](Bucket#fields) for more information on the components. An example
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
/// Alternatively to the submission protocol, metrics can be represented as structured data in JSON.
/// The data type of the `value` field is determined by the metric type.
///
/// In addition to the submission protocol, buckets have a required [`width`](Self::width) field in
/// their JSON representation.
///
/// ```json
#[doc = include_str!("../tests/fixtures/buckets.json")]
/// ```
///
/// To parse a JSON payload, use [`serde_json`].
///
/// # Hashing of Sets
///
/// Set values can be specified as strings in the submission protocol. They are always hashed
/// into a 32-bit value and the original value is dropped. If the submission protocol contains a
/// 32-bit integer, it will be used directly, instead.
///
/// **Example**:
///
/// ```text
#[doc = include_str!("../tests/fixtures/set.statsd.txt")]
/// ```
///
/// The above submission is represented as:
///
/// ```json
#[doc = include_str!("../tests/fixtures/set.json")]
/// ```
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Bucket {
    /// The start time of the bucket's time window.
    ///
    /// If a timestamp is not supplied as part of the submission payload, the default timestamp
    /// supplied to [`Bucket::parse`] or [`Bucket::parse_all`] is associated with the metric. It is
    /// then aligned with the aggregation window.
    ///
    /// # Statsd Format
    ///
    /// In statsd, timestamps are part of the `|`-separated list following values. Timestamps start
    /// with with the literal character `'T'` followed by the UNIX timestamp.
    ///
    /// The timestamp must be a positive integer in decimal notation representing the value of the
    /// UNIX timestamp.
    ///
    /// # Example
    ///
    /// ```text
    /// endpoint.hits:1|c|T1615889440
    /// ```
    pub timestamp: UnixTimestamp,

    /// The length of the time window in seconds.
    ///
    /// To initialize a new bucket, choose `0` as width. Once the bucket is tracked by Relay's
    /// aggregator, the width is aligned with configuration for the namespace and the  timestamp is
    /// adjusted accordingly.
    ///
    /// # Statsd Format
    ///
    /// Specifying the bucket width in statsd is not supported.
    pub width: u64,

    /// The name of the metric in MRI (metric resource identifier) format.
    ///
    /// MRIs have the format `<type>:<ns>/<name>@<unit>`. See [`MetricResourceIdentifier`] for
    /// information on fields and representations.
    ///
    /// # Statsd Format
    ///
    /// MRIs are sent in a more relaxed format: `[<namespace/]name[@unit]`. The value type is not
    /// part of the metric name and namespaces are optional.
    ///
    /// Namespaces and units must consist of ASCII characters and match the regular expression
    /// `/\w+/`. The name component of MRIs consist of unicode characters and must match the
    /// regular expression `/\w[\w\d_-.]+/`. Note that the name must begin with a letter.
    ///
    /// Per convention, dots separate metric names into components, where the leading components are
    /// considered namespaces and the final component is the name of the metric within its
    /// namespace.
    ///
    /// # Examples
    ///
    /// ```text
    /// endpoint.hits:1|c
    /// custom/endpoint.hits:1|c
    /// custom/endpoint.duration@millisecond:21.5|d
    /// ```
    pub name: String,

    /// The type and aggregated values of this bucket.
    ///
    /// Buckets support multiple values that are aggregated and can be accessed using a range of
    /// aggregation functions depending on the value type. While always a variable number of values
    /// can be sent in, some aggregations reduce the raw values to a fixed set of aggregates.
    ///
    /// See [`BucketValue`] for more examples and semantics.
    ///
    /// # Statsd Payload
    ///
    /// The bucket value and its type are specified in separate fields following the metric name in
    /// the format: `<name>:<value>|<type>`. Values must be base-10 floating point numbers with
    /// optional decimal places.
    ///
    /// It is possible to pack multiple values into a single datagram, but note that the type and
    /// the value representation must match for this. Refer to the [`BucketValue`] docs for more
    /// examples.
    ///
    /// # Example
    ///
    /// ```text
    /// endpoint.hits:21|c
    /// endpoint.hits:4.5|c
    /// ```
    #[serde(flatten)]
    pub value: BucketValue,

    /// A list of tags adding dimensions to the metric for filtering and aggregation.
    ///
    /// Tags allow to compute separate aggregates to filter or group metric values by any number of
    /// dimensions. Tags consist of a unique tag key and one associated value. For tags with missing
    /// values, an empty `""` value is assumed at query time.
    ///
    /// # Statsd Format
    ///
    /// Tags are preceded with a hash `#` and specified in a comma (`,`) separated list. Each tag
    /// can either be a tag name, or a `name:value` combination. Tags are optional and can be
    /// omitted.
    ///
    /// Tag keys are restricted to ASCII characters and must match the regular expression
    /// `/[a-zA-Z0-9_/.-]+/`. Tag values can contain unicode characters and must match the regular
    /// expression `/[\w\d_:/@.{}\[\]$-]+/`.
    ///
    /// # Example
    ///
    /// ```text
    /// endpoint.hits:1|c|#route:user_index,environment:production,release:1.4.0
    /// ```
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

impl Bucket {
    /// Parses a statsd-compatible payload.
    ///
    /// ```text
    /// [<ns>/]<name>[@<unit>]:<value>|<type>[|#<tags>]`
    /// ```
    fn parse_str(string: &str, timestamp: UnixTimestamp) -> Option<Self> {
        let mut components = string.split('|');

        let (mri_str, values_str) = components.next()?.split_once(':')?;
        let ty = components.next().and_then(|s| s.parse().ok())?;

        let mri = MetricResourceIdentifier::parse_with_type(mri_str, ty).ok()?;
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
            match component.chars().next() {
                Some('#') => {
                    bucket.tags = parse_tags(component.get(1..)?)?;
                }
                Some('T') => {
                    bucket.timestamp = parse_timestamp(component.get(1..)?)?;
                }
                _ => (),
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
        let string = std::str::from_utf8(slice).map_err(|_| ParseMetricError)?;
        Self::parse_str(string, timestamp).ok_or(ParseMetricError)
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

    /// Returns the value of the specified tag if it exists.
    pub fn tag(&self, name: &str) -> Option<&str> {
        self.tags.get(name).map(|s| s.as_str())
    }

    /// Removes the value of the specified tag.
    ///
    /// If the tag exists, the removed value is returned.
    pub fn remove_tag(&mut self, name: &str) -> Option<String> {
        self.tags.remove(name)
    }
}

impl CardinalityItem for Bucket {
    fn namespace(&self) -> Option<MetricNamespace> {
        let mri = match MetricResourceIdentifier::parse(&self.name) {
            Err(error) => {
                relay_log::debug!(
                    error = &error as &dyn std::error::Error,
                    metric = self.name,
                    "rejecting metric with invalid MRI"
                );
                return None;
            }
            Ok(mri) => mri,
        };

        Some(mri.namespace)
    }

    fn to_hash(&self) -> u32 {
        let mut hasher = FnvHasher::default();
        self.name.hash(&mut hasher);
        self.tags.hash(&mut hasher);
        hasher.finish32()
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
                Err(_) => return Some(Err(ParseMetricError)),
            };

            if !string.is_empty() {
                return Some(Bucket::parse_str(string, self.timestamp).ok_or(ParseMetricError));
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
    fn test_distribution_value_size() {
        // DistributionValue uses a SmallVec internally to prevent an additional allocation and
        // indirection in cases where it needs to store only a small number of items. This is
        // enabled by a comparably large `GaugeValue`, which stores five atoms. Ensure that the
        // `DistributionValue`'s size does not exceed that of `GaugeValue`.
        assert!(
            std::mem::size_of::<DistributionValue>() <= std::mem::size_of::<GaugeValue>(),
            "distribution value should not exceed gauge {}",
            std::mem::size_of::<DistributionValue>()
        );
    }

    #[test]
    fn test_bucket_value_merge_counter() {
        let mut value = BucketValue::Counter(42.into());
        value.merge(BucketValue::Counter(43.into())).unwrap();
        assert_eq!(value, BucketValue::Counter(85.into()));
    }

    #[test]
    fn test_bucket_value_merge_distribution() {
        let mut value = BucketValue::Distribution(dist![1, 2, 3]);
        value.merge(BucketValue::Distribution(dist![2, 4])).unwrap();
        assert_eq!(value, BucketValue::Distribution(dist![1, 2, 3, 2, 4]));
    }

    #[test]
    fn test_bucket_value_merge_set() {
        let mut value = BucketValue::Set(vec![1, 2].into_iter().collect());
        value.merge(BucketValue::Set([2, 3].into())).unwrap();
        assert_eq!(value, BucketValue::Set(vec![1, 2, 3].into_iter().collect()));
    }

    #[test]
    fn test_bucket_value_merge_gauge() {
        let mut value = BucketValue::Gauge(GaugeValue::single(42.into()));
        value.merge(BucketValue::gauge(43.into())).unwrap();

        assert_eq!(
            value,
            BucketValue::Gauge(GaugeValue {
                last: 43.into(),
                min: 42.into(),
                max: 43.into(),
                sum: 85.into(),
                count: 2,
            })
        );
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
        assert_eq!(metric.value, BucketValue::Counter(80.into()));
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
                [
                    17.5,
                ],
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
            BucketValue::Distribution(dist![
                FiniteF64::new(17.5).unwrap(),
                FiniteF64::new(21.9).unwrap(),
                FiniteF64::new(42.7).unwrap()
            ])
        );
    }

    #[test]
    fn test_parse_histogram() {
        let s = "transactions/foo:17.5|h"; // common alias for distribution
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(
            metric.value,
            BucketValue::Distribution(dist![FiniteF64::new(17.5).unwrap()])
        );
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
    fn test_parse_set_hashed() {
        let s = "transactions/foo:e2546e4c-ecd0-43ad-ae27-87960e57a658|s";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(metric.value, BucketValue::Set([4267882815].into()));
    }

    #[test]
    fn test_parse_set_hashed_packed() {
        let s = "transactions/foo:e2546e4c-ecd0-43ad-ae27-87960e57a658:00449b66-d91f-4fb8-b324-4c8bdf2499f6|s";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(
            metric.value,
            BucketValue::Set([181348692, 4267882815].into())
        );
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
    fn test_parse_timestamp() {
        let s = "transactions/foo:17.5|d|T1615889449";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(metric.timestamp, UnixTimestamp::from_secs(1615889449));
    }

    #[test]
    fn test_parse_sample_rate() {
        // Sample rate should be ignored
        let s = "transactions/foo:17.5|d|@0.1";
        let timestamp = UnixTimestamp::from_secs(4711);
        Bucket::parse(s.as_bytes(), timestamp).unwrap();
    }

    #[test]
    fn test_parse_invalid_name() {
        let s = "foo#bar:42|c";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Bucket::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(metric.name, "c:custom/foo_bar@none");
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

        let timestamp = UnixTimestamp::from_secs(0);
        let statsd_metrics = Bucket::parse_all(text.as_bytes(), timestamp)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let json_metrics: Vec<Bucket> = serde_json::from_str(json).unwrap();

        assert_eq!(statsd_metrics, json_metrics);
    }

    #[test]
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
        insta::assert_debug_snapshot!(buckets, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1615889440),
                width: 10,
                name: "endpoint.response_time",
                value: Distribution(
                    [
                        36.0,
                        49.0,
                        57.0,
                        68.0,
                    ],
                ),
                tags: {
                    "route": "user_index",
                },
            },
        ]
        "###);
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
