use std::collections::BTreeMap;
use std::fmt;
use std::iter::FusedIterator;

use hash32::{FnvHasher, Hasher};
use serde::{Deserialize, Serialize};

pub use relay_common::UnixTimestamp;

/// Time duration units used in [`MetricUnit::Duration`].
///
/// Defaults to `millisecond`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum DurationUnit {
    /// Nanosecond (`"nanosecond"`), 10^-9 seconds.
    NanoSecond,
    /// Microsecond (`"microsecond"`), 10^-6 seconds.
    MicroSecond,
    /// Millisecond (`"millisecond"`), 10^-3 seconds.
    MilliSecond,
    /// Full second (`"second"`).
    Second,
    /// Minute (`"minute"`), 60 seconds.
    Minute,
    /// Hour (`"hour"`), 3600 seconds.
    Hour,
    /// Day (`"day"`), 86,400 seconds.
    Day,
    /// Week (`"week"`), 604,800 seconds.
    Week,
}

impl Default for DurationUnit {
    fn default() -> Self {
        Self::MilliSecond
    }
}

impl fmt::Display for DurationUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NanoSecond => f.write_str("nanosecond"),
            Self::MicroSecond => f.write_str("microsecond"),
            Self::MilliSecond => f.write_str("millisecond"),
            Self::Second => f.write_str("second"),
            Self::Minute => f.write_str("minute"),
            Self::Hour => f.write_str("hour"),
            Self::Day => f.write_str("day"),
            Self::Week => f.write_str("week"),
        }
    }
}

/// Size of information derived from bytes, used in [`MetricUnit::Information`].
///
/// Defaults to `byte`. See also [Units of
/// information](https://en.wikipedia.org/wiki/Units_of_information).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum InformationUnit {
    /// Bit (`"bit"`), corresponding to 1/8 of a byte.
    ///
    /// Note that there are computer systems with a different number of bits per byte.
    Bit,
    /// Byte (`"byte"`).
    Byte,
    /// Kilobyte (`"kilobyte"`), 10^3 bytes.
    KiloByte,
    /// Kibibyte (`"kibibyte"`), 2^10 bytes.
    KibiByte,
    /// Megabyte (`"megabyte"`), 10^6 bytes.
    MegaByte,
    /// Mebibyte (`"mebibyte"`), 2^20 bytes.
    MebiByte,
    /// Gigabyte (`"gigabyte"`), 10^9 bytes.
    GigaByte,
    /// Gibibyte (`"gibibyte"`), 2^30 bytes.
    GibiByte,
    /// Terabyte (`"terabyte"`), 10^12 bytes.
    TeraByte,
    /// Tebibyte (`"tebibyte"`), 2^40 bytes.
    TebiByte,
    /// Petabyte (`"petabyte"`), 10^15 bytes.
    PetaByte,
    /// Pebibyte (`"pebibyte"`), 2^50 bytes.
    PebiByte,
    /// Exabyte (`"exabyte"`), 10^18 bytes.
    ExaByte,
    /// Exbibyte (`"exbibyte"`), 2^60 bytes.
    ExbiByte,
}

impl Default for InformationUnit {
    fn default() -> Self {
        Self::Byte
    }
}

impl fmt::Display for InformationUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bit => f.write_str("bit"),
            Self::Byte => f.write_str("byte"),
            Self::KiloByte => f.write_str("kilobyte"),
            Self::KibiByte => f.write_str("kibibyte"),
            Self::MegaByte => f.write_str("megabyte"),
            Self::MebiByte => f.write_str("mebibyte"),
            Self::GigaByte => f.write_str("gigabyte"),
            Self::GibiByte => f.write_str("gibibyte"),
            Self::TeraByte => f.write_str("terabyte"),
            Self::TebiByte => f.write_str("tebibyte"),
            Self::PetaByte => f.write_str("petabyte"),
            Self::PebiByte => f.write_str("pebibyte"),
            Self::ExaByte => f.write_str("exabyte"),
            Self::ExbiByte => f.write_str("exbibyte"),
        }
    }
}

/// Units of fraction used in [`MetricUnit::Fraction`].
///
/// Defaults to `ratio`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum FractionUnit {
    /// Floating point fraction of `1`.
    Ratio,
    /// Ratio expressed as a fraction of `100`. `100%` equals a ratio of `1.0`.
    Percent,
}

impl Default for FractionUnit {
    fn default() -> Self {
        Self::Ratio
    }
}

impl fmt::Display for FractionUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ratio => f.write_str("ratio"),
            Self::Percent => f.write_str("percent"),
        }
    }
}

/// Custom user-defined units without builtin conversion.
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct CustomUnit([u8; 15]);

impl CustomUnit {
    /// Parses a `CustomUnit` from a string.
    pub fn parse(s: &str) -> Result<Self, ParseMetricError> {
        if s.len() > 15 || !s.is_ascii() {
            return Err(ParseMetricError(()));
        }

        let mut unit = Self(Default::default());
        unit.0.copy_from_slice(s.as_bytes());
        unit.0.make_ascii_lowercase();
        Ok(unit)
    }

    /// Returns the string representation of this unit.
    #[inline]
    pub fn as_str(&self) -> &str {
        // Safety: The string is already validated to be of length 32 and valid ASCII when
        // constructing `ProjectKey`.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl fmt::Debug for CustomUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl fmt::Display for CustomUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl std::str::FromStr for CustomUnit {
    type Err = ParseMetricError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl std::ops::Deref for CustomUnit {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

/// The [unit](Metric::unit) of measurement of a metric [value](Metric::value).
///
/// Units augment metric values by giving them a magnitude and semantics. There are certain types of
/// units that are subdivided in their precision, such as the [`DurationUnit`] for time
/// measurements.
///
/// Units and their precisions are uniquely represented by a string identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum MetricUnit {
    /// A time duration, defaulting to `"millisecond"`.
    Duration(DurationUnit),
    /// Size of information derived from bytes, defaulting to `"byte"`.
    Information(InformationUnit),
    /// Fractions such as percentages, defaulting to `"ratio"`.
    Fraction(FractionUnit),
    /// user-defined units without builtin conversion or default.
    Custom(CustomUnit),
    /// Untyped value without a unit (`""`).
    None,
}

impl MetricUnit {
    /// Returns `true` if the metric_unit is [`None`].
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

impl Default for MetricUnit {
    fn default() -> Self {
        MetricUnit::None
    }
}

impl fmt::Display for MetricUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricUnit::Duration(u) => u.fmt(f),
            MetricUnit::Information(u) => u.fmt(f),
            MetricUnit::Fraction(u) => u.fmt(f),
            MetricUnit::Custom(u) => u.fmt(f),
            MetricUnit::None => f.write_str("none"),
        }
    }
}

impl std::str::FromStr for MetricUnit {
    type Err = ParseMetricError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "nanosecond" | "ns" => Self::Duration(DurationUnit::NanoSecond),
            "microsecond" => Self::Duration(DurationUnit::MicroSecond),
            "millisecond" | "ms" => Self::Duration(DurationUnit::MilliSecond),
            "second" | "s" => Self::Duration(DurationUnit::Second),
            "minute" => Self::Duration(DurationUnit::Minute),
            "hour" => Self::Duration(DurationUnit::Hour),
            "day" => Self::Duration(DurationUnit::Day),
            "week" => Self::Duration(DurationUnit::Week),

            "bit" => Self::Information(InformationUnit::Bit),
            "byte" => Self::Information(InformationUnit::Byte),
            "kilobyte" => Self::Information(InformationUnit::KiloByte),
            "kibibyte" => Self::Information(InformationUnit::KibiByte),
            "megabyte" => Self::Information(InformationUnit::MegaByte),
            "mebibyte" => Self::Information(InformationUnit::MebiByte),
            "gigabyte" => Self::Information(InformationUnit::GigaByte),
            "gibibyte" => Self::Information(InformationUnit::GibiByte),
            "terabyte" => Self::Information(InformationUnit::TeraByte),
            "tebibyte" => Self::Information(InformationUnit::TebiByte),
            "petabyte" => Self::Information(InformationUnit::PetaByte),
            "pebibyte" => Self::Information(InformationUnit::PebiByte),
            "exabyte" => Self::Information(InformationUnit::ExaByte),
            "exbibyte" => Self::Information(InformationUnit::ExbiByte),

            "ratio" => Self::Fraction(FractionUnit::Ratio),
            "percent" => Self::Fraction(FractionUnit::Percent),

            "" | "unit" | "none" => Self::None,
            _ => Self::Custom(CustomUnit::parse(s)?),
        })
    }
}

relay_common::impl_str_serde!(MetricUnit, "a metric unit string");

/// The [typed value](Metric::value) of a metric.
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type", content = "value")]
pub enum MetricValue {
    /// Counts instances of an event. See [`MetricType::Counter`].
    #[serde(rename = "c")]
    Counter(f64),
    /// Builds a statistical distribution over values reported. See [`MetricType::Distribution`].
    #[serde(rename = "d")]
    Distribution(f64),
    /// Counts the number of unique reported values. See [`MetricType::Set`].
    ///
    /// Set values can be specified as strings in the submission protocol. They are always hashed
    /// into a 32-bit value and the original value is dropped. If the submission protocol contains a
    /// 32-bit integer, it will be used directly, instead.
    #[serde(rename = "s")]
    Set(u32),
    /// Stores absolute snapshots of values. See [`MetricType::Gauge`].
    #[serde(rename = "g")]
    Gauge(f64),
}

impl MetricValue {
    /// Creates a [`MetricValue::Set`] from the given string.
    pub fn set_from_str(string: &str) -> Self {
        Self::Set(hash_set_value(string))
    }

    /// Creates a [`MetricValue::Set`] from any type that implements `Display`.
    pub fn set_from_display(display: impl fmt::Display) -> Self {
        Self::set_from_str(&display.to_string())
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
}

impl fmt::Display for MetricValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricValue::Counter(value) => value.fmt(f),
            MetricValue::Distribution(value) => value.fmt(f),
            MetricValue::Set(value) => value.fmt(f),
            MetricValue::Gauge(value) => value.fmt(f),
        }
    }
}

/// The type of a [`MetricValue`], determining its aggregation and evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum MetricType {
    /// Counts instances of an event.
    ///
    /// Counters can be incremented and decremented. The default operation is to increment a counter
    /// by `1`, although increments by larger values are equally possible.
    Counter,
    /// Builds a statistical distribution over values reported.
    ///
    /// Based on individual reported values, distributions allow to query the maximum, minimum, or
    /// average of the reported values, as well as statistical quantiles. With an increasing number
    /// of values in the distribution, its accuracy becomes approximate.
    Distribution,
    /// Counts the number of unique reported values.
    ///
    /// Sets allow sending arbitrary discrete values, including strings, and store the deduplicated
    /// count. With an increasing number of unique values in the set, its accuracy becomes
    /// approximate. It is not possible to query individual values from a set.
    Set,
    /// Stores absolute snapshots of values.
    ///
    /// In addition to plain [counters](Self::Counter), gauges store a snapshot of the maximum,
    /// minimum and sum of all values, as well as the last reported value.
    Gauge,
}

impl MetricType {
    /// Return the shortcode for this metric type.
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricType::Counter => "c",
            MetricType::Distribution => "d",
            MetricType::Set => "s",
            MetricType::Gauge => "g",
        }
    }
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for MetricType {
    type Err = ParseMetricError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "c" | "m" => Self::Counter,
            "h" | "d" | "ms" => Self::Distribution,
            "s" => Self::Set,
            "g" => Self::Gauge,
            _ => return Err(ParseMetricError(())),
        })
    }
}

relay_common::impl_str_serde!(MetricType, "a metric type string");

/// An error returned by [`Metric::parse`] and [`Metric::parse_all`].
#[derive(Clone, Copy, Debug)]
pub struct ParseMetricError(());

impl fmt::Display for ParseMetricError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to parse metric")
    }
}

/// Validates a metric name.
///
/// Metric names cannot be empty, must begin with a letter and can consist of ASCII alphanumerics,
/// underscores and periods.
fn is_valid_name(name: &str) -> bool {
    let mut iter = name.as_bytes().iter();
    if let Some(first_byte) = iter.next() {
        if first_byte.is_ascii_alphabetic() {
            return iter.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'/'));
        }
    }
    false
}

/// Validates an MRI of the form `<ty>:<ns>/<name>@<unit>`
///
/// Note that the format used in the statsd protocol is different: Metric names are not prefixed
/// with `<ty>:` as the type is somewhere else in the protocol.
pub(crate) fn is_valid_mri(name: &str) -> bool {
    let mut components = name.splitn(2, ':');
    if components.next().is_none() {
        return false;
    }

    if let Some(name_unit) = components.next() {
        parse_name_unit(name_unit).is_some()
    } else {
        false
    }
}

/// Validates a tag key.
///
/// Tag keys currently only need to not contain ASCII control characters. This might change.
pub(crate) fn is_valid_tag_key(tag_key: &str) -> bool {
    // iterating over bytes produces better asm, and we're only checking for ascii chars
    for &byte in tag_key.as_bytes() {
        if (byte as char).is_ascii_control() {
            return false;
        }
    }
    true
}

/// Validates a tag value.
///
/// Tag values are never entirely rejected, but invalid characters (ASCII control characters) are
/// stripped out.
pub(crate) fn validate_tag_value(tag_value: &mut String) {
    tag_value.retain(|c| !c.is_ascii_control());
}

/// Parses the `name[@unit]` part of a metric string.
///
/// Returns [`MetricUnit::None`] if no unit is specified. Returns `None` if the name or value are
/// invalid.
fn parse_name_unit(string: &str) -> Option<(String, MetricUnit)> {
    let mut components = string.split('@');
    let name = components.next()?;
    if !is_valid_name(name) {
        return None;
    }

    let unit = match components.next() {
        Some(s) => s.parse().ok()?,
        None => MetricUnit::default(),
    };

    Some((name.to_owned(), unit))
}

/// Hashes the given set value.
///
/// Sets only guarantee 32-bit accuracy, but arbitrary strings are allowed on the protocol. Upon
/// parsing, they are hashed and only used as hashes subsequently.
fn hash_set_value(string: &str) -> u32 {
    let mut hasher = FnvHasher::default();
    hasher.write(string.as_bytes());
    hasher.finish()
}

/// Parses a metric value given its type.
///
/// Returns `None` if the value is invalid for the given type.
fn parse_value(string: &str, ty: MetricType) -> Option<MetricValue> {
    Some(match ty {
        MetricType::Counter => MetricValue::Counter(string.parse().ok()?),
        MetricType::Distribution => MetricValue::Distribution(string.parse().ok()?),
        MetricType::Set => {
            MetricValue::Set(string.parse().unwrap_or_else(|_| hash_set_value(string)))
        }
        MetricType::Gauge => MetricValue::Gauge(string.parse().ok()?),
    })
}

/// Parses the `name[@unit]:value` part of a metric string.
///
/// Returns [`MetricUnit::None`] if no unit is specified. Returns `None` if any of the components is
/// invalid.
fn parse_name_unit_value(
    string: &str,
    ty: MetricType,
) -> Option<(String, MetricUnit, MetricValue)> {
    let mut components = string.splitn(2, ':');
    let (name, unit) = components.next().and_then(parse_name_unit)?;
    let value = components.next().and_then(|s| parse_value(s, ty))?;
    Some((name, unit, value))
}

/// Parses tags in the format `tag1,tag2:value`.
///
/// Tag values are optional. For tags with missing values, an empty `""` value is assumed.
fn parse_tags(string: &str) -> Option<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();

    for pair in string.split(',') {
        let mut name_value = pair.splitn(2, ':');

        let name = name_value.next()?;
        if !is_valid_tag_key(name) {
            continue;
        }

        let mut value = name_value.next().unwrap_or_default().to_owned();

        validate_tag_value(&mut value);

        map.insert(name.to_owned(), value);
    }

    Some(map)
}

/// A single metric value representing the payload sent from clients.
///
/// As opposed to bucketed metric aggregations, this single metrics always represent a single
/// submission and cannot store multiple values.
///
/// See the [crate documentation](crate) for general information on Metrics.
///
/// # Submission Protocol
///
/// ```text
/// <name>[@unit]:<value>|<type>|#<tag_key>:<tag_value>,<tag>
/// ```
///
/// See the field documentation on this struct for more information on the components. An example
/// submission looks like this:
///
/// ```text
/// endpoint.response_time@millisecond:57|d|#route:user_index
/// endpoint.hits:1|c|#route:user_index
/// ```
///
/// To parse a submission payload, use [`Metric::parse_all`].
///
/// # JSON Representation
///
/// In addition to the submission protocol, metrics can be represented as structured data in JSON.
/// In addition to the field values from the submission protocol, a timestamp is added to every
/// metric (see [crate documentation](crate)).
///
/// ```json
/// {
///   "name": "endpoint.response_time",
///   "unit": "millisecond",
///   "value": 57,
///   "type": "d",
///   "timestamp": 1615889449,
///   "tags": {
///     "route": "user_index"
///   }
/// }
/// ```
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
/// endpoint.users:e2546e4c-ecd0-43ad-ae27-87960e57a658|s
/// ```
///
/// The above submission is represented as:
///
/// ```json
/// {
///   "name": "endpoint.users",
///   "value": 4267882815,
///   "type": "s"
/// }
/// ```

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Metric {
    /// The name of the metric without its unit.
    ///
    /// Metric names cannot be empty, must start with a letter and can consist of ASCII
    /// alphanumerics, underscores and periods.
    pub name: String,
    /// The unit of the metric value.
    ///
    /// Units augment metric values by giving them a magnitude and semantics. There are certain
    /// types of units that are subdivided in their precision, such as the [`DurationUnit`] for time
    /// measurements.
    ///
    /// The unit can be omitted and defaults to [`MetricUnit::None`].
    #[serde(default, skip_serializing_if = "MetricUnit::is_none")]
    pub unit: MetricUnit,
    /// The value of the metric.
    ///
    /// [Distributions](MetricType::Distribution) and [counters](MetricType::Counter) require numeric
    /// values which can either be integral or floating point. In contrast, [sets](MetricType::Set)
    /// and [gauges](MetricType::Gauge) can store any unique value including custom strings.
    #[serde(flatten)]
    pub value: MetricValue,
    /// The timestamp for this metric value.
    ///
    /// If a timestamp is not supplied in the item header of the envelope, the
    /// default timestamp supplied to [`Metric::parse`] or [`Metric::parse_all`]
    /// is associated with the metric.
    pub timestamp: UnixTimestamp,
    /// A list of tags adding dimensions to the metric for filtering and aggregation.
    ///
    /// Tags are preceded with a hash `#` and specified in a comma (`,`) separated list. Each tag
    /// can either be a tag name, or a `name:value` combination. For tags with missing values, an
    /// empty `""` value is assumed.
    ///
    /// Tags are optional and can be omitted.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}

impl Metric {
    /// Creates a new metric using the MRI naming format.
    ///
    /// MRI is the metric resource identifier in the format `<type>:<ns>/<name>@<unit>`. This name
    /// ensures that just the name determines correct bucketing of metrics with name collisions.
    pub fn new_mri(
        namespace: impl fmt::Display,
        name: impl fmt::Display,
        unit: MetricUnit,
        value: MetricValue,
        timestamp: UnixTimestamp,
        tags: BTreeMap<String, String>,
    ) -> Self {
        Self {
            name: format!("{}:{}/{}@{}", value.ty(), namespace, name, unit),
            unit,
            value,
            timestamp,
            tags,
        }
    }

    fn parse_str(string: &str, timestamp: UnixTimestamp) -> Option<Self> {
        let mut components = string.split('|');

        let name_value_str = components.next()?;
        let ty = components.next().and_then(|s| s.parse().ok())?;
        let (name, unit, value) = parse_name_unit_value(name_value_str, ty)?;

        let mut metric = Self {
            name: format!("{}:{}", ty, name),
            unit,
            value,
            timestamp,
            tags: BTreeMap::new(),
        };

        for component in components {
            if let Some('#') = component.chars().next() {
                metric.tags = parse_tags(component.get(1..)?)?;
            }
        }

        Some(metric)
    }

    /// Parses a single metric value from the raw protocol.
    ///
    /// See the [`Metric`] for more information on the protocol.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_metrics::{Metric, UnixTimestamp};
    ///
    /// let metric = Metric::parse(b"response_time@millisecond:57|d", UnixTimestamp::now())
    ///     .expect("metric should parse");
    /// ```
    pub fn parse(slice: &[u8], timestamp: UnixTimestamp) -> Result<Self, ParseMetricError> {
        let string = std::str::from_utf8(slice).or(Err(ParseMetricError(())))?;
        Self::parse_str(string, timestamp).ok_or(ParseMetricError(()))
    }

    /// Parses a set of metric values from the raw protocol.
    ///
    /// Returns a metric result for each line in `slice`, ignoring empty lines. Both UNIX newlines
    /// (`\n`) and Windows newlines (`\r\n`) are supported.
    ///
    /// It is possible to continue consuming the iterator after `Err` is yielded.
    ///
    /// See the [`Metric`] for more information on the protocol.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_metrics::{Metric, UnixTimestamp};
    ///
    /// let data = br#"
    /// endpoint.response_time@millisecond:57|d
    /// endpoint.hits:1|c
    /// "#;
    ///
    /// for metric_result in Metric::parse_all(data, UnixTimestamp::now()) {
    ///     let metric = metric_result.expect("metric should parse");
    ///     println!("Metric {}: {}", metric.name, metric.value);
    /// }
    /// ```
    pub fn parse_all(slice: &[u8], timestamp: UnixTimestamp) -> ParseMetrics<'_> {
        ParseMetrics { slice, timestamp }
    }
}

/// Iterator over parsed metrics returned from [`Metric::parse_all`].
#[derive(Clone, Debug)]
pub struct ParseMetrics<'a> {
    slice: &'a [u8],
    timestamp: UnixTimestamp,
}

impl Default for ParseMetrics<'_> {
    fn default() -> Self {
        Self {
            slice: &[],
            // The timestamp will never be returned.
            timestamp: UnixTimestamp::from_secs(4711),
        }
    }
}

impl Iterator for ParseMetrics<'_> {
    type Item = Result<Metric, ParseMetricError>;

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
                return Some(Metric::parse_str(string, self.timestamp).ok_or(ParseMetricError(())));
            }
        }
    }
}

impl FusedIterator for ParseMetrics<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sizeof_unit() {
        assert_eq!(std::mem::size_of::<MetricUnit>(), 16);
        assert_eq!(std::mem::align_of::<MetricUnit>(), 1);
    }

    #[test]
    fn test_parse_garbage() {
        let s = "x23-408j17z4232@#34d\nc3456y7^😎";
        let timestamp = UnixTimestamp::from_secs(4711);
        let result = Metric::parse(s.as_bytes(), timestamp);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_counter() {
        let s = "foo:42|c";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Metric::parse(s.as_bytes(), timestamp).unwrap();
        insta::assert_debug_snapshot!(metric, @r###"
        Metric {
            name: "foo",
            unit: None,
            value: Counter(
                42.0,
            ),
            timestamp: UnixTimestamp(4711),
            tags: {},
        }
        "###);
    }

    #[test]
    fn test_parse_distribution() {
        let s = "foo:17.5|d";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Metric::parse(s.as_bytes(), timestamp).unwrap();
        insta::assert_debug_snapshot!(metric, @r###"
        Metric {
            name: "foo",
            unit: None,
            value: Distribution(
                17.5,
            ),
            timestamp: UnixTimestamp(4711),
            tags: {},
        }
        "###);
    }

    #[test]
    fn test_parse_histogram() {
        let s = "foo:17.5|h"; // common alias for distribution
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Metric::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(metric.value, MetricValue::Distribution(17.5));
    }

    #[test]
    fn test_parse_set() {
        let s = "foo:e2546e4c-ecd0-43ad-ae27-87960e57a658|s";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Metric::parse(s.as_bytes(), timestamp).unwrap();
        insta::assert_debug_snapshot!(metric, @r###"
        Metric {
            name: "foo",
            unit: None,
            value: Set(
                4267882815,
            ),
            timestamp: UnixTimestamp(4711),
            tags: {},
        }
        "###);
    }

    #[test]
    fn test_parse_gauge() {
        let s = "foo:42|g";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Metric::parse(s.as_bytes(), timestamp).unwrap();
        insta::assert_debug_snapshot!(metric, @r###"
        Metric {
            name: "foo",
            unit: None,
            value: Gauge(
                42.0,
            ),
            timestamp: UnixTimestamp(4711),
            tags: {},
        }
        "###);
    }

    #[test]
    fn test_parse_unit() {
        let s = "foo@second:17.5|d";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Metric::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(metric.unit, MetricUnit::Duration(DurationUnit::Second));
    }

    #[test]
    fn test_parse_unit_regression() {
        let s = "foo@s:17.5|d";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Metric::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(metric.unit, MetricUnit::Duration(DurationUnit::Second));
    }

    #[test]
    fn test_parse_tags() {
        let s = "foo:17.5|d|#foo,bar:baz";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Metric::parse(s.as_bytes(), timestamp).unwrap();
        insta::assert_debug_snapshot!(metric.tags, @r###"
        {
            "bar": "baz",
            "foo": "",
        }
        "###);
    }

    #[test]
    fn test_parse_invalid_name() {
        let s = "foo#bar:42|c";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Metric::parse(s.as_bytes(), timestamp);
        assert!(metric.is_err());
    }

    #[test]
    fn test_parse_empty_name() {
        let s = ":42|c";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Metric::parse(s.as_bytes(), timestamp);
        assert!(metric.is_err());
    }

    #[test]
    fn test_parse_invalid_name_with_leading_digit() {
        let s = "64bit:42|c";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Metric::parse(s.as_bytes(), timestamp);
        assert!(metric.is_err());
    }

    #[test]
    fn test_serde_json() {
        let json = r#"{
  "name": "foo",
  "unit": "second",
  "type": "c",
  "value": 42.0,
  "timestamp": 4711,
  "tags": {
    "empty": "",
    "full": "value"
  }
}"#;

        let metric = serde_json::from_str::<Metric>(json).unwrap();
        insta::assert_debug_snapshot!(metric, @r###"
        Metric {
            name: "foo",
            unit: Duration(
                Second,
            ),
            value: Counter(
                42.0,
            ),
            timestamp: UnixTimestamp(4711),
            tags: {
                "empty": "",
                "full": "value",
            },
        }
        "###);

        let string = serde_json::to_string_pretty(&metric).unwrap();
        assert_eq!(string, json);
    }

    #[test]
    fn test_serde_json_defaults() {
        // NB: timestamp is required in JSON as opposed to the text representation
        let json = r#"{
            "name": "foo",
            "value": 42,
            "type": "c",
            "timestamp": 4711
        }"#;

        let metric = serde_json::from_str::<Metric>(json).unwrap();
        insta::assert_debug_snapshot!(metric, @r###"
        Metric {
            name: "foo",
            unit: None,
            value: Counter(
                42.0,
            ),
            timestamp: UnixTimestamp(4711),
            tags: {},
        }
        "###);
    }

    #[test]
    fn test_parse_all() {
        let s = "foo:42|c\nbar:17|c";
        let timestamp = UnixTimestamp::from_secs(4711);

        let metrics: Vec<Metric> = Metric::parse_all(s.as_bytes(), timestamp)
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(metrics.len(), 2);
    }

    #[test]
    fn test_parse_all_crlf() {
        let s = "foo:42|c\r\nbar:17|c";
        let timestamp = UnixTimestamp::from_secs(4711);

        let metrics: Vec<Metric> = Metric::parse_all(s.as_bytes(), timestamp)
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(metrics.len(), 2);
    }

    #[test]
    fn test_parse_all_empty_lines() {
        let s = "foo:42|c\n\n\nbar:17|c";
        let timestamp = UnixTimestamp::from_secs(4711);

        let metric_count = Metric::parse_all(s.as_bytes(), timestamp).count();
        assert_eq!(metric_count, 2);
    }

    #[test]
    fn test_parse_all_trailing() {
        let s = "foo:42|c\nbar:17|c\n";
        let timestamp = UnixTimestamp::from_secs(4711);

        let metric_count = Metric::parse_all(s.as_bytes(), timestamp).count();
        assert_eq!(metric_count, 2);
    }
}
