use std::collections::BTreeMap;
use std::fmt::{self, Write};
use std::iter::FusedIterator;

use serde::{Deserialize, Serialize};

pub use relay_common::UnixTimestamp;

/// Time duration units used in [`MetricUnit::Duration`].
///
/// Defaults to `ms`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurationPrecision {
    /// Nanosecond (`"ns"`).
    NanoSecond,
    /// Millisecond (`"ms"`).
    MilliSecond,
    /// Full second (`"s"`).
    Second,
}

impl Default for DurationPrecision {
    fn default() -> Self {
        Self::MilliSecond
    }
}

impl fmt::Display for DurationPrecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NanoSecond => f.write_str("ns"),
            Self::MilliSecond => f.write_str("ms"),
            Self::Second => f.write_str("s"),
        }
    }
}

/// The [unit](Metric::unit) of measurement of a metric [value](Metric::value).
///
/// Units augment metric values by giving them a magnitude and semantics. There are certain types of
/// units that are subdivided in their precision, such as the [`DurationPrecision`] for time
/// measurements.
///
/// Units and their precisions are uniquely represented by a string identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricUnit {
    /// A time duration, defaulting to milliseconds (`"ms"`).
    Duration(DurationPrecision),
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
            MetricUnit::Duration(precision) => precision.fmt(f),
            MetricUnit::None => f.write_str(""),
        }
    }
}

impl std::str::FromStr for MetricUnit {
    type Err = ParseMetricError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "ns" => Self::Duration(DurationPrecision::NanoSecond),
            "ms" => Self::Duration(DurationPrecision::MilliSecond),
            "s" => Self::Duration(DurationPrecision::Second),
            "" | "unit" | "none" => Self::None,
            _ => return Err(ParseMetricError(())),
        })
    }
}

relay_common::impl_str_serde!(MetricUnit, "a metric unit string");

/// The [value](Metric::value) of a metric.
///
/// [Distributions](MetricType::Distribution) and [counters](MetricType::Counter) require numeric values
/// which can either be integral or floating point. In contrast, [sets](MetricType::Set) and
/// [gauges](MetricType::Gauge) can store any unique value including custom strings.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum MetricValue {
    /// A signed integral value.
    Integer(i64),
    /// A signed floating point value.
    Float(f64),
    /// A custom string value.
    ///
    /// This value cannot be used in numeric metrics.
    Custom(String),
    // TODO: Uuid(Uuid),
}

impl fmt::Display for MetricValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricValue::Float(float) => float.fmt(f),
            MetricValue::Integer(int) => int.fmt(f),
            MetricValue::Custom(string) => string.fmt(f),
        }
    }
}

impl std::str::FromStr for MetricValue {
    type Err = ParseMetricError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(if let Ok(int) = s.parse() {
            Self::Integer(int)
        } else if let Ok(float) = s.parse() {
            Self::Float(float)
        } else {
            Self::Custom(s.to_owned())
        })
    }
}

/// The [type](Metric::ty) of a metric, determining its aggregation and evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricType {
    /// Counts instances of an event.
    ///
    /// Counters can be incremented and decremented. The default operation is to increment a counter
    /// by `1`, although increments by larger values are equally possible.
    ///
    /// This metric requires numeric values.
    Counter,
    /// Builds a statistical distribution over values reported.
    ///
    /// Based on individual reported values, distributions allow to query the maximum, minimum, or
    /// average of the reported values, as well as statistical quantiles. With an increasing number
    /// of values in the distribution, its accuracy becomes approximate.
    Distribution,
    /// Counts the number of unique reported values.
    ///
    /// Sets allow sending arbitrary discrete values and store the deduplicated count. With an
    /// increasing number of unique values in the set, its accuracy becomes approximate. It is not
    /// possible to query individual values from a set.
    Set,
    /// Stores absolute snapshots of values.
    ///
    /// Contrary to [counters](Self::Counter), which allow relative changes, gauges always store the
    /// last absolute value submitted.
    Gauge,
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricType::Counter => f.write_str("c"),
            MetricType::Distribution => f.write_str("d"),
            MetricType::Set => f.write_str("s"),
            MetricType::Gauge => f.write_str("g"),
        }
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
/// Metric names can consist of ASCII alphanumerics, underscores and periods.
fn is_valid_name(name: &str) -> bool {
    name.as_bytes()
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_'))
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

/// Parses the `name[@unit]:value` part of a metric string.
///
/// Returns [`MetricUnit::None`] if no unit is specified. Returns `None` if any of the components is
/// invalid.
fn parse_name_unit_value(string: &str) -> Option<(String, MetricUnit, MetricValue)> {
    let mut components = string.splitn(2, ':');
    let (name, unit) = components.next().and_then(parse_name_unit)?;
    let value = components.next().and_then(|s| s.parse().ok())?;
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
        let value = name_value.next().unwrap_or_default();
        map.insert(name.to_owned(), value.to_owned());
    }

    Some(map)
}

/// Parses a UNIX timestamp from the given string.
///
/// The timestamp can be represented as floating point number, in which case it is truncated.
fn parse_timestamp(string: &str) -> Option<UnixTimestamp> {
    if let Ok(int) = string.parse() {
        Some(UnixTimestamp::from_secs(int))
    } else if let Ok(float) = string.parse::<f64>() {
        if float < 0f64 {
            None
        } else {
            Some(UnixTimestamp::from_secs(float.trunc() as u64))
        }
    } else {
        None
    }
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
/// <name>[@unit]:<value>|<type>|'<timestamp>|#<tag_key>:<tag_value>,<tag>
/// ```
///
/// See the field documentation on this struct for more information on the components. An example
/// submission looks like this:
///
/// ```text
/// endpoint.response_time@ms:57|d|'1615889449|#route:user_index
/// endpoint.hits:1|c|'1615889449|#route:user_index
/// ```
///
/// To parse a submission payload, use [`Metric::parse_all`].
///
/// # JSON representation
///
/// In addition to the submission protocol, metrics can be represented as structured data in JSON.
/// Field values are the same with a single exception: The timestamp is required in JSON notation.
///
/// ```json
/// {
///   "name": "endpoint.response_time",
///   "unit": "ms",
///   "value": 57,
///   "type": "d",
///   "timestamp": 1615889449,
///   "tags": {
///     "route": "user_index"
///   }
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Metric {
    /// The name of the metric without its unit.
    ///
    /// Metric names can consist of ASCII alphanumerics, underscores and periods.
    pub name: String,
    /// The unit of the metric value.
    ///
    /// Units augment metric values by giving them a magnitude and semantics. There are certain
    /// types of units that are subdivided in their precision, such as the [`DurationPrecision`] for
    /// time measurements.
    ///
    /// The unit can be omitted and defaults to [`MetricUnit::None`].
    #[serde(default, skip_serializing_if = "MetricUnit::is_none")]
    pub unit: MetricUnit,
    /// The value of the metric.
    ///
    /// [Distributions](MetricType::Distribution) and [counters](MetricType::Counter) require numeric
    /// values which can either be integral or floating point. In contrast, [sets](MetricType::Set)
    /// and [gauges](MetricType::Gauge) can store any unique value including custom strings.
    pub value: MetricValue,
    /// The type of the metric, determining its aggregation and evaluation.
    #[serde(rename = "type")]
    pub ty: MetricType,
    /// The timestamp for this metric value.
    ///
    /// In the SDK protocol, timestamps are optional and preceded with a single quote `'`. Supply a
    /// default timestamp to [`Metric::parse`] or [`Metric::parse_all`] to insert a default
    /// timestamp.
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
    fn parse_str(string: &str, timestamp: UnixTimestamp) -> Option<Self> {
        let mut components = string.split('|');

        let (name, unit, value) = components.next().and_then(parse_name_unit_value)?;
        let ty = components.next().and_then(|s| s.parse().ok())?;

        let mut metric = Self {
            name,
            unit,
            value,
            ty,
            timestamp,
            tags: BTreeMap::new(),
        };

        for component in components {
            match component.chars().next() {
                Some('#') => metric.tags = parse_tags(component.get(1..)?)?,
                Some('\'') => metric.timestamp = parse_timestamp(component.get(1..)?)?,
                _ => (),
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
    /// let metric = Metric::parse(b"response_time@ms:57|d", UnixTimestamp::now())
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
    /// endpoint.response_time@ms:57|d
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

    /// Serializes the metric to the raw protocol.
    ///
    /// See the [`Metric`] for more information on the protocol.
    ///
    /// # Example
    ///
    /// ```
    /// use std::collections::BTreeMap;
    /// use relay_metrics::{Metric, MetricUnit, MetricValue, MetricType, UnixTimestamp};
    ///
    /// let metric = Metric {
    ///     name: "hits".to_owned(),
    ///     unit: MetricUnit::None,
    ///     value: MetricValue::Integer(1),
    ///     ty: MetricType::Counter,
    ///     timestamp: UnixTimestamp::from_secs(1615889449),
    ///     tags: BTreeMap::new(),
    /// };
    ///
    /// assert_eq!(metric.serialize(), "hits:1|c|'1615889449");
    /// ```
    pub fn serialize(&self) -> String {
        let mut string = self.name.clone();

        if self.unit != MetricUnit::None {
            write!(string, "@{}", self.unit).ok();
        }

        write!(string, ":{}|{}|'{}", self.value, self.ty, self.timestamp).ok();

        for (index, (key, value)) in self.tags.iter().enumerate() {
            match index {
                0 => write!(string, "|#{}", key).ok(),
                _ => write!(string, ",{}", key).ok(),
            };

            if !value.is_empty() {
                write!(string, ":{}", value).ok();
            }
        }

        string
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
            value: Integer(
                42,
            ),
            ty: Counter,
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
            value: Float(
                17.5,
            ),
            ty: Distribution,
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
        assert_eq!(metric.ty, MetricType::Distribution);
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
            value: Custom(
                "e2546e4c-ecd0-43ad-ae27-87960e57a658",
            ),
            ty: Set,
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
            value: Integer(
                42,
            ),
            ty: Gauge,
            timestamp: UnixTimestamp(4711),
            tags: {},
        }
        "###);
    }

    #[test]
    fn test_parse_unit() {
        let s = "foo@s:17.5|d";
        let timestamp = UnixTimestamp::from_secs(4711);
        let metric = Metric::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(metric.unit, MetricUnit::Duration(DurationPrecision::Second));
    }

    #[test]
    fn test_parse_timestamp() {
        let s = "foo:17.5|d|'1337";
        let timestamp = UnixTimestamp::from_secs(0xffff_ffff);
        let metric = Metric::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(metric.timestamp, UnixTimestamp::from_secs(1337));
    }

    #[test]
    fn test_parse_timestamp_float() {
        let s = "foo:17.5|d|'1337.666";
        let timestamp = UnixTimestamp::from_secs(0xffff_ffff);
        let metric = Metric::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(metric.timestamp, UnixTimestamp::from_secs(1337));
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
    fn test_serialize_basic() {
        let metric = Metric {
            name: "foo".to_owned(),
            unit: MetricUnit::None,
            value: MetricValue::Integer(42),
            ty: MetricType::Counter,
            timestamp: UnixTimestamp::from_secs(4711),
            tags: BTreeMap::new(),
        };

        assert_eq!(metric.serialize(), "foo:42|c|'4711");
    }

    #[test]
    fn test_serialize_unit() {
        let metric = Metric {
            name: "foo".to_owned(),
            unit: MetricUnit::Duration(DurationPrecision::Second),
            value: MetricValue::Integer(42),
            ty: MetricType::Counter,
            timestamp: UnixTimestamp::from_secs(4711),
            tags: BTreeMap::new(),
        };

        assert_eq!(metric.serialize(), "foo@s:42|c|'4711");
    }

    #[test]
    fn test_serialize_tags() {
        let mut tags = BTreeMap::new();
        tags.insert("empty".to_owned(), "".to_owned());
        tags.insert("full".to_owned(), "value".to_owned());

        let metric = Metric {
            name: "foo".to_owned(),
            unit: MetricUnit::None,
            value: MetricValue::Integer(42),
            ty: MetricType::Counter,
            timestamp: UnixTimestamp::from_secs(4711),
            tags,
        };

        assert_eq!(metric.serialize(), "foo:42|c|'4711|#empty,full:value");
    }

    #[test]
    fn test_roundtrip() {
        let s = "foo@s:17.5|d|'1337|#bar,foo:baz";
        let timestamp = UnixTimestamp::from_secs(0xffff_ffff);
        let metric = Metric::parse(s.as_bytes(), timestamp).unwrap();
        assert_eq!(metric.serialize(), s);
    }

    #[test]
    fn test_serde_json() {
        let json = r#"{
  "name": "foo",
  "unit": "s",
  "value": 42,
  "type": "c",
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
            value: Integer(
                42,
            ),
            ty: Counter,
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
            value: Integer(
                42,
            ),
            ty: Counter,
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
