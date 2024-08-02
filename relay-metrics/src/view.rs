use relay_common::time::UnixTimestamp;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::Serialize;

use crate::{
    aggregator, BucketMetadata, CounterType, DistributionType, GaugeValue, MetricName, SetType,
    SetValue,
};
use relay_base_schema::metrics::MetricType;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;

use crate::bucket::Bucket;
use crate::BucketValue;

/// The fraction of size passed to [`BucketsView::by_size()`] at which buckets will be split. A value of
/// `2` means that all buckets smaller than half of `metrics_max_batch_size` will be moved in their entirety,
/// and buckets larger will be split up.
const BUCKET_SPLIT_FACTOR: usize = 32;

/// The base size of a serialized bucket in bytes.
///
/// This is the size of a bucket's fixed fields in JSON format, excluding the value and tags.
const BUCKET_SIZE: usize = 50;

/// The average size of values when serialized.
const AVG_VALUE_SIZE: usize = 8;

/// An internal type representing an index into a slice of buckets.
///
/// Note: the meaning of fields depends on the context of the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Index {
    /// Index into the slice.
    slice: usize,
    /// Index into a bucket.
    bucket: usize,
}

/// A view into a slice of metric buckets.
///
/// The view can be used to iterate over a large slice
/// of metric data slicing even into the buckets themselves.
///
/// ```txt
///                    Full View
///  /---------------------------------------------\
/// [[C:1], [C:12], [D:0, 1, 2, 3, 5, 5], [S:42, 75]]
///  \--------------------------/  \---------------/
///            View 1                    View 2
/// ```
///
/// Iterating over a [`BucketsView`] yields [`BucketView`] items,
/// only the first and last elements may be partial buckets.
///
/// In the above example `View 1` has a partial bucket at the end and
/// `View 2` has a partial bucket in the beginning.
///
/// Using the above example, iterating over `View 1` yields the buckets:
/// `[C:1], [C:12], [D:0, 1, 2, 3]`.
#[derive(Clone, Copy)]
pub struct BucketsView<T> {
    /// The contained buckets.
    inner: T,
    /// Start index.
    ///
    /// - Slice index indicates bucket.
    /// - Bucket index indicates offset in the selected bucket.
    start: Index,
    /// End index.
    ///
    /// - Slice index indicates exclusive end.
    /// - Bucket index, indicates offset into the *next* bucket past the end.
    end: Index,
}

impl<T> BucketsView<T>
where
    T: AsRef<[Bucket]>,
{
    /// Creates a new buckets view containing all data from the slice.
    pub fn new(buckets: T) -> Self {
        let len = buckets.as_ref().len();
        Self {
            inner: buckets,
            start: Index {
                slice: 0,
                bucket: 0,
            },
            end: Index {
                slice: len,
                bucket: 0,
            },
        }
    }

    /// Returns the amount of partial or full buckets in the view.
    pub fn len(&self) -> usize {
        let mut len = self.end.slice - self.start.slice;
        if self.end.bucket != 0 {
            len += 1;
        }
        len
    }

    /// Returns whether the view contains any buckets.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the same bucket view as a bucket view over a slice.
    pub fn as_slice(&self) -> BucketsView<&[Bucket]> {
        BucketsView {
            inner: self.inner.as_ref(),
            start: self.start,
            end: self.end,
        }
    }

    /// Iterator over all buckets in the view.
    pub fn iter(&self) -> BucketsViewIter<'_> {
        BucketsViewIter::new(self.inner.as_ref(), self.start, self.end)
    }

    /// Iterator which slices the source view into segments with an approximate size of `size_in_bytes`.
    pub fn by_size(self, size_in_bytes: usize) -> BucketsViewBySizeIter<T> {
        BucketsViewBySizeIter::new(self.inner, self.start, self.end, size_in_bytes)
    }
}

impl<'a> From<&'a [Bucket]> for BucketsView<&'a [Bucket]> {
    fn from(value: &'a [Bucket]) -> Self {
        Self::new(value)
    }
}

impl<'a> From<&'a Vec<Bucket>> for BucketsView<&'a [Bucket]> {
    fn from(value: &'a Vec<Bucket>) -> Self {
        Self::new(value.as_slice())
    }
}

impl<T> fmt::Debug for BucketsView<T>
where
    T: AsRef<[Bucket]>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let contents = self.iter().collect::<Vec<_>>();
        f.debug_tuple("BucketsView").field(&contents).finish()
    }
}

impl<'a> IntoIterator for BucketsView<&'a [Bucket]> {
    type Item = BucketView<'a>;
    type IntoIter = BucketsViewIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BucketsViewIter::new(self.inner, self.start, self.end)
    }
}

impl<'a, T> IntoIterator for &'a BucketsView<T>
where
    T: AsRef<[Bucket]>,
{
    type Item = BucketView<'a>;
    type IntoIter = BucketsViewIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BucketsViewIter::new(self.inner.as_ref(), self.start, self.end)
    }
}

/// Iterator yielding all items contained in a [`BucketsView`].
///
/// First and/or last item may be partial buckets.
pub struct BucketsViewIter<'a> {
    /// Source slice of buckets.
    inner: &'a [Bucket],
    /// Current index.
    current: Index,
    /// End index.
    end: Index,
}

impl<'a> BucketsViewIter<'a> {
    /// Creates a new iterator.
    ///
    /// Start and end must be valid indices or iterator may end early.
    fn new(inner: &'a [Bucket], start: Index, end: Index) -> Self {
        Self {
            inner,
            end,
            current: start,
        }
    }
}

impl<'a> Iterator for BucketsViewIter<'a> {
    type Item = BucketView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // We reached the exact end, there is no sub-bucket index.
        if self.current.slice == self.end.slice && self.end.bucket == 0 {
            return None;
        }
        // We are way past, including sub-bucket offset.
        if self.current.slice > self.end.slice {
            return None;
        }

        // This doesn't overflow because the last bucket in the inner slice will always have a 0 bucket index.
        debug_assert!(
            self.current.slice < self.inner.len(),
            "invariant violated, iterator pointing past the slice"
        );
        let next = self.inner.get(self.current.slice)?;

        // Choose the bucket end, this will always be the full bucket except if it is the last.
        let end = match self.current.slice == self.end.slice {
            false => next.value.len(),
            true => self.end.bucket,
        };

        let next = BucketView::new(next).select(self.current.bucket..end);
        let Some(next) = next else {
            debug_assert!(false, "invariant violated, invalid bucket split");
            relay_log::error!("Internal invariant violated, invalid bucket split, dropping all remaining buckets.");
            return None;
        };

        // Even if the current Bucket was partial, the next one will be full,
        // except if it is the last one.
        self.current = Index {
            slice: self.current.slice + 1,
            bucket: 0,
        };

        Some(next)
    }
}

/// Iterator slicing a [`BucketsView`] into smaller views constrained by a given size in bytes.
///
// See [`estimate_size`] for how the size of a bucket is calculated.
pub struct BucketsViewBySizeIter<T> {
    /// Source slice.
    inner: T,
    /// Current position in the slice.
    current: Index,
    /// Terminal position.
    end: Index,
    /// Maximum size in bytes of each slice.
    max_size_bytes: usize,
}

impl<T> BucketsViewBySizeIter<T> {
    /// Creates a new iterator.
    ///
    /// Start and end must be valid indices or iterator may end early.
    fn new(inner: T, start: Index, end: Index, max_size_bytes: usize) -> Self {
        Self {
            inner,
            end,
            current: start,
            max_size_bytes,
        }
    }
}

impl<T> Iterator for BucketsViewBySizeIter<T>
where
    T: AsRef<[Bucket]>,
    T: Clone,
{
    type Item = BucketsView<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.current;

        let mut remaining_bytes = self.max_size_bytes;
        loop {
            // Make sure, we don't shoot past the end ...
            if (self.current.slice > self.end.slice)
                || (self.current.slice == self.end.slice && self.end.bucket == 0)
            {
                break;
            }

            let inner = self.inner.as_ref();
            // Select next potential bucket,
            // this should never overflow because `end` will never go past the slice and
            // we just validated that current is constrained by end.
            debug_assert!(
                self.current.slice < inner.len(),
                "invariant violated, iterator pointing past the slice"
            );
            let bucket = inner.get(self.current.slice)?;

            // Selection should never fail, because either we select the entire range,
            // or we previously already split the bucket, which means this range is good.
            let bucket = BucketView::new(bucket).select(self.current.bucket..bucket.value.len());
            let Some(bucket) = bucket else {
                debug_assert!(false, "internal invariant violated, invalid bucket split");
                relay_log::error!("Internal invariant violated, invalid bucket split, dropping all remaining buckets.");
                return None;
            };

            match split(
                &bucket,
                remaining_bytes,
                self.max_size_bytes / BUCKET_SPLIT_FACTOR,
            ) {
                SplitDecision::BucketFits(size) => {
                    remaining_bytes -= size;
                    self.current = Index {
                        slice: self.current.slice + 1,
                        bucket: 0,
                    };
                    continue;
                }
                SplitDecision::MoveToNextBatch => break,
                SplitDecision::Split(n) => {
                    self.current = Index {
                        slice: self.current.slice,
                        bucket: self.current.bucket + n,
                    };
                    break;
                }
            }
        }

        if start == self.current {
            // Either no progress could be made (not enough space to fit a bucket),
            // or we're done.
            return None;
        }

        // Current is the current for the next batch now,
        // which means, current is the end for this batch.
        Some(BucketsView {
            inner: self.inner.clone(),
            start,
            end: self.current,
        })
    }
}

impl<T> Serialize for BucketsView<T>
where
    T: AsRef<[Bucket]>,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_seq(Some(self.len()))?;

        for bucket in self.iter() {
            state.serialize_element(&bucket)?;
        }

        state.end()
    }
}

/// A view into a metrics bucket. Sometimes also called a partial bucket.
/// A view contains a subset of datapoints of the original bucket.
///
/// ```txt
///             Full Bucket
/// /-------------------------------\
/// [0, 1, 2, 3, 5, 5, 5, 10, 11, 11]
/// \----------------/\-------------/
///       View 1          View 2
/// ```
///
/// A view can be split again into multiple smaller views.
#[derive(Clone)]
pub struct BucketView<'a> {
    /// The source bucket.
    inner: &'a Bucket,
    /// Non-empty and valid range into the bucket.
    /// The full range is constrained by `0..bucket.value.len()`
    range: Range<usize>,
}

impl<'a> BucketView<'a> {
    /// Creates a new bucket view of a bucket.
    ///
    /// The resulting view contains the entire bucket.
    pub fn new(bucket: &'a Bucket) -> Self {
        Self {
            inner: bucket,
            range: 0..bucket.value.len(),
        }
    }

    /// Timestamp of the bucket.
    ///
    /// See also: [`Bucket::timestamp`]
    pub fn timestamp(&self) -> UnixTimestamp {
        self.inner.timestamp
    }

    /// Width of the bucket.
    ///
    /// See also: [`Bucket::width`]
    pub fn width(&self) -> u64 {
        self.inner.width
    }

    /// Name of the bucket.
    ///
    /// See also: [`Bucket::name`]
    pub fn name(&self) -> &'a MetricName {
        &self.inner.name
    }

    /// Value of the bucket view.
    pub fn value(&self) -> BucketViewValue<'a> {
        match &self.inner.value {
            BucketValue::Counter(c) => BucketViewValue::Counter(*c),
            BucketValue::Distribution(d) => BucketViewValue::Distribution(&d[self.range.clone()]),
            BucketValue::Set(s) => BucketViewValue::Set(SetView::new(s, self.range.clone())),
            BucketValue::Gauge(g) => BucketViewValue::Gauge(*g),
        }
    }

    /// Type of the value of the bucket view.
    pub fn ty(&self) -> MetricType {
        match &self.inner.value {
            BucketValue::Counter(_) => MetricType::Counter,
            BucketValue::Distribution(_) => MetricType::Distribution,
            BucketValue::Set(_) => MetricType::Set,
            BucketValue::Gauge(_) => MetricType::Gauge,
        }
    }

    /// Name of the bucket.
    ///
    /// See also: [`Bucket::tags`]
    pub fn tags(&self) -> &'a BTreeMap<String, String> {
        &self.inner.tags
    }

    /// Returns the value of the specified tag if it exists.
    ///
    /// See also: [`Bucket::tag()`]
    pub fn tag(&self, name: &str) -> Option<&'a str> {
        self.inner.tag(name)
    }

    /// Returns the metadata for this bucket.
    ///
    /// The aggregation process of metadata is inheritly lossy, which means
    /// some metadata, for example the amount of merges, can not be accurately split
    /// or divided over multiple bucket views.
    ///
    /// To compensate for this only a bucket view which contains the start of a bucket
    /// will yield this metadata, all other views created from the bucket return an
    /// identity value. Merging all metadata from non-overlapping bucket views must
    /// yield the same values as stored on the original bucket.
    ///
    /// This causes some problems when operations on partial buckets are fallible,
    /// for example transmitting two bucket views in separate http requests.
    /// To deal with this Relay needs to prevent the splitting of buckets in the first place,
    /// by never not creating too large buckets via aggregation in the first place.
    ///
    /// See also: [`Bucket::metadata`].
    pub fn metadata(&self) -> BucketMetadata {
        let merges = if self.range.start == 0 {
            self.inner.metadata.merges
        } else {
            0
        };

        BucketMetadata {
            merges,
            ..self.inner.metadata
        }
    }

    /// Number of raw datapoints in this view.
    ///
    /// See also: [`BucketValue::len()`]
    pub fn len(&self) -> usize {
        self.range.len()
    }

    /// Returns `true` if this bucket view contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Selects a sub-view of the current view.
    ///
    /// Returns `None` when:
    /// - the passed range is not contained in the current view.
    /// - trying to split a counter or gauge bucket.
    pub fn select(mut self, range: Range<usize>) -> Option<Self> {
        if range.start < self.range.start || range.end > self.range.end {
            return None;
        }

        // Make sure the bucket can be split, or the entire bucket range is passed.
        if !self.can_split() && range != (0..self.inner.value.len()) {
            return None;
        }

        self.range = range;
        Some(self)
    }

    /// Estimates the number of bytes needed to serialize the bucket without value.
    ///
    /// Note that this does not match the exact size of the serialized payload. Instead, the size is
    /// approximated through tags and a static overhead.
    fn estimated_base_size(&self) -> usize {
        BUCKET_SIZE + self.name().len() + aggregator::tags_cost(self.tags())
    }

    /// Estimates the number of bytes needed to serialize the bucket.
    ///
    /// Note that this does not match the exact size of the serialized payload. Instead, the size is
    /// approximated through the number of contained values, assuming an average size of serialized
    /// values.
    pub fn estimated_size(&self) -> usize {
        self.estimated_base_size() + self.len() * AVG_VALUE_SIZE
    }

    /// Calculates a split for this bucket if its estimated serialization size exceeds a threshold.
    ///
    /// There are three possible return values:
    ///  - `(Some, None)` if the bucket fits entirely into the size budget. There is no split.
    ///  - `(None, Some)` if the size budget cannot even hold the bucket name and tags. There is no
    ///    split, the entire bucket is moved.
    ///  - `(Some, Some)` if the bucket fits partially. Remaining values are moved into a new bucket
    ///    with all other information cloned.
    ///
    /// This is an approximate function. The bucket is not actually serialized, but rather its
    /// footprint is estimated through the number of data points contained. See
    /// [`estimated_size`](Self::estimated_size) for more information.
    pub fn split(self, size: usize, max_size: Option<usize>) -> (Option<Self>, Option<Self>) {
        match split(&self, size, max_size.unwrap_or(0) / BUCKET_SPLIT_FACTOR) {
            SplitDecision::BucketFits(_) => (Some(self), None),
            SplitDecision::MoveToNextBatch => (None, Some(self)),
            SplitDecision::Split(n) => {
                let Range { start, end } = self.range;
                let at = start + n;
                (self.clone().select(start..at), self.select(at..end))
            }
        }
    }

    /// Whether the bucket can be split into multiple.
    ///
    /// Only set and distribution buckets can be split.
    fn can_split(&self) -> bool {
        matches!(
            self.inner.value,
            BucketValue::Distribution(_) | BucketValue::Set(_)
        )
    }

    /// Returns `true` when this view contains the entire bucket.
    fn is_full_bucket(&self) -> bool {
        self.range.start == 0 && self.range.end == self.inner.value.len()
    }
}

impl<'a> fmt::Debug for BucketView<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BucketView")
            .field("timestamp", &self.inner.timestamp)
            .field("width", &self.inner.width)
            .field("name", &self.inner.name)
            .field("value", &self.value())
            .field("tags", &self.inner.tags)
            .finish()
    }
}

impl<'a> From<&'a Bucket> for BucketView<'a> {
    fn from(value: &'a Bucket) -> Self {
        BucketView::new(value)
    }
}

impl<'a> Serialize for BucketView<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let Bucket {
            timestamp,
            width,
            name,
            value: _,
            tags,
            metadata,
        } = self.inner;

        let len = match tags.is_empty() {
            true => 4,
            false => 5,
        };

        let mut state = serializer.serialize_map(Some(len))?;

        state.serialize_entry("timestamp", timestamp)?;
        state.serialize_entry("width", width)?;
        state.serialize_entry("name", name)?;

        if self.is_full_bucket() {
            self.inner
                .value
                .serialize(serde::__private::ser::FlatMapSerializer(&mut state))?;
        } else {
            self.value()
                .serialize(serde::__private::ser::FlatMapSerializer(&mut state))?;
        }

        if !tags.is_empty() {
            state.serialize_entry("tags", tags)?;
        }
        if !metadata.is_default() {
            state.serialize_entry("metadata", metadata)?;
        }

        state.end()
    }
}

/// A view into the datapoints of a [`BucketValue`].
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", content = "value")]
pub enum BucketViewValue<'a> {
    /// A counter metric.
    ///
    /// See: [`BucketValue::Counter`].
    #[serde(rename = "c")]
    Counter(CounterType),
    /// A distribution metric.
    ///
    /// See: [`BucketValue::Distribution`].
    #[serde(rename = "d")]
    Distribution(&'a [DistributionType]),
    /// A set metric.
    ///
    /// See: [`BucketValue::Set`].
    #[serde(rename = "s")]
    Set(SetView<'a>),
    /// A gauage metric.
    ///
    /// See: [`BucketValue::Gauge`].
    #[serde(rename = "g")]
    Gauge(GaugeValue),
}

impl<'a> From<&'a BucketValue> for BucketViewValue<'a> {
    fn from(value: &'a BucketValue) -> Self {
        match value {
            BucketValue::Counter(c) => BucketViewValue::Counter(*c),
            BucketValue::Distribution(d) => BucketViewValue::Distribution(d),
            BucketValue::Set(s) => BucketViewValue::Set(SetView::new(s, 0..s.len())),
            BucketValue::Gauge(g) => BucketViewValue::Gauge(*g),
        }
    }
}

/// A view into the datapoints of a set metric.
#[derive(Clone)]
pub struct SetView<'a> {
    source: &'a SetValue,
    range: Range<usize>,
}

impl<'a> SetView<'a> {
    fn new(source: &'a SetValue, range: Range<usize>) -> Self {
        Self { source, range }
    }

    /// Amount of datapoints contained within the set view.
    pub fn len(&self) -> usize {
        self.range.len()
    }

    /// Returns `true` if this set contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterator over all datapoints contained in this set metric.
    pub fn iter(&self) -> impl Iterator<Item = &SetType> {
        self.source
            .iter()
            .skip(self.range.start)
            .take(self.range.len())
    }
}

impl<'a> PartialEq for SetView<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<'a> fmt::Debug for SetView<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SetView")
            .field(&self.iter().collect::<Vec<_>>())
            .finish()
    }
}

impl<'a> Serialize for SetView<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_seq(Some(self.len()))?;

        for item in self.iter() {
            state.serialize_element(item)?;
        }

        state.end()
    }
}

/// Result of [`split`].
enum SplitDecision {
    /// Bucket fits within the current budget.
    ///
    /// Contains the size of the bucket to subtract from the budget.
    BucketFits(usize),
    /// Bucket does not fit within the current budget and cannot be split.
    MoveToNextBatch,
    /// The bucket should be split at the specified position.
    Split(usize),
}

/// Calculates a split for this bucket if its estimated serialization size exceeds a threshold.
///
/// There are three possible return values:
///  - `BucketFits(size)` if the bucket fits entirely into the budget and consumes `size` bytes.
///  - `MoveToNextBatch` if the size budget cannot even hold the bucket name and tags. There is no
///    split, the entire bucket is moved.
///  - `Split(n)` if the bucket fits partially, the bucket should be split after `n` elements.
///
/// This is an approximate function. The bucket is not actually serialized, but rather its
/// footprint is estimated through the number of data points contained. See
/// `estimate_size` for more information.
fn split(bucket: &BucketView<'_>, max_size: usize, min_split_size: usize) -> SplitDecision {
    // If there's enough space for the entire bucket, do not perform a split.
    let bucket_size = bucket.estimated_size();
    if max_size >= bucket_size {
        return SplitDecision::BucketFits(bucket_size);
    }

    if !bucket.can_split() {
        return SplitDecision::MoveToNextBatch;
    }

    // If the bucket key can't even fit into the remaining length, move the entire bucket into
    // the right-hand side.
    let own_size = bucket.estimated_base_size();
    if max_size < (own_size + AVG_VALUE_SIZE) {
        // split_at must not be zero
        return SplitDecision::MoveToNextBatch;
    }

    if bucket_size < min_split_size {
        return SplitDecision::MoveToNextBatch;
    }

    // Perform a split with the remaining space after adding the key. We assume an average
    // length of 8 bytes per value and compute the number of items fitting into the left side.
    let split_at = (max_size - own_size) / AVG_VALUE_SIZE;
    SplitDecision::Split(split_at)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use insta::assert_json_snapshot;

    use super::*;

    #[test]
    fn test_bucket_view_select_counter() {
        let bucket = Bucket::parse(b"b0:1|c", UnixTimestamp::from_secs(5000)).unwrap();

        let view = BucketView::new(&bucket).select(0..1).unwrap();
        assert_eq!(view.len(), 1);
        assert_eq!(
            serde_json::to_string(&view).unwrap(),
            serde_json::to_string(&bucket).unwrap()
        );
    }

    #[test]
    fn test_bucket_view_select_invalid_counter() {
        let bucket = Bucket::parse(b"b0:1|c", UnixTimestamp::from_secs(5000)).unwrap();

        assert!(BucketView::new(&bucket).select(0..0).is_none());
        assert!(BucketView::new(&bucket).select(0..2).is_none());
        assert!(BucketView::new(&bucket).select(1..1).is_none());
    }

    #[test]
    fn test_bucket_view_counter_metadata() {
        let bucket = Bucket::parse(b"b0:1|c", UnixTimestamp::from_secs(5000)).unwrap();
        assert_eq!(bucket.metadata, BucketView::new(&bucket).metadata());
    }

    #[test]
    fn test_bucket_view_select_distribution() {
        let bucket = Bucket::parse(b"b2:1:2:3:5:5|d", UnixTimestamp::from_secs(5000)).unwrap();

        let view = BucketView::new(&bucket).select(0..3).unwrap();
        assert_eq!(view.len(), 3);
        assert_eq!(
            view.value(),
            BucketViewValue::Distribution(&[1.into(), 2.into(), 3.into()])
        );
        let view = BucketView::new(&bucket).select(1..3).unwrap();
        assert_eq!(view.len(), 2);
        assert_eq!(
            view.value(),
            BucketViewValue::Distribution(&[2.into(), 3.into()])
        );
        let view = BucketView::new(&bucket).select(1..5).unwrap();
        assert_eq!(view.len(), 4);
        assert_eq!(
            view.value(),
            BucketViewValue::Distribution(&[2.into(), 3.into(), 5.into(), 5.into()])
        );
    }

    #[test]
    fn test_bucket_view_select_invalid_distribution() {
        let bucket = Bucket::parse(b"b2:1:2:3:5:5|d", UnixTimestamp::from_secs(5000)).unwrap();

        assert!(BucketView::new(&bucket).select(0..6).is_none());
        assert!(BucketView::new(&bucket).select(5..6).is_none());
        assert!(BucketView::new(&bucket).select(77..99).is_none());
    }

    #[test]
    fn test_bucket_view_distribution_metadata() {
        let bucket = Bucket::parse(b"b2:1:2:3:5:5|d", UnixTimestamp::from_secs(5000)).unwrap();
        assert_eq!(bucket.metadata, BucketView::new(&bucket).metadata());

        assert_eq!(
            BucketView::new(&bucket).select(0..3).unwrap().metadata(),
            bucket.metadata
        );

        let m = BucketView::new(&bucket).select(1..3).unwrap().metadata();
        assert_eq!(
            m,
            BucketMetadata {
                merges: 0,
                ..bucket.metadata
            }
        );
    }

    #[test]
    fn test_bucket_view_select_set() {
        let bucket = Bucket::parse(b"b3:42:75|s", UnixTimestamp::from_secs(5000)).unwrap();
        let s = [42, 75].into();

        let view = BucketView::new(&bucket).select(0..2).unwrap();
        assert_eq!(view.len(), 2);
        assert_eq!(view.value(), BucketViewValue::Set(SetView::new(&s, 0..2)));
        let view = BucketView::new(&bucket).select(1..2).unwrap();
        assert_eq!(view.len(), 1);
        assert_eq!(view.value(), BucketViewValue::Set(SetView::new(&s, 1..2)));
        let view = BucketView::new(&bucket).select(0..1).unwrap();
        assert_eq!(view.len(), 1);
        assert_eq!(view.value(), BucketViewValue::Set(SetView::new(&s, 0..1)));
    }

    #[test]
    fn test_bucket_view_select_invalid_set() {
        let bucket = Bucket::parse(b"b3:42:75|s", UnixTimestamp::from_secs(5000)).unwrap();

        assert!(BucketView::new(&bucket).select(0..3).is_none());
        assert!(BucketView::new(&bucket).select(2..5).is_none());
        assert!(BucketView::new(&bucket).select(77..99).is_none());
    }

    #[test]
    fn test_bucket_view_set_metadata() {
        let bucket = Bucket::parse(b"b2:1:2:3:5:5|s", UnixTimestamp::from_secs(5000)).unwrap();
        assert_eq!(bucket.metadata, BucketView::new(&bucket).metadata());

        assert_eq!(
            BucketView::new(&bucket).select(0..3).unwrap().metadata(),
            bucket.metadata
        );

        let m = BucketView::new(&bucket).select(1..3).unwrap().metadata();
        assert_eq!(
            m,
            BucketMetadata {
                merges: 0,
                ..bucket.metadata
            }
        );
    }

    #[test]
    fn test_bucket_view_select_gauge() {
        let bucket =
            Bucket::parse(b"b4:25:17:42:220:85|g", UnixTimestamp::from_secs(5000)).unwrap();

        let view = BucketView::new(&bucket).select(0..5).unwrap();
        assert_eq!(view.len(), 5);
        assert_eq!(
            view.value(),
            BucketViewValue::Gauge(GaugeValue {
                last: 25.into(),
                min: 17.into(),
                max: 42.into(),
                sum: 220.into(),
                count: 85
            })
        );
    }

    #[test]
    fn test_bucket_view_select_invalid_gauge() {
        let bucket =
            Bucket::parse(b"b4:25:17:42:220:85|g", UnixTimestamp::from_secs(5000)).unwrap();

        assert!(BucketView::new(&bucket).select(0..1).is_none());
        assert!(BucketView::new(&bucket).select(0..4).is_none());
        assert!(BucketView::new(&bucket).select(5..5).is_none());
        assert!(BucketView::new(&bucket).select(5..6).is_none());
    }

    #[test]
    fn test_bucket_view_gauge_metadata() {
        let bucket =
            Bucket::parse(b"b4:25:17:42:220:85|g", UnixTimestamp::from_secs(5000)).unwrap();
        assert_eq!(BucketView::new(&bucket).metadata(), bucket.metadata);
    }

    fn buckets<T>(s: &[u8]) -> T
    where
        T: FromIterator<Bucket>,
    {
        let timestamp = UnixTimestamp::from_secs(5000);
        Bucket::parse_all(s, timestamp)
            .collect::<Result<T, _>>()
            .unwrap()
    }

    #[test]
    fn test_buckets_view_empty() {
        let view = BucketsView::new(Vec::new());
        assert_eq!(view.len(), 0);
        assert!(view.is_empty());
        let partials = view.iter().collect::<Vec<_>>();
        assert!(partials.is_empty());
    }

    #[test]
    fn test_buckets_view_iter_full() {
        let buckets: Vec<_> = buckets(b"b0:1|c\nb1:12|c\nb2:1:2:3:5:5|d\nb3:42:75|s");

        let view = BucketsView::from(&buckets);
        assert_eq!(view.len(), 4);
        assert!(!view.is_empty());
        let partials = view.iter().collect::<Vec<_>>();
        assert_eq!(partials.len(), 4);
        assert_eq!(partials[0].name(), "c:custom/b0@none");
        assert_eq!(partials[0].len(), 1);
        assert_eq!(partials[1].name(), "c:custom/b1@none");
        assert_eq!(partials[1].len(), 1);
        assert_eq!(partials[2].name(), "d:custom/b2@none");
        assert_eq!(partials[2].len(), 5);
        assert_eq!(partials[3].name(), "s:custom/b3@none");
        assert_eq!(partials[3].len(), 2);
    }

    #[test]
    fn test_buckets_view_iter_partial_end() {
        let buckets: Vec<_> = buckets(b"b0:1|c\nb1:12|c\nb2:1:2:3:5:5|d\nb3:42:75|s");

        let mut view = BucketsView::new(&buckets);
        view.end.slice = 2;
        view.end.bucket = 3;
        assert_eq!(view.len(), 3);
        assert!(!view.is_empty());

        let partials = view.iter().collect::<Vec<_>>();
        assert_eq!(partials.len(), 3);
        assert_eq!(partials[0].name(), "c:custom/b0@none");
        assert_eq!(partials[0].len(), 1);
        assert_eq!(partials[1].name(), "c:custom/b1@none");
        assert_eq!(partials[1].len(), 1);
        assert_eq!(partials[2].name(), "d:custom/b2@none");
        assert_eq!(partials[2].len(), 3);
    }

    #[test]
    fn test_buckets_view_iter_partial_start() {
        let buckets: Vec<_> = buckets(b"b0:1|c\nb1:12|c\nb2:1:2:3:5:5|d\nb3:42:75|s");

        let mut view = BucketsView::new(buckets);
        view.start.slice = 2;
        view.start.bucket = 3;
        assert_eq!(view.len(), 2);
        assert!(!view.is_empty());

        let partials = view.iter().collect::<Vec<_>>();
        assert_eq!(partials.len(), 2);
        assert_eq!(partials[0].name(), "d:custom/b2@none");
        assert_eq!(partials[0].len(), 2);
        assert_eq!(partials[1].name(), "s:custom/b3@none");
        assert_eq!(partials[1].len(), 2);
    }

    #[test]
    fn test_buckets_view_iter_partial_start_and_end() {
        let buckets: Vec<_> = buckets(b"b0:1|c\nb1:12|c\nb2:1:2:3:5:5|d\nb3:42:75|s");

        let mut view = BucketsView::from(&buckets);
        view.start.slice = 2;
        view.start.bucket = 1;
        view.end.slice = 3;
        view.end.bucket = 1;
        assert_eq!(view.len(), 2);
        assert!(!view.is_empty());

        let partials = view.iter().collect::<Vec<_>>();
        assert_eq!(partials.len(), 2);
        assert_eq!(partials[0].name(), "d:custom/b2@none");
        assert_eq!(partials[0].len(), 4);
        assert_eq!(partials[1].name(), "s:custom/b3@none");
        assert_eq!(partials[1].len(), 1);
    }

    #[test]
    fn test_buckets_view_by_size_small() {
        let buckets: Vec<_> = buckets(b"b0:1|c\nb1:12|c\nb2:1:2:3:5:5|d\nb3:42:75|s");

        let view = BucketsView::from(&buckets);
        let partials = view
            .by_size(100)
            .map(|bv| {
                let len: usize = bv.iter().map(|b| b.len()).sum();
                let size: usize = bv.iter().map(|b| b.estimated_size()).sum();
                (len, size)
            })
            .collect::<Vec<_>>();

        assert_eq!(partials, vec![(1, 74), (1, 74), (4, 98), (1, 74), (2, 82),]);
    }

    #[test]
    fn test_buckets_view_by_size_small_as_arc() {
        let buckets: Arc<_> = buckets(b"b0:1|c\nb1:12|c\nb2:1:2:3:5:5|d\nb3:42:75|s");

        let view = BucketsView::new(buckets);
        let partials = view
            .by_size(100)
            .map(|bv| {
                let len: usize = bv.iter().map(|b| b.len()).sum();
                let size: usize = bv.iter().map(|b| b.estimated_size()).sum();
                (len, size)
            })
            .collect::<Vec<_>>();

        assert_eq!(partials, vec![(1, 74), (1, 74), (4, 98), (1, 74), (2, 82),]);
    }

    #[test]
    fn test_buckets_view_by_size_one_split() {
        let buckets: Vec<_> = buckets(b"b0:1|c\nb1:12|c\nb2:1:2:3:5:5|d\nb3:42:75|s");

        let view = BucketsView::from(&buckets);
        let partials = view
            .by_size(250)
            .map(|bv| {
                let len: usize = bv.iter().map(|b| b.len()).sum();
                let size: usize = bv.iter().map(|b| b.estimated_size()).sum();
                (len, size)
            })
            .collect::<Vec<_>>();

        assert_eq!(partials, vec![(6, 246), (3, 156)]);
    }

    #[test]
    fn test_buckets_view_by_size_no_split() {
        let buckets: Vec<_> = buckets(b"b0:1|c\nb1:12|c\nb2:1:2:3:5:5|d\nb3:42:75|s");

        let view = BucketsView::from(&buckets);
        let partials = view
            .by_size(500)
            .map(|bv| {
                let len: usize = bv.iter().map(|b| b.len()).sum();
                let size: usize = bv.iter().map(|b| b.estimated_size()).sum();
                (len, size)
            })
            .collect::<Vec<_>>();

        assert_eq!(partials, vec![(9, 336)]);
    }

    #[test]
    fn test_buckets_view_by_size_no_too_small_no_bucket_fits() {
        let buckets: Vec<_> = buckets(b"b0:1|c\nb1:12|c\nb2:1:2:3:5:5|d\nb3:42:75|s");

        let view = BucketsView::from(&buckets);
        let partials = view
            .by_size(50) // Too small, a bucket requires at least 74 bytes
            .count();

        assert_eq!(partials, 0);
    }

    #[test]
    fn test_buckets_view_by_size_do_not_split_gauge() {
        let buckets: Vec<_> = buckets(b"transactions/foo:25:17:42:220:85|g");

        let view = BucketsView::from(&buckets);
        // 100 is too small to fit the gauge, but it is big enough to fit half a gauage,
        // make sure the gauge does not actually get split.
        let partials = view.by_size(100).count();

        assert_eq!(partials, 0);
    }

    #[test]
    fn test_buckets_view_serialize_full() {
        let buckets: Vec<_> = buckets(b"b0:1|c\nb1:12|c|#foo,bar:baz\nb2:1:2:3:5:5|d|#foo,bar:baz b3:42:75|s\ntransactions/foo:25:17:42:220:85|g");

        assert_eq!(
            serde_json::to_string(&BucketsView::from(&buckets)).unwrap(),
            serde_json::to_string(&buckets).unwrap()
        );
    }

    #[test]
    fn test_buckets_view_serialize_partial() {
        let buckets: Arc<[_]> = buckets(
            b"b1:12|c|#foo,bar:baz\nb2:1:2:3:5:5|d|#foo,bar:baz\nb3:42:75|s\nb4:25:17:42:220:85|g",
        );

        let view = BucketsView::new(buckets);
        // This creates 4 separate views, spanning 1-2, 2-3, 3, 4.
        // 4 is too big to fit into a view together with the remainder of 3.
        let partials = view.by_size(178).collect::<Vec<_>>();

        assert_json_snapshot!(partials);
    }

    #[test]
    fn test_split_repeatedly() {
        let bucket = Bucket::parse(b"b2:1:2:3:5:5|d", UnixTimestamp::from_secs(5000)).unwrap();
        let view = BucketView::new(&bucket);

        // construct this so that we can take 2 values per split and result in 3 parts.
        let split_size = view.estimated_base_size() + 2 * AVG_VALUE_SIZE;

        let (first, rest) = view.split(split_size, None);
        let (second, rest) = rest.unwrap().split(split_size, None);
        let (third, rest) = rest.unwrap().split(split_size, None);

        assert_eq!(first.unwrap().range, 0..2);
        assert_eq!(second.unwrap().range, 2..4);
        assert_eq!(third.unwrap().range, 4..5);
        assert!(rest.is_none());
    }
}
