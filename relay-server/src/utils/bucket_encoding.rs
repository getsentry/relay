use std::io;

use relay_dynamic_config::{BucketEncoding, GlobalConfig};
use relay_metrics::{
    Bucket, BucketValue, FiniteF64, MetricNamespace, MetricResourceIdentifier, SetView,
};
use serde::Serialize;

static BASE64: data_encoding::Encoding = data_encoding::BASE64;

pub struct BucketEncoder<'a> {
    global_config: &'a GlobalConfig,
}

impl<'a> BucketEncoder<'a> {
    /// Creates a new bucket encoder with the provided config.
    pub fn new(global_config: &'a GlobalConfig) -> Self {
        Self { global_config }
    }

    /// Prepares the bucket before encoding.
    ///
    /// Returns the namespace extracted from the bucket.
    ///
    /// Some encodings need the bucket to be sorted or otherwise modified,
    /// afterwards the bucket can be split into multiple smaller views
    /// and encoded one by one.
    pub fn prepare(&self, bucket: &mut Bucket) -> MetricNamespace {
        let namespace = MetricResourceIdentifier::parse(&bucket.name)
            .map(|mri| mri.namespace)
            .unwrap_or(MetricNamespace::Unsupported);

        if let BucketValue::Distribution(ref mut distribution) = bucket.value {
            let enc = self.global_config.options.metric_bucket_dist_encodings;
            let enc = enc.for_namespace(namespace);

            if matches!(enc, BucketEncoding::Zstd) {
                distribution.sort_unstable();
            }
        }

        namespace
    }

    /// Encodes a distribution.
    pub fn encode_distribution<'data>(
        &self,
        namespace: MetricNamespace,
        dist: &'data [FiniteF64],
    ) -> io::Result<ArrayEncoding<&'data [FiniteF64]>> {
        let enc = self.global_config.options.metric_bucket_dist_encodings;
        let enc = enc.for_namespace(namespace);
        Self::do_encode(enc, dist)
    }

    /// Encodes a set.
    pub fn encode_set<'data>(
        &self,
        namespace: MetricNamespace,
        set: SetView<'data>,
    ) -> io::Result<ArrayEncoding<SetView<'data>>> {
        let enc = self.global_config.options.metric_bucket_set_encodings;
        let enc = enc.for_namespace(namespace);
        Self::do_encode(enc, set)
    }

    fn do_encode<T: Encodable>(enc: BucketEncoding, data: T) -> io::Result<ArrayEncoding<T>> {
        match enc {
            BucketEncoding::Legacy => Ok(ArrayEncoding::Legacy(data)),
            BucketEncoding::Array => {
                Ok(ArrayEncoding::Dynamic(DynamicArrayEncoding::Array { data }))
            }
            BucketEncoding::Base64 => base64(data),
            BucketEncoding::Zstd => zstd(data),
        }
    }
}

/// Dynamic array encoding intended for distribution and set metric buckets.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum ArrayEncoding<T> {
    /// The original, legacy, encoding.
    ///
    /// Encodes all values as an array of numbers.
    Legacy(T),
    /// Dynamic encoding supporting multiple formats.
    ///
    /// Adds metadata and adds support for multiple different encodings.
    Dynamic(DynamicArrayEncoding<T>),
}

impl<T> ArrayEncoding<T> {
    /// Name of the encoding.
    ///
    /// Should only be used for debugging purposes.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Legacy(_) => "legacy",
            Self::Dynamic(dynamic) => dynamic.format(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "format", rename_all = "lowercase")]
pub enum DynamicArrayEncoding<T> {
    /// Array encoding.
    ///
    /// Encodes all items as an array.
    Array { data: T },
    /// Base64 (with padding) encoding.
    ///
    /// Converts all items to little endian byte sequences
    /// and Base64 encodes the raw little endian bytes.
    Base64 { data: String },
    /// Zstd encoding.
    ///
    /// Converts all items to little endian byte sequences,
    /// compresses the data using zstd and then encodes the result
    /// using Base64 (with padding).
    ///
    /// Items may be sorted to achieve better compression results.
    Zstd { data: String },
}

impl<T> DynamicArrayEncoding<T> {
    /// Returns the serialized format name.
    pub fn format(&self) -> &'static str {
        match self {
            DynamicArrayEncoding::Array { .. } => "array",
            DynamicArrayEncoding::Base64 { .. } => "base64",
            DynamicArrayEncoding::Zstd { .. } => "zstd",
        }
    }
}

fn base64<T: Encodable>(data: T) -> io::Result<ArrayEncoding<T>> {
    let mut encoded = String::new();

    let mut writer = EncoderWriteAdapter(BASE64.new_encoder(&mut encoded));
    data.write_to(&mut writer)?;
    drop(writer);

    Ok(ArrayEncoding::Dynamic(DynamicArrayEncoding::Base64 {
        data: encoded,
    }))
}

fn zstd<T: Encodable>(data: T) -> io::Result<ArrayEncoding<T>> {
    let mut encoded = String::new();
    let mut writer = zstd::Encoder::new(
        EncoderWriteAdapter(BASE64.new_encoder(&mut encoded)),
        zstd::DEFAULT_COMPRESSION_LEVEL,
    )?;

    data.write_to(&mut writer)?;

    writer.finish()?;

    Ok(ArrayEncoding::Dynamic(DynamicArrayEncoding::Zstd {
        data: encoded,
    }))
}

struct EncoderWriteAdapter<'a>(pub data_encoding::Encoder<'a>);

impl<'a> io::Write for EncoderWriteAdapter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.append(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

trait Encodable {
    fn write_to(&self, writer: impl io::Write) -> io::Result<()>;
}

impl Encodable for SetView<'_> {
    #[inline(always)]
    fn write_to(&self, mut writer: impl io::Write) -> io::Result<()> {
        for value in self.iter() {
            writer.write_all(&value.to_le_bytes())?;
        }
        Ok(())
    }
}

impl Encodable for &[FiniteF64] {
    #[inline(always)]
    fn write_to(&self, mut writer: impl io::Write) -> io::Result<()> {
        for value in self.iter() {
            writer.write_all(&value.to_f64().to_le_bytes())?;
        }
        Ok(())
    }
}
