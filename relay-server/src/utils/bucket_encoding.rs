use std::io;

use relay_dynamic_config::{BucketEncoding, GlobalConfig};
use relay_metrics::{Bucket, BucketValue, FiniteF64, MetricNamespace, SetView};
use serde::Serialize;

static BASE64: data_encoding::Encoding = data_encoding::BASE64;

pub struct BucketEncoder<'a> {
    global_config: &'a GlobalConfig,
    buffer: String,
}

impl<'a> BucketEncoder<'a> {
    /// Creates a new bucket encoder with the provided config.
    pub fn new(global_config: &'a GlobalConfig) -> Self {
        Self {
            global_config,
            buffer: String::new(),
        }
    }

    /// Prepares the bucket before encoding.
    ///
    /// Returns the namespace extracted from the bucket.
    ///
    /// Some encodings need the bucket to be sorted or otherwise modified,
    /// afterwards the bucket can be split into multiple smaller views
    /// and encoded one by one.
    pub fn prepare(&self, bucket: &mut Bucket) -> MetricNamespace {
        let namespace = bucket.name.namespace();

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
        &mut self,
        namespace: MetricNamespace,
        dist: &'data [FiniteF64],
    ) -> io::Result<ArrayEncoding<'_, &'data [FiniteF64]>> {
        let enc = self.global_config.options.metric_bucket_dist_encodings;
        let enc = enc.for_namespace(namespace);
        self.do_encode(enc, dist)
    }

    /// Encodes a set.
    pub fn encode_set<'data>(
        &mut self,
        namespace: MetricNamespace,
        set: SetView<'data>,
    ) -> io::Result<ArrayEncoding<'_, SetView<'data>>> {
        let enc = self.global_config.options.metric_bucket_set_encodings;
        let enc = enc.for_namespace(namespace);
        self.do_encode(enc, set)
    }

    fn do_encode<T: Encodable>(
        &mut self,
        enc: BucketEncoding,
        data: T,
    ) -> io::Result<ArrayEncoding<'_, T>> {
        // If the buffer is not cleared before encoding more data,
        // the new data will just be appended to the end.
        self.buffer.clear();

        match enc {
            BucketEncoding::Legacy => Ok(ArrayEncoding::Legacy(data)),
            BucketEncoding::Array => {
                Ok(ArrayEncoding::Dynamic(DynamicArrayEncoding::Array { data }))
            }
            BucketEncoding::Base64 => base64(data, &mut self.buffer),
            BucketEncoding::Zstd => zstd(data, &mut self.buffer),
        }
    }
}

/// Dynamic array encoding intended for distribution and set metric buckets.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum ArrayEncoding<'a, T> {
    /// The original, legacy, encoding.
    ///
    /// Encodes all values as an array of numbers.
    Legacy(T),
    /// Dynamic encoding supporting multiple formats.
    ///
    /// Adds metadata and adds support for multiple different encodings.
    Dynamic(DynamicArrayEncoding<'a, T>),
}

impl<'a, T> ArrayEncoding<'a, T> {
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
pub enum DynamicArrayEncoding<'a, T> {
    /// Array encoding.
    ///
    /// Encodes all items as an array.
    Array { data: T },
    /// Base64 (with padding) encoding.
    ///
    /// Converts all items to little endian byte sequences
    /// and Base64 encodes the raw little endian bytes.
    Base64 { data: &'a str },
    /// Zstd encoding.
    ///
    /// Converts all items to little endian byte sequences,
    /// compresses the data using zstd and then encodes the result
    /// using Base64 (with padding).
    ///
    /// Items may be sorted to achieve better compression results.
    Zstd { data: &'a str },
}

impl<'a, T> DynamicArrayEncoding<'a, T> {
    /// Returns the serialized format name.
    pub fn format(&self) -> &'static str {
        match self {
            DynamicArrayEncoding::Array { .. } => "array",
            DynamicArrayEncoding::Base64 { .. } => "base64",
            DynamicArrayEncoding::Zstd { .. } => "zstd",
        }
    }
}

fn base64<T: Encodable>(data: T, buffer: &mut String) -> io::Result<ArrayEncoding<T>> {
    let mut writer = EncoderWriteAdapter(BASE64.new_encoder(buffer));
    data.write_to(&mut writer)?;
    drop(writer);

    Ok(ArrayEncoding::Dynamic(DynamicArrayEncoding::Base64 {
        data: buffer,
    }))
}

fn zstd<T: Encodable>(data: T, buffer: &mut String) -> io::Result<ArrayEncoding<T>> {
    let mut writer = zstd::Encoder::new(
        EncoderWriteAdapter(BASE64.new_encoder(buffer)),
        zstd::DEFAULT_COMPRESSION_LEVEL,
    )?;

    data.write_to(&mut writer)?;

    writer.finish()?;

    Ok(ArrayEncoding::Dynamic(DynamicArrayEncoding::Zstd {
        data: buffer,
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
