use std::io::{self, Write};

use relay_metrics::{FiniteF64, SetView};
use serde::Serialize;

static BASE64: data_encoding::Encoding = data_encoding::BASE64;

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
    pub fn name(&self) -> &'static str {
        match self {
            Self::Legacy(_) => "legacy",
            Self::Dynamic(dynamic) => dynamic.format(),
        }
    }

    pub fn legacy(data: T) -> Self {
        Self::Legacy(data)
    }

    pub fn array(data: T) -> Self {
        Self::Dynamic(DynamicArrayEncoding::Array { data })
    }

    pub fn base64(data: SetView<'_>) -> Self {
        let mut encoded = String::new();

        let mut encoder = BASE64.new_encoder(&mut encoded);
        for item in data.iter() {
            encoder.append(&item.to_le_bytes());
        }
        encoder.finalize();

        Self::Dynamic(DynamicArrayEncoding::Base64 { data: encoded })
    }

    pub fn zstd(data: &[FiniteF64]) -> io::Result<Self> {
        let mut encoded = String::new();
        let mut writer = zstd::Encoder::new(
            EncoderWriteAdapter(BASE64.new_encoder(&mut encoded)),
            zstd::DEFAULT_COMPRESSION_LEVEL,
        )?;

        for f in data {
            writer.write_all(&f.to_f64().to_le_bytes())?;
        }

        writer.finish()?;

        Ok(Self::Dynamic(DynamicArrayEncoding::Zstd { data: encoded }))
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
