use std::io;

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
        // Sort data for better compression results.
        let mut data = data
            .iter()
            .map(|f| f.to_f64().to_le_bytes())
            .collect::<Vec<_>>();
        data.sort_unstable();

        let mut encoded = String::new();

        zstd::stream::copy_encode(
            bytemuck::cast_slice(data.as_slice()),
            EncoderWriteAdapter(BASE64.new_encoder(&mut encoded)),
            zstd::DEFAULT_COMPRESSION_LEVEL,
        )?;

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
    /// Base64 encoding.
    ///
    /// Converts all items to little endian byte sequences
    /// and Base64 encodes the raw bytes.
    Base64 { data: String },
    /// Zstd encoding.
    ///
    /// Converts all items to little endian byte sequences,
    /// compresses the data using zstd and then encodes the result
    /// using Base64.
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

impl<'a> std::io::Write for EncoderWriteAdapter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.append(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
