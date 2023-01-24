use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use serde_json::{Error as SError, Value};
use std::fmt::Display;
use std::io::{Error, Read, Write};

/// Deserializes input bytes and returns a tuple of serialized (confirmed valid) JSON headers
/// and deserialized JSON body value.
///
/// Body values are optionally compressed with gzip.
pub fn deserialize(bytes: &[u8], limit: usize) -> Result<(&[u8], Value), ProtocolError> {
    let (headers, body) = read(bytes)?;

    // We always attempt to decompress the body value. If decompression fails we try tp JSON
    // deserialize the body bytes as is.
    match decompress(body, limit) {
        Ok(buf) => {
            let val: Value = serde_json::from_slice(&buf).map_err(ProtocolError::InvalidBody)?;
            Ok((headers, val))
        }
        Err(_) => {
            let val: Value = serde_json::from_slice(body).map_err(ProtocolError::InvalidBody)?;
            Ok((headers, val))
        }
    }
}

/// Serializes the headers and body arguments into a single vec of bytes. The body value is
/// compressed before being merged.  The final output is headers_bytes + "\n" (new-line
/// character) + compressed_body_bytes.
pub fn serialize(headers: &[u8], body: Value) -> Result<Vec<u8>, ProtocolError> {
    let output = serde_json::to_vec(&body).map_err(ProtocolError::InvalidBody)?;
    let output_bytes = compress(output)?;
    Ok([headers.into(), vec![b'\n'], output_bytes].concat())
}

fn compress(output: Vec<u8>) -> Result<Vec<u8>, ProtocolError> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&output)
        .map_err(ProtocolError::EncodingError)?;
    encoder.finish().map_err(ProtocolError::EncodingError)
}

fn decompress(zipped_bytes: &[u8], limit: usize) -> Result<Vec<u8>, ProtocolError> {
    let mut buffer = Vec::new();

    let decoder = ZlibDecoder::new(zipped_bytes);
    decoder
        .take(limit as u64)
        .read_to_end(&mut buffer)
        .map_err(ProtocolError::EncodingError)?;

    Ok(buffer)
}

fn read(bytes: &[u8]) -> Result<(&[u8], &[u8]), ProtocolError> {
    match bytes.is_empty() {
        true => Err(ProtocolError::MissingData),
        false => {
            let mut split = bytes.splitn(2, |b| b == &b'\n');
            let header = split.next().ok_or(ProtocolError::MissingHeaders)?;

            // Try to parse the headers to determine if they are valid JSON. This is a good sanity
            // check to determine if our headers extraction is working properly.
            let _: Value = serde_json::from_slice(header).map_err(ProtocolError::InvalidHeaders)?;

            let body = match split.next() {
                Some(b"") | None => return Err(ProtocolError::MissingBody),
                Some(body) => body,
            };

            Ok((header, body))
        }
    }
}

#[derive(Debug)]
pub enum ProtocolError {
    EncodingError(Error),
    InvalidHeaders(SError),
    InvalidBody(SError),
    MissingData,
    MissingHeaders,
    MissingBody,
}

impl Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolError::InvalidHeaders(e) => {
                write!(f, "recording headers could not be parsed {:?}", e)
            }
            ProtocolError::InvalidBody(e) => {
                write!(f, "recording body could not be parsed {:?}", e)
            }
            ProtocolError::MissingData => write!(f, "no recording found"),
            ProtocolError::MissingHeaders => write!(f, "no recording headers found"),
            ProtocolError::MissingBody => write!(f, "not recording body found"),
            ProtocolError::EncodingError(e) => write!(f, "{:?}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::{deserialize, serialize};
    use serde_json::Value;

    #[test]
    fn test_deserialization() {}

    #[test]
    fn test_serialization() {
        assert_eq!(
            serialize(&[11], Value::String("t".to_string()))
                .unwrap()
                .as_slice(),
            &[11, 10, 120, 156, 83, 42, 81, 2, 0, 1, 115, 0, 185]
        );
    }
}
