/// Protocol.
///
/// The following section includes the behaviors responsible for deserializing a recording from
/// its HTTP transport schema and serializing a recording to its Kafka transport schema.
///
/// We expect recordings to come in the format of plaintext headers (JSON encoded), then a new
/// line character, then an optionally compressed RRWeb recording.  Failure to include any
/// component of this schema results in an error.
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use serde_json::{Error as SError, Value};
use std::fmt::Display;
use std::io::{Error, Read, Write};

/// Deserializes input bytes and returns a tuple of serialized (confirmed valid) JSON headers
/// and the optionally decompressed body value.
pub fn deserialize(bytes: &[u8], limit: usize) -> Result<(&[u8], Vec<u8>), ProtocolError> {
    let (headers, body) = read(bytes)?;

    // We always attempt to decompress the body value. If decompression fails we try to JSON
    // deserialize the body bytes as is.
    match decompress(body, limit) {
        Ok(buf) => Ok((headers, buf)),
        Err(_) => Ok((headers, body.into())),
    }
}

/// Serializes the headers and body arguments into a single vec of bytes. The body value is
/// compressed before being concatenated.  The final output is headers-bytes + "\n" (new-line
/// character) + compressed-body-bytes.
pub fn serialize(headers: &[u8], body: Vec<u8>) -> Result<Vec<u8>, ProtocolError> {
    let output_bytes = compress(body)?;
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
            serde_json::from_slice::<Value>(header).map_err(ProtocolError::InvalidHeaders)?;

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
            ProtocolError::MissingData => write!(f, "no recording found"),
            ProtocolError::MissingHeaders => write!(f, "no recording headers found"),
            ProtocolError::MissingBody => write!(f, "not recording body found"),
            ProtocolError::EncodingError(e) => write!(f, "{:?}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::recording::protocol::{deserialize, serialize, ProtocolError};
    use serde_json::Value;

    #[test]
    fn test_deserialize_message() {
        // Contains a 16 byte header followed by a new line character.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125, 10, 120,
        ];

        let result = deserialize(payload, 9999999999);
        assert!(result.is_ok());

        let (headers, _) = result.unwrap();
        let head: Value = serde_json::from_slice(headers).unwrap();
        let output = serde_json::to_string(&head).unwrap();
        assert_eq!(output, "{\"segment_id\":3}");
    }

    #[test]
    fn test_deserialize_no_contents() {
        let payload: &[u8] = &[];
        let result = deserialize(payload, 100);
        assert!(matches!(result.unwrap_err(), ProtocolError::MissingData));
    }

    #[test]
    fn test_deserialize_no_headers() {
        // No header delimiter.  Entire payload is consumed as headers.  The empty body fails.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125,
        ];
        let result = deserialize(payload, 100);
        assert!(matches!(result.unwrap_err(), ProtocolError::MissingBody));
    }

    #[test]
    fn test_deserialize_invalid_headers() {
        let payload: &[u8] = &[123, 3, 10, 120];
        let result = deserialize(payload, 100);
        assert!(matches!(
            result.unwrap_err(),
            ProtocolError::InvalidHeaders(_)
        ));
    }

    #[test]
    fn test_deserialize_no_body() {
        // Empty bodies can not be decompressed and fail.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125, 10,
        ];
        let result = deserialize(payload, 100);
        assert!(matches!(result.unwrap_err(), ProtocolError::MissingBody));
    }

    #[test]
    fn test_serialization() {
        assert_eq!(
            serialize(&[11], vec![8]).unwrap().as_slice(),
            &[11, 10, 120, 156, 227, 0, 0, 0, 9, 0, 9]
        );
    }
}
