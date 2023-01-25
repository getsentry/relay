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

    // We always attempt to decompress the body value. If decompression fails we try to JSON
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
/// compressed before being concatenated.  The final output is headers-bytes + "\n" (new-line
/// character) + compressed-body-bytes.
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
    fn test_deserialize_compressed() {
        // Valid compressed rrweb payload.  Contains a 16 byte header followed by a new line
        // character and concludes with a gzipped rrweb payload.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125, 10, 120,
            156, 149, 144, 91, 106, 196, 32, 20, 64, 247, 114, 191, 237, 160, 241, 145, 234, 38,
            102, 1, 195, 124, 152, 104, 6, 33, 169, 193, 40, 52, 4, 247, 94, 91, 103, 40, 20, 108,
            59, 191, 247, 30, 207, 225, 122, 57, 32, 238, 171, 5, 69, 17, 24, 29, 53, 168, 3, 54,
            159, 194, 88, 70, 4, 193, 234, 55, 23, 157, 127, 219, 64, 93, 14, 120, 7, 37, 100, 1,
            119, 80, 29, 102, 8, 156, 1, 213, 11, 4, 209, 45, 246, 60, 77, 155, 141, 160, 94, 232,
            43, 206, 232, 206, 118, 127, 176, 132, 177, 7, 203, 42, 75, 36, 175, 44, 231, 63, 88,
            217, 229, 107, 174, 179, 45, 234, 101, 45, 172, 232, 49, 163, 84, 22, 191, 232, 63, 61,
            207, 93, 130, 229, 189, 216, 53, 138, 84, 182, 139, 178, 199, 191, 22, 139, 179, 238,
            196, 227, 244, 134, 137, 240, 158, 60, 101, 34, 255, 18, 241, 6, 116, 42, 212, 119, 35,
            234, 27, 40, 24, 130, 213, 102, 12, 105, 25, 160, 252, 147, 222, 103, 175, 205, 215,
            182, 45, 168, 17, 48, 118, 210, 105, 142, 229, 217, 168, 163, 189, 249, 80, 254, 19,
            146, 59, 13, 115, 10, 144, 115, 190, 126, 0, 2, 68, 180, 16,
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
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_no_headers() {
        // No header delimiter.  Entire payload is consumed as headers.  The empty body fails.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125,
        ];
        let result = deserialize(payload, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_no_body() {
        // Empty bodies can not be decompressed and fail.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125, 10,
        ];
        let result = deserialize(payload, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_bad_body_data() {
        // Invalid gzip body contents.  Can not deflate.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125, 10, 22,
        ];
        let result = deserialize(payload, 100);
        assert!(&result.is_err());
    }

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
