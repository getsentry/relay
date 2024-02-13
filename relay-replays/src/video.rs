use bytes::Bytes;
use serde::Deserialize;
use serde_json;
use std::fmt;

pub fn validate_replay_video(payload: &Bytes) -> Result<(), VideoError> {
    // Validate data was provided.
    if payload.is_empty() {
        return Err(VideoError::Message("no video message found"));
    }

    // Validate we were able to find leading headers.
    let mut split = payload.splitn(2, |b| b == &b'\n');
    let header = split
        .next()
        .ok_or(VideoError::Message("no video headers found"))?;

    // Validate the body contains data.
    match split.next() {
        Some(b"") | None => return Err(VideoError::Message("no video payload found")),
        _ => {}
    };

    // Validate the headers are in the appropriate format.
    serde_json::from_slice::<VideoHeaders>(header)?;

    Ok(())
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct VideoHeaders {
    segment_id: u16,
}

#[derive(Debug)]
pub enum VideoError {
    /// An error parsing the JSON payload.
    Parse(serde_json::Error),
    /// Validation of the payload failed.
    ///
    /// The body is empty, is missing the headers, or the body.
    Message(&'static str),
}

impl fmt::Display for VideoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VideoError::Parse(serde_error) => write!(f, "{serde_error}"),
            VideoError::Message(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for VideoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            VideoError::Parse(e) => Some(e),
            VideoError::Message(_) => None,
        }
    }
}

impl From<serde_json::Error> for VideoError {
    fn from(err: serde_json::Error) -> Self {
        VideoError::Parse(err)
    }
}
