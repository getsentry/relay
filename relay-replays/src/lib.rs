pub mod processor;
pub mod protocol;

use std::fmt::Display;

pub fn scrub_recording_pii(bytes: &[u8], limit: usize) -> Result<Vec<u8>, ReplayError> {
    let (headers, body) =
        protocol::deserialize(bytes, limit).map_err(ReplayError::ProtocolError)?;
    let scrubbed_body = processor::scrub_pii(body).map_err(ReplayError::ProcessorError)?;
    let output_bytes =
        protocol::serialize(headers, scrubbed_body).map_err(ReplayError::ProtocolError)?;
    Ok(output_bytes)
}

#[derive(Debug)]
pub enum ReplayError {
    ProtocolError(protocol::ProtocolError),
    ProcessorError(processor::ProcessorError),
}

impl Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayError::ProtocolError(e) => write!(f, "{:?}", e),
            ReplayError::ProcessorError(e) => write!(f, "{:?}", e),
        }
    }
}
