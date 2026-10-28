use std::io::{self, Read};

use flate2::bufread::ZlibDecoder;
use relay_event_normalization::replay;

use crate::processing::replays::{Error, ExpandedReplay, Result};

/// Checks the structural validity of a replay, rejects it if invalid.
pub fn validate(replay: &ExpandedReplay) -> Result<()> {
    if replay.headers.event_id().is_none() {
        return Err(Error::NoEventId);
    }

    let Some(event) = replay.payload.event() else {
        return Ok(());
    };
    let event = event.value().ok_or(Error::NoEventContent)?;
    replay::validate(event).map_err(Error::from)
}

/// Rejects a replay whose recording inflates to more than `max_size` bytes.
///
/// Recording scrubbing applies the same bound, but only for projects which scrub recordings.
/// Without this check, any other project can store a small payload that inflates to an
/// arbitrary size on every read. Bodies which are not valid zlib streams are left to the
/// consumer, which already handles them.
pub fn recording_size(recording: &[u8], max_size: usize) -> Result<()> {
    // A recording is a JSON header line followed by the body.
    let body = recording
        .splitn(2, |&b| b == b'\n')
        .nth(1)
        .unwrap_or_default();

    // Reading one byte past the limit detects an oversized body without inflating it fully.
    let mut decoder = ZlibDecoder::new(body).take(max_size as u64 + 1);
    let size = io::copy(&mut decoder, &mut io::sink()).unwrap_or(0);

    if size > max_size as u64 {
        relay_log::debug!("replay recording over maximum uncompressed size");
        return Err(Error::TooLarge);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::Compression;
    use flate2::write::ZlibEncoder;

    use super::*;

    fn recording(body: &[u8]) -> Vec<u8> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(body).unwrap();
        let mut recording = b"{\"segment_id\":0}\n".to_vec();
        recording.extend(encoder.finish().unwrap());
        recording
    }

    #[test]
    fn test_recording_size_within_limit() {
        let body = b"[{\"type\":4}]";
        assert!(recording_size(&recording(body), body.len()).is_ok());
    }

    #[test]
    fn test_recording_size_exceeds_limit() {
        let body = b"[{\"type\":4}]";
        assert!(matches!(
            recording_size(&recording(body), body.len() - 1),
            Err(Error::TooLarge)
        ));
    }

    #[test]
    fn test_recording_size_high_compression_ratio() {
        let body = vec![b' '; 1024 * 1024];
        let recording = recording(&body);
        assert!(recording.len() < 2048);
        assert!(matches!(
            recording_size(&recording, 1024),
            Err(Error::TooLarge)
        ));
    }

    #[test]
    fn test_recording_size_ignores_uncompressed_and_invalid() {
        assert!(recording_size(b"{\"segment_id\":0}\n[{\"type\":4}]", 1).is_ok());
        assert!(recording_size(b"{\"segment_id\":0}\nnot zlib", 1).is_ok());
        assert!(recording_size(b"no body", 1).is_ok());
    }
}
