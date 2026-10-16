use std::borrow::Cow;

use android_trace_log::AndroidTraceLog;

use crate::ProfileError;

pub mod chunk;
pub mod legacy;

/// Keys of the `*version` section that are ignored when parsing a method trace.
///
/// [`android_trace_log`] rejects any header key it does not know. These keys are written by ART
/// starting with Android 17 (`TraceWriter::CreateSummary` in `runtime/trace.cc`) and are not
/// used by Relay.
const IGNORED_HEADER_KEYS: &[&[u8]] = &[
    b"is_streaming",
    b"is_precise_trace",
    b"end_time",
    b"end_tsc",
    b"compilation-filter",
];

/// Parses an Android method trace, skipping [`IGNORED_HEADER_KEYS`] in the `*version` header.
fn parse_android_trace(bytes: &[u8]) -> Result<AndroidTraceLog, ProfileError> {
    android_trace_log::parse(&strip_ignored_header_keys(bytes))
        .map_err(|_| ProfileError::InvalidSampledProfile)
}

/// Removes lines with [`IGNORED_HEADER_KEYS`] from the `*version` section of a method trace.
///
/// All other lines are kept, so unknown keys and malformed values are still rejected by the
/// parser. Only the text header is touched; the binary data section starting at the `SLOW`
/// magic is copied as is, and its offsets are relative to that magic.
///
/// Returns the input unchanged if there is nothing to remove.
fn strip_ignored_header_keys(bytes: &[u8]) -> Cow<'_, [u8]> {
    const VERSION_SECTION: &[u8] = b"*version\n";

    if !bytes.starts_with(VERSION_SECTION) {
        return Cow::Borrowed(bytes);
    }

    // The first line of the section is the version number, followed by header lines.
    let Some(version_len) = next_line_len(&bytes[VERSION_SECTION.len()..]) else {
        return Cow::Borrowed(bytes);
    };

    let mut pos = VERSION_SECTION.len() + version_len + 1;
    let mut stripped: Option<Vec<u8>> = None;

    while let Some(len) = next_line_len(&bytes[pos..]) {
        let line = &bytes[pos..pos + len];
        let next = pos + len + 1;

        if line.starts_with(b"*") {
            break;
        }

        if is_ignored_header_line(line) {
            stripped.get_or_insert_with(|| bytes[..pos].to_vec());
        } else if let Some(stripped) = &mut stripped {
            stripped.extend_from_slice(&bytes[pos..next]);
        }

        pos = next;
    }

    match stripped {
        Some(mut stripped) => {
            stripped.extend_from_slice(&bytes[pos..]);
            Cow::Owned(stripped)
        }
        None => Cow::Borrowed(bytes),
    }
}

/// Returns the length of the next line in `bytes`, excluding the trailing newline.
fn next_line_len(bytes: &[u8]) -> Option<usize> {
    bytes.iter().position(|&b| b == b'\n')
}

fn is_ignored_header_line(line: &[u8]) -> bool {
    line.iter()
        .position(|&b| b == b'=')
        .is_some_and(|eq| IGNORED_HEADER_KEYS.contains(&&line[..eq]))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ANDROID_16_HEADER: &[u8] = b"*version\n3\ndata-file-overflow=false\nclock=dual\n\
        elapsed-time-usec=9407646\nnum-method-calls=4341\nclock-call-overhead-nsec=273\n\
        vm=art\npid=4471\n*threads\n1\tmain\n*methods\n*end\nSLOW\x03\x00";

    const ANDROID_17_HEADER: &[u8] = b"*version\n3\ndata-file-overflow=false\nclock=dual\n\
        elapsed-time-usec=9407646\nnum-method-calls=4341\nclock-call-overhead-nsec=273\n\
        vm=art\npid=4471\nis_streaming=false\nis_precise_trace=true\n\
        end_time=1642106300035\nend_tsc=7762889077040\ncompilation-filter=run-from-apk\n\
        *threads\n1\tmain\n*methods\n*end\nSLOW\x03\x00";

    #[test]
    fn test_strip_android_16_header_unchanged() {
        let stripped = strip_ignored_header_keys(ANDROID_16_HEADER);
        assert!(matches!(stripped, Cow::Borrowed(_)));
        assert_eq!(&*stripped, ANDROID_16_HEADER);
    }

    #[test]
    fn test_strip_android_17_keys() {
        let stripped = strip_ignored_header_keys(ANDROID_17_HEADER);
        assert_eq!(&*stripped, ANDROID_16_HEADER);
    }

    #[test]
    fn test_strip_ignored_key_between_known_keys() {
        let input = b"*version\n3\nclock=dual\nend_tsc=1\nvm=art\n*threads\n*end\nSLOW=x\n";
        let stripped = strip_ignored_header_keys(input);
        assert_eq!(
            &*stripped,
            b"*version\n3\nclock=dual\nvm=art\n*threads\n*end\nSLOW=x\n"
        );
    }

    #[test]
    fn test_strip_keeps_unknown_keys() {
        let input = b"*version\n3\nclock=dual\nfoo=bar\nend_time=1\n*threads\n";
        let stripped = strip_ignored_header_keys(input);
        assert_eq!(&*stripped, b"*version\n3\nclock=dual\nfoo=bar\n*threads\n");
    }

    #[test]
    fn test_strip_keeps_lines_without_key() {
        let input = b"*version\n3\ngarbage\n=value\nend_time\n*threads\n";
        let stripped = strip_ignored_header_keys(input);
        assert!(matches!(stripped, Cow::Borrowed(_)));
    }

    #[test]
    fn test_strip_only_version_section() {
        for input in [
            &b""[..],
            b"*version\n",
            b"*threads\nend_time=1\n",
            b"*version\n3\n*threads\nend_time=1\n",
            b"\x00\x01\x02",
        ] {
            let stripped = strip_ignored_header_keys(input);
            assert!(matches!(stripped, Cow::Borrowed(_)));
        }
    }
}
