//! Replay recordings payload and processor.
//!
//! # Data Scrubbing
//!
//! Since recordings contain snapshot of the browser's DOM, network traffic, and console logs, they
//! are likely to contain sensitive data. This module provides [`RecordingScrubber`], which applies
//! data scrubbing on the payload of recordings while leaving their structure and required fields
//! intact.
//!
//! Data scrubbing applies to only Sentry event payloads within the recording event stream,
//! identified by `type: 5`. The scrubber skips all other node types and does not perform any
//! validation beyond JSON parsing.

use std::borrow::Cow;
use std::cell::RefCell;
use std::fmt;
use std::io::Read;
use std::rc::Rc;

use flate2::bufread::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use once_cell::sync::Lazy;
use relay_event_schema::processor::{FieldAttrs, Pii, ProcessingState, Processor, ValueType};
use relay_pii::{PiiConfig, PiiProcessor};
use relay_protocol::Meta;
use serde::{de, ser, Deserializer};
use serde_json::value::RawValue;

use crate::transform::Transform;

/// Paths to fields on which datascrubbing rules should be applied.
///
/// This is equivalent to marking a field as `pii = true` in an `Annotated` schema.
static PII_FIELDS: Lazy<[Vec<&str>; 2]> = Lazy::new(|| {
    [
        vec!["data", "payload", "description"],
        vec!["data", "payload", "data"],
    ]
});

/// Returns `True` if the given path should be treated as `pii = true`.
fn scrub_at_path(path: &Vec<String>) -> bool {
    PII_FIELDS.iter().any(|pii_path| {
        path.len() >= pii_path.len() && pii_path.iter().zip(path).all(|(k1, k2)| k1 == k2)
    })
}

/// Static field attributes used for fields in [`PII_FIELDS`].
const FIELD_ATTRS_PII_TRUE: FieldAttrs = FieldAttrs::new().pii(Pii::True);

/// Static field attributes used for fields without PII scrubbing.
const FIELD_ATTRS_PII_FALSE: FieldAttrs = FieldAttrs::new().pii(Pii::False);

/// Error returned from [`RecordingScrubber`].
#[derive(Debug)]
pub enum ParseRecordingError {
    /// An error parsing the JSON payload.
    Parse(serde_json::Error),
    /// Invalid or broken compression.
    Compression(std::io::Error),
    /// Validation of the payload failed.
    ///
    /// The body is empty, is missing the headers, or the body.
    Message(&'static str),
}

impl fmt::Display for ParseRecordingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseRecordingError::Parse(serde_error) => write!(f, "{serde_error}"),
            ParseRecordingError::Compression(error) => write!(f, "{error}"),
            ParseRecordingError::Message(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ParseRecordingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ParseRecordingError::Parse(e) => Some(e),
            ParseRecordingError::Compression(e) => Some(e),
            ParseRecordingError::Message(_) => None,
        }
    }
}

impl From<serde_json::Error> for ParseRecordingError {
    fn from(err: serde_json::Error) -> Self {
        ParseRecordingError::Parse(err)
    }
}

/// The [`Transform`] implementation for data scrubbing.
///
/// This is used by [`EventStreamVisitor`] and [`ScrubbedValue`] to scrub recording events.
struct ScrubberTransform<'a> {
    /// PII processors that are applied one by one on each value.
    processor1: Option<PiiProcessor<'a>>,
    processor2: Option<PiiProcessor<'a>>,
    /// The state encoding the current path, which is fed by `push_path` and `pop_path`.
    state: ProcessingState<'a>,
    /// The current path. This is redundant with `state`, which also contains the full path,
    /// but easier to match on.
    path: Vec<String>,
}

impl ScrubberTransform<'_> {
    fn ensure_empty(&mut self) {
        if !self.path.is_empty() || self.state.depth() > 0 {
            debug_assert!(false, "ScrubberTransform not empty");
            relay_log::error!("ScrubberTransform not empty");
        }
        self.state = ProcessingState::new_root(None, None);
        self.path.clear();
    }
}

impl<'de> Transform<'de> for &'_ mut ScrubberTransform<'_> {
    fn push_path(&mut self, key: &'de str) {
        self.path.push(key.to_owned());
        let field_attrs = if scrub_at_path(&self.path) {
            &FIELD_ATTRS_PII_TRUE
        } else {
            &FIELD_ATTRS_PII_FALSE
        };

        self.state = std::mem::take(&mut self.state).enter_owned(
            key.to_owned(),
            Some(Cow::Borrowed(field_attrs)),
            Some(ValueType::String), // Pretend everything is a string.
        )
    }

    fn pop_path(&mut self) {
        if let Ok(Some(parent)) = std::mem::take(&mut self.state).try_into_parent() {
            self.state = parent;
        }
        let popped = self.path.pop();
        debug_assert!(popped.is_some()); // pop_path should never be called on an empty state.
    }

    fn transform_str<'a>(&mut self, v: &'a str) -> Cow<'a, str> {
        self.transform_string(v.to_owned())
    }

    fn transform_string(&mut self, mut value: String) -> Cow<'static, str> {
        if let Some(ref mut processor) = self.processor1 {
            if processor
                .process_string(&mut value, &mut Meta::default(), &self.state)
                .is_err()
            {
                return Cow::Borrowed("");
            }
        }

        if let Some(ref mut processor) = self.processor2 {
            if processor
                .process_string(&mut value, &mut Meta::default(), &self.state)
                .is_err()
            {
                return Cow::Borrowed("");
            }
        }

        Cow::Owned(value)
    }
}

/// Helper that runs data scrubbing on a raw JSON value during serialization.
///
/// This is used by [`EventStreamVisitor`] to serialize recording events on-the-fly from a stream.
/// It uses a [`ScrubberTransform`] holding all state to perform the actual work.
struct ScrubbedValue<'a, 'b>(&'a RawValue, Rc<RefCell<ScrubberTransform<'b>>>);

impl serde::Serialize for ScrubbedValue<'_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut transform = self.1.borrow_mut();
        let mut deserializer = serde_json::Deserializer::from_str(self.0.get());
        let scrubber = crate::transform::Deserializer::new(&mut deserializer, &mut *transform);
        serde_transcode::transcode(scrubber, serializer)
    }
}

/// A visitor that deserializes, scrubs, and serializes a stream of recording events.
struct EventStreamVisitor<'a, S> {
    serializer: S,
    scrubber: Rc<RefCell<ScrubberTransform<'a>>>,
}

impl<'a, S> EventStreamVisitor<'a, S> {
    /// The proprietary rrweb node type that identifies Sentry payloads.
    ///
    /// The visitor exclusively scrubs these nodes.
    const SENTRY_EVENT_TYPE: u8 = 5;

    /// Creates a new visitor wrapping a `serializer`.
    fn new(serializer: S, scrubber: Rc<RefCell<ScrubberTransform<'a>>>) -> Self {
        Self {
            serializer,
            scrubber,
        }
    }
}

impl<'de, 'a, S> de::Visitor<'de> for EventStreamVisitor<'a, S>
where
    S: ser::Serializer,
{
    type Value = S::Ok;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a replay recording event stream")
    }

    fn visit_seq<A>(self, mut v: A) -> Result<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        /// Efficiently deserializes the type discriminator of event nodes.
        #[derive(Clone, Copy, serde::Deserialize)]
        struct TypeHelper {
            #[serde(rename = "type")]
            ty: u8,
        }

        use serde::ser::SerializeSeq;
        let mut seq = self.serializer.serialize_seq(v.size_hint()).map_err(s2d)?;

        // NOTE on performance: This loop parses every element 2-3 times:
        //  1. In the call to next_element() to obtain the raw value. This pass is highly efficient
        //     and cannot be removed.
        //  2. Deserializing `TypeHelper` to obtain the node type. It skips most of the content, but
        //     does require a full pass through the raw value.
        //  3. If scrubbing is required, `ScrubbedValue` internally parses the value and runs it
        //     through a transforming deserializer.
        //
        // These redundant passes come with slight performance overhead compared to an
        // implementation that directly parses the `type` and raw `data` into a structure. Together
        // with decompression and data scrubbing, the difference in benchmarks was small. In case
        // this becomes a performance bottleneck, it is worth to first focus on data scrubbing, and
        // then at the redundant parsing.

        while let Some(raw) = v.next_element::<&'de RawValue>()? {
            let helper = serde_json::from_str::<TypeHelper>(raw.get()).map_err(s2d)?;
            // Scrub only sentry-specific events and serialize all others without modification.
            if helper.ty == Self::SENTRY_EVENT_TYPE {
                seq.serialize_element(&ScrubbedValue(raw, self.scrubber.clone()))
                    .map_err(s2d)?;
                // `pop_path` calls should have reset the scrubber's state, but force a
                // reset here just to be sure:
                self.scrubber.borrow_mut().ensure_empty();
            } else {
                seq.serialize_element(raw).map_err(s2d)?;
            }
        }

        seq.end().map_err(s2d)
    }
}

/// Maps a serialization error to a deserialization error.
fn s2d<S, D>(s: S) -> D
where
    S: ser::Error,
    D: de::Error,
{
    D::custom(s.to_string())
}

/// A utility that performs data scrubbing on compressed Replay recording payloads.
///
/// ### Example
///
/// ```
/// use relay_replays::recording::RecordingScrubber;
/// use relay_pii::PiiConfig;
///
/// // Obtain a PII config from the project state or create one on-demand.
/// let pii_config = PiiConfig::default();
/// let mut scrubber = RecordingScrubber::new(1_000_000, Some(&pii_config), None);
///
/// let payload = b"{}\n[]";
/// let result = scrubber.process_recording(payload.as_slice());
/// ```
pub struct RecordingScrubber<'a> {
    limit: usize,
    transform: Rc<RefCell<ScrubberTransform<'a>>>,
}

impl<'a> RecordingScrubber<'a> {
    /// Creates a new `RecordingScrubber` from PII configs.
    ///
    /// `limit` controls the maximum size in bytes during decompression. This function returns an
    /// `Err` if decompressed contents exceed the limit. The two optional configs to be passed here
    /// are from data scrubbing settings and from the dedicated PII config.
    ///
    /// # Performance
    ///
    /// The passed PII configs are [compiled](PiiConfig::compiled) by this constructor if their
    /// compiled version is not yet cached. This can be a CPU-intensive process and should be called
    /// from a blocking context.
    pub fn new(
        limit: usize,
        config1: Option<&'a PiiConfig>,
        config2: Option<&'a PiiConfig>,
    ) -> Self {
        Self {
            limit,
            transform: Rc::new(RefCell::new(ScrubberTransform {
                processor1: config1.map(|c| PiiProcessor::new(c.compiled())),
                processor2: config2.map(|c| PiiProcessor::new(c.compiled())),
                state: ProcessingState::new_root(None, None),
                path: vec![],
            })),
        }
    }

    /// Returns `true` if both configs are empty and no scrubbing would occur.
    pub fn is_empty(&self) -> bool {
        let tmp = self.transform.borrow();
        tmp.processor1.is_none() && tmp.processor2.is_none()
    }

    fn scrub_replay<W>(&mut self, json: &[u8], write: W) -> Result<(), ParseRecordingError>
    where
        W: std::io::Write,
    {
        let mut deserializer = serde_json::Deserializer::from_slice(json);
        let mut serializer = serde_json::Serializer::new(write);

        deserializer.deserialize_seq(EventStreamVisitor::new(
            &mut serializer,
            self.transform.clone(),
        ))?;

        Ok(())
    }

    #[doc(hidden)] // Public for benchmarks.
    pub fn transcode_replay(
        &mut self,
        body: &[u8],
        output: &mut Vec<u8>,
    ) -> Result<(), ParseRecordingError> {
        let encoder = ZlibEncoder::new(output, Compression::default());

        if body.first() == Some(&b'[') {
            self.scrub_replay(body, encoder)
        } else {
            let mut decompressed = Vec::with_capacity(8 * 1024);
            let mut decoder = ZlibDecoder::new(body).take(self.limit as u64);
            decoder
                .read_to_end(&mut decompressed)
                .map_err(ParseRecordingError::Compression)?;

            self.scrub_replay(&decompressed, encoder)
        }
    }

    /// Parses a replay recording payload and applies data scrubbers.
    ///
    /// # Compression
    ///
    /// The recording `bytes` passed to this function can be a raw recording payload or compressed
    /// with zlib. The result is always compressed, regardless of the input.
    ///
    /// During decompression, the scrubber applies a `limit`. If the decompressed buffer exceeds the
    /// configured size, an `Err` is returned. This does not apply to decompressed payloads.
    ///
    /// # Errors
    ///
    /// This function requires a full recording payload including headers and body. This function
    /// will return errors if:
    ///  - Headers or the body are missing.
    ///  - Headers and the body are separated by exactly one UNIX newline (`\n`).
    ///  - The payload size exceeds the configured `limit` of the scrubber after decompression.
    ///  - On errors during decompression or JSON parsing.
    pub fn process_recording(&mut self, bytes: &[u8]) -> Result<Vec<u8>, ParseRecordingError> {
        // Check for null byte condition.
        if bytes.is_empty() {
            return Err(ParseRecordingError::Message("no data found"));
        }

        let mut split = bytes.splitn(2, |b| b == &b'\n');
        let header = split
            .next()
            .ok_or(ParseRecordingError::Message("no headers found"))?;

        let body = match split.next() {
            Some(b"") | None => return Err(ParseRecordingError::Message("no body found")),
            Some(body) => body,
        };

        let mut output = header.to_owned();
        output.push(b'\n');
        // Data scrubbing usually does not change the size of the output by much. We can preallocate
        // enough space for the scrubbed output to avoid resizing the output buffer serveral times.
        // Benchmarks have NOT shown a big difference, however.
        output.reserve(body.len());
        self.transcode_replay(body, &mut output)?;

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    // End to end test coverage.

    use relay_pii::{DataScrubbingConfig, PiiConfig};

    use crate::recording::scrub_at_path;

    use super::RecordingScrubber;

    fn default_pii_config() -> PiiConfig {
        let mut scrubbing_config = DataScrubbingConfig::default();
        scrubbing_config.scrub_data = true;
        scrubbing_config.scrub_defaults = true;
        scrubbing_config.scrub_ip_addresses = true;
        scrubbing_config.pii_config_uncached().unwrap().unwrap()
    }

    fn scrubber(config: &PiiConfig) -> RecordingScrubber {
        RecordingScrubber::new(usize::MAX, Some(config), None)
    }

    #[test]
    fn test_process_recording_end_to_end() {
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

        let config = default_pii_config();
        let result = scrubber(&config).process_recording(payload);
        assert!(!result.unwrap().is_empty());
    }

    #[test]
    fn test_process_recording_no_body_data() {
        // Empty bodies can not be decompressed and fail.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125, 10,
        ];

        let config = default_pii_config();
        let result = scrubber(&config).process_recording(payload);
        assert!(matches!(
            result.unwrap_err(),
            super::ParseRecordingError::Message("no body found"),
        ));
    }

    #[test]
    fn test_process_recording_bad_body_data() {
        // Invalid gzip body contents.  Can not deflate.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125, 10, 22,
        ];

        let config = default_pii_config();
        let result = scrubber(&config).process_recording(payload);
        assert!(matches!(
            result.unwrap_err(),
            super::ParseRecordingError::Compression(_),
        ));
    }

    #[test]
    fn test_process_recording_no_headers() {
        // No header delimiter.  Entire payload is consumed as headers.  The empty body fails.
        let payload: &[u8] = &[
            123, 34, 115, 101, 103, 109, 101, 110, 116, 95, 105, 100, 34, 58, 51, 125,
        ];

        let config = default_pii_config();
        let result = scrubber(&config).process_recording(payload);
        assert!(matches!(
            result.unwrap_err(),
            super::ParseRecordingError::Message("no body found"),
        ));
    }

    #[test]
    fn test_process_recording_no_contents() {
        // Empty payload can not be decompressed.  Header check never fails.
        let payload: &[u8] = &[];

        let config = default_pii_config();
        let result = scrubber(&config).process_recording(payload);
        assert!(matches!(
            result.unwrap_err(),
            super::ParseRecordingError::Message("no data found"),
        ));
    }

    // RRWeb Payload Coverage

    #[ignore = "type 3 nodes are not supported"]
    #[test]
    fn test_pii_credit_card_removal() {
        let payload = include_bytes!("../tests/fixtures/rrweb-pii.json");

        let mut transcoded = Vec::new();
        let config = default_pii_config();
        scrubber(&config)
            .scrub_replay(payload.as_slice(), &mut transcoded)
            .unwrap();

        let parsed = std::str::from_utf8(&transcoded).unwrap();
        assert!(parsed.contains(r#"{"type":3,"textContent":"[Filtered]","id":284}"#));
    }

    #[test]
    fn test_scrub_pii_navigation() {
        let payload = include_bytes!("../tests/fixtures/rrweb-performance-navigation.json");

        let mut transcoded = Vec::new();
        let config = default_pii_config();
        scrubber(&config)
            .scrub_replay(payload.as_slice(), &mut transcoded)
            .unwrap();

        let parsed = std::str::from_utf8(&transcoded).unwrap();
        assert!(parsed.contains("https://sentry.io?credit-card=[Filtered]"));
    }

    #[test]
    fn test_scrub_pii_resource() {
        let payload = include_bytes!("../tests/fixtures/rrweb-performance-resource.json");

        let mut transcoded = Vec::new();
        let config = default_pii_config();
        scrubber(&config)
            .scrub_replay(payload.as_slice(), &mut transcoded)
            .unwrap();

        let parsed = std::str::from_utf8(&transcoded).unwrap();
        assert!(parsed.contains("https://sentry.io?credit-card=[Filtered]"));
    }

    #[ignore = "type 3 nodes are not supported"]
    #[test]
    fn test_pii_ip_address_removal() {
        let payload = include_bytes!("../tests/fixtures/rrweb-pii-ip-address.json");

        let mut transcoded = Vec::new();
        let config = default_pii_config();
        scrubber(&config)
            .scrub_replay(payload.as_slice(), &mut transcoded)
            .unwrap();

        let parsed = std::str::from_utf8(&transcoded).unwrap();
        assert!(parsed.contains("\"value\":\"[ip]\"")); // Assert texts were mutated.
        assert!(parsed.contains("\"textContent\":\"[ip]\"")) // Assert text node was mutated.
    }

    // Event Parsing and Scrubbing.

    #[ignore = "type 2 nodes are not supported"]
    #[test]
    fn test_scrub_pii_full_snapshot_event() {
        let payload = include_bytes!("../tests/fixtures/rrweb-event-2.json");

        let mut transcoded = Vec::new();
        let config = default_pii_config();
        scrubber(&config)
            .scrub_replay(payload.as_slice(), &mut transcoded)
            .unwrap();

        let scrubbed_result = std::str::from_utf8(&transcoded).unwrap();
        // NOTE: The normalization below was removed
        // assert!(scrubbed_result.contains("\"attributes\":{\"src\":\"#\"}"));
        assert!(scrubbed_result.contains("\"textContent\":\"my ssn is [Filtered]\""));
    }

    #[ignore = "type 3 nodes are not supported"]
    #[test]
    fn test_scrub_pii_incremental_snapshot_event() {
        let payload = include_bytes!("../tests/fixtures/rrweb-event-3.json");

        let mut transcoded = Vec::new();
        let config = default_pii_config();
        scrubber(&config)
            .scrub_replay(payload.as_slice(), &mut transcoded)
            .unwrap();

        let scrubbed_result = std::str::from_utf8(&transcoded).unwrap();
        assert!(scrubbed_result.contains("\"textContent\":\"[Filtered]\""));
        assert!(scrubbed_result.contains("\"value\":\"[Filtered]\""));
    }

    #[test]
    fn test_scrub_pii_custom_event() {
        let payload = include_bytes!("../tests/fixtures/rrweb-event-5.json");

        let mut transcoded = Vec::new();
        let config = default_pii_config();
        scrubber(&config)
            .scrub_replay(payload.as_slice(), &mut transcoded)
            .unwrap();

        let scrubbed_result = std::str::from_utf8(&transcoded).unwrap();
        assert!(scrubbed_result.contains("\"description\":\"[Filtered]\""));
        assert!(scrubbed_result.contains("\"description\":\"https://sentry.io?ip-address=[ip]\""));
        // NOTE: default scrubbers do not remove email address
        // assert!(scrubbed_result.contains("\"message\":\"[email]\""));
    }

    #[test]
    fn test_scrub_pii_key_based() {
        let payload = include_bytes!("../tests/fixtures/rrweb-request.json");

        let mut transcoded = Vec::new();
        let config = default_pii_config();

        scrubber(&config)
            .scrub_replay(payload.as_slice(), &mut transcoded)
            .unwrap();

        let scrubbed_result = std::str::from_utf8(&transcoded).unwrap();
        let scrubbed: serde_json::Value = serde_json::from_str(scrubbed_result).unwrap();

        // Normal fields are not scrubbed:
        assert_eq!(scrubbed[0]["data"]["payload"]["data"]["method"], "POST");

        assert_eq!(
            scrubbed[0]["data"]["payload"]["data"]["request"]["body"]["api_key"],
            "[Filtered]"
        );
    }

    #[test]
    fn test_scrub_pii_key_based_edge_cases() {
        let payload = include_bytes!("../tests/fixtures/rrweb-request-edge-cases.json");

        let mut transcoded = Vec::new();
        let config = default_pii_config();

        scrubber(&config)
            .scrub_replay(payload.as_slice(), &mut transcoded)
            .unwrap();

        let scrubbed_result = std::str::from_utf8(&transcoded).unwrap();
        let scrubbed: serde_json::Value = serde_json::from_str(scrubbed_result).unwrap();

        insta::assert_ron_snapshot!(scrubbed);
    }

    #[test]
    fn test_scrub_at_path() {
        for (should_scrub, path) in [
            (false, vec![]),
            (false, vec!["data"]),
            (false, vec!["data", "payload"]),
            (false, vec!["data", "payload", "foo"]),
            (false, vec!["foo", "payload", "data"]),
            (true, vec!["data", "payload", "data"]),
            (true, vec!["data", "payload", "data", "request"]),
            (true, vec!["data", "payload", "data", "request", "body"]),
        ] {
            let path = path.into_iter().map(|p| p.to_owned()).collect::<Vec<_>>();
            assert_eq!(should_scrub, scrub_at_path(&path));
        }
    }
}
