// TODO: Fix casts between RelayGeoIpLookup and GeoIpLookup
#![allow(clippy::cast_ptr_alignment)]
#![deny(unused_must_use)]
#![allow(clippy::derive_partial_eq_without_eq)]

use std::cmp::Ordering;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::slice;
use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use relay_cardinality::CardinalityLimit;
use relay_common::glob::{glob_match_bytes, GlobOptions};
use relay_dynamic_config::{normalize_json, GlobalConfig, ProjectConfig};
use relay_event_normalization::{
    normalize_event, validate_event, BreakdownsConfig, ClientHints, EventValidationConfig,
    GeoIpLookup, NormalizationConfig, RawUserAgentInfo,
};
use relay_event_schema::processor::{process_value, split_chunks, ProcessingState};
use relay_event_schema::protocol::{Event, IpAddr, VALID_PLATFORMS};
use relay_pii::{
    selector_suggestions_from_value, DataScrubbingConfig, InvalidSelectorError, PiiConfig,
    PiiConfigError, PiiProcessor, SelectorSpec,
};
use relay_protocol::{Annotated, Remark, RuleCondition};
use relay_sampling::SamplingConfig;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::core::{RelayBuf, RelayStr};

/// Configuration for the store step -- validation and normalization.
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct StoreNormalizer {
    /// The identifier of the target project, which gets added to the payload.
    pub project_id: Option<u64>,

    /// The IP address of the SDK that sent the event.
    ///
    /// When `{{auto}}` is specified and there is no other IP address in the payload, such as in the
    /// `request` context, this IP address gets added to the `user` context.
    pub client_ip: Option<IpAddr>,

    /// The name and version of the SDK that sent the event.
    pub client: Option<String>,

    /// The internal identifier of the DSN, which gets added to the payload.
    ///
    /// Note that this is different from the DSN's public key. The ID is usually numeric.
    pub key_id: Option<String>,

    /// The version of the protocol.
    ///
    /// This is a deprecated field, as there is no more versioning of Relay event payloads.
    pub protocol_version: Option<String>,

    /// Configuration for issue grouping.
    ///
    /// This configuration is persisted into the event payload to achieve idempotency in the
    /// processing pipeline and for reprocessing.
    pub grouping_config: Option<serde_json::Value>,

    /// The raw user-agent string obtained from the submission request headers.
    ///
    /// The user agent is used to infer device, operating system, and browser information should the
    /// event payload contain no such data.
    ///
    /// Newer browsers have frozen their user agents and send [`client_hints`](Self::client_hints)
    /// instead. If both a user agent and client hints are present, normalization uses client hints.
    pub user_agent: Option<String>,

    /// A collection of headers sent by newer browsers about the device and environment.
    ///
    /// Client hints are the preferred way to infer device, operating system, and browser
    /// information should the event payload contain no such data. If no client hints are present,
    /// normalization falls back to the user agent.
    pub client_hints: ClientHints<String>,

    /// The time at which the event was received in this Relay.
    ///
    /// This timestamp is persisted into the event payload.
    pub received_at: Option<DateTime<Utc>>,

    /// The time at which the event was sent by the client.
    ///
    /// The difference between this and the `received_at` timestamps is used for clock drift
    /// correction, should a significant difference be detected.
    pub sent_at: Option<DateTime<Utc>>,

    /// The maximum amount of seconds an event can be predated into the future.
    ///
    /// If the event's timestamp lies further into the future, the received timestamp is assumed.
    pub max_secs_in_future: Option<i64>,

    /// The maximum amount of seconds an event can be dated in the past.
    ///
    /// If the event's timestamp is older, the received timestamp is assumed.
    pub max_secs_in_past: Option<i64>,

    /// When `Some(true)`, individual parts of the event payload is trimmed to a maximum size.
    ///
    /// See the event schema for size declarations.
    pub enable_trimming: Option<bool>,

    /// When `Some(true)`, it is assumed that the event has been normalized before.
    ///
    /// This disables certain normalizations, especially all that are not idempotent. The
    /// renormalize mode is intended for the use in the processing pipeline, so an event modified
    /// during ingestion can be validated against the schema and large data can be trimmed. However,
    /// advanced normalizations such as inferring contexts or clock drift correction are disabled.
    ///
    /// `None` equals to `false`.
    pub is_renormalize: Option<bool>,

    /// Overrides the default flag for other removal.
    pub remove_other: Option<bool>,

    /// When `Some(true)`, context information is extracted from the user agent.
    pub normalize_user_agent: Option<bool>,

    /// Emit breakdowns based on given configuration.
    pub breakdowns: Option<BreakdownsConfig>,

    /// The SDK's sample rate as communicated via envelope headers.
    ///
    /// It is persisted into the event payload.
    pub client_sample_rate: Option<f64>,

    /// The identifier of the Replay running while this event was created.
    ///
    /// It is persisted into the event payload for correlation.
    pub replay_id: Option<Uuid>,

    /// Controls whether spans should be normalized (e.g. normalizing the exclusive time).
    ///
    /// To normalize spans in [`normalize_event`], `is_renormalize` must
    /// be disabled _and_ `normalize_spans` enabled.
    pub normalize_spans: bool,
}

impl StoreNormalizer {
    /// Helper method to parse *mut StoreConfig -> &StoreConfig
    fn this(&self) -> &Self {
        self
    }
}

/// A geo ip lookup helper based on maxmind db files.
pub struct RelayGeoIpLookup;

/// The processor that normalizes events for store.
pub struct RelayStoreNormalizer;

/// Chunks the given text based on remarks.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_split_chunks(
    string: *const RelayStr,
    remarks: *const RelayStr,
) -> RelayStr {
    let remarks: Vec<Remark> = serde_json::from_str((*remarks).as_str())?;
    let chunks = split_chunks((*string).as_str(), &remarks);
    let json = serde_json::to_string(&chunks)?;
    json.into()
}

/// Opens a maxminddb file by path.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_geoip_lookup_new(path: *const c_char) -> *mut RelayGeoIpLookup {
    let path = CStr::from_ptr(path).to_string_lossy();
    let lookup = GeoIpLookup::open(path.as_ref())?;
    Box::into_raw(Box::new(lookup)) as *mut RelayGeoIpLookup
}

/// Frees a `RelayGeoIpLookup`.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_geoip_lookup_free(lookup: *mut RelayGeoIpLookup) {
    if !lookup.is_null() {
        let lookup = lookup as *mut GeoIpLookup;
        let _dropped = Box::from_raw(lookup);
    }
}

/// Returns a list of all valid platform identifiers.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_valid_platforms(size_out: *mut usize) -> *const RelayStr {
    static VALID_PLATFORM_STRS: OnceLock<Vec<RelayStr>> = OnceLock::new();
    let platforms = VALID_PLATFORM_STRS
        .get_or_init(|| VALID_PLATFORMS.iter().map(|s| RelayStr::new(s)).collect());

    if let Some(size_out) = size_out.as_mut() {
        *size_out = platforms.len();
    }

    platforms.as_ptr()
}

/// Creates a new normalization config.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_store_normalizer_new(
    config: *const RelayStr,
    _geoip_lookup: *const RelayGeoIpLookup,
) -> *mut RelayStoreNormalizer {
    let normalizer: StoreNormalizer = serde_json::from_str((*config).as_str())?;
    Box::into_raw(Box::new(normalizer)) as *mut RelayStoreNormalizer
}

/// Frees a `RelayStoreNormalizer`.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_store_normalizer_free(normalizer: *mut RelayStoreNormalizer) {
    if !normalizer.is_null() {
        let normalizer = normalizer as *mut StoreNormalizer;
        let _dropped = Box::from_raw(normalizer);
    }
}

/// Normalizes the event given as JSON.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_store_normalizer_normalize_event(
    normalizer: *mut RelayStoreNormalizer,
    event: *const RelayStr,
) -> RelayStr {
    let normalizer = normalizer as *mut StoreNormalizer;
    let config = (*normalizer).this();
    let mut event = Annotated::<Event>::from_json((*event).as_str())?;

    let event_validation_config = EventValidationConfig {
        received_at: config.received_at,
        max_secs_in_past: config.max_secs_in_past,
        max_secs_in_future: config.max_secs_in_future,
        timestamp_range: None, // only supported in relay
        is_validated: config.is_renormalize.unwrap_or(false),
    };
    validate_event(&mut event, &event_validation_config)?;

    let is_renormalize = config.is_renormalize.unwrap_or(false);

    let normalization_config = NormalizationConfig {
        project_id: config.project_id,
        client: config.client.clone(),
        protocol_version: config.protocol_version.clone(),
        key_id: config.key_id.clone(),
        grouping_config: config.grouping_config.clone(),
        client_ip: config.client_ip.as_ref(),
        client_sample_rate: config.client_sample_rate,
        user_agent: RawUserAgentInfo {
            user_agent: config.user_agent.as_deref(),
            client_hints: config.client_hints.as_deref(),
        },
        max_name_and_unit_len: None,
        breakdowns_config: None, // only supported in relay
        normalize_user_agent: config.normalize_user_agent,
        transaction_name_config: Default::default(), // only supported in relay
        is_renormalize,
        remove_other: config.remove_other.unwrap_or(!is_renormalize),
        emit_event_errors: !is_renormalize,
        device_class_synthesis_config: false, // only supported in relay
        enrich_spans: false,
        max_tag_value_length: usize::MAX,
        span_description_rules: None,
        performance_score: None,
        geoip_lookup: None,   // only supported in relay
        ai_model_costs: None, // only supported in relay
        enable_trimming: config.enable_trimming.unwrap_or_default(),
        measurements: None,
        normalize_spans: config.normalize_spans,
        replay_id: config.replay_id,
    };
    normalize_event(&mut event, &normalization_config);

    RelayStr::from_string(event.to_json()?)
}

/// Replaces invalid JSON generated by Python.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_translate_legacy_python_json(event: *mut RelayStr) -> bool {
    let data = slice::from_raw_parts_mut((*event).data as *mut u8, (*event).len);
    json_forensics::translate_slice(data);
    true
}

/// Validates a PII selector spec. Used to validate datascrubbing safe fields.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_validate_pii_selector(value: *const RelayStr) -> RelayStr {
    let value = (*value).as_str();
    match value.parse::<SelectorSpec>() {
        Ok(_) => RelayStr::new(""),
        Err(err) => match err {
            InvalidSelectorError::ParseError(_) => {
                // Change the error to something more concise we can show in an UI.
                // Error message follows the same format used for fingerprinting rules.
                RelayStr::from_string(format!("invalid syntax near {value:?}"))
            }
            err => RelayStr::from_string(err.to_string()),
        },
    }
}

/// Validate a PII config against the schema. Used in project options UI.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_validate_pii_config(value: *const RelayStr) -> RelayStr {
    match serde_json::from_str::<PiiConfig>((*value).as_str()) {
        Ok(config) => match config.compiled().force_compile() {
            Ok(_) => RelayStr::new(""),
            Err(PiiConfigError::RegexError(source)) => RelayStr::from_string(source.to_string()),
        },
        Err(e) => RelayStr::from_string(e.to_string()),
    }
}

/// Convert an old datascrubbing config to the new PII config format.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_convert_datascrubbing_config(config: *const RelayStr) -> RelayStr {
    let config: DataScrubbingConfig = serde_json::from_str((*config).as_str())?;
    match config.pii_config() {
        Ok(Some(config)) => RelayStr::from_string(serde_json::to_string(config)?),
        Ok(None) => RelayStr::new("{}"),
        // NOTE: Callers of this function must be able to handle this error.
        Err(e) => RelayStr::from_string(e.to_string()),
    }
}

/// Scrub an event using new PII stripping config.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_pii_strip_event(
    config: *const RelayStr,
    event: *const RelayStr,
) -> RelayStr {
    let config = serde_json::from_str::<PiiConfig>((*config).as_str())?;
    let mut processor = PiiProcessor::new(config.compiled());

    let mut event = Annotated::<Event>::from_json((*event).as_str())?;
    process_value(&mut event, &mut processor, ProcessingState::root())?;

    RelayStr::from_string(event.to_json()?)
}

/// Walk through the event and collect selectors that can be applied to it in a PII config. This
/// function is used in the UI to provide auto-completion of selectors.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_pii_selector_suggestions_from_event(
    event: *const RelayStr,
) -> RelayStr {
    let mut event = Annotated::<Event>::from_json((*event).as_str())?;
    let rv = selector_suggestions_from_value(&mut event);
    RelayStr::from_string(serde_json::to_string(&rv)?)
}

/// A test function that always panics.
#[no_mangle]
#[relay_ffi::catch_unwind]
#[allow(clippy::diverging_sub_expression)]
pub unsafe extern "C" fn relay_test_panic() -> () {
    panic!("this is a test panic")
}

/// Controls the globbing behaviors.
#[repr(u32)]
pub enum GlobFlags {
    /// When enabled `**` matches over path separators and `*` does not.
    DoubleStar = 1,
    /// Enables case insensitive path matching.
    CaseInsensitive = 2,
    /// Enables path normalization.
    PathNormalize = 4,
    /// Allows newlines.
    AllowNewline = 8,
}

/// Performs a glob operation on bytes.
///
/// Returns `true` if the glob matches, `false` otherwise.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_is_glob_match(
    value: *const RelayBuf,
    pat: *const RelayStr,
    flags: GlobFlags,
) -> bool {
    let mut options = GlobOptions::default();
    let flags = flags as u32;
    if (flags & GlobFlags::DoubleStar as u32) != 0 {
        options.double_star = true;
    }
    if (flags & GlobFlags::CaseInsensitive as u32) != 0 {
        options.case_insensitive = true;
    }
    if (flags & GlobFlags::PathNormalize as u32) != 0 {
        options.path_normalize = true;
    }
    if (flags & GlobFlags::AllowNewline as u32) != 0 {
        options.allow_newline = true;
    }
    glob_match_bytes((*value).as_bytes(), (*pat).as_str(), options)
}

/// Parse a sentry release structure from a string.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_parse_release(value: *const RelayStr) -> RelayStr {
    let release = sentry_release_parser::Release::parse((*value).as_str())?;
    RelayStr::from_string(serde_json::to_string(&release)?)
}

/// Compares two versions.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_compare_versions(a: *const RelayStr, b: *const RelayStr) -> i32 {
    let ver_a = sentry_release_parser::Version::parse((*a).as_str())?;
    let ver_b = sentry_release_parser::Version::parse((*b).as_str())?;
    match ver_a.cmp(&ver_b) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

/// Validate a dynamic rule condition.
///
/// Used by dynamic sampling, metric extraction, and metric tagging.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_validate_rule_condition(value: *const RelayStr) -> RelayStr {
    let ret_val = match serde_json::from_str::<RuleCondition>((*value).as_str()) {
        Ok(condition) => {
            if condition.supported() {
                "".to_string()
            } else {
                "unsupported condition".to_string()
            }
        }
        Err(e) => e.to_string(),
    };
    RelayStr::from_string(ret_val)
}

/// Validate whole rule ( this will be also implemented in Sentry for better error messages)
/// The implementation in relay is just to make sure that the Sentry implementation doesn't
/// go out of sync.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_validate_sampling_configuration(value: *const RelayStr) -> RelayStr {
    match serde_json::from_str::<SamplingConfig>((*value).as_str()) {
        Ok(config) => {
            for rule in config.rules {
                if !rule.condition.supported() {
                    return Ok(RelayStr::new("unsupported sampling rule"));
                }
            }
            RelayStr::default()
        }
        Err(e) => RelayStr::from_string(e.to_string()),
    }
}

/// Normalize a project config.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_normalize_project_config(value: *const RelayStr) -> RelayStr {
    let value = (*value).as_str();
    match normalize_json::<ProjectConfig>(value) {
        Ok(normalized) => RelayStr::from_string(normalized),
        Err(e) => RelayStr::from_string(e.to_string()),
    }
}

/// Normalize a cardinality limit config.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn normalize_cardinality_limit_config(value: *const RelayStr) -> RelayStr {
    let value = (*value).as_str();
    match normalize_json::<CardinalityLimit>(value) {
        Ok(normalized) => RelayStr::from_string(normalized),
        Err(e) => RelayStr::from_string(e.to_string()),
    }
}

/// Normalize a global config.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_normalize_global_config(value: *const RelayStr) -> RelayStr {
    let value = (*value).as_str();
    match normalize_json::<GlobalConfig>(value) {
        Ok(normalized) => RelayStr::from_string(normalized),
        Err(e) => RelayStr::from_string(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pii_config_validation_invalid_regex() {
        let config = r#"
        {
          "rules": {
            "strip-fields": {
              "type": "redact_pair",
              "keyPattern": "(not valid regex",
              "redaction": {
                "method": "replace",
                "text": "[Filtered]"
              }
            }
          },
          "applications": {
            "*.everything": ["strip-fields"]
          }
        }
    "#;
        assert_eq!(
            unsafe { relay_validate_pii_config(&RelayStr::from(config)).as_str() },
            "regex parse error:\n    (not valid regex\n    ^\nerror: unclosed group"
        );
    }

    #[test]
    fn pii_config_validation_valid_regex() {
        let config = r#"
        {
          "rules": {
            "strip-fields": {
              "type": "redact_pair",
              "keyPattern": "(\\w+)?+",
              "redaction": {
                "method": "replace",
                "text": "[Filtered]"
              }
            }
          },
          "applications": {
            "*.everything": ["strip-fields"]
          }
        }
    "#;
        assert_eq!(
            unsafe { relay_validate_pii_config(&RelayStr::from(config)).as_str() },
            ""
        );
    }
}
