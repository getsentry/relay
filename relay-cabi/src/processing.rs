// TODO: Fix casts between RelayGeoIpLookup and GeoIpLookup
#![allow(clippy::cast_ptr_alignment)]
#![deny(unused_must_use)]
#![allow(clippy::derive_partial_eq_without_eq)]

use std::cmp::Ordering;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::slice;

use once_cell::sync::OnceCell;
use relay_common::glob::{glob_match_bytes, GlobOptions};
use relay_dynamic_config::{normalize_json, validate_json, GlobalConfig, ProjectConfig};
use relay_event_normalization::{
    GeoIpLookup, NormalizeProcessor, NormalizeProcessorConfig, RawUserAgentInfo, StoreConfig,
    StoreProcessor,
};
use relay_event_schema::processor::{process_value, split_chunks, ProcessingState};
use relay_event_schema::protocol::{Event, VALID_PLATFORMS};
use relay_pii::{
    selector_suggestions_from_value, DataScrubbingConfig, InvalidSelectorError, PiiConfig,
    PiiConfigError, PiiProcessor, SelectorSpec,
};
use relay_protocol::{Annotated, Remark, RuleCondition};
use relay_sampling::SamplingConfig;

use crate::core::{RelayBuf, RelayStr};

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
    static VALID_PLATFORM_STRS: OnceCell<Vec<RelayStr>> = OnceCell::new();
    let platforms = VALID_PLATFORM_STRS
        .get_or_init(|| VALID_PLATFORMS.iter().map(|s| RelayStr::new(s)).collect());

    if let Some(size_out) = size_out.as_mut() {
        *size_out = platforms.len();
    }

    platforms.as_ptr()
}

/// Creates a new normalization processor.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_store_normalizer_new(
    config: *const RelayStr,
    geoip_lookup: *const RelayGeoIpLookup,
) -> *mut RelayStoreNormalizer {
    let config: StoreConfig = serde_json::from_str((*config).as_str())?;
    let geoip_lookup = (geoip_lookup as *const GeoIpLookup).as_ref();
    let normalizer = StoreProcessor::new(config, geoip_lookup);
    Box::into_raw(Box::new(normalizer)) as *mut RelayStoreNormalizer
}

/// Frees a `RelayStoreNormalizer`.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_store_normalizer_free(normalizer: *mut RelayStoreNormalizer) {
    if !normalizer.is_null() {
        let normalizer = normalizer as *mut StoreProcessor;
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
    let processor = normalizer as *mut StoreProcessor;
    let mut event = Annotated::<Event>::from_json((*event).as_str())?;
    let config = (*processor).config();
    let normalization_config = NormalizeProcessorConfig {
        client_ip: config.client_ip.as_ref(),
        user_agent: RawUserAgentInfo {
            user_agent: config.user_agent.as_deref(),
            client_hints: config.client_hints.as_deref(),
        },
        max_name_and_unit_len: None,
        received_at: config.received_at,
        max_secs_in_past: config.max_secs_in_past,
        max_secs_in_future: config.max_secs_in_future,
        transaction_range: None, // only supported in relay
        breakdowns_config: None, // only supported in relay
        normalize_user_agent: config.normalize_user_agent,
        transaction_name_config: Default::default(), // only supported in relay
        is_renormalize: config.is_renormalize.unwrap_or(false),
        device_class_synthesis_config: false, // only supported in relay
        enrich_spans: false,
        light_normalize_spans: false,
        max_tag_value_length: usize::MAX,
        span_description_rules: None,
        performance_score: None,
        geoip_lookup: None, // only supported in relay
        enable_trimming: config.enable_trimming.unwrap_or_default(),
        measurements: None,
    };
    process_value(
        &mut event,
        &mut NormalizeProcessor::new(normalization_config),
        ProcessingState::root(),
    )?;
    process_value(&mut event, &mut *processor, ProcessingState::root())?;
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

/// Validate entire project config.
///
/// If `strict` is true, checks for unknown fields in the input.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_validate_project_config(
    value: *const RelayStr,
    strict: bool,
) -> RelayStr {
    let value = (*value).as_str();
    match validate_json::<ProjectConfig>(value, strict) {
        Ok(()) => RelayStr::default(),
        Err(e) => RelayStr::from_string(e.to_string()),
    }
}

/// Normalize a global config.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn normalize_global_config(value: *const RelayStr) -> RelayStr {
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
