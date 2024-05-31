use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::*;
use relay_cardinality::CardinalityLimit;
use sentry_release_parser::{InvalidRelease, Release, Version};
use serde::{Deserialize, Serialize};
use serde_pyobject::{from_pyobject, to_pyobject};
use uuid::Uuid;

use relay_common::glob::{glob_match_bytes, GlobOptions};
use relay_dynamic_config::{GlobalConfig, ProjectConfig};
use relay_event_normalization::{
    validate_event_timestamps, validate_transaction, BreakdownsConfig, ClientHints,
    EventValidationConfig, GeoIpLookup, NormalizationConfig, RawUserAgentInfo,
    TransactionValidationConfig,
};
use relay_event_schema::processor::{process_value, ProcessingState};
use relay_event_schema::protocol::{Event, IpAddr, VALID_PLATFORMS};
use relay_pii::{
    selector_suggestions_from_value, DataScrubbingConfig, InvalidSelectorError, PiiConfig,
    PiiConfigError, PiiProcessor, SelectorSpec,
};
use relay_protocol::{Annotated, Meta, Remark, RuleCondition};
use relay_sampling::SamplingConfig;

use crate::codeowners::{translate_codeowners_pattern, CODEOWNERS_CACHE};
use crate::exceptions::{
    InvalidReleaseErrorBadCharacters, InvalidReleaseErrorRestrictedName, InvalidReleaseErrorTooLong,
};
use crate::utils;

#[derive(Default, Serialize, Deserialize, Debug, Clone)]
#[pyclass]
pub struct PyClientHintsString {
    /// The client's OS, e.g. macos, android...
    pub sec_ch_ua_platform: Option<String>,
    /// The version number of the client's OS.
    pub sec_ch_ua_platform_version: Option<String>,
    /// Name of the client's web browser and its version.
    pub sec_ch_ua: Option<String>,
    /// Device model, e.g. samsung galaxy 3.
    pub sec_ch_ua_model: Option<String>,
}

impl From<PyClientHintsString> for ClientHints<String> {
    fn from(value: PyClientHintsString) -> Self {
        Self {
            sec_ch_ua_platform: value.sec_ch_ua_platform,
            sec_ch_ua_platform_version: value.sec_ch_ua_platform_version,
            sec_ch_ua: value.sec_ch_ua,
            sec_ch_ua_model: value.sec_ch_ua_model,
        }
    }
}

/// Configuration for the store step -- validation and normalization.
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(default)]
#[pyclass]
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
    pub grouping_config: Option<String>,

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
    pub client_hints: PyClientHintsString,

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
    pub replay_id: Option<String>,

    /// Controls whether spans should be normalized (e.g. normalizing the exclusive time).
    ///
    /// To normalize spans in [`normalize_event`], `is_renormalize` must
    /// be disabled _and_ `normalize_spans` enabled.
    pub normalize_spans: bool,
}

#[pymethods]
#[allow(clippy::too_many_arguments)]
impl StoreNormalizer {
    #[new]
    #[pyo3(signature = (
        client_hints = None,
        project_id = None,
        client_ip = None,
        client = None,
        key_id = None,
        protocol_version = None,
        grouping_config = None,
        user_agent = None,
        received_at = None,
        sent_at = None,
        max_secs_in_future = None,
        max_secs_in_past = None,
        enable_trimming = None,
        is_renormalize = None,
        remove_other = None,
        normalize_user_agent = None,
        breakdowns = None,
        client_sample_rate = None,
        replay_id = None,
        normalize_spans = None
    ))]
    fn new(
        client_hints: Option<PyClientHintsString>,
        project_id: Option<u64>,
        client_ip: Option<IpAddr>,
        client: Option<String>,
        key_id: Option<String>,
        protocol_version: Option<String>,
        grouping_config: Option<String>,
        user_agent: Option<String>,
        received_at: Option<DateTime<Utc>>,
        sent_at: Option<DateTime<Utc>>,
        max_secs_in_future: Option<i64>,
        max_secs_in_past: Option<i64>,
        enable_trimming: Option<bool>,
        is_renormalize: Option<bool>,
        remove_other: Option<bool>,
        normalize_user_agent: Option<bool>,
        breakdowns: Option<BreakdownsConfig>,
        client_sample_rate: Option<f64>,
        replay_id: Option<String>,
        normalize_spans: Option<bool>,
    ) -> Self {
        Self {
            project_id,
            client_ip,
            client,
            key_id,
            protocol_version,
            grouping_config,
            user_agent,
            client_hints: client_hints.unwrap_or_default(),
            received_at,
            sent_at,
            max_secs_in_future,
            max_secs_in_past,
            enable_trimming,
            is_renormalize,
            remove_other,
            normalize_user_agent,
            breakdowns,
            client_sample_rate,
            replay_id,
            normalize_spans: normalize_spans.unwrap_or_default(),
        }
    }

    #[pyo3(signature = (event = None, raw_event = None))]
    pub fn normalize_event<'a>(
        &self,
        event: Option<&Bound<'a, PyAny>>,
        raw_event: Option<&Bound<'a, PyAny>>, // bytes | str
    ) -> PyResult<PyObject> {
        let raw_event = raw_event
            .or(event)
            .expect("Event should exist if raw_event is None");
        let event = raw_event.downcast::<PyString>()?;
        let mut data = event.to_string().into_bytes();
        json_forensics::translate_slice(&mut data);
        let mut event: Annotated<Event> =
            Annotated::<Event>::from_json(std::str::from_utf8(&data)?)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;

        let event_validation_config = EventValidationConfig {
            received_at: self.received_at,
            max_secs_in_past: self.max_secs_in_past,
            max_secs_in_future: self.max_secs_in_future,
            is_validated: self.is_renormalize.unwrap_or(false),
        };
        validate_event_timestamps(&mut event, &event_validation_config)?;

        let tx_validation_config = TransactionValidationConfig {
            timestamp_range: None, // only supported in relay
            is_validated: self.is_renormalize.unwrap_or(false),
        };
        validate_transaction(&mut event, &tx_validation_config)?;

        let is_renormalize = self.is_renormalize.unwrap_or(false);

        let normalization_config = NormalizationConfig {
            project_id: self.project_id,
            client: self.client.clone(),
            protocol_version: self.protocol_version.clone(),
            key_id: self.key_id.clone(),
            grouping_config: self
                .grouping_config
                .as_deref()
                .and_then(|gc| serde_json::from_str(gc).ok()),
            client_ip: self.client_ip.as_ref(),
            client_sample_rate: self.client_sample_rate,
            user_agent: RawUserAgentInfo {
                user_agent: self.user_agent.as_deref(),
                client_hints: ClientHints {
                    sec_ch_ua: self.client_hints.sec_ch_ua.as_deref(),
                    sec_ch_ua_model: self.client_hints.sec_ch_ua_model.as_deref(),
                    sec_ch_ua_platform: self.client_hints.sec_ch_ua_platform.as_deref(),
                    sec_ch_ua_platform_version: self
                        .client_hints
                        .sec_ch_ua_platform_version
                        .as_deref(),
                },
            },
            max_name_and_unit_len: None,
            breakdowns_config: None, // only supported in relay
            normalize_user_agent: self.normalize_user_agent,
            transaction_name_config: Default::default(), // only supported in relay
            is_renormalize,
            remove_other: self.remove_other.unwrap_or(!is_renormalize),
            emit_event_errors: !is_renormalize,
            device_class_synthesis_config: false, // only supported in relay
            enrich_spans: false,
            max_tag_value_length: usize::MAX,
            span_description_rules: None,
            performance_score: None,
            geoip_lookup: None,   // only supported in relay
            ai_model_costs: None, // only supported in relay
            enable_trimming: self.enable_trimming.unwrap_or_default(),
            measurements: None,
            normalize_spans: self.normalize_spans,
            // SAFETY: Unwrap is used instead of `.ok()` since we know it's a valid UUID.
            replay_id: self
                .replay_id
                .as_deref()
                .map(|id| Uuid::parse_str(id).unwrap()),
        };
        relay_event_normalization::normalize_event(&mut event, &normalization_config);
        Python::with_gil(|py| Ok(PyAnnotatedEvent::from(event).into_py(py)))
    }
}

#[pyfunction]
pub fn validate_sampling_configuration(condition: &Bound<'_, PyAny>) -> PyResult<()> {
    let input = utils::extract_bytes_or_str(condition)?;

    match serde_json::from_str::<SamplingConfig>(input) {
        Ok(config) => {
            for rule in config.rules {
                if !rule.condition.supported() {
                    return Err(PyValueError::new_err("unsupported sampling rule"));
                }
            }
            Ok(())
        }
        Err(e) => Err(PyValueError::new_err(e.to_string())),
    }
}

#[pyfunction]
pub fn compare_versions<'a>(a: &Bound<'a, PyString>, b: &Bound<'a, PyString>) -> PyResult<i32> {
    let ver_a = Version::parse(a.to_str()?).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let ver_b = Version::parse(b.to_str()?).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(match ver_a.cmp(&ver_b) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    })
}

#[pyfunction]
pub fn validate_pii_selector(selector: &Bound<'_, PyString>) -> PyResult<()> {
    let value = selector.to_str()?;
    if let Err(err) = value.parse::<SelectorSpec>() {
        return match err {
            InvalidSelectorError::ParseError(_) => Err(PyValueError::new_err(format!(
                "invalid syntax near {value:?}"
            ))),
            _ => Err(PyValueError::new_err(err.to_string()))?,
        };
    }

    Ok(())
}

#[pyfunction]
pub fn validate_pii_config(config: &Bound<'_, PyString>) -> PyResult<()> {
    match serde_json::from_str::<PiiConfig>(config.to_str()?) {
        Ok(config) => match config.compiled().force_compile() {
            Ok(_) => Ok(()),
            Err(PiiConfigError::RegexError(source)) => {
                Err(PyValueError::new_err(source.to_string()))
            }
        },
        Err(e) => Err(PyValueError::new_err(e.to_string())),
    }
}

#[pyfunction]
pub fn validate_rule_condition(condition: &Bound<'_, PyString>) -> PyResult<()> {
    match serde_json::from_str::<RuleCondition>(condition.to_str()?) {
        Ok(condition) => {
            if condition.supported() {
                Ok(())
            } else {
                Err(PyValueError::new_err("unsupported condition"))
            }
        }
        Err(e) => Err(PyValueError::new_err(e.to_string())),
    }
}

/// @deprecated
#[pyfunction]
pub fn validate_sampling_condition(condition: &Bound<'_, PyString>) -> PyResult<()> {
    validate_rule_condition(condition)
}

#[pyfunction]
pub fn is_codeowners_path_match<'a>(
    value: &Bound<'a, PyBytes>,
    pattern: &Bound<'a, PyString>,
) -> PyResult<bool> {
    let value = value.as_bytes();
    let pat = pattern.to_str()?;

    let mut cache = CODEOWNERS_CACHE.lock().unwrap();

    if let Some(pattern) = cache.get(pat) {
        Ok(pattern.is_match(value))
    } else if let Some(pattern) = translate_codeowners_pattern(pat) {
        let result = pattern.is_match(value);
        cache.put(pat.to_owned(), pattern);
        Ok(result)
    } else {
        Ok(false)
    }
}

#[pyfunction]
pub fn split_chunks<'a>(
    input: &Bound<'a, PyString>,
    remarks: &Bound<'a, PyList>,
) -> PyResult<Bound<'a, PyAny>> {
    let py = input.py();
    let remarks = remarks
        .iter()
        .map(|item| from_pyobject(item).unwrap())
        .collect::<Vec<Remark>>();
    let input = input.to_str()?;
    let chunks = relay_event_schema::processor::split_chunks(input, &remarks);
    Ok(to_pyobject(py, &chunks)?)
}

#[derive(Clone, Debug)]
#[pyclass(name = "AnnotatedEvent")]
pub struct PyAnnotatedEvent(pub Option<Event>, pub Meta);

impl From<Annotated<Event>> for PyAnnotatedEvent {
    fn from(value: Annotated<Event>) -> Self {
        Self(value.0, value.1)
    }
}

#[pyfunction]
pub fn pii_strip_event<'a>(
    config: &Bound<'a, PiiConfig>,
    event: &Bound<'a, PyString>,
) -> PyResult<PyObject> {
    let borrowed_config = config.borrow();
    let mut processor = PiiProcessor::new(borrowed_config.compiled());

    let mut event = Annotated::<Event>::from_json(event.to_str()?)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    process_value(&mut event, &mut processor, ProcessingState::root())?;
    Python::with_gil(|py| Ok(PyAnnotatedEvent::from(event).into_py(py)))
}

#[derive(Debug, Clone, Eq, PartialEq)]
#[pyclass(name = "Version")]
pub struct PyVersion {
    major: String,
    minor: String,
    patch: String,
    revision: String,
    pre: String,
    build_code: String,
    raw_short: String,
    components: u8,
    raw_quad: (String, Option<String>, Option<String>, Option<String>),
}

impl From<&Version<'_>> for PyVersion {
    fn from(value: &Version<'_>) -> Self {
        let (major, minor, patch, revision) = value.raw_quad();
        Self {
            major: value.major().to_string(),
            minor: value.minor().to_string(),
            patch: value.patch().to_string(),
            revision: value.revision().to_string(),
            pre: value.pre().map(String::from).unwrap_or_default(),
            build_code: value.build_code().map(String::from).unwrap_or_default(),
            raw_short: value.raw_short().to_string(),
            components: value.components(),
            raw_quad: (
                major.to_string(),
                minor.map(String::from),
                patch.map(String::from),
                revision.map(String::from),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[pyclass(name = "Release")]
pub struct PyRelease {
    raw: String,
    package: String,
    version_raw: String,
    version: Option<PyVersion>,
}

impl From<Release<'_>> for PyRelease {
    fn from(value: Release<'_>) -> Self {
        Self {
            raw: value.raw().to_string(),
            package: value.package().unwrap_or_default().to_string(),
            version_raw: value.version_raw().to_string(),
            version: value.version().map(PyVersion::from),
        }
    }
}

#[pyfunction]
pub fn parse_release<'a>(release: &Bound<'a, PyString>) -> PyResult<Bound<'a, PyAny>> {
    let parsed_release = Release::parse(release.to_str()?).map_err(|e| {
        let e: PyErr = match e {
            InvalidRelease::TooLong => InvalidReleaseErrorTooLong::new().into(),
            InvalidRelease::RestrictedName => InvalidReleaseErrorRestrictedName::new().into(),
            InvalidRelease::BadCharacters => InvalidReleaseErrorBadCharacters::new().into(),
        };
        e
    })?;
    Ok(to_pyobject(release.py(), &parsed_release)?)
}

#[pyfunction]
pub fn normalize_global_config<'a>(config: &Bound<'a, PyAny>) -> PyResult<Bound<'a, PyAny>> {
    let value: GlobalConfig =
        from_pyobject(config.to_owned()).map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok(to_pyobject(config.py(), &value)?)
}

#[pyfunction]
pub fn pii_selector_suggestions_from_event(event: &Bound<'_, PyAny>) -> PyResult<PyObject> {
    let event = event.extract::<PyAnnotatedEvent>()?;
    let mut annotated_event = Annotated::<Event>::new(event.0.unwrap_or_default());
    let rv = selector_suggestions_from_value(&mut annotated_event);
    Python::with_gil(|py| Ok(rv.into_py(py)))
}

#[pyfunction]
pub fn convert_datascrubbing_config<'a>(config: &Bound<'a, PyAny>) -> PyResult<Bound<'a, PyAny>> {
    let py = config.py();
    let dsc: DataScrubbingConfig =
        from_pyobject(config.to_owned()).map_err(|e| PyValueError::new_err(e.to_string()))?;
    match dsc.pii_config() {
        Ok(Some(config)) => Ok(to_pyobject(py, &config)?),
        // Return an empty object: "{}"
        Ok(None) => Ok(HashMap::<String, String>::new()
            .into_py_dict_bound(py)
            .into_any()),
        // NOTE: Callers of this function must be able to handle this error.
        Err(e) => Err(PyValueError::new_err(e.to_string())),
    }
}

#[pyfunction]
pub fn init_valid_platforms() -> HashSet<&'static str> {
    VALID_PLATFORMS.iter().copied().collect()
}

#[pyfunction]
#[pyo3(signature = (value, pat, double_star=false, case_insensitive=false, path_normalize=false, allow_newline=false))]
pub fn is_glob_match<'a>(
    value: &Bound<'a, PyString>,
    pat: &Bound<'a, PyString>,
    double_star: bool,
    case_insensitive: bool,
    path_normalize: bool,
    allow_newline: bool,
) -> PyResult<bool> {
    let options = GlobOptions {
        double_star,
        case_insensitive,
        path_normalize,
        allow_newline,
    };
    Ok(glob_match_bytes(
        value.to_str()?.as_bytes(),
        pat.to_str()?,
        options,
    ))
}

#[pyfunction]
pub fn meta_with_chunks<'py>(
    data: &Bound<'py, PyAny>,
    meta: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let py = data.py();
    if !meta.is_instance_of::<PyDict>() {
        return Ok(meta.clone());
    }

    let meta = meta.downcast::<PyDict>()?;
    let mut result: HashMap<String, Bound<'py, PyAny>> = HashMap::new();

    for (key, item) in meta {
        let key = key.downcast::<PyString>()?;
        if key.is_empty()? && item.is_instance_of::<PyDict>() {
            let item = item.downcast::<PyDict>()?;
            result.insert("".to_string(), item.clone().into_any());

            if let Some(rem) = item
                .get_item("rem")?
                .and_then(|r| r.downcast::<PyList>().ok().cloned())
            {
                if let Ok(data) = data.downcast::<PyString>() {
                    let chunks = split_chunks(data, &rem)?;
                    if let Some(empty_item) = result.get_mut("") {
                        empty_item.set_item("chunks", chunks)?;
                    }
                }
            }
            let rem = item.get_item("rem")?;
            if rem.is_some() && data.is_instance_of::<PyString>() {}
        } else if let Ok(data) = data.downcast::<PyDict>() {
            let current_item = data
                .get_item(key)?
                .expect("Current item should have existed");
            result.insert(key.to_string(), meta_with_chunks(&current_item, &item)?);
        } else if let Ok(data) = data.downcast::<PyList>() {
            let int_key = key.to_str()?.parse::<usize>()?;
            match data.get_item(int_key) {
                Ok(val) => {
                    result.insert(key.to_string(), meta_with_chunks(&val, &item)?);
                }
                Err(_) => {
                    result.insert(
                        key.to_string(),
                        meta_with_chunks(&PyNone::get_bound(py), &item)?,
                    );
                }
            }
        } else {
            result.insert(key.to_string(), item.into_any());
        }
    }

    Ok(result.into_py_dict_bound(data.py()).into_any())
}

#[pyfunction]
fn normalize_cardinality_limit_config<'a>(config: &Bound<'a, PyAny>) -> PyResult<Bound<'a, PyAny>> {
    let obj: CardinalityLimit =
        from_pyobject(config.to_owned()).map_err(|e| PyValueError::new_err(e.to_string()))?;

    Ok(to_pyobject(config.py(), &obj)?)
}

#[pyfunction]
fn normalize_project_config<'a>(config: &Bound<'a, PyAny>) -> PyResult<Bound<'a, PyAny>> {
    let obj: ProjectConfig =
        from_pyobject(config.to_owned()).map_err(|e| PyValueError::new_err(e.to_string()))?;

    Ok(to_pyobject(config.py(), &obj)?)
}

#[pymodule]
pub fn processing(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(validate_sampling_configuration, m)?)?;
    m.add_function(wrap_pyfunction!(compare_versions, m)?)?;
    m.add_function(wrap_pyfunction!(validate_pii_selector, m)?)?;
    m.add_function(wrap_pyfunction!(validate_pii_config, m)?)?;
    m.add_function(wrap_pyfunction!(validate_rule_condition, m)?)?;
    m.add_function(wrap_pyfunction!(validate_sampling_condition, m)?)?;
    m.add_function(wrap_pyfunction!(is_codeowners_path_match, m)?)?;
    m.add_function(wrap_pyfunction!(split_chunks, m)?)?;
    m.add_function(wrap_pyfunction!(pii_strip_event, m)?)?;
    m.add_function(wrap_pyfunction!(parse_release, m)?)?;
    m.add_function(wrap_pyfunction!(normalize_global_config, m)?)?;
    m.add_function(wrap_pyfunction!(pii_selector_suggestions_from_event, m)?)?;
    m.add_function(wrap_pyfunction!(convert_datascrubbing_config, m)?)?;
    m.add_function(wrap_pyfunction!(init_valid_platforms, m)?)?;
    m.add_function(wrap_pyfunction!(is_glob_match, m)?)?;
    m.add_function(wrap_pyfunction!(meta_with_chunks, m)?)?;
    m.add_function(wrap_pyfunction!(normalize_cardinality_limit_config, m)?)?;
    m.add_function(wrap_pyfunction!(normalize_project_config, m)?)?;

    m.add("VALID_PLATFORMS", init_valid_platforms())?;

    m.add_class::<GeoIpLookup>()?;
    m.add_class::<StoreNormalizer>()?;
    Ok(())
}
