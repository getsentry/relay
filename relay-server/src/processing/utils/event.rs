//! Common functionality for transaction and event processing.

//! Event processor related code.

use std::fmt::Debug;
use std::sync::OnceLock;

use chrono::Duration as SignedDuration;
use relay_auth::RelayVersion;
use relay_base_schema::events::EventType;
use relay_base_schema::project::ProjectId;
use relay_config::Config;
use relay_config::NormalizationLevel;
use relay_dynamic_config::Feature;
use relay_event_normalization::GeoIpLookup;
use relay_event_normalization::{
    ClockDriftProcessor, normalize_event as normalize_event_inner, validate_event,
};
use relay_event_normalization::{
    CombinedMeasurementsConfig, EventValidationConfig, MeasurementsConfig, NormalizationConfig,
    RawUserAgentInfo, TransactionNameConfig,
};
use relay_event_schema::processor::{self, ProcessingState};
use relay_event_schema::protocol::IpAddr;
use relay_event_schema::protocol::{Event, Metrics, OtelContext, RelayInfo};
use relay_filter::FilterStatKey;
use relay_metrics::MetricNamespace;
use relay_pii::PiiProcessor;
use relay_protocol::Annotated;
use relay_protocol::Empty;
use relay_quotas::DataCategory;
use relay_statsd::metric;

use crate::constants::DEFAULT_EVENT_RETENTION;
use crate::envelope::{Envelope, EnvelopeHeaders, Item};
use crate::processing::Context;
use crate::services::processor::{MINIMUM_CLOCK_DRIFT, ProcessingError};
use crate::services::projects::project::ProjectInfo;
use crate::statsd::{RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{self};

/// Returns the data category if there is an event.
///
/// The data category is computed from the event type. Both `Default` and `Error` events map to
/// the `Error` data category. If there is no Event, `None` is returned.
pub fn event_category(event: &Annotated<Event>) -> Option<DataCategory> {
    event_type(event).map(DataCategory::from)
}

/// Returns the event type if there is an event.
///
/// If the event does not have a type, `Some(EventType::Default)` is assumed. If, in contrast, there
/// is no event, `None` is returned.
pub fn event_type(event: &Annotated<Event>) -> Option<EventType> {
    event
        .value()
        .map(|event| event.ty.value().copied().unwrap_or_default())
}

pub fn finalize<'a>(
    headers: &EnvelopeHeaders,
    event: &mut Annotated<Event>,
    attachments: impl Iterator<Item = &'a Item>,
    metrics: &mut Metrics,
    config: &Config,
) -> Result<(), ProcessingError> {
    let inner_event = match event.value_mut() {
        Some(event) => event,
        None if !config.processing_enabled() => return Ok(()),
        None => return Err(ProcessingError::NoEventPayload),
    };

    if !config.processing_enabled() {
        static MY_VERSION_STRING: OnceLock<String> = OnceLock::new();
        let my_version = MY_VERSION_STRING.get_or_init(|| RelayVersion::current().to_string());

        inner_event
            .ingest_path
            .get_or_insert_with(Default::default)
            .push(Annotated::new(RelayInfo {
                version: Annotated::new(my_version.clone()),
                public_key: config
                    .public_key()
                    .map_or(Annotated::empty(), |pk| Annotated::new(pk.to_string())),
                other: Default::default(),
            }));
    }

    // Event id is set statically in the ingest path.
    let event_id = headers.event_id().unwrap_or_default();
    debug_assert!(!event_id.is_nil());

    // Ensure that the event id in the payload is consistent with the envelope. If an event
    // id was ingested, this will already be the case. Otherwise, this will insert a new
    // event id. To be defensive, we always overwrite to ensure consistency.
    inner_event.id = Annotated::new(event_id);

    // In processing mode, also write metrics into the event. Most metrics have already been
    // collected at this state, except for the combined size of all attachments.
    if config.processing_enabled() {
        let mut metrics = std::mem::take(metrics);

        let attachment_size = attachments.map(|item| item.len() as u64).sum::<u64>();

        if attachment_size > 0 {
            metrics.bytes_ingested_event_attachment = Annotated::new(attachment_size);
        }

        inner_event._metrics = Annotated::new(metrics);

        if inner_event.ty.value() == Some(&EventType::Transaction) {
            let platform = utils::platform_tag(inner_event);
            let client_name = utils::client_name_tag(headers.meta().client_name());

            metric!(
                counter(RelayCounters::EventTransaction) += 1,
                source = utils::transaction_source_tag(inner_event),
                platform = platform,
                contains_slashes = if inner_event
                    .transaction
                    .as_str()
                    .unwrap_or_default()
                    .contains('/')
                {
                    "true"
                } else {
                    "false"
                }
            );

            let span_count = inner_event.spans.value().map(Vec::len).unwrap_or(0) as u64;
            metric!(
                histogram(RelayHistograms::EventSpans) = span_count,
                sdk = client_name,
                platform = platform,
            );

            let has_otel = inner_event
                .contexts
                .value()
                .is_some_and(|contexts| contexts.contains::<OtelContext>());

            if has_otel {
                metric!(
                    counter(RelayCounters::OpenTelemetryEvent) += 1,
                    sdk = client_name,
                    platform = platform,
                );
            }
        }

        if let Some(dsc) = headers.dsc()
            && let Ok(Some(value)) = relay_protocol::to_value(dsc)
        {
            inner_event._dsc = Annotated::new(value);
        }
    }

    let mut processor = ClockDriftProcessor::new(headers.sent_at(), headers.meta().received_at())
        .at_least(MINIMUM_CLOCK_DRIFT);
    processor::process_value(event, &mut processor, ProcessingState::root()).map_err(|err| {
        relay_log::debug!(
            error = &err as &dyn std::error::Error,
            "invalid transaction"
        );
        ProcessingError::InvalidTransaction
    })?;

    // Log timestamp delays for all events after clock drift correction. This happens before
    // store processing, which could modify the timestamp if it exceeds a threshold. We are
    // interested in the actual delay before this correction.
    if let Some(timestamp) = event.value().and_then(|e| e.timestamp.value()) {
        let event_delay = headers.meta().received_at() - timestamp.into_inner();
        if event_delay > SignedDuration::minutes(1) {
            let category = event_category(event).unwrap_or(DataCategory::Unknown);
            metric!(
                timer(RelayTimers::TimestampDelay) = event_delay.to_std().unwrap(),
                category = category.name(),
            );
        }
    }

    Ok(())
}

/// Performs event normalization and surrounding bookkeeping.
pub fn normalize(
    headers: &EnvelopeHeaders,
    event: &mut Annotated<Event>,
    mut event_fully_normalized: EventFullyNormalized,
    project_id: ProjectId,
    ctx: Context,
    geoip_lookup: &GeoIpLookup,
) -> Result<EventFullyNormalized, ProcessingError> {
    if event.value().is_empty() {
        // NOTE(iker): only processing relays create events from
        // attachments, so these events won't be normalized in
        // non-processing relays even if the config is set to run full
        // normalization.
        return Ok(event_fully_normalized);
    }

    let full_normalization = match ctx.config.normalization_level() {
        NormalizationLevel::Full => true,
        NormalizationLevel::Default => {
            if ctx.config.processing_enabled() && event_fully_normalized.0 {
                return Ok(event_fully_normalized);
            }

            ctx.config.processing_enabled()
        }
    };

    let request_meta = headers.meta();
    let client_ipaddr = request_meta.client_addr().map(IpAddr::from);

    let transaction_aggregator_config = ctx
        .config
        .aggregator_config_for(MetricNamespace::Transactions);

    let ai_model_costs = ctx.global_config.ai_model_costs.as_ref().ok();
    let ai_operation_type_map = ctx.global_config.ai_operation_type_map.as_ref().ok();
    let http_span_allowed_hosts = ctx.global_config.options.http_span_allowed_hosts.as_slice();

    let project_info = ctx.project_info;
    let retention_days: i64 = project_info
        .config
        .event_retention
        .unwrap_or(DEFAULT_EVENT_RETENTION)
        .into();

    utils::log_transaction_name_metrics(event, |event| {
        let event_validation_config = EventValidationConfig {
            received_at: Some(headers.meta().received_at()),
            max_secs_in_past: Some(retention_days * 24 * 3600),
            max_secs_in_future: Some(ctx.config.max_secs_in_future()),
            transaction_timestamp_range: Some(transaction_aggregator_config.timestamp_range()),
            is_validated: false,
        };

        let key_id = project_info
            .get_public_key_config()
            .and_then(|key| Some(key.numeric_id?.to_string()));
        if full_normalization && key_id.is_none() {
            relay_log::error!(
                "project state for key {} is missing key id",
                headers.meta().public_key()
            );
        }

        let normalization_config = NormalizationConfig {
            project_id: Some(project_id.value()),
            client: request_meta.client().map(str::to_owned),
            key_id,
            protocol_version: Some(request_meta.version().to_string()),
            grouping_config: project_info.config.grouping_config.clone(),
            client_ip: client_ipaddr.as_ref(),
            // if the setting is enabled we do not want to infer the ip address
            infer_ip_address: !project_info
                .config
                .datascrubbing_settings
                .scrub_ip_addresses,
            client_sample_rate: headers.dsc().and_then(|ctx| ctx.sample_rate),
            user_agent: RawUserAgentInfo {
                user_agent: request_meta.user_agent(),
                client_hints: request_meta.client_hints(),
            },
            max_name_and_unit_len: Some(
                transaction_aggregator_config
                    .max_name_length
                    .saturating_sub(MeasurementsConfig::MEASUREMENT_MRI_OVERHEAD),
            ),
            breakdowns_config: project_info.config.breakdowns_v2.as_ref(),
            performance_score: project_info.config.performance_score.as_ref(),
            normalize_user_agent: Some(true),
            transaction_name_config: TransactionNameConfig {
                rules: &project_info.config.tx_name_rules,
            },
            device_class_synthesis_config: project_info.has_feature(Feature::DeviceClassSynthesis),
            enrich_spans: true,
            max_tag_value_length: ctx
                .config
                .aggregator_config_for(MetricNamespace::Spans)
                .max_tag_value_length,
            is_renormalize: false,
            remove_other: full_normalization,
            emit_event_errors: full_normalization,
            span_description_rules: project_info.config.span_description_rules.as_ref(),
            geoip_lookup: Some(geoip_lookup),
            ai_model_costs,
            ai_operation_type_map,
            enable_trimming: true,
            measurements: Some(CombinedMeasurementsConfig::new(
                ctx.project_info.config().measurements.as_ref(),
                ctx.global_config.measurements.as_ref(),
            )),
            normalize_spans: true,
            replay_id: headers.dsc().and_then(|ctx| ctx.replay_id),
            span_allowed_hosts: http_span_allowed_hosts,
            span_op_defaults: ctx.global_config.span_op_defaults.borrow(),
            performance_issues_spans: ctx
                .project_info
                .has_feature(Feature::PerformanceIssuesSpans),
        };

        metric!(timer(RelayTimers::EventProcessingNormalization), {
            validate_event(event, &event_validation_config).map_err(|err| {
                relay_log::debug!(
                    error = &err as &dyn std::error::Error,
                    "invalid transaction"
                );
                ProcessingError::InvalidTransaction
            })?;
            normalize_event_inner(event, &normalization_config);
            if full_normalization && has_unprintable_fields(event) {
                metric!(counter(RelayCounters::EventCorrupted) += 1);
            }
            Result::<(), ProcessingError>::Ok(())
        })
    })?;

    event_fully_normalized.0 |= full_normalization;

    Ok(event_fully_normalized)
}

/// Status for applying some filters that don't drop the event.
///
/// The enum represents either the success of running all filters and keeping
/// the event, [`FiltersStatus::Ok`], or not running all the filters because
/// some are unsupported, [`FiltersStatus::Unsupported`].
///
/// If there are unsuppported filters, Relay should forward the event upstream
/// so that a more up-to-date Relay can apply filters appropriately. Actions
/// that depend on the outcome of event filtering, such as metric extraction,
/// should be postponed until a filtering decision is made.
#[must_use]
pub enum FiltersStatus {
    /// All filters have been applied and the event should be kept.
    Ok,
    /// Some filters are not supported and were not applied.
    ///
    /// Relay should forward events upstream for a more up-to-date Relay to apply these filters.
    /// Supported filters were applied and they don't reject the event.
    Unsupported,
}

pub fn filter(
    headers: &EnvelopeHeaders,
    event: &Annotated<Event>,
    ctx: &Context,
) -> Result<FiltersStatus, FilterStatKey> {
    let event = match event.value() {
        Some(event) => event,
        // Some events are created by processing relays (e.g. unreal), so they do not yet
        // exist at this point in non-processing relays.
        None => return Ok(FiltersStatus::Ok),
    };

    let client_ip = headers.meta().client_addr();
    let filter_settings = &ctx.project_info.config.filter_settings;

    metric!(timer(RelayTimers::EventProcessingFiltering), {
        relay_filter::should_filter(
            event,
            client_ip,
            filter_settings,
            ctx.global_config.filters(),
        )
    })?;

    // Don't extract metrics if relay can't apply generic filters.  A filter
    // applied in another up-to-date relay in chain may need to drop the event,
    // and there should not be metrics from dropped events.
    // In processing relays, always extract metrics to avoid losing them.
    let supported_generic_filters = ctx.global_config.filters.is_ok()
        && relay_filter::are_generic_filters_supported(
            ctx.global_config.filters().map(|f| f.version),
            ctx.project_info.config.filter_settings.generic.version,
        );
    if supported_generic_filters {
        Ok(FiltersStatus::Ok)
    } else {
        Ok(FiltersStatus::Unsupported)
    }
}

/// New type representing the normalization state of the event.
#[derive(Copy, Clone)]
pub struct EventFullyNormalized(pub bool);

impl EventFullyNormalized {
    /// Returns `true` if the event is fully normalized, `false` otherwise.
    pub fn new(envelope: &Envelope) -> Self {
        let event_fully_normalized = envelope.meta().request_trust().is_trusted()
            && envelope
                .items()
                .any(|item| item.creates_event() && item.fully_normalized());

        Self(event_fully_normalized)
    }
}

/// New type representing whether metrics were extracted from transactions/spans.
#[derive(Debug, Copy, Clone)]
pub struct EventMetricsExtracted(pub bool);

/// New type representing whether spans were extracted.
#[derive(Debug, Copy, Clone)]
pub struct SpansExtracted(pub bool);

/// Checks if the Event includes unprintable fields.
fn has_unprintable_fields(event: &Annotated<Event>) -> bool {
    fn is_unprintable(value: &&str) -> bool {
        value.chars().any(|c| {
            c == '\u{fffd}' // unicode replacement character
                || (c.is_control() && !c.is_whitespace()) // non-whitespace control characters
        })
    }
    if let Some(event) = event.value() {
        let env = event.environment.as_str().filter(is_unprintable);
        let release = event.release.as_str().filter(is_unprintable);
        env.is_some() || release.is_some()
    } else {
        false
    }
}

/// Apply data privacy rules to the event payload.
///
/// This uses both the general `datascrubbing_settings`, as well as the the PII rules.
pub fn scrub(
    event: &mut Annotated<Event>,
    project_info: &ProjectInfo,
) -> Result<(), ProcessingError> {
    let config = &project_info.config;

    if config.datascrubbing_settings.scrub_data
        && let Some(event) = event.value_mut()
    {
        relay_pii::scrub_graphql(event);
    }

    metric!(timer(RelayTimers::EventProcessingPii), {
        if let Some(ref config) = config.pii_config {
            let mut processor = PiiProcessor::new(config.compiled());
            processor::process_value(event, &mut processor, ProcessingState::root())?;
        }
        let pii_config = config
            .datascrubbing_settings
            .pii_config()
            .map_err(|e| ProcessingError::PiiConfigError(e.clone()))?;
        if let Some(config) = pii_config {
            let mut processor = PiiProcessor::new(config.compiled());
            processor::process_value(event, &mut processor, ProcessingState::root())?;
        }
    });

    Ok(())
}

#[cfg(feature = "processing")]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unprintable_fields() {
        let event = Annotated::new(Event {
            environment: Annotated::new(String::from(
                "�9�~YY���)�����9�~YY���)�����9�~YY���)�����9�~YY���)�����",
            )),
            ..Default::default()
        });
        assert!(has_unprintable_fields(&event));

        let event = Annotated::new(Event {
            release: Annotated::new(
                String::from("���7��#1G����7��#1G����7��#1G����7��#1G����7��#")
                    .into(),
            ),
            ..Default::default()
        });
        assert!(has_unprintable_fields(&event));

        let event = Annotated::new(Event {
            environment: Annotated::new(String::from("production")),
            ..Default::default()
        });
        assert!(!has_unprintable_fields(&event));

        let event = Annotated::new(Event {
            release: Annotated::new(
                String::from("release with\t some\n normal\r\nwhitespace").into(),
            ),
            ..Default::default()
        });
        assert!(!has_unprintable_fields(&event));
    }
}
