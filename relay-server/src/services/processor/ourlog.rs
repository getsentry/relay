//! Log processing code.

use relay_config::Config;
use relay_dynamic_config::{Feature, GlobalConfig};

use crate::envelope::ItemType;
use crate::services::processor::{LogGroup, should_filter};
use crate::services::projects::project::ProjectInfo;
use crate::utils::{ItemAction, TypedEnvelope, sample};

#[cfg(feature = "processing")]
mod processing;
#[cfg(feature = "processing")]
pub use processing::*;

/// Removes logs from the envelope if the feature is not enabled.
pub fn filter(
    managed_envelope: &mut TypedEnvelope<LogGroup>,
    config: &Config,
    project_info: &ProjectInfo,
    global_config: &GlobalConfig,
) {
    let logging_disabled = should_filter(config, project_info, Feature::OurLogsIngestion);
    let logs_sampled = global_config
        .options
        .ourlogs_ingestion_sample_rate
        .map(sample)
        .unwrap_or_default();

    let action = match logging_disabled || logs_sampled.is_discard() {
        true => ItemAction::DropSilently,
        false => ItemAction::Keep,
    };

    managed_envelope.retain_items(move |item| match item.ty() {
        ItemType::OtelLog | ItemType::Log => action.clone(),
        _ => ItemAction::Keep,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{Envelope, ItemType};
    use crate::services::processor::ProcessingGroup;
    use crate::utils::ManagedEnvelope;
    use bytes::Bytes;

    use relay_dynamic_config::GlobalConfig;

    use relay_system::Addr;

    fn params() -> (TypedEnvelope<LogGroup>, ProjectInfo) {
        let bytes = Bytes::from(
            r#"{"dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"}
{"type":"otel_log"}
{}
"#,
        );

        let dummy_envelope = Envelope::parse_bytes(bytes).unwrap();
        let mut project_info = ProjectInfo::default();
        project_info
            .config
            .features
            .0
            .insert(Feature::OurLogsIngestion);

        let managed_envelope = ManagedEnvelope::new(dummy_envelope, Addr::dummy(), Addr::dummy());
        let managed_envelope = (managed_envelope, ProcessingGroup::Log).try_into().unwrap();

        (managed_envelope, project_info)
    }

    #[test]
    fn test_logs_sampled_default() {
        let global_config = GlobalConfig::default();
        let config = Config::default();
        assert!(
            global_config
                .options
                .ourlogs_ingestion_sample_rate
                .is_none()
        );
        let (mut managed_envelope, project_info) = params();
        filter(
            &mut managed_envelope,
            &config,
            &project_info,
            &global_config,
        );
        assert!(
            managed_envelope
                .envelope()
                .items()
                .any(|item| item.ty() == &ItemType::OtelLog),
            "{:?}",
            managed_envelope.envelope()
        );
    }

    #[test]
    fn test_logs_sampled_explicit() {
        let mut global_config = GlobalConfig::default();
        global_config.options.ourlogs_ingestion_sample_rate = Some(1.0);
        let config = Config::default();
        let (mut managed_envelope, project_info) = params();
        filter(
            &mut managed_envelope,
            &config,
            &project_info,
            &global_config,
        );
        assert!(
            managed_envelope
                .envelope()
                .items()
                .any(|item| item.ty() == &ItemType::OtelLog),
            "{:?}",
            managed_envelope.envelope()
        );
    }

    #[test]
    fn test_logs_sampled_dropped() {
        let mut global_config = GlobalConfig::default();
        global_config.options.ourlogs_ingestion_sample_rate = Some(0.0);
        let config = Config::default();
        let (mut managed_envelope, project_info) = params();
        filter(
            &mut managed_envelope,
            &config,
            &project_info,
            &global_config,
        );
        assert!(
            !managed_envelope
                .envelope()
                .items()
                .any(|item| item.ty() == &ItemType::OtelLog),
            "{:?}",
            managed_envelope.envelope()
        );
    }
}
