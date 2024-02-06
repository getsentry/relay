use relay_config::Config;

use crate::envelope::{AttachmentType, Envelope, ItemType};
use crate::utils::{ItemAction, ManagedEnvelope};

/// Checks for size limits of items in this envelope.
///
/// Returns `Ok`, if the envelope adheres to the configured size limits. Otherwise, returns
/// an `Err` containing the offending item type, in which case the envelope should be discarded
/// and a `413 Payload Too Large` response should be given.
///
/// The following limits are checked:
///
///  - `max_attachment_size`
///  - `max_attachments_size`
///  - `max_check_in_size`
///  - `max_event_size`
///  - `max_metric_buckets_size`
///  - `max_metric_meta_size`
///  - `max_profile_size`
///  - `max_replay_compressed_size`
///  - `max_session_count`
///  - `max_span_size`
///  - `max_statsd_size`
pub fn check_envelope_size_limits(config: &Config, envelope: &Envelope) -> Result<(), ItemType> {
    const NO_LIMIT: usize = usize::MAX;

    let mut event_size = 0;
    let mut attachments_size = 0;
    let mut session_count = 0;
    let mut client_reports_size = 0;

    for item in envelope.items() {
        let max_size = match item.ty() {
            ItemType::Event
            | ItemType::Transaction
            | ItemType::Security
            | ItemType::ReplayEvent
            | ItemType::RawSecurity
            | ItemType::Nel
            | ItemType::UserReportV2
            | ItemType::FormData => {
                event_size += item.len();
                NO_LIMIT
            }
            ItemType::Attachment | ItemType::UnrealReport => {
                attachments_size += item.len();
                config.max_attachment_size()
            }
            ItemType::ReplayRecording => config.max_replay_compressed_size(),
            ItemType::Session | ItemType::Sessions => {
                session_count += 1;
                NO_LIMIT
            }
            ItemType::ClientReport => {
                client_reports_size += item.len();
                NO_LIMIT
            }
            // The Combined Replay Envelope isn't generated on the client so its size does not need
            // to be checked.
            ItemType::Profile => config.max_profile_size(),
            ItemType::CheckIn => config.max_check_in_size(),
            ItemType::UserReport => NO_LIMIT,
            ItemType::Statsd => config.max_statsd_size(),
            ItemType::MetricBuckets => config.max_metric_buckets_size(),
            ItemType::MetricMeta => config.max_metric_meta_size(),
            ItemType::Span | ItemType::OtelSpan => config.max_span_size(),
            ItemType::Unknown(_) => NO_LIMIT,
        };

        if item.len() > max_size {
            return Err(item.ty().clone());
        }
    }

    if event_size > config.max_event_size() {
        return Err(ItemType::Event);
    }
    if attachments_size > config.max_attachments_size() {
        return Err(ItemType::Attachment);
    }
    if session_count > config.max_session_count() {
        return Err(ItemType::Session);
    }
    if client_reports_size > config.max_client_reports_size() {
        return Err(ItemType::ClientReport);
    }

    Ok(())
}

/// Checks for valid envelope items.
///
/// If Relay is configured to drop unknown items, this function removes them from the Envelope. All
/// known items will be retained.
pub fn remove_unknown_items(config: &Config, envelope: &mut ManagedEnvelope) {
    if !config.accept_unknown_items() {
        envelope.retain_items(|item| match item.ty() {
            ItemType::Unknown(ty) => {
                relay_log::debug!("dropping unknown item of type '{ty}'");
                ItemAction::DropSilently
            }
            _ => match item.attachment_type() {
                Some(AttachmentType::Unknown(ty)) => {
                    relay_log::debug!("dropping unknown attachment of type '{ty}'");
                    ItemAction::DropSilently
                }
                _ => ItemAction::Keep,
            },
        });
    }
}
