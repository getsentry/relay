use relay_config::Config;

use crate::envelope::{AttachmentType, Envelope, ItemType};
use crate::managed::{ItemAction, ManagedEnvelope};
use crate::services::outcome::{DiscardAttachmentType, DiscardItemType};

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
///  - `max_log_size`
///  - `max_metric_buckets_size`
///  - `max_profile_size`
///  - `max_replay_compressed_size`
///  - `max_session_count`
///  - `max_span_size`
///  - `max_statsd_size`
///  - `max_container_size`
///  - `max_span_count`
///  - `max_log_count`
pub fn check_envelope_size_limits(
    config: &Config,
    envelope: &Envelope,
) -> Result<(), DiscardItemType> {
    const NO_LIMIT: usize = usize::MAX;

    let mut event_size = 0;
    let mut attachments_size = 0;
    let mut session_count = 0;
    let mut span_count = 0;
    let mut log_count = 0;
    let mut client_reports_size = 0;

    for item in envelope.items() {
        if item.is_container() && item.len() > config.max_container_size() {
            return Err(item.ty().into());
        }

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
            ItemType::Attachment | ItemType::UnrealReport | ItemType::UserReport => {
                attachments_size += item.len();
                config.max_attachment_size()
            }
            ItemType::ReplayRecording => config.max_replay_compressed_size(),
            ItemType::ReplayVideo => config.max_replay_compressed_size(),
            ItemType::Session | ItemType::Sessions => {
                session_count += 1;
                NO_LIMIT
            }
            ItemType::ClientReport => {
                client_reports_size += item.len();
                NO_LIMIT
            }
            ItemType::Profile => config.max_profile_size(),
            ItemType::CheckIn => config.max_check_in_size(),
            ItemType::Statsd => config.max_statsd_size(),
            ItemType::MetricBuckets => config.max_metric_buckets_size(),
            ItemType::Log | ItemType::OtelLog => {
                log_count += item.item_count().unwrap_or(1) as usize;
                config.max_log_size()
            }
            ItemType::Span | ItemType::OtelSpan => {
                span_count += item.item_count().unwrap_or(1) as usize;
                config.max_span_size()
            }
            ItemType::OtelTracesData => config.max_event_size(), // a spans container similar to `Transaction`
            ItemType::ProfileChunk => config.max_profile_size(),
            ItemType::Unknown(_) => NO_LIMIT,
        };

        // For item containers, we want to check that the contained items obey
        // the size limits *on average*.
        // For standalone items, this is just the item size itself.
        let avg_item_size = item.len() / (item.item_count().unwrap_or(1).max(1) as usize);
        if avg_item_size > max_size {
            return Err(item
                .attachment_type()
                .map(|t| t.into())
                .unwrap_or_else(|| item.ty().into()));
        }
    }

    if event_size > config.max_event_size() {
        return Err(DiscardItemType::Event);
    }
    if attachments_size > config.max_attachments_size() {
        return Err(DiscardItemType::Attachment(
            DiscardAttachmentType::Attachment,
        ));
    }
    if session_count > config.max_session_count() {
        return Err(DiscardItemType::Session);
    }
    if span_count > config.max_span_count() {
        return Err(DiscardItemType::Span);
    }
    if log_count > config.max_log_count() {
        return Err(DiscardItemType::Log);
    }
    if client_reports_size > config.max_client_reports_size() {
        return Err(DiscardItemType::ClientReport);
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
