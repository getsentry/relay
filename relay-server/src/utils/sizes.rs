use relay_config::Config;

use crate::envelope::{Envelope, ItemType};

/// Checks for size limits of items in this envelope.
///
/// Returns `true`, if the envelope adheres to the configured size limits. Otherwise, returns
/// `false`, in which case the envelope should be discarded and a `413 Payload Too Large` response
/// should be given.
///
/// The following limits are checked:
///
///  - `max_event_size`
///  - `max_attachment_size`
///  - `max_attachments_size`
///  - `max_session_count`
pub fn check_envelope_size_limits(config: &Config, envelope: &Envelope) -> bool {
    let mut event_size = 0;
    let mut attachments_size = 0;
    let mut session_count = 0;
    let mut client_reports_size = 0;

    for item in envelope.items() {
        match item.ty() {
            ItemType::Event
            | ItemType::Transaction
            | ItemType::Security
            | ItemType::RawSecurity
            | ItemType::FormData => event_size += item.len(),
            ItemType::Attachment | ItemType::UnrealReport => {
                if item.len() > config.max_attachment_size() {
                    return false;
                }

                attachments_size += item.len()
            }
            ItemType::Session => session_count += 1,
            ItemType::Sessions => session_count += 1,
            ItemType::UserReport => (),
            ItemType::Metrics => (),
            ItemType::MetricBuckets => (),
            ItemType::ClientReport => client_reports_size += item.len(),
            ItemType::Profile => {
                if item.len() > config.max_profile_size() {
                    return false;
                }
            }
            ItemType::Unknown(_) => (),
        }
    }

    event_size <= config.max_event_size()
        && attachments_size <= config.max_attachments_size()
        && session_count <= config.max_session_count()
        && client_reports_size <= config.max_client_reports_size()
}
