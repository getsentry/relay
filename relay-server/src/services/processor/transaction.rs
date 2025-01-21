//! Processing logic specific to transaction envelopes.

use relay_dynamic_config::GlobalConfig;

/// Drops attachments in transaction envelopes.
pub fn drop_invalid_items(envelope: &mut ManagedEnvelope, global_config: &GlobalConfig) {
    if global_config.options.drop_transaction_attachments {
        envelope.retain_items(|item| item.ty() != &ItemType::Attachment);
    }
}
