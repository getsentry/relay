//! Processing logic specific to transaction envelopes.

use relay_dynamic_config::GlobalConfig;

use crate::envelope::ItemType;
use crate::managed::{ItemAction, ManagedEnvelope};
use crate::services::outcome::{DiscardReason, Outcome};

/// Drops attachments in transaction envelopes.
pub fn drop_invalid_items(envelope: &mut ManagedEnvelope, global_config: &GlobalConfig) {
    if global_config.options.drop_transaction_attachments {
        envelope.retain_items(|item| match item.ty() {
            &ItemType::Attachment => {
                ItemAction::Drop(Outcome::Invalid(DiscardReason::TransactionAttachment))
            }
            _ => ItemAction::Keep,
        });
    }
}
