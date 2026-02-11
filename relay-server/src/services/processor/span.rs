//! Processor code related to standalone spans.

use relay_dynamic_config::Feature;

use crate::envelope::ItemType;
use crate::managed::{ItemAction, TypedEnvelope};
use crate::services::processor::{SpanGroup, should_filter};

#[cfg(feature = "processing")]
mod processing;
use crate::services::projects::project::ProjectInfo;
#[cfg(feature = "processing")]
pub use processing::*;
use relay_config::Config;

pub fn filter(
    managed_envelope: &mut TypedEnvelope<SpanGroup>,
    config: &Config,
    project_info: &ProjectInfo,
) {
    let disabled = should_filter(config, project_info, Feature::StandaloneSpanIngestion);
    if !disabled {
        return;
    }

    managed_envelope.retain_items(|item| match matches!(item.ty(), &ItemType::Span) {
        true => {
            relay_log::debug!("dropping span because feature is disabled");
            ItemAction::DropSilently
        }
        false => ItemAction::Keep,
    });
}
