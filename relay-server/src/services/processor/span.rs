//! Processor code related to standalone spans.

use relay_dynamic_config::Feature;

use crate::services::processor::state::{Container, ProcessSpan, ProcessState};
use crate::{envelope::ItemType, utils::ItemAction};

#[cfg(feature = "processing")]
mod processing;
#[cfg(feature = "processing")]
pub use processing::*;

pub fn filter<S, D>(state: &mut S, data: &mut D)
where
    S: ProcessState + ProcessSpan,
    D: Container,
{
    let standalone_span_ingestion_enabled = state
        .project_state()
        .has_feature(Feature::StandaloneSpanIngestion);
    data.managed_envelope_mut()
        .retain_items(|item| match item.ty() {
            ItemType::OtelSpan | ItemType::Span => {
                if !standalone_span_ingestion_enabled {
                    relay_log::warn!("dropping span because feature is disabled");
                    ItemAction::DropSilently
                } else {
                    ItemAction::Keep
                }
            }
            _ => ItemAction::Keep,
        });
}
