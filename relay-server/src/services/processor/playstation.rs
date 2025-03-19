//! Playstation related code.
//!
//! These functions are included only in the processing mode.

use relay_dynamic_config::Feature;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{AttachmentType, ItemType};
use crate::services::processor::metric;
use crate::services::processor::{ErrorGroup, EventFullyNormalized, ProcessingError};
use crate::services::projects::project::ProjectInfo;
use crate::statsd::RelayCounters;
use crate::utils::TypedEnvelope;

pub fn expand(
    managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    project_info: &ProjectInfo,
) -> Result<(), ProcessingError> {
    let envelope = &mut managed_envelope.envelope_mut();
    if !project_info.has_feature(Feature::PlaystationIngestion) {
        return Ok(());
    }

    if let Some(_item) = envelope.take_item_by(|item| {
        item.ty() == &ItemType::Attachment
            && item.attachment_type() == Some(&AttachmentType::Prosperodump)
    }) {
        // TODO: Add the expand logic here
        metric!(counter(RelayCounters::PlaystationProcessing) += 1);
    }

    Ok(())
}

pub fn process(
    _managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    _event: &mut Annotated<Event>,
) -> Result<Option<EventFullyNormalized>, ProcessingError> {
    // TODO: Add the processing logic here.
    Ok(None)
}
