use relay_event_schema::processor::{
    ProcessValue, ProcessingResult, ProcessingState, ValueType, process_value,
};
use relay_protocol::Annotated;

use crate::{AttributeMode, PiiConfig, PiiProcessor};

pub fn scrub_eap_item<T: ProcessValue>(
    value_type: ValueType,
    item: &mut Annotated<T>,
    pii_config: Option<&PiiConfig>,
    pii_config_from_scrubbing: Option<&PiiConfig>,
) -> ProcessingResult {
    let state = ProcessingState::root().enter_borrowed("", None, [value_type]);

    if let Some(config) = pii_config {
        let mut processor = PiiProcessor::new(config.compiled())
            // For advanced rules we want to treat attributes as objects.
            .attribute_mode(AttributeMode::Object);
        process_value(item, &mut processor, &state)?;
    }

    if let Some(config) = pii_config_from_scrubbing {
        let mut processor = PiiProcessor::new(config.compiled())
            // For "legacy" rules we want to identify attributes with their values.
            .attribute_mode(AttributeMode::ValueOnly);
        process_value(item, &mut processor, &state)?;
    }

    Ok(())
}
