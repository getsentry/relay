use relay_event_schema::protocol::Metrics;
use relay_protocol::Annotated;

use crate::envelope::{Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, ParsedError, SentryError, utils};
use crate::services::processor::ProcessingError;

#[derive(Debug)]
pub struct FormData {}

impl SentryError for FormData {
    fn try_expand(items: &mut Vec<Item>, _ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        let Some(form_data) = utils::take_item_of_type(items, ItemType::FormData) else {
            return Ok(None);
        };

        let mut metrics = Metrics::default();

        let event = {
            let mut value = serde_json::Value::Object(Default::default());
            crate::services::processor::event::merge_formdata(&mut value, &form_data);
            Annotated::deserialize_with_meta(value).map_err(ProcessingError::InvalidJson)
        }?;
        metrics.bytes_ingested_event = Annotated::new(form_data.len() as u64);

        Ok(Some(ParsedError {
            event,
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            error: Self {},
            metrics,
            fully_normalized: false,
        }))
    }
}

impl Counted for FormData {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}
