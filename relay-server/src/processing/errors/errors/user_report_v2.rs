use relay_base_schema::events::EventType;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{ItemType, Items};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{
    Context, ErrorRef, ErrorRefMut, ParsedError, SentryError, utils,
};
use crate::statsd::RelayCounters;

#[derive(Debug)]
pub struct UserReportV2 {
    pub event: Annotated<Event>,
    pub attachments: Items,
}

impl SentryError for UserReportV2 {
    fn try_expand(items: &mut Items, _ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        let Some(ev) = utils::take_item_of_type(items, ItemType::UserReportV2) else {
            return Ok(None);
        };

        let error = Self {
            event: utils::event_from_json_payload(ev, EventType::UserReportV2)?,
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
        };

        relay_statsd::metric!(
            counter(RelayCounters::FeedbackAttachments) += error.attachments.len() as u64
        );

        Ok(Some(ParsedError {
            error,
            fully_normalized: false,
        }))
    }

    fn as_ref(&self) -> ErrorRef<'_> {
        ErrorRef {
            event: &self.event,
            attachments: &self.attachments,
            user_reports: &[],
        }
    }

    fn as_ref_mut(&mut self) -> ErrorRefMut<'_> {
        ErrorRefMut {
            event: &mut self.event,
            attachments: &mut self.attachments,
            user_reports: None,
        }
    }
}

impl Counted for UserReportV2 {
    fn quantities(&self) -> Quantities {
        self.as_ref().to_quantities()
    }
}
