use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{ItemType, Items};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{
    Context, ErrorRef, ErrorRefMut, ParsedError, SentryError, utils,
};

#[derive(Debug)]
pub struct RawSecurity {
    pub event: Annotated<Event>,
    pub attachments: Items,
    pub user_reports: Items,
}

impl SentryError for RawSecurity {
    fn try_expand(items: &mut Items, ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        let Some(item) = utils::take_item_of_type(items, ItemType::RawSecurity) else {
            return Ok(None);
        };

        let (event, _) = crate::services::processor::event::event_from_security_report(
            item,
            ctx.envelope.meta(),
        )?;

        let error = Self {
            event,
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
        };

        Ok(Some(ParsedError {
            error,
            fully_normalized: false,
        }))
    }

    fn as_ref(&self) -> ErrorRef<'_> {
        ErrorRef {
            event: &self.event,
            attachments: &self.attachments,
            user_reports: &self.user_reports,
        }
    }

    fn as_ref_mut(&mut self) -> ErrorRefMut<'_> {
        ErrorRefMut {
            event: &mut self.event,
            attachments: &mut self.attachments,
            user_reports: Some(&mut self.user_reports),
        }
    }
}

impl Counted for RawSecurity {
    fn quantities(&self) -> Quantities {
        self.as_ref().to_quantities()
    }
}
