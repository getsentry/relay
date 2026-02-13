use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{ItemType, Items};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{
    Context, ErrorRef, ErrorRefMut, ParsedError, SentryError, utils,
};
use crate::services::processor::ProcessingError;
use crate::utils::UnrealExpansion;

#[derive(Debug)]
pub struct Unreal {
    pub event: Annotated<Event>,
    pub attachments: Items,
    pub user_reports: Items,
}

impl SentryError for Unreal {
    fn try_expand(items: &mut Items, ctx: Context<'_>) -> Result<Option<ParsedError<Self>>> {
        let Some(report) = utils::take_item_of_type(items, ItemType::UnrealReport) else {
            return Ok(None);
        };

        let UnrealExpansion {
            event,
            mut attachments,
        } = crate::utils::expand_unreal(report, ctx.processing.config)?;

        let event = match utils::take_item_of_type(items, ItemType::Event).or(event) {
            Some(event) => utils::event_from_json_payload(event, None)?,
            // `process` later fills this event in, ideally the event is already filled in here,
            // during the expansion, it is split into two phases now, to keep compatibility with
            // the existing unreal code.
            None => Annotated::empty(),
        };

        attachments.extend(items.drain_filter(|item| *item.ty() == ItemType::Attachment));

        let error = Self {
            event,
            attachments,
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
        };

        Ok(Some(ParsedError {
            error,
            fully_normalized: false,
        }))
    }

    fn process(&mut self, ctx: Context<'_>) -> Result<()> {
        let event_id = ctx.envelope.event_id().unwrap_or_default();
        debug_assert_ne!(event_id, Default::default(), "event id must always be set");

        let user_header = ctx
            .envelope
            .get_header(crate::constants::UNREAL_USER_HEADER)
            .and_then(|v| v.as_str());

        if let Some(result) =
            crate::utils::process_unreal(event_id, &mut self.event, &self.attachments, user_header)
                .map_err(ProcessingError::InvalidUnrealReport)?
        {
            self.user_reports.extend(result.user_reports);
        }

        Ok(())
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

impl Counted for Unreal {
    fn quantities(&self) -> Quantities {
        self.as_ref().to_quantities()
    }
}
