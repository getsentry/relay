use relay_quotas::DataCategory;

use crate::envelope::{Item, ItemType};
use crate::managed::{Counted, Quantities};
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, Expansion, SentryError, utils};

#[derive(Debug)]
pub struct Security {}

impl SentryError for Security {
    fn event_category(&self) -> DataCategory {
        DataCategory::Security
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(ev) = utils::take_item_of_type(items, ItemType::Security) else {
            return Ok(None);
        };

        let mut metrics = Default::default();

        Ok(Some(Expansion {
            event: Box::new(utils::event_from_json_payload(ev, None, &mut metrics, ctx)?),
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            error: Self {},
            metrics,
            fully_normalized: false,
        }))
    }
}

impl Counted for Security {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}
