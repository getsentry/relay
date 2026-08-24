use relay_quotas::DataCategory;

use crate::envelope::{Item, ItemType};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, Expansion, SentryError, utils};
use crate::processing::utils::event::event_category;

#[derive(Debug)]
pub struct Security {
    category: DataCategory,
}

impl SentryError for Security {
    fn event_category(&self) -> DataCategory {
        self.category
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(ev) = utils::take_item_of_type(items, ItemType::Security) else {
            return Ok(None);
        };

        let mut metrics = Default::default();
        let event = utils::event_from_json_payload(ev, None, &mut metrics, ctx)?;

        // An older upstream Relay may still send the removed `hpkp`, `expectct` and `expectstaple`
        // types. They no longer parse and are serialized back into an `ItemType::Event`, so the
        // category has to follow the event type. Otherwise quotas and outcomes are tracked against
        // a different category than the item which is eventually forwarded.
        let category = event_category(&event).unwrap_or(DataCategory::Security);

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            error: Self { category },
            metrics,
            fully_normalized: false,
        }))
    }

    fn apply_rate_limit(
        &mut self,
        _category: DataCategory,
        _limits: relay_quotas::RateLimits,
        _records: &mut RecordKeeper<'_>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_into(self, _items: &mut Vec<Item>, _ctx: ForwardContext<'_>) -> Result<()> {
        Ok(())
    }

    fn minidump_mut(&mut self) -> Option<&mut Item> {
        None
    }
}

impl Counted for Security {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::envelope::ContentType;

    fn expand(payload: &str) -> Security {
        let mut item = Item::new(ItemType::Security);
        item.set_payload(ContentType::Json, payload.to_owned());

        Security::try_expand(&mut vec![item], Context::for_test())
            .unwrap()
            .unwrap()
            .error
    }

    #[test]
    fn test_csp_event_category() {
        let security = expand(r#"{"type":"csp","csp":{"effective_directive":"style-src"}}"#);
        assert_eq!(security.event_category(), DataCategory::Security);
    }

    #[test]
    fn test_removed_event_type_category() {
        for ty in ["hpkp", "expectct", "expectstaple"] {
            let security = expand(&format!(r#"{{"type":"{ty}"}}"#));
            assert_eq!(security.event_category(), DataCategory::Error, "{ty}");
        }
    }
}
