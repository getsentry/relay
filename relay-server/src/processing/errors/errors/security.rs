use relay_base_schema::events::EventType;
use relay_event_normalization::infer_event_type;
use relay_quotas::DataCategory;

use crate::envelope::{Item, ItemType};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, Expansion, SentryError, utils};
use crate::services::processor::ProcessingError;

#[derive(Debug)]
pub struct Security;

impl SentryError for Security {
    fn event_category(&self) -> DataCategory {
        DataCategory::Security
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(ev) = utils::take_item_of_type(items, ItemType::Security) else {
            return Ok(None);
        };

        let payload = ev.payload();
        let mut metrics = Default::default();
        let mut event = utils::event_from_json_payload(ev, None, &mut metrics, ctx)?;

        // Normalization honours a declared `transaction` or `feedback` type. Discard it, a
        // security item must not turn into an event of a different data category.
        if let Some(event) = event.value_mut() {
            event.ty.set_value(None);
        }

        // CSP is the only remaining security report. Older Relays may still forward the removed
        // `hpkp`, `expectct` and `expectstaple` types, which no longer parse into one.
        if event.value().map(infer_event_type) != Some(EventType::Csp) {
            return Err(ProcessingError::InvalidSecurityType(payload).into());
        }

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            error: Self,
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
    use crate::processing::errors::Error;

    fn expand(payload: &str) -> Result<Expansion<Security>> {
        let mut item = Item::new(ItemType::Security);
        item.set_payload(ContentType::Json, payload.to_owned());

        Ok(Security::try_expand(&mut vec![item], Context::for_test())?
            .expect("a security item is expanded by `Security`"))
    }

    fn assert_invalid_security_type(payload: &str) {
        let error = expand(payload).expect_err("expected the item to be rejected");
        assert!(
            matches!(
                error,
                Error::ProcessingFailed(ProcessingError::InvalidSecurityType(_))
            ),
            "{payload}: {error:?}"
        );
    }

    #[test]
    fn test_csp_report() {
        let expansion =
            expand(r#"{"type":"csp","csp":{"effective_directive":"style-src"}}"#).unwrap();

        assert_eq!(expansion.error.event_category(), DataCategory::Security);
    }

    #[test]
    fn test_declared_event_type_ignored() {
        // A security item must not be able to declare itself a transaction.
        let expansion =
            expand(r#"{"type":"transaction","csp":{"effective_directive":"style-src"}}"#).unwrap();

        assert_eq!(expansion.error.event_category(), DataCategory::Security);
        assert!(expansion.event.value().unwrap().ty.value().is_none());
    }

    #[test]
    fn test_removed_report_types_rejected() {
        for ty in ["hpkp", "expectct", "expectstaple"] {
            assert_invalid_security_type(&format!(
                r#"{{"type":"{ty}","{ty}":{{"hostname":"example.com"}}}}"#
            ));
        }
    }

    #[test]
    fn test_non_security_event_rejected() {
        assert_invalid_security_type(r#"{"message":"not a security report"}"#);
    }
}
