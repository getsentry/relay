use relay_quotas::DataCategory;

use crate::envelope::{Item, ItemType};
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::errors::{Context, Expansion, SentryError};
use crate::processing::errors::{Result, utils};

#[derive(Debug)]
pub struct Generic;

impl SentryError for Generic {
    fn event_category(&self) -> DataCategory {
        DataCategory::Error
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(ev) = utils::take_item_of_type(items, ItemType::Event) else {
            return Ok(None);
        };

        let fully_normalized = ev.fully_normalized();
        let mut metrics = Default::default();

        let mut event = utils::event_from_json_payload(ev, None, &mut metrics, ctx.processing)?;

        let skip_normalization = ctx.processing.is_processing()
            && ctx.envelope.meta().request_trust().is_trusted()
            && fully_normalized;
        if !skip_normalization && let Some(event) = event.value_mut() {
            // Event items can never include transactions, so retain the event type and let
            // inference deal with this during normalization.
            event.ty.set_value(None);
        }

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            intermediates: vec![],
            error: Self,
            metrics,
            fully_normalized,
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

impl Counted for Generic {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use relay_config::{Config, OverridableConfig};
    use relay_event_schema::protocol::EventType;

    use crate::envelope::ContentType;
    use crate::extractors::RequestTrust;
    use crate::processing;

    /// Expands a fully normalized event item with the given payload and returns the resulting
    /// event type.
    fn expand(
        payload: &'static str,
        processing_enabled: bool,
        trust: RequestTrust,
    ) -> Option<EventType> {
        let mut config = Config::default();
        if processing_enabled {
            config
                .apply_override(OverridableConfig {
                    processing: Some("true".to_owned()),
                    ..Default::default()
                })
                .unwrap();
        }
        let config = config.current();

        let mut envelope = crate::testutils::new_envelope(false, "");
        envelope.meta_mut().set_request_trust(trust);

        let mut item = Item::new(ItemType::Event);
        item.set_payload(ContentType::Json, payload);
        item.set_fully_normalized(true);

        let ctx = Context {
            envelope: envelope.headers(),
            processing: processing::Context {
                config: &config,
                ..processing::Context::for_test()
            },
        };

        let expansion = Generic::try_expand(&mut vec![item], ctx)
            .unwrap()
            .expect("an event item is expanded by `Generic`");

        expansion.event.value().unwrap().ty.value().copied()
    }

    #[test]
    #[cfg(feature = "processing")]
    fn test_fully_normalized_untrusted_resets_event_type() {
        // Only internal Relays are allowed to skip normalization.
        for payload in [r#"{"type":"transaction"}"#, r#"{"type":"feedback"}"#] {
            assert_eq!(
                expand(payload, true, RequestTrust::Untrusted),
                None,
                "{payload}"
            );
        }
    }

    #[test]
    #[cfg(feature = "processing")]
    fn test_fully_normalized_trusted_keeps_event_type() {
        assert_eq!(
            expand(r#"{"type":"transaction"}"#, true, RequestTrust::Trusted),
            Some(EventType::Transaction)
        );
    }

    #[test]
    fn test_fully_normalized_not_processing_resets_event_type() {
        assert_eq!(
            expand(r#"{"type":"transaction"}"#, false, RequestTrust::Trusted),
            None
        );
    }
}
