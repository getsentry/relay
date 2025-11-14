//! Contains code related to validation and normalization of the user and client reports.

use std::error::Error;

use relay_event_schema::protocol::UserReport;

use crate::envelope::{ContentType, ItemType};
use crate::managed::{ItemAction, TypedEnvelope};
use crate::services::outcome::{DiscardReason, Outcome};

/// Validates and normalizes all user report items in the envelope.
///
/// User feedback items are removed from the envelope if they contain invalid JSON or if the
/// JSON violates the schema (basic type validation). Otherwise, their normalized representation
/// is written back into the item.
pub fn process_user_reports<Group>(managed_envelope: &mut TypedEnvelope<Group>) {
    managed_envelope.retain_items(|item| {
        if item.ty() != &ItemType::UserReport {
            return ItemAction::Keep;
        };

        let payload = item.payload();
        // There is a customer SDK which sends invalid reports with a trailing `\n`,
        // strip it here, even if they update/fix their SDK there will still be many old
        // versions with the broken SDK out there.
        let payload = trim_whitespaces(&payload);
        let report = match serde_json::from_slice::<UserReport>(payload) {
            Ok(report) => report,
            Err(error) => {
                relay_log::error!(error = &error as &dyn Error, "failed to store user report");
                return ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidJson));
            }
        };

        let json_string = match serde_json::to_string(&report) {
            Ok(json) => json,
            Err(err) => {
                relay_log::error!(
                    error = &err as &dyn Error,
                    "failed to serialize user report"
                );
                return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
            }
        };

        item.set_payload(ContentType::Json, json_string);
        ItemAction::Keep
    });
}

fn trim_whitespaces(data: &[u8]) -> &[u8] {
    let Some(from) = data.iter().position(|x| !x.is_ascii_whitespace()) else {
        return &[];
    };
    let Some(to) = data.iter().rposition(|x| !x.is_ascii_whitespace()) else {
        return &[];
    };
    &data[from..to + 1]
}

#[cfg(test)]
mod tests {
    use relay_cogs::Token;
    use relay_config::Config;
    use relay_event_schema::protocol::EventId;

    use crate::envelope::{Envelope, Item};
    use crate::extractors::RequestMeta;
    use crate::managed::ManagedEnvelope;
    use crate::processing::{self};
    use crate::services::processor::{ProcessEnvelopeGrouped, ProcessingGroup, Submit};
    use crate::testutils::create_test_processor;

    use super::*;

    // FIXME: Ask if moving the tests over is worth the changes to ProcessEnvelopeGrouped and Submit or if they should just stay here (hard to find).
    #[tokio::test]
    async fn test_client_report_removal() {
        relay_test::setup();
        let outcome_aggregator = Addr::dummy();

        let config = Config::from_json_value(serde_json::json!({
            "outcomes": {
                "emit_outcomes": true,
            }
        }))
        .unwrap();

        let processor = create_test_processor(Default::default()).await;

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(None, request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::ClientReport);
            item.set_payload(
                ContentType::Json,
                r#"
                    {
                        "discarded_events": [
                            ["queue_full", "error", 42]
                        ]
                    }
                "#,
            );
            item
        });

        let mut envelopes = ProcessingGroup::split_envelope(*envelope, &Default::default());
        assert_eq!(envelopes.len(), 1);
        let (group, envelope) = envelopes.pop().unwrap();

        let envelope = ManagedEnvelope::new(envelope, outcome_aggregator);
        let message = ProcessEnvelopeGrouped {
            group,
            envelope,
            ctx: processing::Context {
                config: &config,
                ..processing::Context::for_test()
            },
        };

        let envelope = processor
            .process(&mut Token::noop(), message)
            .await
            .unwrap();
        assert!(envelope.is_none());
    }

    #[tokio::test]
    #[cfg(feature = "processing")]
    async fn test_client_report_removal_in_processing() {
        relay_test::setup();
        let outcome_aggregator = Addr::dummy();

        let config = Config::from_json_value(serde_json::json!({
            "outcomes": {
                "emit_outcomes": true,
            },
            "processing": {
                "enabled": true,
                "kafka_config": [],
            }
        }))
        .unwrap();

        let processor = create_test_processor(Default::default()).await;

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(None, request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::ClientReport);
            item.set_payload(
                ContentType::Json,
                r#"
                    {
                        "discarded_events": [
                            ["queue_full", "error", 42]
                        ]
                    }
                "#,
            );
            item
        });

        let mut envelopes = ProcessingGroup::split_envelope(*envelope, &Default::default());
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();
        let envelope = ManagedEnvelope::new(envelope, outcome_aggregator);
        let message = ProcessEnvelopeGrouped {
            group,
            envelope,
            ctx: processing::Context {
                config: &config,
                ..processing::Context::for_test()
            },
        };

        let envelope = processor
            .process(&mut Token::noop(), message)
            .await
            .unwrap();
        assert!(envelope.is_none());
    }

    #[tokio::test]
    async fn test_user_report_only() {
        relay_log::init_test!();
        let processor = create_test_processor(Default::default()).await;
        let outcome_aggregator = Addr::dummy();
        let event_id = EventId::new();

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::UserReport);
            item.set_payload(
                ContentType::Json,
                format!(r#"{{"event_id": "{event_id}"}}"#),
            );
            item
        });

        let mut envelopes = ProcessingGroup::split_envelope(*envelope, &Default::default());
        assert_eq!(envelopes.len(), 1);

        let (group, envelope) = envelopes.pop().unwrap();

        let envelope = ManagedEnvelope::new(envelope, outcome_aggregator);
        let message = ProcessEnvelopeGrouped {
            group,
            envelope,
            ctx: processing::Context::for_test(),
        };

        let Ok(Some(Submit::Envelope(new_envelope))) =
            processor.process(&mut Token::noop(), message).await
        else {
            panic!();
        };
        let new_envelope = new_envelope.envelope();

        assert_eq!(new_envelope.len(), 1);
        assert_eq!(
            new_envelope.items().next().unwrap().ty(),
            &ItemType::UserReport
        );
    }

    #[tokio::test]
    async fn test_user_report_invalid() {
        let processor = create_test_processor(Default::default()).await;
        let outcome_aggregator = Addr::dummy();
        let event_id = EventId::new();

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();

        let request_meta = RequestMeta::new(dsn);
        let mut envelope = Envelope::from_request(Some(event_id), request_meta);

        envelope.add_item({
            let mut item = Item::new(ItemType::UserReport);
            item.set_payload(ContentType::Json, r#"{"foo": "bar"}"#);
            item
        });

        envelope.add_item({
            let mut item = Item::new(ItemType::Event);
            item.set_payload(ContentType::Json, "{}");
            item
        });

        let mut envelopes = ProcessingGroup::split_envelope(*envelope, &Default::default());
        assert_eq!(envelopes.len(), 1);
        let (group, envelope) = envelopes.pop().unwrap();
        let envelope = ManagedEnvelope::new(envelope, outcome_aggregator);

        let message = ProcessEnvelopeGrouped {
            group,
            envelope,
            ctx: processing::Context::for_test(),
        };

        let Ok(Some(Submit::Envelope(new_envelope))) =
            processor.process(&mut Token::noop(), message).await
        else {
            panic!();
        };
        let new_envelope = new_envelope.envelope();

        assert_eq!(new_envelope.len(), 1);
        assert_eq!(new_envelope.items().next().unwrap().ty(), &ItemType::Event);
    }

    #[test]
    fn test_trim_whitespaces() {
        assert_eq!(trim_whitespaces(b""), b"");
        assert_eq!(trim_whitespaces(b" \n\r "), b"");
        assert_eq!(trim_whitespaces(b" \nx\r "), b"x");
        assert_eq!(trim_whitespaces(b" {foo: bar} "), b"{foo: bar}");
        assert_eq!(trim_whitespaces(b"{ foo: bar}"), b"{ foo: bar}");
    }
}
