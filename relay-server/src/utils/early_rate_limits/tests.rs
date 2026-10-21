use super::*;
use std::collections::BTreeMap;

use crate::envelope::{ParentId, SourceQuantities};
use relay_base_schema::project::ProjectId;
use relay_quotas::{DimensionMap, RateLimit, RateLimitScope, ReasonCode, RetryAfter};
use relay_system::Addr;
use smallvec::smallvec;

struct RateLimitTestCase {
    name: &'static str,
    denied_categories: &'static [DataCategory],
    expect_attachment_limit_active: bool,
    expected_outcomes: &'static [(DataCategory, usize)],
}

macro_rules! envelope {
        ($( $item_type:ident $( :: $attachment_type:ident )? ),*) => {{
            let bytes = "{\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}";
            #[allow(unused_mut)]
            let mut envelope = Envelope::parse_bytes(bytes.into()).unwrap();
            $(
                let mut item = Item::new(ItemType::$item_type);
                item.set_payload(ContentType::OctetStream, "0123456789");
                $( item.set_attachment_type(AttachmentType::$attachment_type); )?
                envelope.add_item(item);
            )*

            envelope
        }}
    }

fn rate_limit(category: DataCategory) -> RateLimit {
    RateLimit {
        categories: [category].into(),
        scope: RateLimitScope::Project(ProjectId::new(42)),
        reason_code: None,
        retry_after: RetryAfter::from_secs(60),
        namespaces: smallvec![],
        dimensions: DimensionMap::default(),
    }
}

fn trace_attachment_item(bytes: usize, parent_id: Option<ParentId>) -> Item {
    let mut item = Item::new(ItemType::Attachment);
    item.set_payload(ContentType::TraceAttachment, "0".repeat(bytes));
    item.set_parent_id(parent_id);
    item
}

fn cached_limits(categories: &[DataCategory]) -> RateLimits {
    let mut cached = RateLimits::new();
    for &category in categories {
        cached.add(rate_limit(category));
    }
    cached
}

fn enforce_and_apply(
    cached: RateLimits,
    envelope: Box<Envelope>,
) -> (Box<Envelope>, RateLimits, BTreeMap<DataCategory, usize>) {
    let (outcome_aggregator, mut outcomes) = Addr::custom();
    let mut envelope = Managed::from_envelope(envelope, outcome_aggregator);
    let limits = enforce_rate_limits(&mut envelope, &cached, &[]);
    let envelope = envelope.accept(|envelope| envelope);
    let mut emitted = BTreeMap::new();
    while let Ok(outcome) = outcomes.try_recv() {
        assert_eq!(outcome.outcome, Outcome::RateLimited(None));
        *emitted.entry(outcome.category).or_insert(0) += outcome.quantity as usize;
    }
    (envelope, limits, emitted)
}

#[test]
fn test_enforce_pass_empty() {
    let envelope = envelope![];

    let cached = cached_limits(&[]);
    let (envelope, limits, _) = enforce_and_apply(cached, envelope);

    assert!(!limits.is_limited());
    assert!(envelope.is_empty());
}

#[test]
fn test_transaction_span_counts_without_header_writes() {
    for (body, header, spans) in [
        (r#"{"spans":[{},null,1]}"#, None, 3),
        (r#"{"spans":[],"spans":[{}]}"#, None, 0),
        (r#"{"spans":null}"#, None, 0),
        (r#"{}"#, None, 0),
        (r#"{"spans":[{}]}"#, Some(0), 0),
        (r#"{"spans":[{}]}"#, Some(12), 12),
    ] {
        let mut transaction = Item::new(ItemType::Transaction);
        transaction.set_payload(ContentType::Json, body);
        transaction.set_span_count(header);
        assert_eq!(calc_span_count(&transaction), spans);
        assert_eq!(
            transaction.header::<usize>(ItemHeaderKey::SpanCount),
            header
        );

        // Count each rejected transaction, including repeated items without count headers.
        let mut envelope = envelope![];
        envelope.add_item(transaction.clone());
        envelope.add_item(transaction.clone());
        let (envelope, _, outcomes) =
            enforce_and_apply(cached_limits(&[DataCategory::Transaction]), envelope);
        assert!(envelope.is_empty());
        assert_eq!(
            outcomes,
            BTreeMap::from([
                (DataCategory::Transaction, 1),
                (DataCategory::TransactionIndexed, 1),
                (DataCategory::Span, 2 * (1 + spans)),
                (DataCategory::SpanIndexed, 2 * (1 + spans)),
            ])
        );

        // An error event selects the shared category, so a transaction limit leaves this
        // transaction intact. Its missing header must stay missing even with that limit.
        let mut original = envelope![Event];
        original.add_item(transaction);
        let (kept, _, outcomes) = enforce_and_apply(
            cached_limits(&[DataCategory::Transaction]),
            original.clone(),
        );
        assert_eq!(kept.to_vec().unwrap(), original.to_vec().unwrap());
        assert!(outcomes.is_empty());
    }
}

#[test]
fn test_metrics_return_namespace_backoff_without_discarding_items() {
    use relay_base_schema::metrics::MetricNamespace;

    let mut limit = rate_limit(DataCategory::MetricBucket);
    limit.namespaces = smallvec![MetricNamespace::Transactions, MetricNamespace::Spans];
    let mut cached = RateLimits::new();
    cached.add(limit);
    for already_limited in [false, true] {
        let mut original = envelope![MetricBuckets];
        let metric = original.items_mut().next().unwrap();
        metric.set_rate_limited(already_limited);
        metric.set_source_quantities(SourceQuantities {
            transactions: 2,
            spans: 3,
            buckets: 4,
        });
        let (kept, client_rate_limits, outcomes) =
            enforce_and_apply(cached.clone(), original.clone());
        assert_eq!(kept.to_vec().unwrap(), original.to_vec().unwrap());
        assert_eq!(kept.items().next().unwrap().rate_limited(), already_limited);
        assert_eq!(
            kept.items().next().unwrap().source_quantities(),
            original.items().next().unwrap().source_quantities(),
        );
        assert!(outcomes.is_empty());
        assert_eq!(
            crate::utils::format_rate_limits(&client_rate_limits),
            crate::utils::format_rate_limits(&cached),
        );
    }
}

#[test]
fn test_error_and_user_report_outcomes() {
    let error_outcome = Outcome::RateLimited(Some(ReasonCode::new("error_quota")));
    let feedback_outcome = Outcome::RateLimited(Some(ReasonCode::new("feedback_quota")));
    for error_blocked in [false, true] {
        for feedback_blocked in [false, true] {
            let (mut envelope, mut outcomes) =
                Managed::for_test(envelope![Event, UserReport]).build();
            let mut cached = RateLimits::new();
            for (category, blocked, reason) in [
                (DataCategory::Error, error_blocked, "error_quota"),
                (
                    DataCategory::UserReportV2,
                    feedback_blocked,
                    "feedback_quota",
                ),
            ] {
                if blocked {
                    let mut limit = rate_limit(category);
                    limit.reason_code = Some(ReasonCode::new(reason));
                    cached.add(limit);
                }
            }
            enforce_rate_limits(&mut envelope, &cached, &[]);
            assert_eq!(
                envelope
                    .items()
                    .map(|item| item.ty().clone())
                    .collect::<Vec<_>>(),
                match (error_blocked, feedback_blocked) {
                    (true, _) => vec![],
                    (false, true) => vec![ItemType::Event],
                    (false, false) => vec![ItemType::Event, ItemType::UserReport],
                }
            );
            if error_blocked {
                outcomes.assert_outcome(&error_outcome, DataCategory::Error, 1);
            }
            if error_blocked || feedback_blocked {
                let outcome = if error_blocked {
                    &error_outcome
                } else {
                    &feedback_outcome
                };
                outcomes.assert_outcome(outcome, DataCategory::UserReportV2, 1);
            }
            envelope.accept(drop);
            outcomes.assert_no_outcomes();
        }
    }
}

#[test]
fn test_enforce_counts_implicit_event_once() {
    for denied_categories in [
        &[][..],
        &[DataCategory::Error][..],
        &[DataCategory::Attachment][..],
    ] {
        let envelope = envelope![Attachment::Minidump, Attachment::Minidump];
        let cached = cached_limits(denied_categories);
        let (envelope, _, outcomes) = enforce_and_apply(cached, envelope);

        let error_blocked = denied_categories.contains(&DataCategory::Error);
        let attachments_blocked = denied_categories.contains(&DataCategory::Attachment);
        assert_eq!(envelope.len(), if error_blocked { 0 } else { 2 });
        let mut expected = BTreeMap::new();
        if error_blocked || attachments_blocked {
            expected.insert(DataCategory::Attachment, 20);
            expected.insert(DataCategory::AttachmentItem, 2);
        }
        if error_blocked {
            expected.insert(DataCategory::Error, 1);
        }
        assert_eq!(outcomes, expected);
        assert!(
            envelope
                .items()
                .all(|item| item.rate_limited() == attachments_blocked)
        );
    }
}

#[test]
fn test_parent_rejection_counts_only_removed_items() {
    for (item_type, dropped_category) in [
        (ItemType::ReplayEvent, Some(DataCategory::Replay)),
        (ItemType::Span, None),
    ] {
        let mut envelope = envelope![Event];
        envelope.add_item(Item::new(item_type));
        let (mut envelope, mut outcomes) = Managed::for_test(envelope).build();
        enforce_rate_limits(&mut envelope, &cached_limits(&[DataCategory::Error]), &[]);
        outcomes.assert_outcome(&Outcome::RateLimited(None), DataCategory::Error, 1);
        if let Some(category) = dropped_category {
            assert!(envelope.is_empty());
            outcomes.assert_outcome(&Outcome::RateLimited(None), category, 1);
        } else {
            assert_eq!(envelope.len(), 1);
            assert_eq!(envelope.items().next().unwrap().ty(), &ItemType::Span);
        }
        envelope.accept(drop);
        outcomes.assert_no_outcomes();
    }
}

#[test]
fn test_surviving_event_creator_keeps_associated_items() {
    for parents in [
        [Some(ParentId::SpanId(None)), None],
        [None, Some(ParentId::SpanId(None))],
    ] {
        let mut envelope = envelope![UserReport];
        for parent in parents {
            let mut item = trace_attachment_item(10, parent);
            item.set_attachment_type(AttachmentType::Minidump);
            envelope.add_item(item);
        }
        envelope.add_item(Item::new(ItemType::Session));

        // Only the span-parent creator is removed. The trace-parent creator still
        // supplies the shared event, regardless of which creator appeared first.
        let (envelope, _, outcomes) =
            enforce_and_apply(cached_limits(&[DataCategory::Span]), envelope);
        assert_eq!(
            envelope.items().map(Item::ty).collect::<Vec<_>>(),
            [
                &ItemType::UserReport,
                &ItemType::Attachment,
                &ItemType::Session
            ],
        );
        assert!(matches!(
            envelope.items().nth(1).unwrap().attachment_parent_type(),
            AttachmentParentType::Trace,
        ));
        assert_eq!(
            outcomes,
            BTreeMap::from([
                (DataCategory::Attachment, 10),
                (DataCategory::AttachmentItem, 1),
            ]),
        );
    }
}

#[test]
fn test_last_event_contributor_rejects_associated_items() {
    // The current header model allows a crash subtype on a trace attachment. Its attachment
    // quota can remove the event contributor without an error quota. Associated items on
    // either side must follow it; the independent session must survive.
    let mut envelope = envelope![UserReport, Attachment::Minidump, FormData, Session];
    envelope
        .items_mut()
        .nth(1)
        .unwrap()
        .set_payload(ContentType::TraceAttachment, "0123456789");
    let cached = cached_limits(&[DataCategory::Attachment]);
    let (envelope, _, outcomes) = enforce_and_apply(cached, envelope);

    assert_eq!(
        envelope.items().map(Item::ty).collect::<Vec<_>>(),
        [&ItemType::Session]
    );
    assert_eq!(
        outcomes,
        BTreeMap::from([
            (DataCategory::Error, 1),
            (DataCategory::Attachment, 10),
            (DataCategory::AttachmentItem, 1),
            (DataCategory::UserReportV2, 1),
        ])
    );
}

#[test]
fn test_enforce_limit_error_event() {
    let envelope = envelope![Event];

    let cached = cached_limits(&[DataCategory::Error]);
    let (envelope, limits, _) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert!(envelope.is_empty());
}

#[test]
fn test_enforce_limit_error_with_attachments() {
    let envelope = envelope![Event, Attachment];

    let cached = cached_limits(&[DataCategory::Error]);
    let (envelope, limits, _) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert!(envelope.is_empty());
}

#[test]
fn test_enforce_limit_minidump() {
    let envelope = envelope![Attachment::Minidump];

    let cached = cached_limits(&[DataCategory::Error]);
    let (envelope, limits, _) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert!(envelope.is_empty());
}

#[test]
fn test_enforce_limit_attachments() {
    for subtype in [AttachmentType::Attachment, AttachmentType::Flamegraph] {
        let mut envelope = envelope![Attachment::Minidump, Attachment];
        envelope
            .items_mut()
            .last()
            .unwrap()
            .set_attachment_type(subtype);

        let cached = cached_limits(&[DataCategory::Attachment]);
        let (envelope, limits, _) = enforce_and_apply(cached, envelope);

        // Attachments would be limited, but crash reports create events and are thus allowed.
        assert!(limits.is_limited());
        assert_eq!(envelope.len(), 1);
    }
}

#[test]
fn test_enforce_nintendo_dying_message_with_attachment_limit() {
    let mut envelope = envelope![Attachment::NintendoSwitchDyingMessage, Attachment];
    envelope.items_mut().next().unwrap().set_payload(
        ContentType::OctetStream,
        &include_bytes!(
            "../../../../tests/integration/fixtures/native/nnswitch_dying_message_plain.dat"
        )[..],
    );

    let cached = cached_limits(&[DataCategory::Attachment]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    // The dying message carries the crash event and is expanded during processing;
    // it is not itself a billable attachment. Only the ordinary attachment is rejected.
    assert!(limits.is_limited());
    assert_eq!(envelope.len(), 1);
    assert_eq!(
        envelope.items().next().unwrap().attachment_type(),
        Some(AttachmentType::NintendoSwitchDyingMessage)
    );
    assert_eq!(
        outcomes,
        BTreeMap::from([
            (DataCategory::Attachment, 10),
            (DataCategory::AttachmentItem, 1),
        ])
    );
}

#[test]
fn test_enforce_nintendo_parent_error_limit() {
    let envelope = envelope![Event, Attachment::NintendoSwitchDyingMessage, Attachment];
    let cached = cached_limits(&[DataCategory::Error]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert!(envelope.is_empty());
    // The event and dying message represent one error. Only the ordinary attachment counts.
    assert_eq!(
        outcomes,
        BTreeMap::from([
            (DataCategory::Error, 1),
            (DataCategory::Attachment, 10),
            (DataCategory::AttachmentItem, 1),
        ])
    );
}

/// Limit stand-alone profiles.
#[test]
fn test_enforce_limit_profiles() {
    let envelope = envelope![Profile, Profile];

    let cached = cached_limits(&[DataCategory::Profile]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert_eq!(envelope.len(), 0);

    assert_eq!(
        outcomes,
        BTreeMap::from([
            (DataCategory::Profile, 2),
            (DataCategory::ProfileIndexed, 2)
        ])
    );
}

/// Limit profile chunks.
#[test]
fn test_enforce_limit_profile_chunks_no_profile_type() {
    // In this test we have profile chunks which have not yet been classified, which means they
    // should not be rate limited.
    let envelope = envelope![ProfileChunk, ProfileChunk];

    let cached = cached_limits(&[DataCategory::ProfileChunk]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);
    assert!(!limits.is_limited());
    assert_eq!(outcomes, BTreeMap::new());

    let cached = cached_limits(&[DataCategory::ProfileChunkUi]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);
    assert!(!limits.is_limited());
    assert_eq!(outcomes, BTreeMap::new());

    assert_eq!(envelope.len(), 2);
}

#[test]
fn test_enforce_limit_profile_chunks_ui() {
    let mut envelope = envelope![];

    let mut item = Item::new(ItemType::ProfileChunk);
    item.set_platform("python".to_owned());
    envelope.add_item(item);
    let mut item = Item::new(ItemType::ProfileChunk);
    item.set_platform("javascript".to_owned());
    envelope.add_item(item);

    let cached = cached_limits(&[DataCategory::ProfileChunkUi]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert_eq!(envelope.len(), 1);

    assert_eq!(
        outcomes,
        BTreeMap::from([(DataCategory::ProfileChunkUi, 1)])
    );
}

#[test]
fn test_enforce_limit_profile_chunks_backend() {
    let mut envelope = envelope![];

    let mut item = Item::new(ItemType::ProfileChunk);
    item.set_platform("python".to_owned());
    envelope.add_item(item);
    let mut item = Item::new(ItemType::ProfileChunk);
    item.set_platform("javascript".to_owned());
    envelope.add_item(item);

    let cached = cached_limits(&[DataCategory::ProfileChunk]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert_eq!(envelope.len(), 1);

    assert_eq!(outcomes, BTreeMap::from([(DataCategory::ProfileChunk, 1)]));
}

/// Limit replays.
#[test]
fn test_enforce_limit_replays() {
    let envelope = envelope![ReplayEvent, ReplayRecording, ReplayVideo];

    let cached = cached_limits(&[DataCategory::Replay]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert_eq!(envelope.len(), 0);

    assert_eq!(outcomes, BTreeMap::from([(DataCategory::Replay, 3),]));
}

/// Limit monitor checkins.
#[test]
fn test_enforce_limit_monitor_checkins() {
    let envelope = envelope![CheckIn];

    let cached = cached_limits(&[DataCategory::Monitor]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert_eq!(envelope.len(), 0);

    assert_eq!(outcomes, BTreeMap::from([(DataCategory::Monitor, 1)]))
}

#[test]
fn test_enforce_pass_minidump() {
    let envelope = envelope![Attachment::Minidump];

    let cached = cached_limits(&[DataCategory::Attachment]);
    let (envelope, limits, _) = enforce_and_apply(cached, envelope);

    // If only crash report attachments are present, we don't emit a rate limit.
    assert!(!limits.is_limited());
    assert_eq!(envelope.len(), 1);
}

#[test]
fn test_enforce_skip_rate_limited() {
    let mut envelope = envelope![];

    let mut item = Item::new(ItemType::Attachment);
    item.set_payload(ContentType::OctetStream, "0123456789");
    item.set_rate_limited(true);
    envelope.add_item(item);

    let cached = cached_limits(&[DataCategory::Error]);
    let (envelope, limits, _) = enforce_and_apply(cached, envelope);

    assert!(!limits.is_limited()); // No new rate limits applied.
    assert_eq!(envelope.len(), 1); // The item was retained
}

#[test]
fn test_enforce_skip_rate_limited_minidump() {
    let mut envelope = envelope![Attachment::Minidump, Attachment];
    envelope.items_mut().next().unwrap().set_rate_limited(true);

    let cached = cached_limits(&[DataCategory::Attachment]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert_eq!(envelope.len(), 1);
    let minidump = envelope.items().next().unwrap();
    assert_eq!(minidump.attachment_type(), Some(AttachmentType::Minidump));
    assert!(minidump.rate_limited());
    // A new attachment limit must not emit another outcome for the retained minidump.
    assert_eq!(
        outcomes,
        BTreeMap::from([
            (DataCategory::Attachment, 10),
            (DataCategory::AttachmentItem, 1),
        ])
    );
}

#[test]
fn test_enforce_pass_sessions() {
    let envelope = envelope![Session, Session, Session];

    let cached = cached_limits(&[DataCategory::Error]);
    let (envelope, limits, _) = enforce_and_apply(cached, envelope);

    // If only crash report attachments are present, we don't emit a rate limit.
    assert!(!limits.is_limited());
    assert_eq!(envelope.len(), 3);
}

#[test]
fn test_enforce_limit_sessions() {
    let envelope = envelope![Session, Session, Event];

    let cached = cached_limits(&[DataCategory::Session]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    // If only crash report attachments are present, we don't emit a rate limit.
    assert!(limits.is_limited());
    assert_eq!(envelope.len(), 1);
    assert!(outcomes.is_empty());
}

#[test]
#[cfg(feature = "processing")]
fn test_enforce_limit_assumed_event() {
    let envelope = envelope![Transaction];

    let cached = cached_limits(&[DataCategory::Transaction]);
    let (envelope, limits, _) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert!(envelope.is_empty()); // obviously
}

#[test]
#[cfg(feature = "processing")]
fn test_enforce_limit_assumed_attachments() {
    let envelope = envelope![Event, Attachment, Attachment];

    let cached = cached_limits(&[DataCategory::Error]);
    let (envelope, limits, _) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert!(envelope.is_empty());
}

#[test]
fn test_enforce_transaction() {
    let envelope = envelope![Transaction];

    let cached = cached_limits(&[DataCategory::Transaction]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());

    assert!(envelope.is_empty());

    assert_eq!(
        outcomes,
        BTreeMap::from([
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
            (DataCategory::Span, 1),
            (DataCategory::SpanIndexed, 1),
        ])
    );
}

#[test]
fn test_enforce_transaction_non_indexed() {
    let original = envelope![Transaction, Profile, Attachment];
    let (envelope, limits, outcomes) = enforce_and_apply(
        cached_limits(&[
            DataCategory::TransactionIndexed,
            DataCategory::ProfileIndexed,
            DataCategory::SpanIndexed,
        ]),
        original.clone(),
    );
    assert!(!limits.is_limited());
    assert!(outcomes.is_empty());
    assert_eq!(envelope.to_vec().unwrap(), original.to_vec().unwrap());
}

#[test]
fn test_enforce_transaction_attachment_enforced() {
    let envelope = envelope![Transaction, Attachment];

    let cached = cached_limits(&[DataCategory::Transaction]);
    let (envelope, _, _) = enforce_and_apply(cached, envelope);

    assert!(envelope.is_empty());
}

#[test]
fn test_enforce_transaction_profile_enforced() {
    let envelope = envelope![Transaction, Profile];

    let cached = cached_limits(&[DataCategory::Transaction]);
    let (envelope, _, outcomes) = enforce_and_apply(cached, envelope);

    assert!(envelope.is_empty());

    assert_eq!(
        outcomes,
        BTreeMap::from([
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
            (DataCategory::Profile, 1),
            (DataCategory::ProfileIndexed, 1),
            (DataCategory::Span, 1),
            (DataCategory::SpanIndexed, 1),
        ])
    );
}

#[test]
fn test_enforce_transaction_standalone_profile_enforced() {
    // When the transaction is sampled, the profile survives as standalone.
    let envelope = envelope![Profile];

    let cached = cached_limits(&[DataCategory::Transaction]);
    let (envelope, _, outcomes) = enforce_and_apply(cached, envelope);

    assert!(envelope.is_empty());

    assert_eq!(
        outcomes,
        BTreeMap::from([
            (DataCategory::Profile, 1),
            (DataCategory::ProfileIndexed, 1),
        ])
    );
}

#[test]
fn test_enforce_span() {
    let envelope = envelope![Span, Span];

    let cached = cached_limits(&[DataCategory::Span]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert!(envelope.is_empty());

    assert_eq!(
        outcomes,
        BTreeMap::from([(DataCategory::Span, 2), (DataCategory::SpanIndexed, 2)])
    );
}

#[test]
fn test_enforce_span_no_indexing_quota() {
    let envelope = envelope![Span, Span];

    let cached = cached_limits(&[DataCategory::SpanIndexed]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    assert!(!limits.is_limited());
    assert_eq!(envelope.len(), 2);

    assert_eq!(outcomes, BTreeMap::new());
}

#[test]
fn test_enforce_limit_logs_count() {
    let envelope = envelope![Log, Log];

    let cached = cached_limits(&[DataCategory::LogItem]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert_eq!(envelope.len(), 0);

    assert_eq!(
        outcomes,
        BTreeMap::from([(DataCategory::LogItem, 2), (DataCategory::LogByte, 20)])
    );
}

#[test]
fn test_enforce_limit_logs_bytes() {
    let envelope = envelope![Log, Log];

    let cached = cached_limits(&[DataCategory::LogByte]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert_eq!(envelope.len(), 0);

    assert_eq!(
        outcomes,
        BTreeMap::from([(DataCategory::LogItem, 2), (DataCategory::LogByte, 20)])
    );
}

#[test]
fn test_container_counts_and_byte_quota_fallback() {
    for (ty, count_category, byte_category) in [
        (ItemType::Log, DataCategory::LogItem, DataCategory::LogByte),
        (
            ItemType::TraceMetric,
            DataCategory::TraceMetric,
            DataCategory::TraceMetricByte,
        ),
    ] {
        for count in [0, 3] {
            for limited_category in [count_category, byte_category] {
                let mut envelope = envelope![];
                let mut item = Item::new(ty.clone());
                item.set_payload_with_item_count(ContentType::Json, "0123456789", count);
                envelope.add_item(item);
                let (envelope, limits, outcomes) =
                    enforce_and_apply(cached_limits(&[limited_category]), envelope);

                // An empty container has no count to reject, but still has billable bytes.
                let rejected = count > 0 || limited_category == byte_category;
                assert_eq!(envelope.is_empty(), rejected);
                assert_eq!(limits.is_limited(), rejected);
                let mut expected = BTreeMap::new();
                if rejected {
                    expected.insert(byte_category, 10);
                    if count > 0 {
                        expected.insert(count_category, count as usize);
                    }
                }
                assert_eq!(outcomes, expected);
            }
        }
    }
}

#[test]
fn test_integration_items_use_native_limits() {
    use crate::integrations::{LogsIntegration, OtelFormat, SpansIntegration};

    for (integration, category, expected) in [
        (
            Integration::Logs(LogsIntegration::Nel),
            DataCategory::LogByte,
            [(DataCategory::LogItem, 1), (DataCategory::LogByte, 10)],
        ),
        (
            Integration::Spans(SpansIntegration::OtelV1 {
                format: OtelFormat::Json,
            }),
            DataCategory::Span,
            [(DataCategory::Span, 1), (DataCategory::SpanIndexed, 1)],
        ),
    ] {
        let mut envelope = envelope![Integration];
        envelope
            .items_mut()
            .next()
            .unwrap()
            .set_payload(ContentType::Integration(integration), "0123456789");
        let (envelope, limits, outcomes) = enforce_and_apply(cached_limits(&[category]), envelope);
        assert!(envelope.is_empty());
        assert!(limits.is_limited());
        assert_eq!(outcomes, BTreeMap::from(expected));
    }
}

#[test]
fn test_rejected_source_quantities_are_total_only() {
    for already_limited in [false, true] {
        let mut envelope = envelope![Span, Span];
        let item = envelope.items_mut().next().unwrap();
        item.set_source_quantities(SourceQuantities {
            transactions: 2,
            spans: 3,
            buckets: 4,
        });
        item.set_rate_limited(already_limited);
        let (envelope, _, outcomes) =
            enforce_and_apply(cached_limits(&[DataCategory::Span]), envelope);
        assert!(envelope.is_empty());
        assert_eq!(
            outcomes,
            if already_limited {
                BTreeMap::from([(DataCategory::Span, 1), (DataCategory::SpanIndexed, 1)])
            } else {
                // Two raw spans plus three source spans; only the raw spans are indexed.
                BTreeMap::from([
                    (DataCategory::Transaction, 2),
                    (DataCategory::Span, 5),
                    (DataCategory::SpanIndexed, 2),
                ])
            },
        );
    }
}

#[test]
fn test_enforce_standalone_span_attachment() {
    let test_cases = &[
        RateLimitTestCase {
            name: "span_limit",
            denied_categories: &[DataCategory::Span],
            expect_attachment_limit_active: true,
            expected_outcomes: &[
                (DataCategory::Attachment, 7),
                (DataCategory::AttachmentItem, 1),
            ],
        },
        RateLimitTestCase {
            name: "span_indexed_limit",
            denied_categories: &[DataCategory::SpanIndexed],
            expect_attachment_limit_active: false,
            expected_outcomes: &[],
        },
        RateLimitTestCase {
            name: "attachment_limit",
            denied_categories: &[DataCategory::Attachment],
            expect_attachment_limit_active: true,
            expected_outcomes: &[
                (DataCategory::Attachment, 7),
                (DataCategory::AttachmentItem, 1),
            ],
        },
        RateLimitTestCase {
            name: "attachment_indexed_limit",
            denied_categories: &[DataCategory::AttachmentItem],
            expect_attachment_limit_active: true,
            expected_outcomes: &[
                (DataCategory::Attachment, 7),
                (DataCategory::AttachmentItem, 1),
            ],
        },
        RateLimitTestCase {
            name: "transaction_limit",
            denied_categories: &[DataCategory::Transaction],
            expect_attachment_limit_active: false,
            expected_outcomes: &[],
        },
        RateLimitTestCase {
            name: "error_limit",
            denied_categories: &[DataCategory::Error],
            expect_attachment_limit_active: false,
            expected_outcomes: &[],
        },
        RateLimitTestCase {
            name: "no_limits",
            denied_categories: &[],
            expect_attachment_limit_active: false,
            expected_outcomes: &[],
        },
    ];

    for RateLimitTestCase {
        name,
        denied_categories,
        expect_attachment_limit_active,
        expected_outcomes,
    } in test_cases
    {
        let mut envelope = envelope![];
        envelope.add_item(trace_attachment_item(7, Some(ParentId::SpanId(None))));

        let cached = cached_limits(denied_categories);
        let (envelope, _, outcomes) = enforce_and_apply(cached, envelope);

        assert_eq!(
            envelope
                .items()
                .all(|item| item.ty() != &ItemType::Attachment),
            *expect_attachment_limit_active,
            "{name}: span_attachment limit mismatch"
        );

        assert_eq!(
            outcomes,
            expected_outcomes
                .iter()
                .copied()
                .collect::<BTreeMap<_, _>>(),
            "{name}: outcome mismatch"
        );
    }
}

#[test]
fn test_enforce_span_with_span_attachment() {
    let test_cases = &[
        RateLimitTestCase {
            name: "span_limit",
            denied_categories: &[DataCategory::Span, DataCategory::Attachment], // Attachment here has no effect
            expect_attachment_limit_active: true,
            expected_outcomes: &[
                (DataCategory::Attachment, 7),
                (DataCategory::AttachmentItem, 1),
                (DataCategory::Span, 1),
                (DataCategory::SpanIndexed, 1),
            ],
        },
        RateLimitTestCase {
            name: "span_indexed_limit",
            denied_categories: &[DataCategory::SpanIndexed, DataCategory::Attachment],
            expect_attachment_limit_active: true,
            expected_outcomes: &[
                (DataCategory::Attachment, 7),
                (DataCategory::AttachmentItem, 1),
            ],
        },
        RateLimitTestCase {
            name: "attachment_limit",
            denied_categories: &[DataCategory::Attachment],
            expect_attachment_limit_active: true,
            expected_outcomes: &[
                (DataCategory::Attachment, 7),
                (DataCategory::AttachmentItem, 1),
            ],
        },
        RateLimitTestCase {
            name: "attachment_indexed_limit",
            denied_categories: &[DataCategory::AttachmentItem],
            expect_attachment_limit_active: true,
            expected_outcomes: &[
                (DataCategory::Attachment, 7),
                (DataCategory::AttachmentItem, 1),
            ],
        },
        RateLimitTestCase {
            name: "transaction_limit",
            denied_categories: &[DataCategory::Transaction],
            expect_attachment_limit_active: false,
            expected_outcomes: &[],
        },
        RateLimitTestCase {
            name: "error_limit",
            denied_categories: &[DataCategory::Error],
            expect_attachment_limit_active: false,
            expected_outcomes: &[],
        },
        RateLimitTestCase {
            name: "no_limits",
            denied_categories: &[],
            expect_attachment_limit_active: false,
            expected_outcomes: &[],
        },
    ];

    for RateLimitTestCase {
        name,
        denied_categories,
        expect_attachment_limit_active,
        expected_outcomes,
    } in test_cases
    {
        let mut envelope = envelope![Span];
        envelope.add_item(trace_attachment_item(7, Some(ParentId::SpanId(None))));

        let cached = cached_limits(denied_categories);
        let (envelope, _, outcomes) = enforce_and_apply(cached, envelope);

        assert_eq!(
            envelope
                .items()
                .all(|item| item.ty() != &ItemType::Attachment),
            *expect_attachment_limit_active,
            "{name}: span_attachment limit mismatch"
        );

        assert_eq!(
            outcomes,
            expected_outcomes
                .iter()
                .copied()
                .collect::<BTreeMap<_, _>>(),
            "{name}: outcome mismatch"
        );
    }
}

#[test]
fn test_enforce_transaction_span_attachment() {
    let test_cases = &[
        RateLimitTestCase {
            name: "transaction_limit",
            denied_categories: &[DataCategory::Transaction],
            expect_attachment_limit_active: true,
            expected_outcomes: &[
                (DataCategory::Transaction, 1),
                (DataCategory::TransactionIndexed, 1),
                (DataCategory::Attachment, 7),
                (DataCategory::AttachmentItem, 1),
                (DataCategory::Span, 1),
                (DataCategory::SpanIndexed, 1),
            ],
        },
        RateLimitTestCase {
            name: "error_limit",
            denied_categories: &[DataCategory::Error],
            expect_attachment_limit_active: false,
            expected_outcomes: &[],
        },
        RateLimitTestCase {
            name: "no_limits",
            denied_categories: &[],
            expect_attachment_limit_active: false,
            expected_outcomes: &[],
        },
    ];

    for RateLimitTestCase {
        name,
        denied_categories,
        expect_attachment_limit_active,
        expected_outcomes,
    } in test_cases
    {
        let mut envelope = envelope![Transaction];
        envelope.add_item(trace_attachment_item(7, Some(ParentId::SpanId(None))));

        let cached = cached_limits(denied_categories);
        let (envelope, _, outcomes) = enforce_and_apply(cached, envelope);

        assert_eq!(
            envelope
                .items()
                .all(|item| item.ty() != &ItemType::Attachment),
            *expect_attachment_limit_active,
            "{name}: span_attachment limit mismatch"
        );

        assert_eq!(
            outcomes,
            expected_outcomes
                .iter()
                .copied()
                .collect::<BTreeMap<_, _>>(),
            "{name}: outcome mismatch"
        );
    }
}

#[test]
fn test_enforce_standalone_trace_attachment() {
    let test_cases = &[
        RateLimitTestCase {
            name: "attachment_limit",
            denied_categories: &[DataCategory::Attachment],
            expect_attachment_limit_active: true,
            expected_outcomes: &[
                (DataCategory::Attachment, 7),
                (DataCategory::AttachmentItem, 1),
            ],
        },
        RateLimitTestCase {
            name: "attachment_limit_and_attachment_item_limit",
            denied_categories: &[DataCategory::Attachment, DataCategory::AttachmentItem],
            expect_attachment_limit_active: true,
            expected_outcomes: &[
                (DataCategory::Attachment, 7),
                (DataCategory::AttachmentItem, 1),
            ],
        },
        RateLimitTestCase {
            name: "attachment_item_limit",
            denied_categories: &[DataCategory::AttachmentItem],
            expect_attachment_limit_active: true,
            expected_outcomes: &[
                (DataCategory::Attachment, 7),
                (DataCategory::AttachmentItem, 1),
            ],
        },
        RateLimitTestCase {
            name: "no_limits",
            denied_categories: &[],
            expect_attachment_limit_active: false,
            expected_outcomes: &[],
        },
    ];

    for RateLimitTestCase {
        name,
        denied_categories,
        expect_attachment_limit_active,
        expected_outcomes,
    } in test_cases
    {
        let mut envelope = envelope![];
        envelope.add_item(trace_attachment_item(7, None));

        let cached = cached_limits(denied_categories);
        let (envelope, _, outcomes) = enforce_and_apply(cached, envelope);

        assert_eq!(
            envelope
                .items()
                .all(|item| item.ty() != &ItemType::Attachment),
            *expect_attachment_limit_active,
            "{name}: trace_attachment limit mismatch"
        );

        assert_eq!(
            outcomes,
            expected_outcomes
                .iter()
                .copied()
                .collect::<BTreeMap<_, _>>(),
            "{name}: outcome mismatch"
        );
    }
}
