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
                $( item.as_attachment_mut().unwrap().set_attachment_type(AttachmentType::$attachment_type); )?
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
    item.as_attachment_mut().unwrap().set_parent_id(parent_id);
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
    for (body, header, declared_spans, spans) in [
        (r#"{"spans":[{},null,1]}"#, None, None, 3),
        (r#"{"spans":[],"spans":[{}]}"#, None, None, 0),
        (r#"{"spans":null}"#, None, None, 0),
        (r#"{}"#, None, None, 0),
        (r#"{"spans":[{}]}"#, Some("0"), Some(0), 0),
        (r#"{"spans":[{}]}"#, Some("12"), Some(12), 12),
        (r#"{"spans":[{}]}"#, Some("null"), None, 1),
        (r#"{"spans":[{}]}"#, Some(r#""invalid""#), None, 1),
        (r#"{"spans":[{}]}"#, Some("-1"), None, 1),
        (r#"{"spans":[{}]}"#, Some("4294967296"), None, 1),
    ] {
        let header = header.map_or(String::new(), |value| format!(r#", "span_count":{value}"#));
        let bytes = format!("{{\"type\":\"transaction\"{header}}}\n{body}\n");
        let (transaction, _) = Item::parse(bytes.into()).unwrap();
        // Summarizing counts only declared spans; payload parsing belongs to rejection.
        let summary = EnvelopeSummary::compute_items([&transaction]);
        assert_eq!(
            summary.span_quantity,
            1 + declared_spans.unwrap_or(0) as usize
        );
        assert_eq!(
            transaction.as_transaction().unwrap().count_embedded_spans(),
            spans
        );
        assert_eq!(
            transaction.as_transaction().unwrap().span_count(),
            declared_spans
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

        // An error event selects the envelope's event category, so a transaction limit leaves this
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
fn test_user_report_with_nintendo_header() {
    // Attachment headers do not change a feedback item's category or quantity.
    let mut original = envelope![UserReport];
    original.items_mut().next().unwrap().set_test_header(
        crate::envelope::ItemHeaderKey::AttachmentType,
        Some(AttachmentType::NintendoSwitchDyingMessage),
    );
    assert_eq!(EnvelopeSummary::compute(&original).user_report_quantity, 1);

    let (kept, limits, outcomes) =
        enforce_and_apply(cached_limits(&[DataCategory::UserReportV2]), original);
    assert!(kept.is_empty());
    assert!(limits.is_limited());
    assert_eq!(outcomes, BTreeMap::from([(DataCategory::UserReportV2, 1)]));
}

#[test]
fn test_user_report_with_nintendo_header_counts_when_parent_rejected() {
    let mut envelope = envelope![Event, UserReport];
    envelope.items_mut().nth(1).unwrap().set_test_header(
        crate::envelope::ItemHeaderKey::AttachmentType,
        Some(AttachmentType::NintendoSwitchDyingMessage),
    );

    let (kept, _, outcomes) = enforce_and_apply(cached_limits(&[DataCategory::Error]), envelope);
    assert!(kept.is_empty());
    assert_eq!(
        outcomes,
        BTreeMap::from([(DataCategory::Error, 1), (DataCategory::UserReportV2, 1),])
    );
}

#[test]
fn test_nintendo_attachment_has_no_individual_attachment_quantity() {
    let original = envelope![Attachment::NintendoSwitchDyingMessage];
    let (kept, limits, outcomes) = enforce_and_apply(
        cached_limits(&[DataCategory::Attachment, DataCategory::AttachmentItem]),
        original.clone(),
    );
    assert_eq!(kept.to_vec().unwrap(), original.to_vec().unwrap());
    assert!(limits.is_empty());
    assert!(outcomes.is_empty());

    let (kept, _, outcomes) = enforce_and_apply(cached_limits(&[DataCategory::Error]), original);
    assert!(kept.is_empty());
    assert_eq!(outcomes, BTreeMap::from([(DataCategory::Error, 1)]));
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
fn test_event_rejection_preserves_independent_items() {
    for item_type in [ItemType::ReplayEvent, ItemType::Span] {
        let mut envelope = envelope![Event];
        envelope.add_item(Item::new(item_type.clone()));
        let (mut envelope, mut outcomes) = Managed::for_test(envelope).build();
        enforce_rate_limits(&mut envelope, &cached_limits(&[DataCategory::Error]), &[]);
        outcomes.assert_outcome(&Outcome::RateLimited(None), DataCategory::Error, 1);
        assert_eq!(envelope.len(), 1);
        assert_eq!(envelope.items().next().unwrap().ty(), &item_type);
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
            item.as_attachment_mut()
                .unwrap()
                .set_attachment_type(AttachmentType::Minidump);
            envelope.add_item(item);
        }
        envelope.add_item(Item::new(ItemType::Session));

        // The span limit removes only the minidump attached to the span. The minidump
        // attached to the trace still creates an event, so the user report stays in
        // either item order.
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
            envelope
                .items()
                .nth(1)
                .unwrap()
                .as_attachment()
                .unwrap()
                .attachment_parent_type(),
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
    // An attachment quota removes this minidump attached to a trace. Without another
    // item that creates an event, the user report, form data, and profiles must also
    // be dropped. The session stays.
    let mut envelope = envelope![UserReport, Attachment::Minidump, FormData, Session, Profile];
    envelope
        .items_mut()
        .nth(1)
        .unwrap()
        .set_payload(ContentType::TraceAttachment, "0123456789");
    for platform in ["python", "cocoa"] {
        let mut profile = Item::new(ItemType::Profile);
        profile
            .as_profile_mut()
            .unwrap()
            .set_platform(platform.into());
        envelope.add_item(profile);
    }
    let mut cached = cached_limits(&[DataCategory::Attachment]);
    let mut profile_limit = rate_limit(DataCategory::Profile);
    profile_limit.reason_code = Some(ReasonCode::new("profile"));
    cached.add(profile_limit);
    // This helper also checks that every outcome uses the attachment rejection's reason
    // (None), overriding the earlier profile quota's different reason.
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
            (DataCategory::Profile, 3),
            (DataCategory::ProfileIndexed, 3),
            (DataCategory::ProfileBackend, 1),
            (DataCategory::ProfileUi, 1),
        ])
    );
}

#[test]
fn test_event_category_survives_removable_crash_attachments() {
    // These mixed-creator envelopes are not normal SDK inputs. If we retain a direct event,
    // its category must stay the same when earlier crash attachments are removed.
    for (ty, category) in [
        (ItemType::Event, DataCategory::Error),
        (ItemType::Transaction, DataCategory::Transaction),
        (ItemType::Security, DataCategory::Security),
        (ItemType::UserReportV2, DataCategory::UserReportV2),
    ] {
        for reverse in [false, true] {
            let mut original = envelope![];
            let mut span_crash = trace_attachment_item(10, Some(ParentId::SpanId(None)));
            span_crash
                .as_attachment_mut()
                .unwrap()
                .set_attachment_type(AttachmentType::Minidump);
            let mut trace_crash = trace_attachment_item(10, None);
            trace_crash
                .as_attachment_mut()
                .unwrap()
                .set_attachment_type(AttachmentType::Minidump);
            let mut event = Item::new(ty.clone());
            event.set_payload(ContentType::Json, "{}");
            if ty == ItemType::Transaction {
                event.as_transaction_mut().unwrap().set_span_count(Some(0));
            }
            let mut items = vec![span_crash, event, trace_crash];
            if reverse {
                items.reverse();
            }
            for item in items {
                original.add_item(item);
            }

            assert_eq!(
                EnvelopeSummary::compute(&original).event.category(),
                Some(category)
            );
            for (denied, retained, rejected_attachments) in [
                (&[][..], 3, 0),
                (&[DataCategory::Span][..], 2, 1),
                (&[DataCategory::Attachment][..], 1, 2),
                (&[DataCategory::AttachmentItem][..], 1, 2),
                (&[DataCategory::Span, DataCategory::Attachment][..], 1, 2),
                (
                    &[DataCategory::Span, DataCategory::AttachmentItem][..],
                    1,
                    2,
                ),
                (
                    &[DataCategory::Attachment, DataCategory::AttachmentItem][..],
                    1,
                    2,
                ),
                (
                    &[
                        DataCategory::Span,
                        DataCategory::Attachment,
                        DataCategory::AttachmentItem,
                    ][..],
                    1,
                    2,
                ),
            ] {
                let (kept, _, outcomes) =
                    enforce_and_apply(cached_limits(denied), original.clone());
                assert_eq!(kept.len(), retained);
                assert!(kept.items().any(|item| item.ty() == &ty));
                assert_eq!(
                    EnvelopeSummary::compute(&kept).event.category(),
                    Some(category)
                );
                let expected = if rejected_attachments == 0 {
                    BTreeMap::new()
                } else {
                    BTreeMap::from([
                        (DataCategory::Attachment, 10 * rejected_attachments),
                        (DataCategory::AttachmentItem, rejected_attachments),
                    ])
                };
                assert_eq!(outcomes, expected);
            }

            // Check the selected category's own quota too, including its indexed accounting.
            let (kept, limits, outcomes) =
                enforce_and_apply(cached_limits(&[category]), original.clone());
            assert!(kept.is_empty());
            assert!(limits.is_limited());
            let mut expected = BTreeMap::from([
                (category, 1),
                (DataCategory::Attachment, 20),
                (DataCategory::AttachmentItem, 2),
            ]);
            if category == DataCategory::Transaction {
                expected.extend([
                    (DataCategory::TransactionIndexed, 1),
                    (DataCategory::Span, 1),
                    (DataCategory::SpanIndexed, 1),
                ]);
            }
            assert_eq!(outcomes, expected);

            // A preceding crash attachment must not make a non-error event use error quotas.
            if category != DataCategory::Error {
                let (kept, limits, outcomes) =
                    enforce_and_apply(cached_limits(&[DataCategory::Error]), original.clone());
                assert_eq!(kept.to_vec().unwrap(), original.to_vec().unwrap());
                assert!(limits.is_empty());
                assert!(outcomes.is_empty());
            }
        }
    }
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
            .as_attachment_mut()
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
        envelope
            .items()
            .next()
            .unwrap()
            .as_attachment()
            .unwrap()
            .attachment_type(),
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
    item.as_profile_chunk_mut()
        .unwrap()
        .set_platform("python".to_owned());
    envelope.add_item(item);
    let mut item = Item::new(ItemType::ProfileChunk);
    item.as_profile_chunk_mut()
        .unwrap()
        .set_platform("javascript".to_owned());
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
    item.as_profile_chunk_mut()
        .unwrap()
        .set_platform("python".to_owned());
    envelope.add_item(item);
    let mut item = Item::new(ItemType::ProfileChunk);
    item.as_profile_chunk_mut()
        .unwrap()
        .set_platform("javascript".to_owned());
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

#[test]
fn test_replay_limits_are_independent_of_event_limits() {
    for replay_types in [
        vec![ItemType::ReplayEvent, ItemType::ReplayRecording],
        vec![ItemType::ReplayRecording],
        vec![ItemType::ReplayVideo],
    ] {
        for event_blocked in [false, true] {
            for replay_blocked in [false, true] {
                let mut original = envelope![Event];
                for ty in &replay_types {
                    original.add_item(Item::new(ty.clone()));
                }
                let (mut envelope, mut outcomes) = Managed::for_test(original).build();
                let mut cached = RateLimits::new();
                for (category, blocked, reason) in [
                    (DataCategory::Error, event_blocked, "event_quota"),
                    (DataCategory::Replay, replay_blocked, "replay_quota"),
                ] {
                    if blocked {
                        let mut limit = rate_limit(category);
                        limit.reason_code = Some(ReasonCode::new(reason));
                        cached.add(limit);
                    }
                }

                let limits = enforce_rate_limits(&mut envelope, &cached, &[]);
                let mut expected = Vec::new();
                if !event_blocked {
                    expected.push(ItemType::Event);
                }
                if !replay_blocked {
                    expected.extend(replay_types.iter().cloned());
                }
                assert_eq!(
                    envelope
                        .items()
                        .map(|item| item.ty().clone())
                        .collect::<Vec<_>>(),
                    expected
                );
                assert_eq!(
                    limits.iter().count(),
                    usize::from(event_blocked) + usize::from(replay_blocked)
                );
                if event_blocked {
                    outcomes.assert_outcome(
                        &Outcome::RateLimited(Some(ReasonCode::new("event_quota"))),
                        DataCategory::Error,
                        1,
                    );
                }
                if replay_blocked {
                    for _ in &replay_types {
                        outcomes.assert_outcome(
                            &Outcome::RateLimited(Some(ReasonCode::new("replay_quota"))),
                            DataCategory::Replay,
                            1,
                        );
                    }
                }
                envelope.accept(drop);
                outcomes.assert_no_outcomes();
            }
        }
    }
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
    let minidump = envelope.items().next().unwrap().as_attachment().unwrap();
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
    let envelope = envelope![Session, Sessions, Session];

    let cached = cached_limits(&[DataCategory::Error]);
    let (envelope, limits, _) = enforce_and_apply(cached, envelope);

    assert!(!limits.is_limited());
    assert_eq!(envelope.len(), 3);
}

#[test]
fn test_enforce_limit_sessions() {
    let envelope = envelope![Session, Sessions, Event];

    let cached = cached_limits(&[DataCategory::Session]);
    let (envelope, limits, outcomes) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert_eq!(envelope.len(), 1);
    assert!(outcomes.is_empty());
}

#[test]
fn test_enforce_aggregate_sessions_without_updates() {
    for limited in [false, true] {
        let original = envelope![Sessions, Event];
        let cached = cached_limits(if limited {
            &[DataCategory::Session]
        } else {
            &[]
        });
        let (kept, limits, outcomes) = enforce_and_apply(cached, original);

        let expected = if limited {
            vec![ItemType::Event]
        } else {
            vec![ItemType::Sessions, ItemType::Event]
        };
        assert_eq!(
            kept.items()
                .map(|item| item.ty().clone())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(limits.is_limited(), limited);
        assert!(outcomes.is_empty());
    }
}

#[test]
#[cfg(feature = "processing")]
fn test_enforce_limit_assumed_event() {
    let envelope = envelope![Transaction];

    let cached = cached_limits(&[DataCategory::Transaction]);
    let (envelope, limits, _) = enforce_and_apply(cached, envelope);

    assert!(limits.is_limited());
    assert!(envelope.is_empty());
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
fn test_event_rejection_does_not_skip_standalone_span_limits() {
    for (event_type, event_category) in [
        (ItemType::Event, DataCategory::Error),
        (ItemType::Security, DataCategory::Security),
        (ItemType::UserReportV2, DataCategory::UserReportV2),
    ] {
        let mut original = envelope![Span];
        original.add_item(Item::new(event_type));
        let (mut envelope, mut outcomes) = Managed::for_test(original).build();
        let mut cached = RateLimits::new();
        for (category, reason) in [
            (event_category, "event_quota"),
            (DataCategory::Span, "span_quota"),
        ] {
            let mut limit = rate_limit(category);
            limit.reason_code = Some(ReasonCode::new(reason));
            cached.add(limit);
        }

        let client_limits = enforce_rate_limits(&mut envelope, &cached, &[]);

        assert!(envelope.is_empty());
        assert_eq!(client_limits.iter().count(), 2);
        outcomes.assert_outcome(
            &Outcome::RateLimited(Some(ReasonCode::new("event_quota"))),
            event_category,
            1,
        );
        for category in [DataCategory::Span, DataCategory::SpanIndexed] {
            outcomes.assert_outcome(
                &Outcome::RateLimited(Some(ReasonCode::new("span_quota"))),
                category,
                1,
            );
        }
        envelope.accept(drop);
        outcomes.assert_no_outcomes();
    }
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
                item.set_payload(ContentType::Json, "0123456789");
                item.set_test_header(crate::envelope::ItemHeaderKey::ItemCount, Some(count));
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
