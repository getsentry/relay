use relay_quotas::{DimensionMap, RateLimit, RateLimitScope, ReasonCode, RetryAfter};

use super::*;
use crate::envelope::{AttachmentType, ContentType, ItemType};
use crate::managed::Managed;
use crate::services::outcome::Outcome;

pub(super) fn attachment(ty: AttachmentType, payload: &'static str) -> Item {
    let mut item = Item::new(ItemType::Attachment);
    item.set_attachment_type(ty);
    item.set_payload(ContentType::OctetStream, payload);
    item
}

pub(super) fn assert_crash_rate_limit<T: SentryError>(
    make: impl Fn() -> T,
    expected_bytes: &[u64],
    assert_retained: impl Fn(T),
) {
    for category in [DataCategory::Attachment, DataCategory::AttachmentItem] {
        let (mut crash, mut handle) = Managed::for_test(make()).build();
        let mut limits = RateLimits::new();
        limits.add(RateLimit {
            categories: [category].into(),
            scope: RateLimitScope::Project(crash.scoping().project_id),
            reason_code: Some(ReasonCode::new("crash_attachment_limit")),
            retry_after: RetryAfter::from_secs(60),
            namespaces: Default::default(),
            dimensions: DimensionMap::default(),
        });
        // Recording after setting rate_limited loses the original quantities and panics here.
        // Applying the limit again must not produce duplicate outcomes.
        for _ in 0..2 {
            crash.modify(|crash, records| {
                crash
                    .apply_rate_limit(category, limits.clone(), records)
                    .unwrap();
            });
        }
        crash.accept(&assert_retained);
        let outcome = Outcome::RateLimited(Some(ReasonCode::new("crash_attachment_limit")));
        for &bytes in expected_bytes {
            handle.assert_outcome(&outcome, DataCategory::Attachment, bytes);
            handle.assert_outcome(&outcome, DataCategory::AttachmentItem, 1);
        }
        handle.assert_no_outcomes();
    }
}

#[test]
fn test_expansion_keeps_only_one_flamegraph() {
    let mut event = Item::new(ItemType::Event);
    event.set_payload(
        ContentType::Json,
        r#"{"exception":{"values":[{"type":"hang"}]}}"#,
    );
    let mut items = vec![
        event,
        attachment(AttachmentType::Flamegraph, "first"),
        attachment(AttachmentType::Attachment, "ordinary"),
        attachment(AttachmentType::Flamegraph, "second"),
    ];
    let expanded = ErrorKind::try_expand(&mut items, Context::for_test())
        .unwrap()
        .unwrap();
    let retained_flamegraphs = expanded
        .attachments
        .iter()
        .filter(|item| item.attachment_type() == Some(AttachmentType::Flamegraph))
        .count();
    assert_eq!(retained_flamegraphs, 0);
    // Removing the event swaps the last item into its slot before attachments are extracted.
    assert_eq!(expanded.flamegraph.unwrap().payload().as_ref(), b"second");
    assert_eq!(expanded.attachments.len(), 1);
    assert_eq!(expanded.attachments[0].payload().as_ref(), b"ordinary");
}

#[test]
fn test_forwarding_retains_separate_flamegraph() {
    use crate::processing::Forward as _;
    use crate::processing::errors::{ErrorOutput, SerializedError, process};

    let envelope = crate::testutils::new_envelope(false, "");
    let mut event = Item::new(ItemType::Event);
    event.set_payload(ContentType::Json, "{}");
    let (error, _handle) = Managed::for_test(SerializedError {
        headers: envelope.headers().clone(),
        items: vec![event, attachment(AttachmentType::Flamegraph, "graph")],
    })
    .build();
    let ctx = processing::Context::for_test();
    let (expanded, _) = process::expand(error, ctx).unwrap();
    assert!(expanded.attachments.is_empty());
    assert!(expanded.flamegraph.is_some());
    let output = ErrorOutput(expanded)
        .serialize_envelope(ctx.to_forward())
        .unwrap();
    output.accept(|envelope| {
        let graphs: Vec<_> = envelope
            .items()
            .filter(|item| item.attachment_type() == Some(AttachmentType::Flamegraph))
            .collect();
        assert_eq!(graphs.len(), 1);
        assert_eq!(graphs[0].payload().as_ref(), b"graph");
    });
}

#[tokio::test]
async fn test_attachment_quotas_do_not_infer_another_error() {
    use crate::processing::errors::{SerializedError, process};
    use crate::processing::{RateLimited as _, RateLimiter};

    struct RecordingLimiter<'a>(&'a mut Vec<(DataCategory, usize)>);

    impl RateLimiter for RecordingLimiter<'_> {
        async fn try_consume(
            &mut self,
            scope: &relay_quotas::ItemScoping,
            quantity: usize,
        ) -> RateLimits {
            self.0.push((scope.category, quantity));
            RateLimits::new()
        }
    }

    let envelope = crate::testutils::new_envelope(false, "");
    let (error, mut handle) = Managed::for_test(SerializedError {
        headers: envelope.headers().clone(),
        items: vec![
            attachment(AttachmentType::Minidump, "dump"),
            attachment(AttachmentType::Minidump, "extra"),
            attachment(AttachmentType::Flamegraph, "graph"),
        ],
    })
    .build();
    let ctx = processing::Context::for_test();
    let (expanded, _) = process::expand(error, ctx).unwrap();
    assert!(expanded.attachments[0].creates_event());
    let mut requests = Vec::new();
    let error = expanded
        .enforce(RecordingLimiter(&mut requests), ctx)
        .await
        .unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|(category, _)| *category == DataCategory::Error)
            .count(),
        1,
    );
    // The selected dump is charged separately. The extra dump and flamegraph share a budget.
    assert!(requests.contains(&(DataCategory::Attachment, 4)));
    assert!(requests.contains(&(DataCategory::AttachmentItem, 1)));
    assert!(requests.contains(&(DataCategory::Attachment, 10)));
    assert!(requests.contains(&(DataCategory::AttachmentItem, 2)));
    error.accept(|error| {
        assert_eq!(error.attachments.len(), 1);
        assert!(error.flamegraph.is_some());
    });
    handle.assert_no_outcomes();
}

#[test]
fn test_minidump_crash_rate_limit() {
    assert_crash_rate_limit(
        || Minidump(attachment(AttachmentType::Minidump, "dump")),
        &[4],
        |crash| {
            assert!(crash.0.rate_limited());
            assert_eq!(crash.0.payload().as_ref(), b"dump");
        },
    );
}

#[test]
fn test_apple_crash_rate_limit() {
    assert_crash_rate_limit(
        || AppleCrashReport(attachment(AttachmentType::AppleCrashReport, "report")),
        &[6],
        |crash| {
            assert!(crash.0.rate_limited());
            assert_eq!(crash.0.payload().as_ref(), b"report");
        },
    );
}

#[test]
fn test_gpu_crash_rate_limit() {
    assert_crash_rate_limit(
        || GpuCrash(attachment(AttachmentType::NvGpuDump, "gpu dump")),
        &[8],
        |crash| {
            assert!(crash.0.rate_limited());
            assert_eq!(crash.0.payload().as_ref(), b"gpu dump");
        },
    );
}
