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

#[cfg(feature = "processing")]
#[test]
fn test_unreal_crash_rate_limit() {
    assert_crash_rate_limit(
        || Unreal::Process {
            minidump: Some(attachment(AttachmentType::Minidump, "dump")),
            apple_crash_report: Some(attachment(AttachmentType::AppleCrashReport, "report")),
        },
        &[6, 4],
        |crash| {
            let Unreal::Process {
                minidump,
                apple_crash_report,
            } = crash
            else {
                panic!("expected expanded Unreal crash");
            };
            let minidump = minidump.unwrap();
            let report = apple_crash_report.unwrap();
            assert!(minidump.rate_limited());
            assert!(report.rate_limited());
            assert_eq!(minidump.payload().as_ref(), b"dump");
            assert_eq!(report.payload().as_ref(), b"report");
        },
    );
}
