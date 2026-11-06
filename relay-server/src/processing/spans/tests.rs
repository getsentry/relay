use relay_quotas::{DimensionMap, ItemScoping, RateLimit, RateLimitScope, RetryAfter};

use super::*;
use crate::envelope::ContentType;
use crate::processing::{Processor, RateLimiter};

struct AttachmentLimit {
    category: Option<DataCategory>,
    bytes: usize,
}

impl RateLimiter for AttachmentLimit {
    async fn try_consume(&mut self, scope: &ItemScoping, quantity: usize) -> RateLimits {
        assert_eq!(
            quantity,
            match scope.category {
                DataCategory::Attachment => self.bytes,
                DataCategory::Span | DataCategory::SpanIndexed | DataCategory::AttachmentItem => 1,
                other => unreachable!("unexpected quota category: {other:?}"),
            }
        );
        let mut limits = RateLimits::new();
        if Some(scope.category) == self.category {
            limits.add(RateLimit {
                categories: [scope.category].into(),
                scope: RateLimitScope::Project(scope.scoping.project_id),
                reason_code: None,
                retry_after: RetryAfter::from_secs(60),
                namespaces: Default::default(),
                dimensions: DimensionMap::default(),
            });
        }
        limits
    }
}

fn envelope_from_wire(body: &str, associated: bool) -> Box<Envelope> {
    let span = r#"{"items":[{"trace_id":"0123456789abcdef0123456789abcdef","span_id":"0000000000000001","start_timestamp":1700000000.0,"end_timestamp":1700000001.0,"name":"test","status":"ok"}]}"#;
    let meta = r#"{"trace_id":"0123456789abcdef0123456789abcdef","attachment_id":"00000000-0000-7000-8000-000000000001","timestamp":1700000000.0,"filename":"test.txt","content_type":"text/plain"}"#;
    let payload = format!("{meta}{body}");
    let mut wire =
        b"{\"dsn\":\"https://a94ae32be2584e0bbd7a4cbb95971fee@sentry.io/42\"}\n".to_vec();
    for (header, payload) in [
        (
            serde_json::json!({
                "type": "span", "content_type": ContentType::SpanV2Container.as_str(),
                "item_count": 1, "length": span.len()
            }),
            span,
        ),
        (
            serde_json::json!({
                "type": "attachment", "content_type": ContentType::TraceAttachment.as_str(),
                "span_id": associated.then_some("0000000000000001"),
                "meta_length": meta.len(), "length": payload.len()
            }),
            payload.as_str(),
        ),
    ] {
        wire.extend(serde_json::to_vec(&header).unwrap());
        wire.push(b'\n');
        wire.extend(payload.as_bytes());
        wire.push(b'\n');
    }
    Envelope::parse_request(
        wire.into(),
        crate::extractors::RequestMeta::new(
            "https://a94ae32be2584e0bbd7a4cbb95971fee@sentry.io/42"
                .parse()
                .unwrap(),
        ),
    )
    .unwrap()
}

#[tokio::test]
async fn test_attachment_accounting_from_wire() {
    #[cfg(feature = "processing")]
    let limiter = QuotaRateLimiter::new(
        crate::services::projects::cache::ProjectCacheHandle::for_test(),
        None,
    );
    #[cfg(not(feature = "processing"))]
    let limiter = QuotaRateLimiter::new();
    let processor = SpansProcessor::new(Arc::new(limiter), GeoIpLookup::empty());

    for (body, bytes) in [("", 1), ("x", 1), ("payload", 7)] {
        for associated in [false, true] {
            for category in [
                None,
                Some(DataCategory::Attachment),
                Some(DataCategory::AttachmentItem),
            ] {
                let (outcomes, mut received) = relay_system::Addr::custom();
                let mut envelope =
                    ManagedEnvelope::new(envelope_from_wire(body, associated), outcomes);
                let serialized = processor.prepare_envelope(&mut envelope).unwrap();
                assert!(envelope.envelope().is_empty());
                envelope.accept();
                validate::invalid(&serialized).unwrap();

                // Keep Managed's original strict accounting through expansion and enforcement.
                let expanded = process::expand(serialized, usize::MAX).unwrap();
                let output = expanded
                    .enforce(AttachmentLimit { category, bytes }, Context::for_test())
                    .await
                    .unwrap();
                let serialized = output.map(|output, _| {
                    let spans = output.unwrap_left();
                    assert_eq!(spans.spans.len(), 1);
                    let attachments: Vec<_> = spans.spans[0]
                        .attachments
                        .iter()
                        .chain(&spans.stand_alone_attachments)
                        .collect();
                    if category.is_none() {
                        assert_eq!(attachments.len(), 1);
                        assert_eq!(attachments[0].body.as_ref(), body.as_bytes());
                    } else {
                        assert!(attachments.is_empty());
                    }
                    spans.serialize_envelope().unwrap()
                });
                serialized.accept(|_| ());
                if category.is_some() {
                    for (category, quantity) in [
                        (DataCategory::Attachment, bytes as u64),
                        (DataCategory::AttachmentItem, 1),
                    ] {
                        let outcome = received.try_recv().unwrap();
                        assert_eq!(outcome.outcome, Outcome::RateLimited(None));
                        assert_eq!(outcome.category, category);
                        assert_eq!(outcome.quantity, quantity);
                    }
                }
                assert!(received.try_recv().is_err());
            }
        }
    }
}
