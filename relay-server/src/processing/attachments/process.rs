use crate::envelope::AttachmentType;
use crate::managed::{Managed, Rejected};
use crate::processing::attachments::{Error, SerializedAttachments};
use crate::processing::{self, utils};
use crate::services::processor::ProcessingError;

/// Runs PiiProcessors on the attachments.
pub fn scrub(
    attachments: &mut Managed<SerializedAttachments>,
    ctx: processing::Context<'_>,
) -> Result<(), Rejected<Error>> {
    attachments.try_modify(|attachments, records| {
        utils::attachments::scrub(
            attachments.attachments.iter_mut(),
            ctx.project_info,
            Some(records),
        );
        Ok::<_, Error>(())
    })
}

/// Drops attachments that require an explicit event from envelopes without one.
pub fn remove_illegal_attachments(managed: &mut Managed<SerializedAttachments>) {
    managed.retain(
        |managed| &mut managed.attachments,
        |attachment, _| match attachment.attachment_type() {
            Some(AttachmentType::Flamegraph) => Err(ProcessingError::InvalidFlamegraph),
            _ => Ok(()),
        },
    );
}

#[test]
fn test_flamegraph_requires_explicit_event() {
    use relay_event_schema::protocol::EventId;
    use smallvec::smallvec;

    use crate::envelope::{AttachmentItem, ContentType, Envelope};
    use crate::extractors::RequestMeta;

    let dsn = "https://a94ae32be2584e0bbd7a4cbb95971fee:@sentry.io/42"
        .parse()
        .unwrap();
    let envelope = Envelope::from_request(Some(EventId::new()), RequestMeta::new(dsn));

    let mut flamegraph = AttachmentItem::new();
    flamegraph.set_attachment_type(AttachmentType::Flamegraph);
    let rejected_size = flamegraph.attachment_body_size() as u64;

    let mut ordinary = AttachmentItem::new();
    ordinary.set_payload(ContentType::Text, "keep me");
    let (mut attachments, mut handle) = Managed::for_test(SerializedAttachments {
        headers: envelope.headers().clone(),
        attachments: smallvec![flamegraph, ordinary],
    })
    .build();

    remove_illegal_attachments(&mut attachments);

    assert_eq!(attachments.attachments.len(), 1);
    assert_eq!(attachments.attachments[0].payload().as_ref(), b"keep me");
    attachments.accept(|_| ());
    let outcome = crate::services::outcome::Outcome::Invalid(
        crate::services::outcome::DiscardReason::InvalidFlamegraph,
    );
    handle.assert_outcome(
        &outcome,
        relay_quotas::DataCategory::Attachment,
        rejected_size,
    );
    handle.assert_outcome(&outcome, relay_quotas::DataCategory::AttachmentItem, 1);
}
