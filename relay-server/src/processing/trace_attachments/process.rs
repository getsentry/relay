use bytes::Bytes;
use relay_event_schema::processor::{ProcessingAction, ValueType};
use relay_pii::PiiAttachmentsProcessor;
use relay_protocol::Annotated;
use relay_statsd::metric;

use crate::envelope::Item;
use crate::managed::{Managed, Rejected};
use crate::processing::Context;
use crate::processing::trace_attachments::types::ExpandedAttachment;
use crate::processing::trace_attachments::{
    Error, ExpandedAttachments, SampledAttachments, SerializedAttachments,
};
use crate::processing::utils::dynamic_sampling;
use crate::services::outcome::{DiscardReason, Outcome};
use crate::statsd::RelayTimers;

/// Parses serialized attachments into attachments with expanded metadata.
///
/// Invalid envelope items are rejected.
pub fn expand(work: Managed<SampledAttachments>) -> Managed<ExpandedAttachments> {
    work.map(
        |SampledAttachments {
             headers,
             server_sample_rate,
             items,
         },
         record_keeper| {
            let mut attachments = vec![];
            for item in items {
                match parse_and_validate(&item) {
                    Ok(attachment) => attachments.push(attachment),
                    Err(e) => record_keeper.reject_err(Outcome::Invalid(e), item),
                }
            }
            ExpandedAttachments {
                headers,
                server_sample_rate,
                attachments,
            }
        },
    )
}

/// Converts an envelope item into an expanded trace attachment.
pub fn parse_and_validate(item: &Item) -> Result<ExpandedAttachment, DiscardReason> {
    let meta_length = item.meta_length().ok_or_else(|| {
        relay_log::debug!("trace attachment missing meta_length");
        DiscardReason::InvalidTraceAttachment
    })? as usize;

    let payload = item.payload();
    let Some((meta_bytes, body)) = payload.split_at_checked(meta_length) else {
        relay_log::debug!(
            "trace attachment meta_length ({}) exceeds total length ({})",
            meta_length,
            payload.len()
        );
        return Err(DiscardReason::InvalidTraceAttachment);
    };

    let meta = Annotated::from_json_bytes(meta_bytes).map_err(|err| {
        relay_log::debug!("failed to parse trace attachment: {err}");
        DiscardReason::InvalidJson
    })?;

    Ok(ExpandedAttachment {
        parent_id: item.parent_id().cloned(),
        meta,
        body: payload.slice_ref(body),
    })
}

/// Runs dynamic-sampling on the attachments.
pub async fn sample(
    work: Managed<SerializedAttachments>,
    ctx: Context<'_>,
) -> Result<Managed<SampledAttachments>, Rejected<Error>> {
    let event = None; // only apply trace-based rules.
    let reservoir = None; // legacy

    let result = dynamic_sampling::run(work.headers.dsc(), event, &ctx, reservoir).await;
    let server_sample_rate = result.sample_rate();

    work.try_map(|work, _| {
        if let Some(outcome) = result.into_dropped_outcome() {
            return Err(Error::Sampled(outcome));
        }

        let SerializedAttachments { headers, items } = work;

        Ok::<_, Error>(SampledAttachments {
            headers,
            server_sample_rate,
            items,
        })
    })
}

/// Applies PII scrubbing to individual attachment entries.
pub fn scrub(work: &mut Managed<ExpandedAttachments>, ctx: Context<'_>) {
    work.retain(
        |work| &mut work.attachments,
        |attachment, _| {
            scrub_attachment(attachment, ctx)
                .inspect_err(|err| relay_log::debug!("failed to scrub pii from attachment: {err}"))
                .map_err(Error::from)
        },
    );
}

/// Errors that can occur during attachment scrubbing.
#[derive(Debug, thiserror::Error)]
pub enum ScrubAttachmentError {
    #[error("pii config")]
    PiiConfig,
    #[error("processing error: {0}")]
    ProcessingFailed(#[from] ProcessingAction),
}

/// Applies PII scrubbing to a single attachment.
pub fn scrub_attachment<'a>(
    attachment: &mut ExpandedAttachment,
    ctx: Context<'a>,
) -> Result<(), ScrubAttachmentError> {
    let pii_config_from_scrubbing = ctx
        .project_info
        .config
        .datascrubbing_settings
        .pii_config()
        .map_err(|_| ScrubAttachmentError::PiiConfig)?;

    let ExpandedAttachment {
        parent_id: _,
        meta,
        body,
    } = attachment;

    relay_pii::eap::scrub(
        ValueType::Object, // we may want to introduce a dedicated $attachment_meta type at some point.
        meta,
        ctx.project_info.config.pii_config.as_ref(),
        pii_config_from_scrubbing.as_ref(),
    )?;

    if let Some(config) = ctx.project_info.config.pii_config.as_ref() {
        let filename = meta
            .value()
            .and_then(|m| m.filename.as_str())
            .unwrap_or_default();

        let processor = PiiAttachmentsProcessor::new(config.compiled());
        let mut payload = body.to_vec();

        metric!(
            timer(RelayTimers::AttachmentScrubbing),
            attachment_type = "trace_attachment",
            {
                if processor.scrub_attachment(filename, &mut payload) {
                    *body = Bytes::from(payload);
                };
            }
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{AttachmentId, TraceAttachmentMeta};
    use relay_pii::PiiConfig;
    use relay_protocol::SerializableAnnotated;

    use crate::envelope::ParentId;
    use crate::services::projects::project::ProjectInfo;

    use super::*;

    #[test]
    fn test_scrub_attachment_body() {
        let pii_config = serde_json::from_value::<PiiConfig>(serde_json::json!({
            "rules": {"0": {"type": "ip", "redaction": {"method": "remove"}}},
            "applications": {"$attachments.'data.txt'": ["0"]}
        }))
        .unwrap();

        let mut attachment = ExpandedAttachment {
            parent_id: None,
            meta: Annotated::new(TraceAttachmentMeta {
                attachment_id: Annotated::new(AttachmentId::random()),
                filename: Annotated::new("data.txt".to_owned()),
                content_type: Annotated::new("text/plain".to_owned()),
                ..Default::default()
            }),
            body: Bytes::from("Some IP: 127.0.0.1"),
        };

        let ctx = Context {
            project_info: &ProjectInfo {
                config: relay_dynamic_config::ProjectConfig {
                    pii_config: Some(pii_config),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Context::for_test()
        };

        scrub_attachment(&mut attachment, ctx).unwrap();

        assert_eq!(attachment.body, "Some IP: *********");
    }

    #[test]
    fn test_scrub_attachment_meta() {
        use relay_event_schema::protocol::{Attribute, AttributeType, Attributes};
        use relay_protocol::Value;

        let pii_config = serde_json::from_value::<PiiConfig>(serde_json::json!({
            "applications": {"$string": ["@email:replace"]}
        }))
        .unwrap();

        let mut attributes = Attributes::new();
        attributes.0.insert(
            "email_attr".to_owned(),
            Annotated::new(Attribute::new(
                AttributeType::String,
                Value::String("john.doe@example.com".to_owned()),
            )),
        );

        let mut attachment = ExpandedAttachment {
            parent_id: Some(ParentId::SpanId(None)),
            meta: Annotated::new(TraceAttachmentMeta {
                attachment_id: Annotated::new(AttachmentId::random()),
                filename: Annotated::new("data.txt".to_owned()),
                content_type: Annotated::new("text/plain".to_owned()),
                attributes: Annotated::new(attributes),
                ..Default::default()
            }),
            body: Bytes::from("Some attachment body"),
        };

        let ctx = Context {
            project_info: &ProjectInfo {
                config: relay_dynamic_config::ProjectConfig {
                    pii_config: Some(pii_config),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Context::for_test()
        };

        scrub_attachment(&mut attachment, ctx).unwrap();

        let attrs = &attachment.meta.value().unwrap().attributes;
        insta::assert_json_snapshot!(SerializableAnnotated(attrs), @r#"
        {
          "email_attr": {
            "type": "string",
            "value": "[email]"
          },
          "_meta": {
            "email_attr": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@email:replace",
                      "s",
                      0,
                      7
                    ]
                  ],
                  "len": 20
                }
              }
            }
          }
        }
        "#);
    }
}
