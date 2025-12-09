use bytes::Bytes;
use relay_dynamic_config::ErrorBoundary;
use relay_protocol::Annotated;

use crate::envelope::Item;
use crate::managed::{Managed, Rejected};
use crate::processing::Context;
use crate::processing::trace_attachments::types::ExpandedAttachment;
use crate::processing::trace_attachments::{
    Error, ExpandedAttachments, SampledAttachments, SerializedAttachments,
};
use crate::services::outcome::{DiscardReason, Outcome};

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
    // TODO(jjbayer): should this function take ownership?
    let meta_length = item.meta_length().ok_or_else(|| {
        relay_log::debug!("trace attachment missing meta_length");
        DiscardReason::InvalidSpanAttachment
    })? as usize;

    let payload = item.payload();
    let Some((meta_bytes, body)) = payload.split_at_checked(meta_length) else {
        relay_log::debug!(
            "trace attachment meta_length ({}) exceeds total length ({})",
            meta_length,
            payload.len()
        );
        return Err(DiscardReason::InvalidSpanAttachment);
    };

    let meta = Annotated::from_json_bytes(meta_bytes).map_err(|err| {
        relay_log::debug!("failed to parse span attachment: {err}");
        DiscardReason::InvalidJson
    })?;

    Ok(ExpandedAttachment {
        parent_id: item.parent_id().cloned(), // TODO(jjbayer): zero-copy
        meta,
        body: Bytes::copy_from_slice(body), // TODO(jjbayer): ptr to slice
    })
}

pub fn sample(
    work: Managed<SerializedAttachments>,
    ctx: Context<'_>,
) -> Result<Managed<SampledAttachments>, Rejected<Error>> {
    work.try_map(|work, record_keeper| {
        let SerializedAttachments { headers, items } = work;
        let Some(ErrorBoundary::Ok(config)) = &ctx.project_info.config.sampling else {
            return Ok::<_, Error>(SampledAttachments {
                headers,
                server_sample_rate: None,
                items,
            });
        };

        // FIXME: do like spans does.

        let server_sample_rate = Some(1.0); //FIXME
        Ok(SampledAttachments {
            headers,
            server_sample_rate,
            items,
        })
    })
}
