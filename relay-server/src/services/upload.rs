//! Service that uploads attachments.
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use relay_config::UploadServiceConfig;
use relay_quotas::DataCategory;
use relay_system::{Interface, NoResponse, Receiver, Recipient, Service};
use smallvec::smallvec;
use uuid::Uuid;

use crate::managed::{Counted, Managed, OutcomeError, Quantities};
use crate::services::outcome::DiscardReason;
use crate::statsd::{RelayCounters, RelayGauges};

use super::outcome::Outcome;

/// Message that requests an attachment upload.
///
/// This might become an enum in the future once we support multiple upload types.
pub struct Upload {
    /// The attachment to be uploaded.
    pub attachment: Managed<Attachment>,

    /// The return address in case of a successful upload.
    pub respond_to: Recipient<Managed<UploadedAttachment>, NoResponse>,
}

impl Interface for Upload {}

/// The attachment to upload.
pub struct Attachment {
    /// Attachment metadata.
    pub meta: AttachmentMeta,
    /// The attachment body.
    payload: Bytes,
}

impl Counted for Attachment {
    fn quantities(&self) -> Quantities {
        smallvec![
            (DataCategory::Attachment, self.payload.len()),
            (DataCategory::AttachmentItem, 1),
        ]
    }
}

/// The result of a successful attachment upload.
///
/// This is tracked so that the recipient of the success message can emit outcomes for the
/// attachment.
pub struct UploadedAttachment {
    meta: AttachmentMeta,
    uploaded_bytes: usize,
}

impl Counted for UploadedAttachment {
    fn quantities(&self) -> Quantities {
        smallvec![
            (DataCategory::Attachment, self.uploaded_bytes),
            (DataCategory::AttachmentItem, 1),
        ]
    }
}

/// Metadata of the attachment (stub).
pub struct AttachmentMeta {
    attachment_id: Option<Uuid>,
    // TODO: more fields
}

/// Errors that can occur when trying to upload an attachment.
pub enum Error {
    LoadShed,
    Timeout,
    UploadFailed,
}

impl Error {
    fn as_str(&self) -> &str {
        match self {
            Error::LoadShed => "load_shed",
            Error::Timeout => "timeout",
            Error::UploadFailed => "upload_failed",
        }
    }
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        relay_statsd::metric!(
            counter(RelayCounters::AttachmentUploadFailed) += 1,
            reason = self.as_str()
        );
        (Some(Outcome::Invalid(DiscardReason::Internal)), self)
    }
}

/// The service that uploads the attachments.
///
/// Accepts upload requests and maintains a list of concurrent uploads.
pub struct UploadService {
    inflight: FuturesUnordered<BoxFuture<'static, UploadResult>>,
    max_inflight: usize,
    timeout: std::time::Duration,
}

impl UploadService {
    pub fn new(config: &UploadServiceConfig) -> Self {
        let UploadServiceConfig {
            max_inflight,
            timeout,
        } = config;
        Self {
            inflight: FuturesUnordered::new(),
            max_inflight: *max_inflight,
            timeout: *timeout,
        }
    }
}

impl Service for UploadService {
    type Interface = Upload;

    async fn run(mut self, mut rx: Receiver<Self::Interface>) {
        loop {
            tokio::select! {
                biased;

                Some(result) = self.inflight.next() => {
                    relay_statsd::metric!(gauge(RelayGauges::AttachmentUploadsInFlight) = self.inflight.len() as u64);
                    if let Some((uploaded_attachment, respond_to)) = result {
                        respond_to.send(uploaded_attachment);
                    }
                }
                Some(message) = rx.recv() => {
                    let Upload { attachment, respond_to } = message;
                    if self.inflight.len() > self.max_inflight {
                        // Load shed to prevent backlogging in the service queue and affecting other parts of Relay.
                        // We might want to have a less aggressive mechanism in the future.
                        drop(attachment.reject_err(Error::LoadShed));
                        continue;
                    }

                    self.inflight.push(managed_upload(self.timeout, attachment, respond_to).boxed());
                    relay_statsd::metric!(gauge(RelayGauges::AttachmentUploadsInFlight) = self.inflight.len() as u64);
                }
            }
        }
    }
}

type UploadResult = Option<(
    Managed<UploadedAttachment>,
    Recipient<Managed<UploadedAttachment>, NoResponse>,
)>;

/// Spend a limited time trying to upload an attachment, and emit outcomes if this fails.
///
/// Returns an [`UploadedAttachment`] if the upload was successful.
async fn managed_upload(
    timeout: Duration,
    attachment: Managed<Attachment>,
    respond_to: Recipient<Managed<UploadedAttachment>, NoResponse>,
) -> UploadResult {
    let timeout = tokio::time::timeout(timeout, async {
        let key = attachment.meta.attachment_id.as_ref().map(Uuid::to_string);
        upload(key.as_deref(), &attachment.payload).await
    })
    .await;

    let Ok(result) = timeout else {
        drop(attachment.reject_err(Error::Timeout));
        return None;
    };

    match result {
        Ok(_key) => Some((
            attachment.map(|Attachment { meta, payload }, _| UploadedAttachment {
                meta,
                uploaded_bytes: payload.len(),
            }),
            respond_to,
        )),
        Err(()) => {
            drop(attachment.reject_err(Error::UploadFailed));
            None
        }
    }
}

/// Returns the key of the objectstore upload.
///
/// If `key` is not `None`, write to object store with the given key.
async fn upload(key: Option<&str>, payload: &[u8]) -> Result<String, ()> {
    // TODO: call objectstore
    Ok(String::new())
}
