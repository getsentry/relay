//! Service that uploads attachments.
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use relay_config::UploadServiceConfig;
use relay_quotas::DataCategory;
use relay_system::{FromMessage, Interface, NoResponse, Receiver, Recipient, Service};
use smallvec::smallvec;
use uuid::Uuid;

use crate::managed::{Counted, Managed, OutcomeError, Quantities, Rejected};
use crate::services::outcome::DiscardReason;
use crate::statsd::{RelayCounters, RelayGauges};

use super::outcome::Outcome;

/// Message that requests an attachment upload.
pub struct UploadAttachment {
    /// The attachment to be uploaded.
    pub attachment: Managed<Attachment>,

    /// The return address in case of a successful upload.
    pub respond_to: Recipient<Managed<UploadedAttachment>, NoResponse>,
}

pub enum Upload {
    Attachment(UploadAttachment),
}

impl Interface for Upload {}

impl FromMessage<UploadAttachment> for Upload {
    type Response = NoResponse;

    fn from_message(message: UploadAttachment, sender: ()) -> Self {
        Self::Attachment(message)
    }
}

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
    fn as_str(&self) -> &'static str {
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
        (Some(Outcome::Invalid(DiscardReason::Internal)), self)
    }
}

/// The service that uploads the attachments.
///
/// Accepts upload requests and maintains a list of concurrent uploads.
pub struct UploadService {
    concurrent_requests: FuturesUnordered<BoxFuture<'static, Result<(), Rejected<Error>>>>,
    max_concurrent_requests: usize,
    timeout: Duration,
}

impl UploadService {
    pub fn new(config: &UploadServiceConfig) -> Self {
        let UploadServiceConfig {
            max_concurrent_requests,
            timeout,
        } = config;
        Self {
            concurrent_requests: FuturesUnordered::new(),
            max_concurrent_requests: *max_concurrent_requests,
            timeout: Duration::from_secs(*timeout),
        }
    }

    fn handle_message(&mut self, message: Upload) {
        let Upload::Attachment(UploadAttachment {
            attachment,
            respond_to,
        }) = message;
        if self.concurrent_requests.len() >= self.max_concurrent_requests {
            // Load shed to prevent backlogging in the service queue and affecting other parts of Relay.
            // We might want to have a less aggressive mechanism in the future.
            count_upload(Err(attachment.reject_err(Error::LoadShed)));
            return;
        }

        self.concurrent_requests
            .push(managed_upload(self.timeout, attachment, respond_to).boxed());
    }
}

impl Service for UploadService {
    type Interface = Upload;

    async fn run(mut self, mut rx: Receiver<Self::Interface>) {
        loop {
            relay_log::trace!("Upload loop iteration");
            tokio::select! {
                //Bias towards handling responses so that there's space for new incoming requests.
                biased;

                Some(result) = self.concurrent_requests.next() => {
                    count_upload(result);
                }
                Some(message) = rx.recv() => self.handle_message(message),

                else => break,
            }
            relay_statsd::metric!(
                gauge(RelayGauges::ConcurrentAttachmentUploads) =
                    self.concurrent_requests.len() as u64
            );
        }
        relay_log::info!("Upload service stopped");
    }
}

fn count_upload(result: Result<(), Rejected<Error>>) {
    let result_msg = match result {
        Ok(()) => "success",
        Err(e) => e.into_inner().as_str(),
    };
    relay_statsd::metric!(
        counter(RelayCounters::AttachmentUpload) += 1,
        result = result_msg
    );
}

/// Spend a limited time trying to upload an attachment, and emit outcomes if this fails.
///
/// Returns an [`UploadedAttachment`] if the upload was successful.
async fn managed_upload(
    timeout: Duration,
    attachment: Managed<Attachment>,
    respond_to: Recipient<Managed<UploadedAttachment>, NoResponse>,
) -> Result<(), Rejected<Error>> {
    let key = attachment.meta.attachment_id.as_ref().map(Uuid::to_string);
    let result = tokio::time::timeout(timeout, async {
        upload(key.as_deref(), &attachment.payload).await
    })
    .await
    .map_err(|_elapsed| attachment.reject_err(Error::Timeout))?
    .map_err(|_error| attachment.reject_err(Error::UploadFailed))?;

    let uploaded_attachment =
        attachment.map(|Attachment { meta, payload }, _| UploadedAttachment {
            meta,
            uploaded_bytes: payload.len(),
        });

    respond_to.send(uploaded_attachment);
    Ok(())
}

/// Returns the key of the objectstore upload.
///
/// If `key` is not `None`, write to object store with the given key.
async fn upload(key: Option<&str>, payload: &[u8]) -> Result<String, ()> {
    // TODO: call objectstore
    Ok(String::new())
}

#[cfg(test)]
mod tests {
    use relay_redis::redis::FromRedisValue;
    use relay_system::Addr;

    use super::*;

    #[tokio::test]
    async fn test_basic() {
        let upload_service = UploadService::new(&UploadServiceConfig {
            max_concurrent_requests: 2,
            timeout: 1,
        })
        .start_detached();

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        let (attachment, mut managed_handle) = Managed::for_test(Attachment {
            meta: AttachmentMeta {
                attachment_id: Some(Uuid::now_v7()),
            },
            payload: Bytes::from("hello world"),
        })
        .build();

        upload_service.send(UploadAttachment {
            attachment,
            respond_to: Recipient::<_, NoResponse>::new(tx),
        });

        let uploaded = rx.blocking_recv().unwrap();
        uploaded.accept(|_| {});

        drop(upload_service);
        assert!(matches!(rx.blocking_recv(), None));
    }
}
