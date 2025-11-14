//! Service that uploads attachments.
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use objectstore_client::{Client, ExpirationPolicy, Session, Usecase};
use relay_config::UploadServiceConfig;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Receiver, Service};

use crate::constants::DEFAULT_ATTACHMENT_RETENTION;
use crate::envelope::{Item, ItemType};
use crate::managed::{Managed, OutcomeError, TypedEnvelope};
use crate::services::outcome::DiscardReason;
use crate::services::processor::Processed;
use crate::services::store::{Store, StoreEnvelope};
use crate::statsd::{RelayCounters, RelayGauges};

use super::outcome::Outcome;

/// Message that requests all [`Attachment`]s of the [`Envelope`](crate::envelope::Envelope) to be uploaded.
pub struct UploadAttachments {
    /// The envelope containing [`Attachment`]s for upload.
    pub envelope: TypedEnvelope<Processed>,
}

impl Interface for UploadAttachments {}

impl FromMessage<UploadAttachments> for UploadAttachments {
    type Response = NoResponse;

    fn from_message(message: UploadAttachments, _sender: ()) -> Self {
        message
    }
}

/// Errors that can occur when trying to upload an attachment.
#[derive(Debug)]
pub enum Error {
    Timeout,
    UploadFailed,
}

impl Error {
    fn as_str(&self) -> &'static str {
        match self {
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
    pending_requests: FuturesUnordered<BoxFuture<'static, ()>>,
    max_concurrent_requests: usize,
    inner: Arc<UploadServiceInner>,
}

impl UploadService {
    pub fn new(
        config: &UploadServiceConfig,
        store: Option<Addr<Store>>,
    ) -> anyhow::Result<Option<Self>> {
        let Some(store) = store else { return Ok(None) };
        let UploadServiceConfig {
            objectstore_url,
            max_concurrent_requests,
            timeout,
        } = config;
        let Some(objectstore_url) = objectstore_url else {
            return Ok(None);
        };

        let objectstore_client = Client::builder(objectstore_url).build()?;
        let attachments_usecase = Usecase::new("attachments")
            .with_expiration_policy(ExpirationPolicy::TimeToLive(DEFAULT_ATTACHMENT_RETENTION));

        let inner = UploadServiceInner {
            timeout: Duration::from_secs(*timeout),

            store,

            objectstore_client,
            attachments_usecase,
        };

        Ok(Some(Self {
            pending_requests: FuturesUnordered::new(),
            max_concurrent_requests: *max_concurrent_requests,
            inner: Arc::new(inner),
        }))
    }

    fn handle_message(&self, message: UploadAttachments) {
        let UploadAttachments { envelope } = message;
        if self.pending_requests.len() >= self.max_concurrent_requests {
            // Load shed to prevent backlogging in the service queue and affecting other parts of Relay.
            // We might want to have a less aggressive mechanism in the future.

            // for now, this will just forward to the store endpoint without uploading attachments.
            self.inner.store.send(StoreEnvelope { envelope });
            return;
        }

        let inner = self.inner.clone();
        let future = async move { inner.handle_envelope(envelope).await };
        self.pending_requests.push(future.boxed());
    }
}

impl Service for UploadService {
    type Interface = UploadAttachments;

    async fn run(mut self, mut rx: Receiver<Self::Interface>) {
        relay_log::info!("Upload service started");
        loop {
            relay_log::trace!("Upload loop iteration");
            tokio::select! {
                // Bias towards handling responses so that there's space for new incoming requests.
                biased;

                Some(_) = self.pending_requests.next() => {},
                Some(message) = rx.recv() => self.handle_message(message),

                else => break,
            }
            relay_statsd::metric!(
                gauge(RelayGauges::ConcurrentAttachmentUploads) =
                    self.pending_requests.len() as u64
            );
        }
        relay_log::info!("Upload service stopped");
    }
}

struct UploadServiceInner {
    timeout: Duration,
    store: Addr<Store>,

    objectstore_client: Client,
    attachments_usecase: Usecase,
}

impl UploadServiceInner {
    /// Spend a limited time trying to upload an attachment, and emit outcomes if this fails.
    ///
    /// Returns an [`UploadedAttachment`] if the upload was successful.
    async fn handle_envelope(&self, mut envelope: TypedEnvelope<Processed>) -> () {
        let scoping = envelope.scoping();
        let session = self
            .attachments_usecase
            .for_project(scoping.organization_id.value(), scoping.project_id.value())
            .session(&self.objectstore_client);

        let attachments = envelope
            .envelope_mut()
            .items_mut()
            .filter(|item| *item.ty() == ItemType::Attachment);
        let upload_results = if let Ok(session) = session {
            futures::future::join_all(
                attachments.map(|attachment| self.handle_attachment(&session, attachment)),
            )
            .await
        } else {
            // if we have any kind of error constructing the upload session, we mark all attachments as failed
            attachments.map(|_| Err(Error::UploadFailed)).collect()
        };

        // track the results of attachment upload
        let managed_attachments = envelope
            .envelope()
            .items()
            .filter(|item| *item.ty() == ItemType::Attachment)
            .map(|item| Managed::from_envelope(&envelope, item));
        for (attachment, result) in managed_attachments.zip(upload_results) {
            let result_msg = match result {
                Ok(()) => {
                    attachment.accept(|_| ());
                    "success"
                }
                Err(error) => attachment.reject_err(error).into_inner().as_str(),
            };
            relay_statsd::metric!(
                counter(RelayCounters::AttachmentUpload) += 1,
                result = result_msg
            );
        }

        // last but not least, forward the envelope to the store endpoint
        self.store.send(StoreEnvelope { envelope });
    }

    async fn handle_attachment(
        &self,
        session: &Session,
        attachment: &mut Item,
    ) -> Result<(), Error> {
        // we are not storing zero-size attachments in objectstore
        if attachment.is_empty() {
            return Ok(());
        }
        relay_log::trace!("Starting attachment upload");
        let future = async {
            let result = session.put(attachment.payload()).send().await?;
            Ok(result.key)
        };

        let stored_key = tokio::time::timeout(self.timeout, future)
            .await
            .map_err(|_elapsed| Error::Timeout)?
            .map_err(|_error: objectstore_client::Error| Error::UploadFailed)?;

        attachment.set_stored_key(stored_key);
        relay_log::trace!("Finished attachment upload");

        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[tokio::test]
//     async fn test_basic() {
//         let upload_service = UploadService::new(&UploadServiceConfig {
//             max_concurrent_requests: 2,
//             timeout: 1,
//             objectstore_url: Some("http://127.0.0.1:8888/".into()),
//         })
//         .unwrap()
//         .unwrap()
//         .start_detached();

//         let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

//         let (attachment, mut _managed_handle) = Managed::for_test(Attachment {
//             meta: AttachmentMeta {
//                 attachment_id: Some("my_key".to_owned()),
//                 scope: AttachmentScope {
//                     organization_id: 123,
//                     project_id: 456,
//                 },
//             },
//             payload: Bytes::from("hello world"),
//         })
//         .build();

//         upload_service.send(UploadAttachment {
//             attachment,
//             respond_to: Recipient::<_, NoResponse>::new(tx),
//         });
//         let uploaded = rx.recv().await.unwrap();
//         assert_eq!(uploaded.stored_id, "my_key");
//         uploaded.accept(|_| {});

//         drop(upload_service);
//         assert!(rx.recv().await.is_none());
//     }
// }
