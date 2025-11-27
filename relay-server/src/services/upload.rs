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
use crate::managed::{Managed, ManagedResult, OutcomeError, Rejected, TypedEnvelope};
use crate::processing::spans::ValidatedSpanAttachment;
use crate::services::outcome::DiscardReason;
use crate::services::processor::Processed;
use crate::services::store::{Store, StoreEnvelope};
use crate::statsd::{RelayCounters, RelayGauges};

use super::outcome::Outcome;

/// Messages that the upload service can handle.
pub enum Upload {
    Envelope(StoreEnvelope),
    Attachment(Managed<ValidatedSpanAttachment>),
}

impl Upload {
    fn ty(&self) -> &str {
        match self {
            Upload::Envelope(store_envelope) => "envelope",
            Upload::Attachment(managed) => "attachment_v2",
        }
    }
}

impl Interface for Upload {}

impl FromMessage<StoreEnvelope> for Upload {
    type Response = NoResponse;

    fn from_message(message: StoreEnvelope, _sender: ()) -> Self {
        Self::Envelope(message)
    }
}

impl FromMessage<Managed<ValidatedSpanAttachment>> for Upload {
    type Response = NoResponse;

    fn from_message(message: Managed<ValidatedSpanAttachment>, _sender: ()) -> Self {
        Self::Attachment(message)
    }
}

/// Errors that can occur when trying to upload an attachment.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("timeout")]
    Timeout,
    #[error("upload failed: {0}")]
    UploadFailed(#[from] objectstore_client::Error),
}

impl Error {
    fn as_str(&self) -> &'static str {
        match self {
            Error::Timeout => "timeout",
            Error::UploadFailed(_) => "upload_failed",
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
        let attachments_v1 = Usecase::new("attachments")
            .with_expiration_policy(ExpirationPolicy::TimeToLive(DEFAULT_ATTACHMENT_RETENTION));
        let attachments_v2 = Usecase::new("trace_attachments")
            .with_expiration_policy(ExpirationPolicy::TimeToLive(DEFAULT_ATTACHMENT_RETENTION));

        let inner = UploadServiceInner {
            timeout: Duration::from_secs(*timeout),

            store,

            objectstore_client,
            attachments_v1,
            attachments_v2,
        };

        Ok(Some(Self {
            pending_requests: FuturesUnordered::new(),
            max_concurrent_requests: *max_concurrent_requests,
            inner: Arc::new(inner),
        }))
    }

    fn handle_message(&self, message: Upload) {
        if self.pending_requests.len() >= self.max_concurrent_requests {
            // Load shed to prevent backlogging in the service queue and affecting other parts of Relay.
            // We might want to have a less aggressive mechanism in the future.
            relay_statsd::metric!(
                counter(RelayCounters::AttachmentUpload) += 1,
                result = "load-shed",
                type = message.ty(),
            );
            if let Upload::Envelope(envelope) = message {
                // for now, this will just forward to the store endpoint without uploading attachment
                self.inner.store.send(envelope);
            }
            return;
        }

        let inner = self.inner.clone();
        self.pending_requests
            .push(async move { inner.handle_message(message).await }.boxed());
    }
}

impl Service for UploadService {
    type Interface = Upload;

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
    attachments_v1: Usecase,
    attachments_v2: Usecase,
}

impl UploadServiceInner {
    async fn handle_message(&self, message: Upload) {
        match message {
            Upload::Envelope(StoreEnvelope { envelope }) => {
                self.handle_envelope(envelope).await;
            }
            Upload::Attachment(managed) => self.handle_managed(managed).await,
        }
    }

    /// Uploads all attachments belonging to the given envelope.
    ///
    /// This mutates the attachment items in-place, setting their `stored_key` field to the key
    /// in objectstore.
    async fn handle_envelope(&self, mut envelope: TypedEnvelope<Processed>) -> () {
        let scoping = envelope.scoping();
        let session = self
            .attachments_v1
            .for_project(scoping.organization_id.value(), scoping.project_id.value())
            .session(&self.objectstore_client);

        let attachments = envelope
            .envelope_mut()
            .items_mut()
            .filter(|item| *item.ty() == ItemType::Attachment);

        match session {
            Err(error) => {
                relay_statsd::metric!(
                    counter(RelayCounters::AttachmentUpload) += attachments.count() as u64,
                    result = error.to_string().as_str(),
                    type = "envelope",
                );
            }
            Ok(session) => {
                for attachment in attachments {
                    let result = self.handle_envelope_attachment(&session, attachment).await;
                    relay_statsd::metric!(
                        counter(RelayCounters::AttachmentUpload) += 1,
                        result = match result {
                            Ok(()) => "success",
                            Err(error) => error.as_str(),
                        },
                        type = "envelope",
                    );
                }
            }
        }

        // last but not least, forward the envelope to the store endpoint
        self.store.send(StoreEnvelope { envelope });
    }

    async fn handle_managed(&self, managed: Managed<ValidatedSpanAttachment>) {
        let result = self.do_handle_managed(managed).await;

        relay_statsd::metric!(
            counter(RelayCounters::AttachmentUpload) += 1,
            result = match result {
                Ok(()) => "success",
                Err(e) => e.into_inner().as_str(),
            },
            type = "attachment_v2",
        );
    }

    async fn do_handle_managed(
        &self,
        managed: Managed<ValidatedSpanAttachment>,
    ) -> Result<(), Rejected<Error>> {
        let scoping = managed.scoping();
        let session = self
            .attachments_v1
            .for_project(scoping.organization_id.value(), scoping.project_id.value())
            .session(&self.objectstore_client)
            .map_err(Error::UploadFailed)
            .reject(&managed)?;

        let ValidatedSpanAttachment { meta, body } = &*managed;

        if !body.is_empty() {
            relay_log::trace!("Starting attachment upload");
            let future = async {
                let result = session
                    .put(body.clone())
                    .send()
                    .await
                    .map_err(Error::UploadFailed)
                    .reject(&managed)?;
                Ok(result.key)
            };

            // FIXME: Pick your own key.
            let stored_key = tokio::time::timeout(self.timeout, future)
                .await
                .map_err(|_elapsed| Error::Timeout)
                .reject(&managed)??;

            // attachment.set_stored_key(stored_key);
            relay_log::trace!("Finished attachment upload");
        }

        // FIXME: Send metadata and payload size to Store service to write to EAP.

        Ok(())
    }

    async fn handle_envelope_attachment(
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
            .map_err(Error::UploadFailed)?;

        attachment.set_stored_key(stored_key);
        relay_log::trace!("Finished attachment upload");

        Ok(())
    }
}
