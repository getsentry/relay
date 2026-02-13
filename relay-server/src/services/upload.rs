//! Service that uploads attachments.
use std::array::TryFromSliceError;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use objectstore_client::{Client, ExpirationPolicy, PutBuilder, Session, Usecase};
use relay_config::UploadServiceConfig;
use relay_quotas::DataCategory;
use relay_system::{
    Addr, AsyncResponse, FromMessage, Interface, NoResponse, Receiver, Sender, Service,
};
use sentry_protos::snuba::v1::TraceItem;
use smallvec::smallvec;
use uuid::Uuid;

use crate::constants::DEFAULT_ATTACHMENT_RETENTION;
use crate::envelope::ItemType;
use crate::managed::{
    Counted, Managed, ManagedResult, OutcomeError, Quantities, Rejected, TypedEnvelope,
};
use crate::processing::utils::store::item_id_to_uuid;
use crate::services::outcome::DiscardReason;
use crate::services::processor::Processed;
use crate::services::store::{Store, StoreEnvelope, StoreTraceItem};
use crate::statsd::{RelayCounters, RelayGauges, RelayTimers};
use crate::utils::upload;

use super::outcome::Outcome;

/// Messages that the upload service can handle.
pub enum Upload {
    Envelope(StoreEnvelope),
    Attachment(Managed<StoreAttachment>),
    Stream {
        message: upload::Stream,
        sender: Sender<Result<UploadKey, Error>>,
    },
}

impl Upload {
    fn ty(&self) -> &str {
        match self {
            Upload::Envelope(_) => "envelope",
            Upload::Attachment(_) => "attachment_v2",
            Upload::Stream { .. } => "stream",
        }
    }

    fn attachment_count(&self) -> usize {
        match self {
            Self::Envelope(StoreEnvelope { envelope }) => envelope
                .envelope()
                .items()
                .filter(|item| *item.ty() == ItemType::Attachment)
                .count(),
            Self::Attachment(_) => 1,
            Self::Stream { .. } => 1,
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

impl FromMessage<Managed<StoreAttachment>> for Upload {
    type Response = NoResponse;

    fn from_message(message: Managed<StoreAttachment>, _sender: ()) -> Self {
        Self::Attachment(message)
    }
}

impl FromMessage<upload::Stream> for Upload {
    type Response = AsyncResponse<Result<UploadKey, Error>>;

    fn from_message(message: upload::Stream, sender: Sender<Result<UploadKey, Error>>) -> Self {
        Self::Stream { message, sender }
    }
}

/// An attachment that is ready for upload / EAP storage.
pub struct StoreAttachment {
    /// The body to be uploaded to objectstore.
    pub body: Bytes,
    /// The trace item to be published via Kafka.
    pub trace_item: TraceItem,
}

impl Counted for StoreAttachment {
    fn quantities(&self) -> Quantities {
        smallvec![
            (DataCategory::AttachmentItem, 1),
            (DataCategory::Attachment, self.body.len()),
        ]
    }
}

/// Errors that can occur when trying to upload an attachment.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("timeout")]
    Timeout,
    #[error("load shed")]
    LoadShed,
    #[error("upload failed: {0}")]
    UploadFailed(#[from] objectstore_client::Error),
    #[error("UUID conversion failed: {0}")]
    Uuid(#[from] TryFromSliceError),
}

impl Error {
    fn as_str(&self) -> &'static str {
        match self {
            Error::Timeout => "timeout",
            Error::LoadShed => "load-shed",
            Error::UploadFailed(_) => "upload_failed",
            Error::Uuid(_) => "uuid",
        }
    }
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        (Some(Outcome::Invalid(DiscardReason::Internal)), self)
    }
}

/// The objectstore key that identifies a successfull upload.
#[derive(Debug, PartialEq)]
pub struct UploadKey(String);

impl UploadKey {
    fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for UploadKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
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
        let event_attachments = Usecase::new("attachments")
            .with_expiration_policy(ExpirationPolicy::TimeToLive(DEFAULT_ATTACHMENT_RETENTION));
        let trace_attachments = Usecase::new("trace_attachments")
            .with_expiration_policy(ExpirationPolicy::TimeToLive(DEFAULT_ATTACHMENT_RETENTION));

        let inner = UploadServiceInner {
            timeout: Duration::from_secs(*timeout),

            store,

            objectstore_client,
            event_attachments,
            trace_attachments,
        };

        Ok(Some(Self {
            pending_requests: FuturesUnordered::new(),
            max_concurrent_requests: *max_concurrent_requests,
            inner: Arc::new(inner),
        }))
    }

    fn handle_message(&self, message: Upload) {
        if self.pending_requests.len() >= self.max_concurrent_requests {
            relay_statsd::metric!(
                counter(RelayCounters::AttachmentUpload) += message.attachment_count() as u64,
                result = "load_shed",
                type = message.ty(),
            );
            match message {
                Upload::Envelope(envelope) => {
                    // Event attachments can still go the old route.

                    self.inner.store.send(envelope);
                }
                Upload::Attachment(managed) => {
                    let _ = managed.reject_err(Error::LoadShed);
                }
                Upload::Stream { message: _, sender } => {
                    sender.send(Err(Error::LoadShed));
                }
            };
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
    event_attachments: Usecase,
    trace_attachments: Usecase,
}

impl UploadServiceInner {
    async fn handle_message(&self, message: Upload) {
        match message {
            Upload::Envelope(StoreEnvelope { envelope }) => {
                self.handle_envelope(envelope).await;
            }
            Upload::Attachment(attachment) => self.handle_attachment(attachment).await,
            Upload::Stream {
                message: managed,
                sender,
            } => self.handle_stream(managed, sender).await,
        }
    }

    /// Uploads all attachments belonging to the given envelope.
    ///
    /// This mutates the attachment items in-place, setting their `stored_key` field to the key
    /// in objectstore.
    async fn handle_envelope(&self, mut envelope: TypedEnvelope<Processed>) -> () {
        let scoping = envelope.scoping();
        let session = self
            .event_attachments
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
                    // we are not storing zero-size attachments in objectstore
                    if attachment.is_empty() {
                        continue;
                    }
                    let result = self
                        .upload_bytes("envelope", &session, attachment.payload(), None)
                        .await;

                    relay_statsd::metric!(
                        counter(RelayCounters::AttachmentUpload) += 1,
                        result = match &result {
                            Ok(_) => "success",
                            Err(e) => e.as_str(),
                        },
                        type = "envelope",
                    );
                    if let Ok(stored_key) = result {
                        attachment.set_stored_key(stored_key.into_inner());
                    }
                }
            }
        }

        // last but not least, forward the envelope to the store endpoint
        self.store.send(StoreEnvelope { envelope });
    }

    async fn handle_attachment(&self, managed: Managed<StoreAttachment>) {
        let result = self.do_handle_store_attachment(managed).await;

        relay_statsd::metric!(
            counter(RelayCounters::AttachmentUpload) += 1,
            result = match result {
                Ok(()) => "success",
                Err(e) => e.into_inner().as_str(),
            },
            type = "attachment_v2",
        );
    }

    async fn do_handle_store_attachment(
        &self,
        managed: Managed<StoreAttachment>,
    ) -> Result<(), Rejected<Error>> {
        let scoping = managed.scoping();
        let session = self
            .trace_attachments
            .for_project(scoping.organization_id.value(), scoping.project_id.value())
            .session(&self.objectstore_client)
            .map_err(Error::UploadFailed)
            .reject(&managed)?;

        let quantities = managed.quantities();
        let body = Bytes::clone(&managed.body);

        // Make sure that the attachment can be converted into a trace item:
        let trace_item = managed.try_map(|attachment, _record_keeper| {
            let StoreAttachment {
                trace_item,
                body: _,
            } = attachment;
            Ok::<_, Error>(StoreTraceItem {
                trace_item,
                quantities,
            })
        })?;

        // Upload the attachment:
        if !body.is_empty() {
            relay_log::trace!("Starting attachment upload");
            let key = item_id_to_uuid(&trace_item.trace_item.item_id)
                .map_err(Error::from)
                .reject(&trace_item)?
                .as_simple()
                .to_string();

            #[cfg(debug_assertions)]
            let original_key = key.clone();

            let _stored_key = self
                .upload_bytes("attachment_v2", &session, body, Some(key))
                .await
                .reject(&trace_item)?;

            #[cfg(debug_assertions)]
            debug_assert_eq!(_stored_key.into_inner(), original_key);
        }

        // Only after successful upload forward the attachment to the store.
        self.store.send(trace_item);

        Ok(())
    }

    async fn handle_stream(
        &self,
        upload::Stream { scoping, stream }: upload::Stream,
        sender: Sender<Result<UploadKey, Error>>,
    ) {
        let session = match self
            .event_attachments
            .for_project(
                dbg!(scoping.organization_id.value()),
                scoping.project_id.value(),
            )
            .session(&self.objectstore_client)
        {
            Ok(session) => session,
            Err(error) => {
                relay_statsd::metric!(
                    counter(RelayCounters::AttachmentUpload) += 1,
                    result = error.to_string().as_str(),
                    type = "stream",
                );
                sender.send(Err(Error::UploadFailed(error)));
                return;
            }
        };

        let request = session
            .put_stream(stream.boxed())
            // generate ID here to drop the hyphens and be consistent with other attachment uploads.
            .key(Uuid::now_v7().as_simple().to_string());

        let result = self.upload("stream", request).await;

        relay_statsd::metric!(
            counter(RelayCounters::AttachmentUpload) += 1,
            result = match &result {
                Ok(_) => "success",
                Err(e) => e.as_str(),
            },
            type = "stream",
        );

        sender.send(result);
    }

    async fn upload_bytes(
        &self,
        ty: &str,
        session: &Session,
        payload: Bytes,
        key: Option<String>,
    ) -> Result<UploadKey, Error> {
        let mut request = session.put(payload);
        if let Some(key) = key {
            request = request.key(key);
        }
        self.upload(ty, request).await
    }

    async fn upload(&self, ty: &str, request: PutBuilder) -> Result<UploadKey, Error> {
        relay_log::trace!("Starting attachment upload");
        let response = relay_statsd::metric!(
            timer(RelayTimers::AttachmentUploadDuration),
            type = ty,
            {
                tokio::time::timeout(self.timeout, request.send())
                    .await
                    .map_err(|_elapsed| Error::Timeout)?
                    .map_err(Error::UploadFailed)?
            }
        );

        relay_log::trace!("Finished attachment upload");

        Ok(UploadKey(response.key))
    }
}
