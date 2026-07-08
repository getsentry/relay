//! Objectstore service for uploading attachments.
use std::array::TryFromSliceError;
use std::fmt;
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use async_compression::tokio::bufread::ZstdEncoder;
use bytes::Bytes;
use futures::StreamExt;
use http::StatusCode;
use objectstore_client::{
    Client, Compression, ExpirationPolicy, SecretKey as SigningKey, Session, TokenGenerator,
    UploadId, Usecase,
};

use objectstore_types::multipart::InvalidUploadId;
use relay_base_schema::organization::OrganizationId;
use relay_base_schema::project::ProjectId;
use relay_config::ObjectstoreServiceConfig;
use relay_quotas::Scoping;
use relay_system::{
    Addr, AsyncResponse, FromMessage, Interface, LoadShed, NoResponse, Sender, SimpleService,
};
use sentry_protos::snuba::v1::TraceItem;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::constants::DEFAULT_ATTACHMENT_RETENTION;
use crate::envelope::{ContentType, Item, ItemType};
use crate::managed::{
    Counted, ItemAction, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities,
    Rejected,
};
use crate::processing::utils::store::item_id_to_uuid;
use crate::services::outcome::DiscardReason;
use crate::services::store::{
    ProfileAttachment, Store, StoreAttachment, StoreEnvelope, StoreProfileChunk, StoreTraceItem,
};
use crate::services::upload::ByteStream;
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::{
    BoundedStream, MeteredStream, Rechunk, RetryableStream, TakeOnce, find_error_source,
};

use super::outcome::Outcome;

/// Size of an individual request to objectstore.
const CHUNK_SIZE: NonZeroUsize = NonZeroUsize::new(5 * 1024 * 1024).unwrap();

/// Messages that the objectstore service can handle.
pub enum Objectstore {
    Envelope(StoreEnvelope),
    TraceAttachment(Managed<StoreTraceAttachment>),
    EventAttachment(Managed<StoreAttachment>),
    RawProfile(Managed<StoreRawProfile>),
    Create(Create, Sender<Result<UploadRef, Error>>),
    Stream(Stream, Sender<Result<ObjectstoreKey, Error>>),
}

impl Objectstore {
    fn kind(&self) -> MessageKind {
        match self {
            Self::Envelope(_) => MessageKind::Envelope,
            Self::TraceAttachment(_) => MessageKind::TraceAttachment,
            Self::EventAttachment(_) => MessageKind::EventAttachment,
            Self::RawProfile(_) => MessageKind::RawProfile,
            Self::Stream { .. } => MessageKind::Stream,
            Self::Create { .. } => MessageKind::Create,
        }
    }

    fn attachment_count(&self) -> usize {
        match self {
            Self::Envelope(StoreEnvelope { envelope }) => envelope
                .envelope()
                .items()
                .filter(|item| *item.ty() == ItemType::Attachment)
                .count(),
            Self::TraceAttachment(_) => 1,
            Self::EventAttachment(_) => 1,
            Self::RawProfile(_) => 1,
            Self::Stream { .. } => 1,
            Self::Create { .. } => 0,
        }
    }
}

impl Interface for Objectstore {}

impl FromMessage<StoreEnvelope> for Objectstore {
    type Response = NoResponse;

    fn from_message(message: StoreEnvelope, _sender: ()) -> Self {
        Self::Envelope(message)
    }
}

impl FromMessage<Managed<StoreTraceAttachment>> for Objectstore {
    type Response = NoResponse;

    fn from_message(message: Managed<StoreTraceAttachment>, _sender: ()) -> Self {
        Self::TraceAttachment(message)
    }
}

impl FromMessage<Managed<StoreAttachment>> for Objectstore {
    type Response = NoResponse;

    fn from_message(message: Managed<StoreAttachment>, _sender: ()) -> Self {
        Self::EventAttachment(message)
    }
}

impl FromMessage<Managed<StoreRawProfile>> for Objectstore {
    type Response = NoResponse;

    fn from_message(message: Managed<StoreRawProfile>, _sender: ()) -> Self {
        Self::RawProfile(message)
    }
}

/// A type tag used for logging.
#[derive(Debug, Clone, Copy)]
enum MessageKind {
    Envelope,
    EventAttachment,
    TraceAttachment,
    RawProfile,
    Stream,
    Create,
}

impl MessageKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Envelope => "envelope",
            Self::EventAttachment => "attachment",
            Self::TraceAttachment => "attachment_v2",
            Self::RawProfile => "profile_raw",
            Self::Stream => "stream",
            Self::Create => "create",
        }
    }
}

/// A request to create a new objectstore multipart upload.
pub struct Create {
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub key: String,
}

impl FromMessage<Create> for Objectstore {
    type Response = AsyncResponse<Result<UploadRef, Error>>;

    fn from_message(message: Create, sender: Sender<Result<UploadRef, Error>>) -> Self {
        Self::Create(message, sender)
    }
}

/// A stream that can be uploaded to objectstore.
pub struct Stream {
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub upload_ref: UploadRef,
    pub stream: BoundedStream<MeteredStream<ByteStream>>,
}

impl FromMessage<Stream> for Objectstore {
    type Response = AsyncResponse<Result<ObjectstoreKey, Error>>;

    fn from_message(message: Stream, sender: Sender<Result<ObjectstoreKey, Error>>) -> Self {
        Self::Stream(message, sender)
    }
}

/// An attachment that is ready for upload / EAP storage.
pub struct StoreTraceAttachment {
    /// The body to be uploaded to objectstore.
    pub body: Bytes,
    /// The trace item to be published via Kafka.
    pub trace_item: TraceItem,
    /// Data retention in days for this attachment.
    pub retention: u16,
}

impl Counted for StoreTraceAttachment {
    fn quantities(&self) -> Quantities {
        self.trace_item.quantities()
    }
}

/// A raw profile (e.g. Perfetto trace) ready for objectstore upload.
///
/// After upload, the [`StoreProfileChunk`] is forwarded to the Store service
/// with the objectstore key set, so the Kafka message carries a reference
/// instead of the full binary blob.
pub struct StoreRawProfile {
    /// The profile chunk message to forward to Kafka after upload.
    pub profile_chunk: StoreProfileChunk,
    /// The raw profile to be stored in objectstore.
    pub raw: RawProfile,
}

impl Counted for StoreRawProfile {
    fn quantities(&self) -> Quantities {
        self.profile_chunk.quantities()
    }
}

/// A raw profile to be stored in objectstore.
pub struct RawProfile {
    /// Name of the raw profile attachment.
    pub name: String,
    /// Bytes of the raw profile.
    pub payload: Bytes,
    /// Content type of the raw profile.
    pub content_type: ContentType,
}

#[derive(Debug, thiserror::Error)]
#[error("objectstore upload failed")]
pub struct Error {
    /// The source of the error.
    #[source]
    pub kind: ErrorKind,
    /// The number of upload attempts.
    ///
    /// Zero for errors that occur before the first upload attempt.
    pub attempts: u16,
    /// The amount of attachments that failed.
    pub amount: u64,
}

impl Error {
    fn with_attempts(mut self, attempts: u16) -> Self {
        self.attempts = attempts;
        self
    }

    fn with_amount(mut self, amount: usize) -> Self {
        self.amount = amount as u64;
        self
    }

    fn log(&self, kind: MessageKind) {
        relay_statsd::metric!(
            counter(RelayCounters::AttachmentUpload) += self.amount,
            result = self.kind.as_str(),
            type = kind.as_str(),
            attempts = self.attempts.to_string(),
        );
        if self.kind.is_client_error() {
            return;
        }
        relay_log::error!(
            error = &self.kind as &dyn std::error::Error,
            amount = self.amount,
            attempts = self.attempts,
            type = kind.as_str(),
            "failed to upload {} attachment(s) to objectstore in {} attempt(s)",
            self.amount,
            self.attempts
        )
    }
}

impl<E: Into<ErrorKind>> From<E> for Error {
    fn from(value: E) -> Self {
        Self {
            kind: value.into(),
            attempts: 0,
            amount: 1,
        }
    }
}

/// Errors that can occur when trying to upload an attachment.
#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    #[error("invalid scoping")]
    InvalidScoping,
    #[error("timeout: {0}")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("load shed")]
    LoadShed,
    #[error("upload failed: {0}")]
    UploadFailed(#[from] objectstore_client::Error),
    #[error("UUID conversion failed: {0}")]
    Uuid(#[from] TryFromSliceError),
}

impl ErrorKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidScoping => "invalid_scoping",
            Self::Timeout(_) => "timeout",
            Self::LoadShed => "load_shed",
            Self::UploadFailed(_) => "upload_failed",
            Self::Uuid(_) => "uuid",
        }
    }

    fn is_client_error(&self) -> bool {
        match self {
            ErrorKind::UploadFailed(objectstore_client::Error::Reqwest(error)) => {
                find_error_source(error, is_user_error).is_some()
            }
            _ => false,
        }
    }
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        (Some(Outcome::Invalid(DiscardReason::Internal)), self)
    }
}

/// The objectstore key that identifies a successful upload.
#[derive(Debug, PartialEq)]
pub struct ObjectstoreKey(String);

impl ObjectstoreKey {
    /// Returns the underlying [`String`].
    pub fn into_inner(self) -> String {
        self.0
    }
}

/// Identifier needed to resume an existing upload.
#[derive(Debug, Clone)]
pub struct UploadRef {
    /// They key of the file (chosen by relay).
    pub key: String,
    /// The ID of the multipart upload session (chosen by objectstore).
    /// `None` if the upload is not multipart.
    pub upload_id: Option<UploadId>,
}

impl UploadRef {
    /// Validates the upload ID and returns a new upload reference.
    pub fn new(key: String, upload_id: Option<String>) -> Result<Self, InvalidUploadId> {
        let upload_id = match upload_id {
            Some(s) => Some(UploadId::new(s)?),
            None => None,
        };
        Ok(Self { key, upload_id })
    }
}

impl fmt::Display for ObjectstoreKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The objectstore service that uploads attachments.
///
/// Accepts upload requests and maintains a list of concurrent uploads.
#[derive(Clone)]
pub struct ObjectstoreService {
    inner: Arc<ObjectstoreServiceInner>,
}

impl ObjectstoreService {
    pub fn new(
        config: &ObjectstoreServiceConfig,
        store: Option<Addr<Store>>,
    ) -> anyhow::Result<Option<Self>> {
        let Some(store) = store else { return Ok(None) };
        let ObjectstoreServiceConfig {
            objectstore_url,
            max_concurrent_requests: _,
            max_backlog: _,
            timeout,
            stream_timeout,
            retry_delay,
            max_attempts,
            fallback_to_kafka,
            auth,
        } = config;
        let Some(objectstore_url) = objectstore_url else {
            return Ok(None);
        };

        let objectstore_client = {
            let mut builder = Client::builder(objectstore_url);

            if let Some(auth) = auth {
                // TODO(FS-313): when Objectstore starts enforcing auth, propagate error with ?
                let token_generator = TokenGenerator::new(SigningKey {
                    kid: auth.key_id.clone(),
                    secret_key: auth.signing_key.clone(),
                });

                builder = match token_generator {
                    Ok(token_generator) => builder.token(token_generator),
                    Err(error) => {
                        relay_log::error!(
                            error = &error as &dyn std::error::Error,
                            "failed to configure objectstore auth"
                        );
                        builder
                    }
                };
            }

            builder.build()?
        };
        let event_attachments = Usecase::new("attachments")
            .with_expiration_policy(ExpirationPolicy::TimeToLive(DEFAULT_ATTACHMENT_RETENTION));
        let trace_attachments = Usecase::new("trace_attachments")
            .with_expiration_policy(ExpirationPolicy::TimeToLive(DEFAULT_ATTACHMENT_RETENTION));
        let profile_attachments = Usecase::new("profile_attachments")
            .with_expiration_policy(ExpirationPolicy::TimeToLive(DEFAULT_ATTACHMENT_RETENTION));

        let inner = ObjectstoreServiceInner {
            store,
            objectstore_client,
            event_attachments,
            trace_attachments,
            profile_attachments,
            timeout: Duration::from_secs(*timeout),
            stream_timeout: Duration::from_secs(*stream_timeout),
            retry_interval: Duration::from_secs_f64(*retry_delay),
            max_attempts: *max_attempts,
            fallback_to_kafka: *fallback_to_kafka,
        };

        Ok(Some(Self {
            inner: Arc::new(inner),
        }))
    }
}

impl SimpleService for ObjectstoreService {
    type Interface = Objectstore;

    async fn handle_message(&self, message: Self::Interface) {
        self.inner.handle_message(message).await
    }
}

impl LoadShed<Objectstore> for ObjectstoreService {
    fn handle_loadshed(&self, message: Objectstore) {
        let error = Error::from(ErrorKind::LoadShed).with_amount(message.attachment_count());
        error.log(message.kind());
        match message {
            Objectstore::Envelope(envelope) => {
                let StoreEnvelope { mut envelope } = envelope;
                if !self.inner.fallback_to_kafka {
                    drop_failed_uploads(&mut envelope);
                }
                self.inner.store.send(StoreEnvelope { envelope });
            }
            Objectstore::EventAttachment(message) => {
                if self.inner.fallback_to_kafka {
                    self.inner.store.send(message);
                } else {
                    let _ = message.reject_err(Outcome::Invalid(DiscardReason::UploadFailed));
                }
            }
            Objectstore::TraceAttachment(managed) => {
                let _ = managed.reject_err(error);
            }
            Objectstore::RawProfile(managed) => {
                self.inner
                    .store
                    .send(managed.map(|profile, _| profile.profile_chunk));
            }
            Objectstore::Stream(_, sender) => {
                sender.send(Err(error));
            }
            Objectstore::Create(_, sender) => {
                sender.send(Err(error));
            }
        }
    }
}

struct ObjectstoreServiceInner {
    store: Addr<Store>,

    objectstore_client: Client,
    event_attachments: Usecase,
    trace_attachments: Usecase,
    profile_attachments: Usecase,
    timeout: Duration,
    stream_timeout: Duration,
    retry_interval: Duration,
    max_attempts: NonZeroU16,
    fallback_to_kafka: bool,
}

impl ObjectstoreServiceInner {
    async fn handle_message(&self, message: Objectstore) {
        match message {
            Objectstore::Envelope(StoreEnvelope { envelope }) => {
                self.handle_envelope(envelope).await;
            }
            Objectstore::TraceAttachment(attachment) => {
                let result = self
                    .handle_trace_attachment(attachment)
                    .await
                    .map_err(Rejected::into_inner);
                if let Err(error) = result {
                    error.log(MessageKind::TraceAttachment);
                }
            }
            Objectstore::EventAttachment(attachment) => {
                self.handle_event_attachment(attachment).await;
            }
            Objectstore::RawProfile(profile) => {
                self.handle_raw_profile(profile).await;
            }
            Objectstore::Create(create, sender) => {
                let result = self.handle_create(create).await;
                if let Err(error) = &result {
                    error.log(MessageKind::Create);
                }
                sender.send(result);
            }
            Objectstore::Stream(stream, sender) => {
                let result = self.handle_stream(stream).await;
                if let Err(error) = &result {
                    error.log(MessageKind::Stream);
                }
                sender.send(result);
            }
        };
    }

    /// Uploads all attachments belonging to the given envelope.
    ///
    /// This mutates the attachment items in-place, setting their `stored_key` field to the key
    /// in objectstore.
    async fn handle_envelope(&self, mut envelope: ManagedEnvelope) {
        let scoping = envelope.scoping();
        let session = self.session(
            &self.event_attachments,
            scoping.organization_id,
            scoping.project_id,
        );
        let retention = envelope.envelope().retention();

        let attachments = envelope
            .envelope_mut()
            .items_mut()
            .filter(|item| should_upload(item));

        match session {
            Err(error) => {
                error
                    .with_amount(attachments.count())
                    .log(MessageKind::Envelope);

                if !self.fallback_to_kafka {
                    drop_failed_uploads(&mut envelope);
                }
            }
            Ok(session) => {
                for attachment in attachments {
                    let result = self
                        .upload_bytes(
                            MessageKind::Envelope,
                            &session,
                            attachment.payload(),
                            retention,
                            None,
                            None,
                        )
                        .await;

                    match result {
                        Ok(stored_key) => {
                            attachment.set_stored_key(stored_key.into_inner());
                        }
                        Err(error) => {
                            error.log(MessageKind::Envelope);
                        }
                    }
                }

                if !self.fallback_to_kafka {
                    drop_failed_uploads(&mut envelope);
                }
            }
        }

        // last but not least, forward the envelope to the store endpoint
        self.store.send(StoreEnvelope { envelope });
    }

    /// Uploads the attachment.
    ///
    /// This mutates the attachment item in-place, setting the `stored_key` field to the key in the
    /// objectstore.
    async fn handle_event_attachment(&self, mut attachment: Managed<StoreAttachment>) {
        if !should_upload(&attachment.attachment) {
            self.store.send(attachment);
            return;
        }

        let scoping = attachment.scoping();
        let session = self.session(
            &self.event_attachments,
            scoping.organization_id,
            scoping.project_id,
        );

        let upload_result = match session {
            Err(error) => Err(error),
            Ok(session) => {
                self.upload_bytes(
                    MessageKind::EventAttachment,
                    &session,
                    attachment.attachment.payload(),
                    attachment.retention,
                    None,
                    None,
                )
                .await
            }
        };

        match upload_result {
            Ok(stored_key) => {
                attachment.modify(|attachment, _| {
                    attachment
                        .attachment
                        .set_stored_key(stored_key.into_inner());
                });
                self.store.send(attachment);
            }
            Err(error) => {
                error.log(MessageKind::EventAttachment);
                if self.fallback_to_kafka {
                    self.store.send(attachment)
                } else {
                    let _ = attachment.reject_err(Outcome::Invalid(DiscardReason::UploadFailed));
                }
            }
        };
    }

    async fn handle_trace_attachment(
        &self,
        managed: Managed<StoreTraceAttachment>,
    ) -> Result<(), Rejected<Error>> {
        let scoping = managed.scoping();
        let session = self
            .session(
                &self.trace_attachments,
                scoping.organization_id,
                scoping.project_id,
            )
            .reject(&managed)?;

        let body = Bytes::clone(&managed.body);
        let retention = managed.retention;

        // Make sure that the attachment can be converted into a trace item:
        let trace_item = managed.try_map(|attachment, _record_keeper| {
            let StoreTraceAttachment {
                trace_item,
                body: _,
                retention: _,
            } = attachment;
            Ok::<_, Error>(StoreTraceItem { trace_item })
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
                .upload_bytes(
                    MessageKind::TraceAttachment,
                    &session,
                    body,
                    retention,
                    Some(key),
                    None,
                )
                .await
                .reject(&trace_item)?;

            #[cfg(debug_assertions)]
            debug_assert_eq!(_stored_key.into_inner(), original_key);
        }

        // Only after successful upload forward the attachment to the store.
        self.store.send(trace_item);

        Ok(())
    }

    async fn handle_raw_profile(&self, managed: Managed<StoreRawProfile>) {
        let scoping = managed.scoping();

        let payload = managed.raw.payload.clone();
        let content_type = managed.raw.content_type;
        let name = managed.raw.name.clone();

        let mut store_message = managed.map(|profile, _| profile.profile_chunk);

        match self
            .try_upload_raw_profile(payload, content_type, store_message.retention_days, scoping)
            .await
        {
            Ok(Some(stored_id)) => {
                store_message.modify(|message, _| {
                    message.attachments.push(ProfileAttachment {
                        name,
                        content_type,
                        stored_id,
                    })
                });
            }
            Ok(None) => {}
            Err(error) => {
                // Always forward to store even if the raw profile upload failed,
                // to ensure the Kafka message is produced.
                error.log(MessageKind::RawProfile);
            }
        }

        self.store.send(store_message);
    }

    async fn try_upload_raw_profile(
        &self,
        payload: Bytes,
        content_type: ContentType,
        retention: u16,
        scoping: Scoping,
    ) -> Result<Option<ObjectstoreKey>, Error> {
        if payload.is_empty() {
            return Ok(None);
        }

        let session = self
            .profile_attachments
            .for_project(scoping.organization_id.value(), scoping.project_id.value())
            .session(&self.objectstore_client)?;

        let stored_key = self
            .upload_bytes(
                MessageKind::RawProfile,
                &session,
                payload,
                retention,
                None,
                Some(content_type),
            )
            .await?;

        Ok(Some(stored_key))
    }

    async fn handle_create(&self, create: Create) -> Result<UploadRef, Error> {
        let Create {
            organization_id,
            project_id,
            key,
        } = create;
        let session = self.session(&self.event_attachments, organization_id, project_id)?;

        let multipart_upload = session
            .initiate_multipart_upload()
            .key(&key)
            .compression(Compression::Zstd) // make explicit because parts need to be manually compressed.
            .send()
            .await?;
        debug_assert_eq!(&key, multipart_upload.key());
        let upload_id = multipart_upload.upload_id();

        Ok(UploadRef {
            key,
            upload_id: Some(upload_id.clone()),
        })
    }

    async fn handle_stream(&self, stream: Stream) -> Result<ObjectstoreKey, Error> {
        let Stream {
            organization_id,
            project_id,
            upload_ref,
            stream,
        } = stream;
        let session = self.session(&self.event_attachments, organization_id, project_id)?;

        self.upload(
            MessageKind::Stream,
            &session,
            Upload::Stream {
                body: TakeOnce::new(stream),
                upload_ref,
            },
        )
        .await
    }

    async fn upload_bytes(
        &self,
        kind: MessageKind,
        session: &Session,
        payload: Bytes,
        retention: u16,
        key: Option<String>,
        content_type: Option<ContentType>,
    ) -> Result<ObjectstoreKey, Error> {
        let retention_hours = retention.checked_mul(24);
        self.upload(
            kind,
            session,
            Upload::Bytes {
                body: payload,
                key,
                retention_hours,
                content_type,
            },
        )
        .await
    }

    async fn upload(
        &self,
        kind: MessageKind,
        session: &Session,
        body: Upload,
    ) -> Result<ObjectstoreKey, Error> {
        let mut attempts = 0;
        let timeout = match &body {
            Upload::Bytes { .. } => self.timeout,
            Upload::Stream { .. } => self.stream_timeout,
        };
        let result = tokio::time::timeout(timeout, async {
            let mut result = None;
            loop {
                let Some(body) = body.try_clone() else {
                    break;
                };
                attempts += 1;
                result.replace(self.attempt_upload(kind, session, body).await);

                if attempts < self.max_attempts.get()
                    && matches!(&result, Some(Err(e)) if is_retryable(e))
                {
                    tokio::time::sleep(self.retry_interval).await;
                } else {
                    break;
                }
            }

            result
                .expect("try_clone() should succeed at least once")
                .map_err(Error::from)
        })
        .await
        .map_err(Error::from)
        .flatten();

        if result.is_ok() {
            relay_statsd::metric!(
                counter(RelayCounters::AttachmentUpload) += 1,
                result = "success",
                type = kind.as_str(),
                attempts = attempts.to_string()
            );
        }

        result.map_err(|e| e.with_attempts(attempts))
    }

    async fn attempt_upload(
        &self,
        kind: MessageKind,
        session: &Session,
        body: UploadAttempt,
    ) -> Result<ObjectstoreKey, objectstore_client::Error> {
        match body {
            UploadAttempt::Bytes {
                body,
                key,
                retention_hours,
                content_type,
            } => {
                let mut request = session.put(body);
                if let Some(content_type) = content_type {
                    request = request.content_type(content_type.as_str());
                }
                if let Some(retention_hours) = retention_hours {
                    request = request.expiration_policy(ExpirationPolicy::TimeToLive(
                        Duration::from_hours(retention_hours.into()),
                    ));
                }
                if let Some(key) = key {
                    request = request.key(key);
                }
                let response = relay_statsd::metric!(
                    timer(RelayTimers::AttachmentUploadDuration),
                    type = kind.as_str(),
                {
                    request.send().await?
                });

                Ok(ObjectstoreKey(response.key))
            }
            UploadAttempt::Stream { body, upload_ref } => {
                let UploadRef { key, upload_id } = upload_ref;
                let Some(upload_id) = upload_id else {
                    // No upload ID: simple upload in a single request.
                    let request = session.put_stream(body.boxed()).key(key);
                    let response = request.send().await?;
                    return Ok(ObjectstoreKey(response.key));
                };

                let multipart_upload =
                    session.resume_multipart_upload(key, upload_id.to_string())?;

                let body = ReaderStream::new(ZstdEncoder::new(StreamReader::new(body)));

                // Unfortunately, MinIO has the limitation that the length of a multipart request
                // has to be known. Therefore, we need to materialize the stream into concrete
                // chunks of bytes and send each chunk as an individual request.
                let chunks = Rechunk::new(body, CHUNK_SIZE);
                let mut body = chunks.enumerate();

                let result = relay_statsd::metric!(
                    timer(RelayTimers::AttachmentUploadDuration),
                    type = kind.as_str(),
                {
                    let mut parts = vec![];
                    // NOTE: Once every upload is a multipart upload, we can remove `RetryableStream`
                    // because streams will never be effectively retried.
                    while let Some((i, chunk)) = body.next().await {
                        let chunk = chunk?;
                        let part_number = u32::try_from(i + 1)
                            .map_err(|_| objectstore_client::Error::InvalidPartNumber(u32::MAX))?;
                        let part = multipart_upload.put(chunk, part_number, None).await?;
                        parts.push(part);
                    }
                    multipart_upload.complete(parts).await?
                });

                Ok(ObjectstoreKey(result))
            }
        }
    }

    fn session(
        &self,
        usecase: &Usecase,
        organization_id: OrganizationId,
        project_id: ProjectId,
    ) -> Result<Session, Error> {
        if organization_id.value() == 0 || project_id.value() == 0 {
            return Err(ErrorKind::InvalidScoping.into());
        }
        let session = usecase
            .for_project(organization_id.value(), project_id.value())
            .session(&self.objectstore_client)?;
        Ok(session)
    }
}

/// Common interface for calls to [`ObjectstoreServiceInner::upload`].
///
/// This type is shared across retries.
enum Upload {
    Bytes {
        body: Bytes,
        key: Option<String>,
        retention_hours: Option<u16>,
        content_type: Option<ContentType>,
    },
    Stream {
        body: TakeOnce<BoundedStream<MeteredStream<ByteStream>>>,
        upload_ref: UploadRef,
    },
}

impl Upload {
    fn try_clone(&self) -> Option<UploadAttempt> {
        match self {
            Self::Bytes {
                body,
                key,
                retention_hours,
                content_type,
            } => Some(UploadAttempt::Bytes {
                body: body.clone(),
                key: key.clone(),
                retention_hours: *retention_hours,
                content_type: *content_type,
            }),
            Self::Stream { body, upload_ref } => {
                RetryableStream::new(body.clone()).map(|body| UploadAttempt::Stream {
                    body,
                    upload_ref: upload_ref.clone(),
                })
            }
        }
    }
}

/// Common interface for calls to [`ObjectstoreServiceInner::attempt_upload`].
///
/// This type is instantiated for every retry.
enum UploadAttempt {
    Bytes {
        body: Bytes,
        key: Option<String>,
        retention_hours: Option<u16>,
        content_type: Option<ContentType>,
    },
    Stream {
        body: RetryableStream<BoundedStream<MeteredStream<ByteStream>>>,
        upload_ref: UploadRef,
    },
}

fn is_retryable(error: &objectstore_client::Error) -> bool {
    match error {
        objectstore_client::Error::Reqwest(error) => {
            error.is_connect()
                || error.is_timeout()
                || matches!(
                    error.status(),
                    Some(
                        // TODO(follow-up): Does retrying 429 actually help, or does it cascade?
                        // Might need a larger retry delay for 429 (ideally objectstore sends Retry-After header).
                        StatusCode::TOO_MANY_REQUESTS
                            | StatusCode::BAD_GATEWAY
                            | StatusCode::SERVICE_UNAVAILABLE
                            | StatusCode::GATEWAY_TIMEOUT
                            | StatusCode::INTERNAL_SERVER_ERROR
                    )
                )
        }
        _ => false,
    }
}

fn is_user_error(error: &(dyn std::error::Error + 'static)) -> bool {
    error.downcast_ref::<std::io::Error>().is_some_and(|error| {
        matches!(
            error.kind(),
            std::io::ErrorKind::FileTooLarge | std::io::ErrorKind::UnexpectedEof
        )
    })
}

fn should_upload(item: &Item) -> bool {
    *item.ty() == ItemType::Attachment
        && item.stored_key().is_none()
        && !item.is_empty()
        && !item.is_attachment_ref()
}

fn drop_failed_uploads(envelope: &mut ManagedEnvelope) {
    envelope.retain_items(|item| {
        if should_upload(item) {
            ItemAction::Drop(Outcome::Invalid(DiscardReason::UploadFailed))
        } else {
            ItemAction::Keep
        }
    });
    envelope.update();
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use relay_event_schema::protocol::EventId;
    use relay_quotas::DataCategory;
    use relay_system::Service;

    use crate::Envelope;

    use super::*;

    #[tokio::test]
    async fn org_zero_rejected() {
        let service = ObjectstoreService::new(&test_config(), Some(Addr::dummy()))
            .unwrap()
            .unwrap();
        let addr = service.start_detached();

        let stream = BoundedStream::new(
            MeteredStream::new(futures::stream::empty().boxed(), "test"),
            0,
            usize::MAX,
        );

        let result = addr
            .send(Stream {
                organization_id: OrganizationId::new(0),
                project_id: ProjectId::new(1),
                upload_ref: UploadRef {
                    key: "my_file".to_owned(),
                    upload_id: Some(UploadId::new("my_upload".to_owned()).unwrap()),
                },
                stream,
            })
            .await
            .unwrap();
        let err = result.unwrap_err();

        assert!(matches!(err.kind, ErrorKind::InvalidScoping));
    }

    #[tokio::test]
    async fn envelope_falls_back_to_kafka_by_default() {
        let (store, mut store_rx) = Addr::custom();
        let (outcomes, mut outcome_rx) = Addr::custom();
        let service = ObjectstoreService::new(&test_config(), Some(store))
            .unwrap()
            .unwrap();

        service
            .inner
            .handle_envelope(ManagedEnvelope::new(test_envelope(), outcomes))
            .await;

        let Store::Envelope(StoreEnvelope { envelope }) = store_rx.try_recv().unwrap() else {
            panic!("expected envelope");
        };
        {
            let items = envelope.envelope().items().collect::<Vec<_>>();
            assert_eq!(items.len(), 2);
            assert_eq!(items[0].ty(), &ItemType::Event);
            assert_eq!(items[1].ty(), &ItemType::Attachment);
            assert_eq!(items[1].stored_key(), None);
        }
        envelope.accept();

        assert!(outcome_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn envelope_rejects_failed_attachments_without_fallback() {
        let (store, mut store_rx) = Addr::custom();
        let (outcomes, mut outcome_rx) = Addr::custom();
        let mut config = test_config();
        config.fallback_to_kafka = false;
        let service = ObjectstoreService::new(&config, Some(store))
            .unwrap()
            .unwrap();

        service
            .inner
            .handle_envelope(ManagedEnvelope::new(test_envelope(), outcomes))
            .await;

        let Store::Envelope(StoreEnvelope { envelope }) = store_rx.try_recv().unwrap() else {
            panic!("expected envelope");
        };
        {
            let items = envelope.envelope().items().collect::<Vec<_>>();
            assert_eq!(items.len(), 1);
            assert_eq!(items[0].ty(), &ItemType::Event);
        }
        envelope.accept();

        let outcome = outcome_rx.try_recv().unwrap();
        assert_eq!(
            outcome.outcome,
            Outcome::Invalid(DiscardReason::UploadFailed)
        );
        assert_eq!(outcome.category, DataCategory::Attachment);
        assert_eq!(outcome.quantity, 5);

        let outcome = outcome_rx.try_recv().unwrap();
        assert_eq!(
            outcome.outcome,
            Outcome::Invalid(DiscardReason::UploadFailed)
        );
        assert_eq!(outcome.category, DataCategory::AttachmentItem);
        assert_eq!(outcome.quantity, 1);

        assert!(outcome_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn event_attachment_rejects_without_fallback() {
        let (store, mut store_rx) = Addr::custom();
        let (outcomes, mut outcome_rx) = Addr::custom();
        let mut config = test_config();
        config.fallback_to_kafka = false;
        let service = ObjectstoreService::new(&config, Some(store))
            .unwrap()
            .unwrap();

        let envelope = ManagedEnvelope::untracked(test_envelope(), outcomes.clone());
        let mut item = Item::new(ItemType::Attachment);
        item.set_payload(ContentType::Text, "hello");
        let quantities = item.quantities();
        let attachment = Managed::with_meta_from_managed_envelope(
            &envelope,
            StoreAttachment {
                event_id: EventId::new(),
                attachment: item,
                quantities,
                retention: 90,
            },
        );

        service.inner.handle_event_attachment(attachment).await;

        assert!(store_rx.try_recv().is_err());

        let outcome = outcome_rx.try_recv().unwrap();
        assert_eq!(
            outcome.outcome,
            Outcome::Invalid(DiscardReason::UploadFailed)
        );
        assert_eq!(outcome.category, DataCategory::Attachment);
        assert_eq!(outcome.quantity, 5);

        let outcome = outcome_rx.try_recv().unwrap();
        assert_eq!(
            outcome.outcome,
            Outcome::Invalid(DiscardReason::UploadFailed)
        );
        assert_eq!(outcome.category, DataCategory::AttachmentItem);
        assert_eq!(outcome.quantity, 1);

        assert!(outcome_rx.try_recv().is_err());
    }

    fn test_envelope() -> Box<Envelope> {
        Envelope::parse_bytes(Bytes::from_static(
            b"{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}\n\
              {\"type\":\"event\",\"length\":2}\n\
              {}\n\
              {\"type\":\"attachment\",\"length\":5,\"filename\":\"hello.txt\"}\n\
              hello\n",
        ))
        .unwrap()
    }

    fn test_config() -> ObjectstoreServiceConfig {
        ObjectstoreServiceConfig {
            objectstore_url: Some("http://objectstore".to_owned()),
            max_concurrent_requests: 1,
            max_backlog: 1,
            timeout: 1,
            stream_timeout: 1,
            retry_delay: 1.0,
            max_attempts: 1.try_into().unwrap(),
            fallback_to_kafka: true,
            auth: None,
        }
    }
}
