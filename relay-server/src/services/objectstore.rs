//! Objectstore service for uploading attachments.
use std::array::TryFromSliceError;
use std::fmt;
use std::io::Write;
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use http::StatusCode;
use objectstore_client::{
    Client, CompletePart, Compression, ExpirationPolicy, MultipartUpload, PartInfo,
    SecretKey as SigningKey, Session, TokenGenerator, UploadId, Usecase,
};

use relay_base_schema::organization::OrganizationId;
use relay_base_schema::project::ProjectId;
use relay_config::ObjectstoreServiceConfig;
use relay_quotas::Scoping;
use relay_system::{
    Addr, AsyncResponse, FromMessage, Interface, LoadShed, NoResponse, Sender, SimpleService,
};
use sentry_protos::snuba::v1::TraceItem;

use crate::constants::DEFAULT_ATTACHMENT_RETENTION;
use crate::envelope::{ContentType, Item, ItemType};
use crate::managed::{Counted, Managed, ManagedResult, OutcomeError, Quantities, Rejected};
use crate::processing::utils::store::item_id_to_uuid;
use crate::services::outcome::DiscardReason;
use crate::services::store::{
    ProfileAttachment, Store, StoreAttachment, StoreEvent, StoreProfileChunk, StoreTraceItem,
};
use crate::services::upload::ByteStream;
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::{
    BoundedStream, ByteCounter, MeteredStream, Rechunk, RetryableStream, TakeOnce,
    find_error_source,
};

use super::outcome::Outcome;

/// The minimum distance between two `Upload-Offset`s in bytes.
///
/// Every `Upload-Offset` must be 0 modulo `UPLOAD_GRANULARITY`.
const UPLOAD_GRANULARITY: NonZeroUsize = NonZeroUsize::new(1024 * 1024).unwrap(); // 1 MiB

/// Every part except the last needs to be at least this size.
const MULTIPART_MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5 MiB

/// Messages that the objectstore service can handle.
pub enum Objectstore {
    Event(Managed<Box<StoreEvent>>),
    TraceAttachment(Managed<StoreTraceAttachment>),
    EventAttachment(Managed<StoreAttachment>),
    RawProfile(Managed<StoreRawProfile>),
    Create(CreateMultipart, Sender<Result<UploadRef, Error>>),
    Stream(Stream, Sender<Result<UploadRef, Error>>),
}

impl Objectstore {
    fn kind(&self) -> MessageKind {
        match self {
            Self::Event(_) => MessageKind::Event,
            Self::TraceAttachment(_) => MessageKind::TraceAttachment,
            Self::EventAttachment(_) => MessageKind::EventAttachment,
            Self::RawProfile(_) => MessageKind::RawProfile,
            Self::Stream { .. } => MessageKind::Stream,
            Self::Create { .. } => MessageKind::Create,
        }
    }

    fn attachment_count(&self) -> usize {
        match self {
            Self::Event(e) => e.attachments.len(),
            Self::TraceAttachment(_) => 1,
            Self::EventAttachment(_) => 1,
            Self::RawProfile(_) => 1,
            Self::Stream { .. } => 1,
            Self::Create { .. } => 0,
        }
    }
}

impl Interface for Objectstore {}

impl FromMessage<Managed<Box<StoreEvent>>> for Objectstore {
    type Response = NoResponse;

    fn from_message(message: Managed<Box<StoreEvent>>, _sender: ()) -> Self {
        Self::Event(message)
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
    Event,
    EventAttachment,
    TraceAttachment,
    RawProfile,
    Stream,
    Create,
}

impl MessageKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Event => "envelope",
            Self::EventAttachment => "attachment",
            Self::TraceAttachment => "attachment_v2",
            Self::RawProfile => "profile_raw",
            Self::Stream => "stream",
            Self::Create => "create",
        }
    }
}

/// A request to create a new objectstore multipart upload.
pub struct CreateMultipart {
    /// The sentry org.
    pub organization_id: OrganizationId,
    /// The sentry project.
    pub project_id: ProjectId,
    /// The desired objectstore key.
    pub key: String,
}

impl FromMessage<CreateMultipart> for Objectstore {
    type Response = AsyncResponse<Result<UploadRef, Error>>;

    fn from_message(message: CreateMultipart, sender: Sender<Result<UploadRef, Error>>) -> Self {
        Self::Create(message, sender)
    }
}

/// Whether the current request contains the full upload.
#[derive(Clone)]
pub enum StreamMode {
    /// The current request contains the full upload.
    Oneshot(ByteCounter),
    /// The current request might be partial and requires multipart upload under the hood.
    Multipart {
        upload_id: String,
        offset: usize,
        length: usize,
    },
}

/// A stream that can be uploaded to objectstore.
pub struct Stream {
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub key: String,
    pub mode: StreamMode,
    pub stream: BoundedStream<MeteredStream<ByteStream>>,
}

impl FromMessage<Stream> for Objectstore {
    type Response = AsyncResponse<Result<UploadRef, Error>>;

    fn from_message(message: Stream, sender: Sender<Result<UploadRef, Error>>) -> Self {
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
    #[error(
        "invalid upload offset {offset}: must be aligned to the upload granularity {granularity}"
    )]
    InvalidOffset { offset: usize, granularity: usize },
    #[error("offset without upload ID")]
    OffsetWithoutUploadId,
    #[error("invalid Upload-Length {length}: server has {server_offset} bytes")]
    InvalidUploadLength { length: usize, server_offset: usize },
    #[error("request too small: compressed body is {size}, need at least {min_size}")]
    RequestTooSmall { size: usize, min_size: usize },
    #[error(
        "the body of a non-final request must be a multiple of the upload granularity {granularity}, found {trailing} trailing bytes"
    )]
    UnalignedBody { trailing: usize, granularity: usize },
    #[error("unknown key {0}")]
    UnknownKey(String),
    #[error("timeout: {0}")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("load shed")]
    LoadShed,
    #[error("compression failed: {0}")]
    CompressionFailed(#[source] std::io::Error),
    #[error("upload failed: {0}")]
    UploadFailed(#[from] objectstore_client::Error),
    #[error("UUID conversion failed: {0}")]
    Uuid(#[from] TryFromSliceError),
}

impl ErrorKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidScoping => "invalid_scoping",
            Self::InvalidOffset { .. } => "invalid_offset",
            Self::OffsetWithoutUploadId => "offset_without_upload_id",
            Self::InvalidUploadLength { .. } => "invalid_upload_length",
            Self::RequestTooSmall { .. } => "request_too_small",
            Self::UnalignedBody { .. } => "unaligned_body",
            Self::UnknownKey { .. } => "invalid_length",
            Self::Timeout(_) => "timeout",
            Self::LoadShed => "load_shed",
            Self::CompressionFailed(_) => "compression_failed",
            Self::UploadFailed(_) => "upload_failed",
            Self::Uuid(_) => "uuid",
        }
    }

    fn is_client_error(&self) -> bool {
        match self {
            ErrorKind::InvalidOffset { .. }
            | ErrorKind::InvalidUploadLength { .. }
            | ErrorKind::UnalignedBody { .. } => true,
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
    /// The byte offset from which to resume the upload.
    pub offset: usize,
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
            Objectstore::Event(mut event) => {
                if !self.inner.fallback_to_kafka {
                    drop_attachments(&mut event);
                }
                self.inner.store.send(event);
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
            Objectstore::Event(event) => {
                self.handle_event(event).await;
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

    async fn handle_event(&self, mut event: Managed<Box<StoreEvent>>) {
        let scoping = event.scoping();
        let session = self.session(
            &self.event_attachments,
            scoping.organization_id,
            scoping.project_id,
        );

        let session = match session {
            Err(error) => {
                error
                    .with_amount(event.attachments.len())
                    .log(MessageKind::Event);
                if !self.fallback_to_kafka {
                    drop_attachments(&mut event);
                }
                self.store.send(event);
                return;
            }
            Ok(session) => session,
        };

        let (mut event, attachments) = event.split_once(|mut e, _| {
            let attachments = e
                .attachments
                .extract_if(.., |item| should_upload(item))
                .collect::<Vec<_>>();

            (e, attachments)
        });

        for mut attachment in attachments.split(|e| e) {
            let result = self
                .upload_bytes(
                    MessageKind::Event,
                    &session,
                    attachment.payload(),
                    event.retention_days,
                    None,
                    None,
                )
                .await;

            match result {
                Ok(UploadRef { key, .. }) => {
                    attachment.modify(|a, _| a.set_stored_key(key));
                }
                Err(error) => {
                    error.log(MessageKind::Event);
                    if !self.fallback_to_kafka {
                        let reason = Outcome::Invalid(DiscardReason::UploadFailed);
                        let _ = attachment.reject_err(reason);
                        continue;
                    }
                }
            }

            event.merge_with(attachment, |e, attachment, _| {
                e.attachments.push(attachment)
            });
        }

        self.store.send(event);
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
            Ok(UploadRef { key, .. }) => {
                attachment.modify(|attachment, _| {
                    attachment.attachment.set_stored_key(key);
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

            let _upload_ref = self
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
            debug_assert_eq!(_upload_ref.key, original_key);
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
            Ok(Some(UploadRef { key, .. })) => {
                store_message.modify(|message, _| {
                    message.attachments.push(ProfileAttachment {
                        name,
                        content_type,
                        stored_id: ObjectstoreKey(key),
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
    ) -> Result<Option<UploadRef>, Error> {
        if payload.is_empty() {
            return Ok(None);
        }

        let session = self
            .profile_attachments
            .for_project(scoping.organization_id.value(), scoping.project_id.value())
            .session(&self.objectstore_client)?;

        let upload_ref = self
            .upload_bytes(
                MessageKind::RawProfile,
                &session,
                payload,
                retention,
                None,
                Some(content_type),
            )
            .await?;

        Ok(Some(upload_ref))
    }

    async fn handle_create(&self, create: CreateMultipart) -> Result<UploadRef, Error> {
        let CreateMultipart {
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
            offset: 0,
        })
    }

    async fn handle_stream(&self, stream: Stream) -> Result<UploadRef, Error> {
        let Stream {
            organization_id,
            project_id,
            key,
            mode,
            stream,
        } = stream;

        let session = self.session(&self.event_attachments, organization_id, project_id)?;

        self.upload(
            MessageKind::Stream,
            &session,
            Upload::Stream {
                body: TakeOnce::new(stream),
                key,
                mode,
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
    ) -> Result<UploadRef, Error> {
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
    ) -> Result<UploadRef, Error> {
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
    ) -> Result<UploadRef, AttemptUploadError> {
        match body {
            UploadAttempt::Bytes {
                body,
                key,
                retention_hours,
                content_type,
            } => {
                let offset = body.len();
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

                Ok(UploadRef {
                    key: response.key,
                    upload_id: None,
                    offset,
                })
            }
            UploadAttempt::Stream { body, key, mode } => {
                let original_key = key.clone();
                match mode {
                    StreamMode::Oneshot(byte_counter) => {
                        let request = session.put_stream(body.boxed()).key(key);
                        let response = request.send().await?;
                        Ok(UploadRef {
                            key: response.key,
                            upload_id: None,
                            offset: byte_counter.get(),
                        })
                    }
                    StreamMode::Multipart {
                        upload_id,
                        offset,
                        length,
                    } => {
                        let granularity = UPLOAD_GRANULARITY.get();

                        if (offset % granularity) != 0 {
                            return Err(AttemptUploadError::InvalidOffset {
                                offset,
                                granularity,
                            });
                        }

                        let mut multipart = session.resume_multipart_upload(key, upload_id)?;
                        let mut final_upload_id = Some(multipart.upload_id().clone());

                        relay_statsd::metric!(
                            timer(RelayTimers::AttachmentUploadDuration),
                            type = kind.as_str(),
                        {
                            // Ensure that the stream ends at the granularity boundary.
                            // NOTE: Once every upload is a multipart upload, we can remove `RetryableStream`
                            // because streams will never be effectively retried.
                            let mut body = Rechunk::new(body, UPLOAD_GRANULARITY);

                            // Unfortunately, MinIO has the limitation that the length of a multipart request
                            // has to be known. Therefore, we need to materialize the stream into concrete
                            // chunks of bytes and send each chunk as an individual request.
                            //
                            // Every chunk needs to be > 5 MB because that's what multipart requires.
                            let mut flushed_parts = vec![];
                            let mut previous_buffer = (offset, vec![]);
                            let mut compressor = zstd::stream::write::Encoder::new(vec![], 0).map_err(AttemptUploadError::Zstd)?;
                            let mut new_offset = offset;
                            let mut is_closing = false;
                            let mut misalignment = 0;
                            while let Some(bytes) = body.next().await {
                                let bytes = bytes.map_err(objectstore_client::Error::Io)?;
                                debug_assert!(bytes.len() <= granularity);

                                // Only accept full grains, unless the upload is done.
                                is_closing = new_offset + bytes.len() == length;
                                if is_closing || bytes.len() == granularity {
                                    compressor.write_all(&bytes).map_err(AttemptUploadError::Zstd)?;
                                    new_offset += bytes.len();

                                    // If the current compressed part is large enough, flush the previous one.
                                    // We keep the current one around just in case we need to join it with the last part.
                                    // (see concatenation after while loop).
                                    if is_closing || compressor.get_ref().len() > MULTIPART_MIN_PART_SIZE {
                                        let mut new_compressor = zstd::stream::write::Encoder::new(vec![], 0).map_err(AttemptUploadError::Zstd)?;

                                        // Replace previous buffer with current compressor content.
                                        std::mem::swap(&mut compressor, &mut new_compressor);
                                        let compressed = new_compressor.finish().map_err(AttemptUploadError::Zstd)?;
                                        let mut current_buffer = (new_offset, compressed);
                                        std::mem::swap(&mut current_buffer, &mut previous_buffer);

                                        // Flush previous buffer.
                                        self.try_flush(current_buffer, &mut multipart, &mut flushed_parts).await?;
                                    }
                                } else {
                                    // We're at the end of a non-final request, but it is not
                                    // aligned with granularity: return an error.
                                    misalignment = bytes.len();
                                }

                                if is_closing {
                                    // This should already be guaranteed by the BoundedStream.
                                    #[cfg(debug_assertions)]
                                    debug_assert!(body.next().await.is_none());
                                    break;
                                }
                            }

                            // Remaining bytes are now < 5 MiB. Concatenate it with the previous part
                            // so we reach the limit.
                            let (_, mut remaining) = previous_buffer;
                            if !compressor.get_ref().is_empty() {
                                let remaining2 = compressor.finish().map_err(AttemptUploadError::Zstd)?;
                                relay_log::trace!("Combining {} previous bytes with {} remaining", remaining.len(), remaining2.len());
                                remaining.extend(remaining2);
                            }
                            if is_closing || remaining.len() >= MULTIPART_MIN_PART_SIZE {
                                self.try_flush((new_offset, remaining), &mut multipart, &mut flushed_parts).await?;
                            } else if !remaining.is_empty() && misalignment == 0 {
                                // This can happen when a non-closing request body compresses so well that it
                                // fits under the S3 limit. In this case the client is forced
                                // to query the `Upload-Offset` again (see INGEST-1025) and retry with a larger chunk.
                                return Err(AttemptUploadError::RequestTooSmall {size: remaining.len(), min_size: MULTIPART_MIN_PART_SIZE});
                            }

                            // A non-final request did not end at the granularity boundary.
                            // All full grains were stored, but signal an error to the client.
                            if misalignment > 0 {
                                return Err(AttemptUploadError::UnalignedBody { trailing: misalignment, granularity });
                            }

                            if is_closing {
                                relay_log::trace!("Completing multipart upload");
                                let parts = multipart.list_parts().await?;

                                // Verify that the last written part number is the highest part number in the list.
                                // Otherwise clients could fake a small `Upload-Length` for a large upload.
                                let max_part_number = parts.iter().map(|p|p.part_number.get()).max().unwrap_or(0) as usize;
                                if max_part_number != new_offset.div_ceil(granularity) {
                                    return Err(AttemptUploadError::InvalidUploadLength {
                                        length, server_offset: max_part_number * granularity
                                    })
                                }

                                let parts = parts.into_iter().map(|item| {
                                    let PartInfo{ part_number, etag, .. } = item;
                                    CompletePart { part_number, etag }
                                });

                                let final_key = multipart.complete(parts).await?;
                                final_upload_id = None;
                                debug_assert_eq!(&original_key, &final_key);
                            }

                            Ok(UploadRef {
                                key: original_key,
                                upload_id: final_upload_id,
                                offset: new_offset,
                            })
                        })
                    }
                }
            }
        }
    }

    async fn try_flush(
        &self,
        buffer: (usize, Vec<u8>),
        multipart: &mut MultipartUpload,
        parts: &mut Vec<CompletePart>,
    ) -> Result<(), AttemptUploadError> {
        let (end_offset, bytes) = buffer;
        relay_log::trace!("Trying to flush {} bytes", bytes.len());
        if bytes.is_empty() {
            return Ok(());
        }
        let bytes = Bytes::from(bytes);
        let part_number = end_offset.div_ceil(UPLOAD_GRANULARITY.get());
        let part_number = u32::try_from(part_number)
            .map_err(|_| objectstore_client::Error::InvalidPartNumber(u32::MAX))?;

        // NOTE: This is a retry loop within a retry loop (see caller of this function).
        // if we keep the Rechunked approach we might as well remove the outer loop for streaming uploads.
        let mut attempts = 0;
        let part = loop {
            let result = multipart
                .put(bytes.clone(), part_number, None)
                .await
                .map_err(AttemptUploadError::Objectstore);
            attempts += 1;
            if attempts < self.max_attempts.get() && matches!(&result, Err(e) if is_retryable(e)) {
                relay_log::trace!("Multipart attempt {attempts}: Failed with {result:?}, retrying");
                tokio::time::sleep(self.retry_interval).await;
            } else {
                relay_log::trace!("Final attempt");
                break result;
            }
        }?;

        parts.push(part);

        Ok(())
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

#[derive(Debug, thiserror::Error)]
enum AttemptUploadError {
    #[error(transparent)]
    Objectstore(#[from] objectstore_client::Error),
    #[error("zstd error: {0}")]
    Zstd(#[source] std::io::Error),
    #[error("invalid Upload-Offset {offset}: must be a multiple of {granularity}")]
    InvalidOffset { offset: usize, granularity: usize },
    #[error("invalid Upload-Length {length}: server has {server_offset} bytes")]
    InvalidUploadLength { length: usize, server_offset: usize },
    #[error("Request too small: compressed body is {size}, need at least {min_size}")]
    RequestTooSmall { size: usize, min_size: usize },
    #[error(
        "the body of a non-final request must be a multiple of the upload granularity {granularity}, found {trailing} trailing bytes"
    )]
    UnalignedBody { trailing: usize, granularity: usize },
}

impl From<AttemptUploadError> for ErrorKind {
    fn from(value: AttemptUploadError) -> Self {
        match value {
            AttemptUploadError::Objectstore(error) => ErrorKind::UploadFailed(error),
            AttemptUploadError::Zstd(error) => ErrorKind::CompressionFailed(error),
            AttemptUploadError::InvalidOffset {
                offset,
                granularity,
            } => ErrorKind::InvalidOffset {
                offset,
                granularity,
            },
            AttemptUploadError::InvalidUploadLength {
                length,
                server_offset,
            } => ErrorKind::InvalidUploadLength {
                length,
                server_offset,
            },
            AttemptUploadError::RequestTooSmall { size, min_size } => {
                ErrorKind::RequestTooSmall { size, min_size }
            }
            AttemptUploadError::UnalignedBody {
                trailing,
                granularity,
            } => ErrorKind::UnalignedBody {
                trailing,
                granularity,
            },
        }
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
        key: String,
        mode: StreamMode,
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
            Self::Stream { body, key, mode } => {
                RetryableStream::new(body.clone()).map(|body| UploadAttempt::Stream {
                    body,
                    key: key.clone(),
                    mode: mode.clone(),
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
        key: String,
        mode: StreamMode,
    },
}

fn is_retryable(error: &AttemptUploadError) -> bool {
    match error {
        AttemptUploadError::Objectstore(objectstore_client::Error::Reqwest(error)) => {
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

fn drop_attachments(event: &mut Managed<Box<StoreEvent>>) {
    event.modify(|event, records| {
        let reason = Outcome::Invalid(DiscardReason::UploadFailed);
        let attachments = event
            .attachments
            .extract_if(.., |item| should_upload(item))
            .collect::<Vec<_>>();
        records.reject_err(reason, attachments);
    });
}

#[cfg(test)]
mod tests {

    use relay_event_schema::protocol::EventId;
    use relay_quotas::DataCategory;
    use relay_system::Service;

    use crate::managed::ManagedTestHandle;

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

        let byte_counter = stream.byte_counter();
        let result = addr
            .send(Stream {
                organization_id: OrganizationId::new(0),
                project_id: ProjectId::new(1),
                stream,
                key: "my_key".to_owned(),
                mode: StreamMode::Oneshot(byte_counter),
            })
            .await
            .unwrap();
        let err = result.unwrap_err();

        assert!(matches!(err.kind, ErrorKind::InvalidScoping));
    }

    #[tokio::test]
    async fn event_falls_back_to_kafka_by_default() {
        let (store, mut store_rx) = Addr::custom();
        let service = ObjectstoreService::new(&test_config(), Some(store))
            .unwrap()
            .unwrap();

        let (event, _handle) = test_event();
        service.inner.handle_event(event).await;

        let Store::Event(event) = store_rx.try_recv().unwrap() else {
            panic!("expected event");
        };
        {
            assert_eq!(event.attachments.len(), 1);
            assert_eq!(event.attachments[0].ty(), &ItemType::Attachment);
            assert_eq!(event.attachments[0].stored_key(), None);
        }
        event.accept(|_| ());
    }

    #[tokio::test]
    async fn event_rejects_failed_attachments_without_fallback() {
        let (store, mut store_rx) = Addr::custom();
        let mut config = test_config();
        config.fallback_to_kafka = false;
        let service = ObjectstoreService::new(&config, Some(store))
            .unwrap()
            .unwrap();

        let (event, mut handle) = test_event();
        service.inner.handle_event(event).await;

        let Store::Event(event) = store_rx.try_recv().unwrap() else {
            panic!("expected event");
        };
        assert!(event.attachments.is_empty());
        event.accept(|_| ());

        handle.assert_outcome(
            &Outcome::Invalid(DiscardReason::UploadFailed),
            DataCategory::Attachment,
            5,
        );
        handle.assert_outcome(
            &Outcome::Invalid(DiscardReason::UploadFailed),
            DataCategory::AttachmentItem,
            1,
        );
    }

    #[tokio::test]
    async fn event_attachment_rejects_without_fallback() {
        let (store, mut store_rx) = Addr::custom();
        let mut config = test_config();
        config.fallback_to_kafka = false;
        let service = ObjectstoreService::new(&config, Some(store))
            .unwrap()
            .unwrap();

        let mut item = Item::new(ItemType::Attachment);
        item.set_payload(ContentType::Text, "hello");
        let quantities = item.quantities();
        let (attachment, mut handle) = Managed::for_test(StoreAttachment {
            event_id: EventId::new(),
            attachment: item,
            quantities,
            retention: 90,
        })
        .build();

        service.inner.handle_event_attachment(attachment).await;

        assert!(store_rx.try_recv().is_err());

        handle.assert_outcome(
            &Outcome::Invalid(DiscardReason::UploadFailed),
            DataCategory::Attachment,
            5,
        );
        handle.assert_outcome(
            &Outcome::Invalid(DiscardReason::UploadFailed),
            DataCategory::AttachmentItem,
            1,
        );
    }

    fn test_event() -> (Managed<Box<StoreEvent>>, ManagedTestHandle) {
        let mut attachment = Item::new(ItemType::Attachment);
        attachment.set_payload(ContentType::Text, "hello");

        Managed::for_test(Box::new(StoreEvent {
            event_category: DataCategory::Error,
            event: Default::default(),
            attachments: vec![attachment],
            user_reports: Vec::new(),
            retention_days: 90,
        }))
        .build()
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
