//! Objectstore service for uploading attachments.
use std::array::TryFromSliceError;
use std::fmt;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use http::StatusCode;
use objectstore_client::{
    Client, ExpirationPolicy, SecretKey as SigningKey, Session, TokenGenerator, Usecase,
};
use relay_base_schema::organization::OrganizationId;
use relay_base_schema::project::ProjectId;
use relay_config::ObjectstoreServiceConfig;
use relay_system::{
    Addr, AsyncResponse, FromMessage, Interface, LoadShed, NoResponse, Sender, SimpleService,
};
use sentry_protos::snuba::v1::TraceItem;

use crate::constants::DEFAULT_ATTACHMENT_RETENTION;
use crate::envelope::{Item, ItemType};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::processing::utils::store::item_id_to_uuid;
use crate::services::outcome::DiscardReason;
use crate::services::store::{Store, StoreAttachment, StoreEnvelope, StoreTraceItem};
use crate::services::upload::ByteStream;
use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::{BoundedStream, MeteredStream, RetryableStream, TakeOnce};

use super::outcome::Outcome;

/// Messages that the objectstore service can handle.
pub enum Objectstore {
    Envelope(StoreEnvelope),
    TraceAttachment(Managed<StoreTraceAttachment>),
    EventAttachment(Managed<StoreAttachment>),
    Stream(Stream, Sender<Result<ObjectstoreKey, Error>>),
}

impl Objectstore {
    fn kind(&self) -> MessageKind {
        match self {
            Self::Envelope(_) => MessageKind::Envelope,
            Self::TraceAttachment(_) => MessageKind::TraceAttachment,
            Self::EventAttachment(_) => MessageKind::EventAttachment,
            Self::Stream { .. } => MessageKind::Stream,
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
            Self::Stream { .. } => 1,
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

/// A type tag used for logging.
#[derive(Debug, Clone, Copy)]
enum MessageKind {
    Envelope,
    EventAttachment,
    TraceAttachment,
    Stream,
}

impl MessageKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Envelope => "envelope",
            Self::EventAttachment => "attachment",
            Self::TraceAttachment => "attachment_v2",
            Self::Stream => "stream",
        }
    }
}

/// A stream that can be uploaded to objectstore.
pub struct Stream {
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub key: String,
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
            Self::Timeout(_) => "timeout",
            Self::LoadShed => "load_shed",
            Self::UploadFailed(_) => "upload_failed",
            Self::Uuid(_) => "uuid",
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

        let inner = ObjectstoreServiceInner {
            store,
            objectstore_client,
            event_attachments,
            trace_attachments,
            timeout: Duration::from_secs(*timeout),
            stream_timeout: Duration::from_secs(*stream_timeout),
            retry_interval: Duration::from_secs_f64(*retry_delay),
            max_attempts: *max_attempts,
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
                // Event attachments can still go the old route.
                self.inner.store.send(envelope);
            }
            Objectstore::EventAttachment(message) => {
                // Event attachments can still go the old route.
                self.inner.store.send(message);
            }
            Objectstore::TraceAttachment(managed) => {
                let _ = managed.reject_err(error);
            }
            Objectstore::Stream(_, sender) => {
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
    timeout: Duration,
    stream_timeout: Duration,
    retry_interval: Duration,
    max_attempts: NonZeroU16,
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
        let session = self
            .event_attachments
            .for_project(scoping.organization_id.value(), scoping.project_id.value())
            .session(&self.objectstore_client);
        let retention = envelope.envelope().retention();

        let attachments = envelope
            .envelope_mut()
            .items_mut()
            .filter(|item| *item.ty() == ItemType::Attachment);

        match session {
            Err(error) => Error::from(error)
                .with_amount(attachments.count())
                .log(MessageKind::Envelope),
            Ok(session) => {
                for attachment in attachments {
                    if Self::should_skip_upload(attachment) {
                        continue;
                    }
                    let result = self
                        .upload_bytes(
                            MessageKind::Envelope,
                            &session,
                            attachment.payload(),
                            retention,
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
        if Self::should_skip_upload(&attachment.attachment) {
            self.store.send(attachment);
            return;
        }

        let scoping = attachment.scoping();
        let session = self
            .event_attachments
            .for_project(scoping.organization_id.value(), scoping.project_id.value())
            .session(&self.objectstore_client);

        match session {
            Err(error) => Error::from(error).log(MessageKind::EventAttachment),
            Ok(session) => {
                let result = self
                    .upload_bytes(
                        MessageKind::EventAttachment,
                        &session,
                        attachment.attachment.payload(),
                        attachment.retention,
                        None,
                    )
                    .await;

                match result {
                    Ok(stored_key) => {
                        attachment.modify(|attachment, _| {
                            attachment
                                .attachment
                                .set_stored_key(stored_key.into_inner());
                        });
                    }
                    Err(e) => e.log(MessageKind::EventAttachment),
                }
            }
        }

        self.store.send(attachment)
    }

    async fn handle_trace_attachment(
        &self,
        managed: Managed<StoreTraceAttachment>,
    ) -> Result<(), Rejected<Error>> {
        let scoping = managed.scoping();
        let session = self
            .trace_attachments
            .for_project(scoping.organization_id.value(), scoping.project_id.value())
            .session(&self.objectstore_client)
            .map_err(|e| Error::from(ErrorKind::UploadFailed(e)))
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

    async fn handle_stream(&self, stream: Stream) -> Result<ObjectstoreKey, Error> {
        let Stream {
            organization_id,
            project_id,
            key,
            stream,
        } = stream;
        let session = self
            .event_attachments
            .for_project(organization_id.value(), project_id.value())
            .session(&self.objectstore_client)?;

        self.upload(
            MessageKind::Stream,
            &session,
            Some(key),
            Body::Stream(TakeOnce::new(stream)),
            None,
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
    ) -> Result<ObjectstoreKey, Error> {
        let retention_hours = retention.checked_mul(24);
        self.upload(kind, session, key, Body::Bytes(payload), retention_hours)
            .await
    }

    async fn upload(
        &self,
        kind: MessageKind,
        session: &Session,
        key: Option<String>,
        body: Body,
        retention_hours: Option<u16>,
    ) -> Result<ObjectstoreKey, Error> {
        let mut attempts = 0;
        let timeout = match &body {
            Body::Bytes(_) => self.timeout,
            Body::Stream(_) => self.stream_timeout,
        };
        let result = tokio::time::timeout(timeout, async {
            let mut result = None;
            loop {
                let Some(body) = body.try_clone() else {
                    break;
                };
                attempts += 1;
                result.replace(
                    self.attempt_upload(kind, session, key.clone(), body, retention_hours)
                        .await,
                );

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
        key: Option<String>,
        body: BodyAttempt,
        retention_hours: Option<u16>,
    ) -> Result<ObjectstoreKey, objectstore_client::Error> {
        let mut request = match body {
            BodyAttempt::Bytes(bytes) => session.put(bytes),
            BodyAttempt::Stream(stream) => session.put_stream(stream.boxed()),
        };

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
                request.send().await
            }
        )?;

        Ok(ObjectstoreKey(response.key))
    }

    /// Returns `true` if the item should **not** be uploaded to the objectstore.
    ///
    /// This is the case for:
    /// - Zero-size attachments
    /// - Attachment placeholders
    fn should_skip_upload(item: &Item) -> bool {
        item.is_empty() || item.is_attachment_ref()
    }
}

/// Common interface for calls to [`ObjectstoreServiceInner::upload`].
///
/// This type is shared across retries.
enum Body {
    Bytes(Bytes),
    Stream(TakeOnce<BoundedStream<MeteredStream<ByteStream>>>),
}

impl Body {
    fn try_clone(&self) -> Option<BodyAttempt> {
        match self {
            Self::Bytes(bytes) => Some(BodyAttempt::Bytes(bytes.clone())),
            Self::Stream(stream) => RetryableStream::new(stream.clone()).map(BodyAttempt::Stream),
        }
    }
}

/// Common interface for calls to [`ObjectstoreServiceInner::attempt_upload`].
///
/// This type is instantiated for every retry.
enum BodyAttempt {
    Bytes(Bytes),
    Stream(RetryableStream<BoundedStream<MeteredStream<ByteStream>>>),
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
                    )
                )
        }
        _ => false,
    }
}
