//! Utilities for uploading large files.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_compression::tokio::bufread::{BrotliEncoder, DeflateEncoder, GzipEncoder, ZstdEncoder};
use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use futures::StreamExt;
use futures::future::Either;
use futures::stream::BoxStream;
use http::{HeaderValue, Method};
use relay_auth::Signature;
use relay_auth::SignatureError;
#[cfg(feature = "processing")]
use relay_auth::SignatureHeader;
use relay_base_schema::project::ProjectId;
#[cfg(feature = "processing")]
use relay_config::ConfigSnapshot;
use relay_config::{Config, HttpCompression, HttpEncoding, UpstreamDescriptor};
use relay_quotas::Scoping;
use relay_system::{
    Addr, AsyncResponse, ConcurrentService, FromMessage, Interface, LoadShed, SendError, Sender,
    SimpleService,
};
use serde::{Deserialize, Serialize};
use tokio::io::BufReader;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tokio_util::io::{ReaderStream, StreamReader};
#[cfg(feature = "processing")]
use uuid::Uuid;

use crate::envelope::AttachmentType;
use crate::http::{HttpError, RequestBuilder, Response};

#[cfg(feature = "processing")]
use crate::services::objectstore::{self, Objectstore};
use crate::services::upstream::{
    SendRequest, UpstreamRelay, UpstreamRequest, UpstreamRequestError,
};
use crate::statsd::RelayCounters;
use crate::utils::MeteredStream;
use crate::utils::{BoundedStream, RetryableStream, TakeOnce, tus};

/// The URL template for uploading bytes to a known location.
pub const UPLOAD_PATCH_PATH: &str = "/api/{project_id}/upload/{key}/";

/// Header advertising the chunk granularity.
///
/// Sentry-specific extension to the TUS upload protocol.
pub const UPLOAD_CHUNK_GRANULARITY: &str = "Upload-Chunk-Granularity";

/// An error that occurs during upload.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("send failed: {0}")]
    Send(#[from] RecvError),
    #[error("request failed: {0}")]
    UpstreamRequest(#[from] UpstreamRequestError),
    #[error("request timeout: {0}")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("error response from upstream: {0}")]
    Upstream(#[source] reqwest::Error),
    #[error("upstream provided invalid location: {0:?}")]
    InvalidLocation(Option<HeaderValue>),
    #[error("upstream provided invalid data for {0}: {1:?}")]
    InvalidFromUpstream(&'static str, Option<HeaderValue>),
    #[cfg(feature = "processing")]
    #[error(transparent)]
    InvalidSessionToken(#[from] objectstore_types::resumable::InvalidSessionToken),
    #[error("serializing location failed: {0}")]
    SerializeFailed(#[from] serde_urlencoded::ser::Error),
    #[error("failed to sign location")]
    SigningFailed,
    #[error("invalid signature: {0}")]
    InvalidSignature(#[from] SignatureError),
    #[error("invalid input: {0}")]
    InvalidInput(&'static str),
    #[error("objectstore service unavailable: {0}")]
    ObjectstoreServiceUnavailable(#[source] SendError),
    #[cfg(feature = "processing")]
    #[error(transparent)]
    Objectstore(#[from] objectstore::Error),
    #[error("loadshed")]
    LoadShed,
    #[error("resumable upload declined")]
    ResumableUploadDeclined,
    #[error("internal error")]
    Internal(#[source] http::header::InvalidHeaderValue),
}

impl Error {
    fn variant(&self) -> &'static str {
        match self {
            Error::Send(_) => "send_failed",
            Error::UpstreamRequest(_) => "upstream_request",
            Error::Timeout(_) => "timeout",
            Error::Upstream(_) => "upstream_response",
            Error::InvalidLocation(_) => "invalid_location",
            Error::InvalidFromUpstream { .. } => "invalid_from_upstream",
            #[cfg(feature = "processing")]
            Error::InvalidSessionToken(_) => "invalid_session_token",
            Error::SigningFailed => "signing_failed",
            Error::SerializeFailed(_) => "serialize_failed",
            Error::InvalidSignature(_) => "invalid_signature",
            Error::InvalidInput { .. } => "invalid_input",
            Error::ObjectstoreServiceUnavailable(_) => "service_unavailable",
            #[cfg(feature = "processing")]
            Error::Objectstore(_) => "objectstore_error",
            Error::LoadShed => "load_shed",
            Error::ResumableUploadDeclined => "resumable_upload_declined",
            Error::Internal(_) => "internal",
        }
    }
}

#[allow(clippy::enum_variant_names)]
/// The message interface for this service.
pub enum Upload {
    /// Creates an upload resource.
    ///
    /// Returns the trusted identifier of the upload and metadata.
    Create(Create, InstrumentedSender<CreateResult>),
    /// Upload a stream of bytes for a given location.
    ///
    /// The service also returns the signed location. This is redundant, but creates a simpler
    /// flow for the caller side.
    Upload(Box<Stream>, InstrumentedSender<StreamResult>),
    /// Queries the current progress of an upload at a given location.
    Head(Head, InstrumentedSender<HeadResult>),
}

impl Interface for Upload {}

/// Project information necessary for uploading.
#[derive(Debug, Clone)]
pub struct ProjectContext {
    /// The organization and project identifiers.
    pub scoping: Scoping,
    /// Where to send the request.
    pub upstream: Option<UpstreamDescriptor>,
    /// The retention to use for the uploaded object (in days).
    pub retention: u16,
    /// Whether the project has resumable uploads enabled.
    pub resumable: bool,
}

/// A request to get the upload state from objecstore.
pub struct Head {
    /// Time of arrival of the request.
    pub received: DateTime<Utc>,
    /// The project of the upload.
    pub project: ProjectContext,
    /// The location to upload to.
    pub location: SignedLocation<Provisional>,
}

/// Response to a [`Head`] message.
pub struct HeadResult {
    /// The number of bytes objectstore has persisted for this upload.
    pub offset: usize,
    /// The total length of the upload (if known).
    pub length: Option<usize>,
}

impl HeadResult {
    fn try_from_response(response: Response) -> Result<Self, Error> {
        let offset = response
            .headers()
            .get(tus::UPLOAD_OFFSET)
            .ok_or(Error::InvalidFromUpstream(tus::UPLOAD_OFFSET, None))?
            .to_str()
            .map_err(|_| Error::InvalidFromUpstream(tus::UPLOAD_OFFSET, None))?
            .parse()
            .map_err(|_| Error::InvalidFromUpstream(tus::UPLOAD_OFFSET, None))?;

        let length: Option<usize> = response
            .headers()
            .get(tus::UPLOAD_LENGTH)
            .and_then(|v| v.to_str().ok()?.parse().ok());

        Ok(HeadResult { offset, length })
    }
}
/// Response to a [`Create`] message.
pub struct CreateResult {
    /// The signed location of the created upload.
    pub location: SignedLocation<Provisional>,
    /// Chunk granularity required by objectstore, if any.
    pub granularity: Option<usize>,
}

impl CreateResult {
    fn try_from_response(response: &Response) -> Result<Self, Error> {
        let granularity = response
            .headers()
            .get(UPLOAD_CHUNK_GRANULARITY)
            .and_then(|v| v.to_str().ok()?.parse().ok());

        Ok(Self {
            location: SignedLocation::try_from_response(response)?,
            granularity,
        })
    }
}

/// Request to create an upload resource.
pub struct Create {
    /// The project to create the upload for.
    pub project: ProjectContext,
    /// The size of the intended upload in bytes, as specified in the `Upload-Length` header.
    ///
    /// `None` indicates that the length is not yet known (see `Upload-Defer-Length: 1`).
    pub length: Option<usize>,
    /// The attachment type of the upload.
    pub attachment_type: Option<AttachmentType>,
}

/// The type used to stream a request body.
pub type ByteStream = BoxStream<'static, std::io::Result<Bytes>>;

/// An upload stream with bounds in the same units as its bytes.
pub type UploadStream = BoundedStream<MeteredStream<ByteStream>>;

// Keep the passthrough stream inline; only the existing encoder path needs a boxed stream.
type PreparedStream = Either<ByteStream, UploadStream>;

/// A stream of bytes to be uploaded to objectstore or the upstream.
pub struct Stream {
    /// Time of arrival of the request.
    pub received: DateTime<Utc>,
    /// The project of the upload.
    pub project: ProjectContext,
    /// The location to upload to.
    pub location: SignedLocation<Provisional>,
    /// The upload body, with bounds in the same units as its bytes.
    pub stream: UploadStream,
    /// The body's compression codec, or `None` for decoded bytes.
    pub compression: Option<HttpCompression>,
    /// The upload mode to use, either Oneshot or Resumable.
    pub mode: UploadMode,
}

/// Indicates whether a stream will be uploaded via oneshot or resumable upload.
///
/// Note that the mode of a stream needs to match its location.
/// See also [`Provisional`].
pub enum UploadMode {
    Oneshot {
        length: Option<usize>,
    },
    Resumable {
        /// The offset from which to resume the upload.
        offset: usize,
        /// The length of the current chunk of the resumable upload.
        chunk_length: usize,
    },
}

impl UploadMode {
    /// Returns the stream offset.
    pub fn offset(&self) -> usize {
        match self {
            // Oneshot uploads start from offset 0.
            UploadMode::Oneshot { .. } => 0,
            UploadMode::Resumable { offset, .. } => *offset,
        }
    }

    /// Returns the stream chunk length if the stream is resumable.
    pub fn chunk_length(&self) -> Option<usize> {
        match self {
            UploadMode::Oneshot { .. } => None,
            UploadMode::Resumable { chunk_length, .. } => Some(*chunk_length),
        }
    }
}

/// The result of a [`Stream`] operation.
pub enum StreamResult {
    Incomplete {
        location: SignedLocation<Provisional>,
        offset: usize,
    },
    Complete {
        location: SignedLocation<Final>,
        offset: usize,
    },
}

impl StreamResult {
    /// Returns the byte offset stored on the server after the operation.
    pub fn offset(&self) -> usize {
        match self {
            Self::Complete { offset, .. } | Self::Incomplete { offset, .. } => *offset,
        }
    }

    /// Converts the location into a value for the `Location` response header.
    pub fn location_into_header_value(self) -> Result<HeaderValue, Error> {
        match self {
            StreamResult::Incomplete { location, .. } => location.into_header_value(),
            StreamResult::Complete { location, .. } => location.into_header_value(),
        }
    }
}

impl StreamResult {
    fn try_from_response(response: Response) -> Result<Self, Error> {
        let offset = response
            .headers()
            .get(tus::UPLOAD_OFFSET)
            .ok_or(Error::InvalidFromUpstream(tus::UPLOAD_OFFSET, None))?
            .to_str()
            .map_err(|_| Error::InvalidFromUpstream(tus::UPLOAD_OFFSET, None))?
            .parse()
            .map_err(|_| Error::InvalidFromUpstream(tus::UPLOAD_OFFSET, None))?;

        // Need to parse the final first since it could collide with the Provisional::Oneshot, now
        // in reality this should never happen because you should not get a Provisional::Oneshot back
        // after an upload.
        if let Ok(location) = SignedLocation::<Final>::try_from_response(&response) {
            return Ok(Self::Complete { location, offset });
        }

        let location = SignedLocation::<Provisional>::try_from_response(&response)?;
        Ok(Self::Incomplete { location, offset })
    }
}

impl FromMessage<Create> for Upload {
    type Response = AsyncResponse<Result<CreateResult, Error>>;

    fn from_message(message: Create, sender: Sender<Result<CreateResult, Error>>) -> Self {
        Self::Create(
            message,
            InstrumentedSender {
                metric: RelayCounters::UploadCreate,
                inner: sender,
            },
        )
    }
}

impl FromMessage<Stream> for Upload {
    type Response = AsyncResponse<Result<StreamResult, Error>>;

    fn from_message(message: Stream, sender: Sender<Result<StreamResult, Error>>) -> Self {
        Self::Upload(
            Box::new(message),
            InstrumentedSender {
                metric: RelayCounters::UploadUpload,
                inner: sender,
            },
        )
    }
}

impl FromMessage<Head> for Upload {
    type Response = AsyncResponse<Result<HeadResult, Error>>;

    fn from_message(message: Head, sender: Sender<Result<HeadResult, Error>>) -> Self {
        Self::Head(
            message,
            InstrumentedSender {
                metric: RelayCounters::UploadHead,
                inner: sender,
            },
        )
    }
}

/// Creates a new upload service.
pub fn create_service(
    config: &Arc<Config>,
    upstream: &Addr<UpstreamRelay>,
    #[cfg(feature = "processing")] objectstore: &Option<Addr<Objectstore>>,
) -> ConcurrentService<Service> {
    let current_config = config.current();
    let backend = create_backend(
        config,
        upstream,
        #[cfg(feature = "processing")]
        objectstore,
    );
    let service = Service {
        timeout: Duration::from_secs(current_config.upload().timeout),
        backend,
    };
    ConcurrentService::new(service)
        .with_backlog_limit(0)
        .with_concurrency_limit(current_config.upload().max_concurrent_requests)
}

fn create_backend(
    #[allow(unused)] config: &Arc<Config>,
    upstream: &Addr<UpstreamRelay>,
    #[cfg(feature = "processing")] objectstore: &Option<Addr<Objectstore>>,
) -> Backend {
    #[cfg(feature = "processing")]
    if let Some(addr) = objectstore.as_ref() {
        return Backend::Objectstore {
            addr: addr.clone(),
            config: config.clone(),
        };
    }
    Backend::Upstream {
        addr: upstream.clone(),
        config: config.clone(),
    }
}

/// A dispatcher for uploading large files.
///
/// Uploads go to either the upstream relay or objectstore.
#[derive(Debug, Clone)]
pub struct Service {
    timeout: Duration,
    backend: Backend,
}

/// A response channel that emits a metric for each response.
pub struct InstrumentedSender<T> {
    metric: RelayCounters,
    inner: Sender<Result<T, Error>>,
}

impl<T> InstrumentedSender<T> {
    fn send(self, result: Result<T, Error>) {
        let result_msg = match &result {
            Ok(_) => "success",
            Err(e) => e.variant(),
        };
        relay_statsd::metric!(counter(self.metric) += 1, result = result_msg);
        self.inner.send(result)
    }
}

#[derive(Debug, Clone)]
enum Backend {
    Upstream {
        addr: Addr<UpstreamRelay>,
        config: Arc<Config>,
    },
    #[cfg(feature = "processing")]
    Objectstore {
        addr: Addr<Objectstore>,
        config: Arc<Config>,
    },
}

impl Service {
    async fn create(
        &self,
        Create {
            project,
            length,
            attachment_type,
        }: Create,
    ) -> Result<CreateResult, Error> {
        match &self.backend {
            Backend::Upstream { addr, .. } => {
                let (request, rx) = UploadRequest::create(project, length, attachment_type);
                addr.send(SendRequest(request));
                let response = rx.await??;
                CreateResult::try_from_response(&response)
            }
            #[cfg(feature = "processing")]
            Backend::Objectstore { addr, config } => {
                use crate::services::objectstore::UploadRef;
                let config = config.current();

                // Create the key:
                let key = Uuid::now_v7().as_simple().to_string();
                #[cfg(debug_assertions)]
                let original_key = key.clone();

                let Scoping {
                    organization_id,
                    project_id,
                    ..
                } = project.scoping;

                let (key, kind, granularity) = match (project.resumable, length) {
                    (true, Some(length)) => {
                        let objectstore::CreateResult {
                            upload_ref:
                                UploadRef {
                                    key,
                                    session_token,
                                    offset: _,
                                },
                            granularity,
                        } = addr
                            .send(objectstore::Create {
                                organization_id,
                                project_id,
                                key,
                                upload_length: length,
                                retention: project.retention,
                            })
                            .await
                            .map_err(Error::ObjectstoreServiceUnavailable)??;
                        #[cfg(debug_assertions)]
                        debug_assert_eq!(&key, &original_key);

                        let kind = match session_token {
                            Some(token) => Provisional::Resumable {
                                length,
                                upload_id: token.to_base64url(),
                            },
                            None => return Err(Error::ResumableUploadDeclined),
                        };
                        (key, kind, granularity)
                    }
                    // Support legacy clients which would send the length when creating oneshot uploads.
                    // Note: Need to migrate them before GA resumable uploads.
                    (false, Some(length)) => (
                        key,
                        Provisional::Oneshot {
                            length: Some(length),
                        },
                        None,
                    ),
                    // If the create has `Upload-Defer-Length: 1` then skip going to objectstore.
                    // This is because objectstore requires us to know the size of a resumable upload
                    // when creating it (which we don't).
                    _ => (key, Provisional::Oneshot { length: None }, None),
                };

                Ok(CreateResult {
                    location: Location {
                        project_id,
                        key,
                        kind,
                        other: Default::default(),
                    }
                    .try_sign(&config)?,
                    granularity,
                })
            }
        }
    }

    async fn upload(&self, stream: Stream) -> Result<StreamResult, Error> {
        let Stream {
            #[cfg_attr(not(feature = "processing"), expect(unused))]
            received,
            project,
            location,
            stream,
            compression,
            mode,
        } = stream;
        match &self.backend {
            Backend::Upstream { addr, config } => {
                let uri = location.try_to_uri()?;
                let (stream, encoding) = match compression {
                    Some(codec) => (Either::Right(stream), codec.encoding()),
                    None => {
                        let encoding = config.current().http_encoding();
                        (encode_body(stream, encoding), encoding)
                    }
                };
                let (request, rx) = UploadRequest::upload(project, uri, mode, stream, encoding);
                addr.send(SendRequest(request));
                let response = rx.await??;
                StreamResult::try_from_response(response)
            }
            #[cfg(feature = "processing")]
            Backend::Objectstore { addr, config } => {
                use crate::services::objectstore::{StreamContext, UploadRef};
                use objectstore_client::SessionToken;

                match compression {
                    None => {}
                    Some(_) => {
                        return Err(Error::InvalidInput("objectstore requires a decoded upload"));
                    }
                };
                let config = config.current();

                let Location {
                    project_id,
                    key,
                    kind,
                    other,
                } = location.verify(received, &config)?;

                let scoping = project.scoping;
                debug_assert_eq!(scoping.project_id, project_id);

                let context = match &kind {
                    Provisional::Oneshot { length } => StreamContext::Oneshot {
                        byte_counter: stream.byte_counter(),
                        key: length.is_some().then_some(key),
                    },
                    Provisional::Resumable { length, upload_id } => {
                        let UploadMode::Resumable {
                            offset,
                            chunk_length,
                        } = mode
                        else {
                            return Err(Error::InvalidInput("missing chunk length"));
                        };

                        StreamContext::Resumable {
                            key,
                            session_token: SessionToken::from_base64url(upload_id)?,
                            offset,
                            chunk_length,
                            total_length: *length,
                        }
                    }
                };

                let UploadRef {
                    key,
                    session_token,
                    offset,
                } = addr
                    .send(objectstore::Stream {
                        organization_id: scoping.organization_id,
                        project_id,
                        context,
                        retention: project.retention,
                        stream,
                    })
                    .await
                    .map_err(Error::ObjectstoreServiceUnavailable)??;

                // If the location contains a length, communicate that back as is. If it doesn't
                // (because it is a oneshot upload) derive the length based on the offset (progress).
                let length = kind.upload_length().unwrap_or(offset);

                match session_token {
                    Some(token) => {
                        let kind = Provisional::Resumable {
                            length,
                            upload_id: token.to_base64url(),
                        };
                        let location = Location {
                            project_id,
                            key,
                            kind,
                            other,
                        }
                        .try_sign(&config)?;
                        Ok(StreamResult::Incomplete { location, offset })
                    }
                    None => {
                        let kind = Final { length };
                        let location = Location {
                            project_id,
                            key,
                            kind,
                            other,
                        }
                        .try_sign(&config)?;
                        Ok(StreamResult::Complete { location, offset })
                    }
                }
            }
        }
    }

    async fn head(&self, head: Head) -> Result<HeadResult, Error> {
        let Head {
            #[cfg_attr(not(feature = "processing"), expect(unused))]
            received,
            project,
            location,
        } = head;
        match &self.backend {
            Backend::Upstream { addr, .. } => {
                let (request, rx) = UploadRequest::head(project, location.try_to_uri()?);
                addr.send(SendRequest(request));
                let response = rx.await??;
                HeadResult::try_from_response(response)
            }
            #[cfg(feature = "processing")]
            Backend::Objectstore { addr, config } => {
                use crate::services::objectstore::HeadKind;
                use objectstore_client::SessionToken;

                let config = config.current();

                // Note: Side effect of this is that only locations in the last hour can be used
                // in the head request.
                let Location {
                    project_id,
                    key,
                    kind,
                    other: _,
                } = location.verify(received, &config)?;

                let kind = match kind {
                    Provisional::Oneshot { length } => HeadKind::Oneshot { length },
                    Provisional::Resumable { length, upload_id } => HeadKind::Resumable {
                        session_token: SessionToken::from_base64url(&upload_id)?,
                        length,
                    },
                };

                let objectstore::HeadResult { offset, length } = addr
                    .send(objectstore::Head {
                        organization_id: project.scoping.organization_id,
                        project_id,
                        key,
                        kind,
                    })
                    .await
                    .map_err(Error::ObjectstoreServiceUnavailable)??;
                Ok(HeadResult { offset, length })
            }
        }
    }

    async fn timeout<T, F>(&self, future: F) -> Result<T, Error>
    where
        F: IntoFuture<Output = Result<T, Error>>,
    {
        tokio::time::timeout(self.timeout, future).await?
    }
}

impl SimpleService for Service {
    type Interface = Upload;

    async fn handle_message(&self, message: Upload) {
        match message {
            Upload::Create(create, sender) => {
                sender.send(self.timeout(self.create(create)).await);
            }
            Upload::Upload(stream, sender) => {
                sender.send(self.timeout(self.upload(*stream)).await);
            }
            Upload::Head(head, sender) => {
                sender.send(self.timeout(self.head(head)).await);
            }
        }
    }
}

impl LoadShed<Upload> for Service {
    fn handle_loadshed(&self, message: Upload) {
        match message {
            Upload::Create(_, tx) => tx.send(Err(Error::LoadShed)),
            Upload::Upload(_, tx) => tx.send(Err(Error::LoadShed)),
            Upload::Head(_, tx) => tx.send(Err(Error::LoadShed)),
        }
    }
}

/// An interface for different location kinds.
///
/// See also [`Provisional`] and [`Final`].
pub trait LocationKind: Sized {
    /// Creates the kind from the `upload_length` and `upload_id` query parameters.
    ///
    /// Fails if the combination of parameters is not valid for this kind.
    fn from_params(upload_length: Option<usize>, upload_id: Option<String>) -> Result<Self, Error>;

    /// Returns the value of the `upload_length` query parameter, if any.
    fn upload_length(&self) -> Option<usize>;

    /// Returns the value of the `upload_id` query parameter, if any.
    fn upload_id(&self) -> Option<&str>;
}

/// A provisional location which may still be used for uploading.
///
/// See also [`Final`].
#[derive(Debug, Clone)]
pub enum Provisional {
    /// A location that is uploaded to with a single PATCH request.
    ///
    /// Size doesn't need to be defined at creation, but legacy clients do it.
    Oneshot { length: Option<usize> },
    /// A location with a resumable upload session in objectstore.
    ///
    /// The session is bound to the key in the location and the total length is fixed at creation.
    Resumable { length: usize, upload_id: String },
}

impl LocationKind for Provisional {
    fn from_params(upload_length: Option<usize>, upload_id: Option<String>) -> Result<Self, Error> {
        match (upload_length, upload_id) {
            (length, None) => Ok(Self::Oneshot { length }),
            (Some(length), Some(upload_id)) => Ok(Self::Resumable { length, upload_id }),
            _ => Err(Error::InvalidInput(
                "expected both or neither of upload_length and upload_id",
            )),
        }
    }

    fn upload_length(&self) -> Option<usize> {
        match self {
            Provisional::Oneshot { length } => *length,
            Provisional::Resumable { length, .. } => Some(*length),
        }
    }

    fn upload_id(&self) -> Option<&str> {
        match self {
            Provisional::Oneshot { .. } => None,
            Provisional::Resumable { upload_id, .. } => Some(upload_id),
        }
    }
}

/// A final location which no longer can be used for uploading.
///
/// See also [`Provisional`].
#[derive(Debug, Clone, Copy)]
pub struct Final {
    pub length: usize,
}

impl LocationKind for Final {
    fn from_params(upload_length: Option<usize>, upload_id: Option<String>) -> Result<Self, Error> {
        match (upload_length, upload_id) {
            (Some(length), None) => Ok(Self { length }),
            _ => Err(Error::InvalidInput(
                "expected upload_length without upload_id",
            )),
        }
    }

    fn upload_length(&self) -> Option<usize> {
        Some(self.length)
    }

    fn upload_id(&self) -> Option<&str> {
        None
    }
}

/// An identifier for the upload.
///
/// The location can be converted into a URI to be put in the `Location` HTTP header
/// used by the TUS protocol.
///
/// Calling [`Self::try_sign`] appends an `&upload_signature=` query parameter that can later be used
/// to validate whether the URI (especially the kind/length) has been tampered with.
#[derive(Debug)]
pub struct Location<K: LocationKind> {
    /// Sentry project ID.
    pub project_id: ProjectId,
    /// Objectstore identifier.
    pub key: String,
    /// The kind of location.
    pub kind: K,
    pub other: UploadParams,
}

impl<K: LocationKind> Location<K> {
    fn try_to_uri(&self) -> Result<String, Error> {
        let Location {
            project_id,
            key,
            kind,
            other,
        } = self;
        #[derive(Debug, Serialize)]
        struct QueryParams<'a> {
            pub upload_length: Option<usize>,
            pub upload_id: Option<&'a str>,
            #[serde(flatten)]
            pub other: &'a UploadParams,
        }
        let params = QueryParams {
            upload_length: kind.upload_length(),
            upload_id: kind.upload_id(),
            other,
        };
        let query = serde_urlencoded::to_string(params)?;
        match query.as_str() {
            "" => Ok(format!("/api/{project_id}/upload/{key}/")),
            _ => Ok(format!("/api/{project_id}/upload/{key}/?{query}")),
        }
    }

    #[cfg(feature = "processing")]
    fn try_sign(self, config: &ConfigSnapshot) -> Result<SignedLocation<K>, Error> {
        let uri = self.try_to_uri()?;
        let secret_key = config.upload_signing_key().ok_or(Error::SigningFailed)?;
        let signature = secret_key.sign_with_header(
            uri.as_bytes(),
            &SignatureHeader {
                timestamp: Utc::now(),
                signature_algorithm: None,
            },
        );

        Ok(SignedLocation {
            location: self,
            signature,
        })
    }
}

/// Path parameters for the upload endpoint (`/api/:project_id/upload/:key/`).
#[derive(Debug, Deserialize)]
pub struct LocationPath {
    pub project_id: ProjectId,
    pub key: String,
}

/// Query parameters for the upload endpoint.
#[derive(Debug, Deserialize)]
pub struct LocationQueryParams {
    #[serde(alias = "length")]
    pub upload_length: Option<usize>,
    pub upload_id: Option<String>,
    #[serde(alias = "signature")]
    pub upload_signature: String,
    #[serde(flatten)]
    pub other: UploadParams,
}

#[derive(Debug, Default, Serialize)]
pub struct UploadParams(BTreeMap<String, String>);

impl<'de> Deserialize<'de> for UploadParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct UploadParamsVisitor;

        impl<'de> serde::de::Visitor<'de> for UploadParamsVisitor {
            type Value = UploadParams;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("upload query parameters")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut upload_params = BTreeMap::new();

                while let Some(key) = map.next_key::<&str>()? {
                    if key.starts_with("upload_") {
                        upload_params.insert(key.to_owned(), map.next_value()?);
                    } else {
                        map.next_value::<serde::de::IgnoredAny>()?;
                    }
                }

                Ok(UploadParams(upload_params))
            }
        }

        deserializer.deserialize_map(UploadParamsVisitor)
    }
}

/// A verifiable [`Location`] signed by this Relay or an upstream Relay.
#[derive(Debug)]
pub struct SignedLocation<K: LocationKind> {
    location: Location<K>,
    signature: Signature,
}

impl<K: LocationKind> SignedLocation<K> {
    /// Creates an unverified location from path and query params.
    ///
    /// Call `verify` to make sure the signature is correct.
    pub fn from_parts(
        project_id: ProjectId,
        key: String,
        kind: K,
        signature: String,
        other: UploadParams,
    ) -> Self {
        Self {
            location: Location {
                project_id,
                key,
                kind,
                other,
            },
            signature: Signature(signature),
        }
    }

    /// Converts the location into an URI for future reference.
    pub fn into_header_value(self) -> Result<HeaderValue, Error> {
        HeaderValue::from_str(&self.try_to_uri()?).map_err(Error::Internal)
    }

    /// Converts the location into a URI.
    pub fn try_to_uri(&self) -> Result<String, Error> {
        let Self {
            location,
            signature,
        } = self;

        let mut uri = location.try_to_uri()?;
        uri.push(if uri.ends_with('/') { '?' } else { '&' });
        uri.push_str("upload_signature=");
        uri.push_str(&signature.to_string());
        Ok(uri)
    }

    /// Converts the signed location into a location object.
    ///
    /// Fails if the signature is outdated or incorrect.
    #[cfg(feature = "processing")]
    pub fn verify(
        self,
        received: DateTime<Utc>,
        config: &ConfigSnapshot,
    ) -> Result<Location<K>, Error> {
        let location = self.location.try_to_uri()?;
        let max_age = chrono::Duration::seconds(config.upload().max_age);
        let public_key = config
            .upload_verification_key()
            .ok_or(SignatureError::Unverifiable)?;

        let _ = self
            .signature
            .verify(location.as_bytes(), public_key, received, max_age)?;

        Ok(self.location)
    }

    fn try_from_response(response: &Response) -> Result<Self, Error> {
        let response = response.0.error_for_status_ref().map_err(Error::Upstream)?;
        let header = response
            .headers()
            .get(hyper::header::LOCATION)
            .ok_or(Error::InvalidLocation(None))?;
        let uri = header
            .to_str()
            .map_err(|_| Error::InvalidLocation(Some(header.clone())))?;
        Self::try_from_str(uri).ok_or(Error::InvalidLocation(Some(header.clone())))
    }

    pub fn try_from_str(uri: &str) -> Option<Self> {
        static ROUTER: std::sync::LazyLock<matchit::Router<()>> = std::sync::LazyLock::new(|| {
            let mut router = matchit::Router::new();
            router
                .insert(UPLOAD_PATCH_PATH, ())
                .expect("valid route pattern");
            router
        });

        let (path, query) = uri.split_once('?')?;
        let matched = ROUTER.at(path).ok()?;
        let LocationPath { project_id, key } = LocationPath {
            project_id: matched.params.get("project_id")?.parse().ok()?,
            key: matched.params.get("key")?.to_owned(),
        };

        // Parse query parameters.
        let LocationQueryParams {
            upload_length,
            upload_id,
            upload_signature,
            other,
        } = serde_urlencoded::from_str(query).ok()?;
        let kind = K::from_params(upload_length, upload_id).ok()?;

        Some(Self::from_parts(
            project_id,
            key,
            kind,
            upload_signature,
            other,
        ))
    }
}

enum RequestKind {
    Create {
        length: Option<usize>,
        attachment_type: Option<AttachmentType>,
    },
    Upload {
        uri: String,
        mode: UploadMode,
        stream: TakeOnce<PreparedStream>,
        encoding: HttpEncoding,
    },
    Head {
        uri: String,
    },
}

/// An upstream request made to the `/upload` endpoint.
struct UploadRequest {
    project: ProjectContext,
    kind: RequestKind,
    sender: oneshot::Sender<Result<Response, UpstreamRequestError>>,
}

impl UploadRequest {
    fn create(
        project: ProjectContext,
        length: Option<usize>,
        attachment_type: Option<AttachmentType>,
    ) -> (
        Self,
        oneshot::Receiver<Result<Response, UpstreamRequestError>>,
    ) {
        let (sender, rx) = oneshot::channel();

        (
            Self {
                project,
                kind: RequestKind::Create {
                    length,
                    attachment_type,
                },
                sender,
            },
            rx,
        )
    }

    fn upload(
        project: ProjectContext,
        uri: String,
        mode: UploadMode,
        stream: PreparedStream,
        encoding: HttpEncoding,
    ) -> (
        Self,
        oneshot::Receiver<Result<Response, UpstreamRequestError>>,
    ) {
        let (sender, rx) = oneshot::channel();
        (
            Self {
                project,
                kind: RequestKind::Upload {
                    uri,
                    mode,
                    stream: TakeOnce::new(stream),
                    encoding,
                },
                sender,
            },
            rx,
        )
    }

    fn head(
        project: ProjectContext,
        uri: String,
    ) -> (
        Self,
        oneshot::Receiver<Result<Response, UpstreamRequestError>>,
    ) {
        let (sender, rx) = oneshot::channel();
        (
            Self {
                project,
                kind: RequestKind::Head { uri },
                sender,
            },
            rx,
        )
    }
}

impl fmt::Debug for UploadRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            project,
            kind: _,
            sender: _,
        } = self;
        f.debug_struct("UploadRequest")
            .field("project", project)
            .finish()
    }
}

impl UpstreamRequest for UploadRequest {
    fn upstream(&self) -> Option<&UpstreamDescriptor> {
        self.project.upstream.as_ref()
    }

    fn method(&self) -> Method {
        match self.kind {
            RequestKind::Create { .. } => Method::POST,
            RequestKind::Upload { .. } => Method::PATCH,
            RequestKind::Head { .. } => Method::HEAD,
        }
    }

    fn path(&self) -> Cow<'_, str> {
        let project_id = self.project.scoping.project_id;
        match &self.kind {
            RequestKind::Create { .. } => Cow::Owned(format!("/api/{project_id}/upload/")),
            RequestKind::Upload { uri, .. } => Cow::Borrowed(uri),
            RequestKind::Head { uri } => Cow::Borrowed(uri),
        }
    }

    fn route(&self) -> &'static str {
        "upload"
    }

    fn respond(
        self: Box<Self>,
        result: Result<Response, UpstreamRequestError>,
    ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(async move {
            let _ = self.sender.send(result);
        })
    }

    fn retry(&self) -> bool {
        match &self.kind {
            RequestKind::Create { .. } => true,
            // Once the body has been polled, it cannot be replayed — give up instead.
            RequestKind::Upload { stream, .. } => !stream.is_taken(),
            RequestKind::Head { .. } => true,
        }
    }

    fn intercept_status_errors(&self) -> bool {
        true
    }

    fn set_relay_id(&self) -> bool {
        true // needed for trusted requests with `Upload-Defer-Length: 1`
    }

    fn build(&mut self, builder: &mut RequestBuilder) -> Result<(), HttpError> {
        match &mut self.kind {
            RequestKind::Create {
                length,
                attachment_type,
            } => {
                tus::add_creation_headers(*length, *attachment_type, builder)?;
            }
            RequestKind::Upload {
                uri: _,
                mode,
                stream,
                encoding,
            } => {
                let Some(body) = RetryableStream::new(stream.clone()) else {
                    relay_log::error!("upload request stream was already consumed");
                    return Err(HttpError::Misconfigured);
                };
                tus::add_upload_headers(builder, mode);

                builder.content_encoding(*encoding);
                builder.body(reqwest::Body::wrap_stream(body));
            }
            RequestKind::Head { .. } => tus::add_head_headers(builder),
        };

        let project_key = self.project.scoping.project_key;
        builder.header("X-Sentry-Auth", format!("Sentry sentry_key={project_key}"));
        builder.timeout(Duration::MAX); // rely on service timeout to cancel requests

        Ok(())
    }
}

fn encode_body(stream: UploadStream, encoding: HttpEncoding) -> PreparedStream {
    let compression = match HttpCompression::from_encoding(encoding) {
        None => return Either::Right(stream),
        Some(compression) => compression,
    };
    let reader = BufReader::new(StreamReader::new(stream));
    Either::Left(match compression {
        HttpCompression::Deflate => ReaderStream::new(DeflateEncoder::new(reader)).boxed(),
        HttpCompression::Gzip => ReaderStream::new(GzipEncoder::new(reader)).boxed(),
        HttpCompression::Br => ReaderStream::new(BrotliEncoder::new(reader)).boxed(),
        HttpCompression::Zstd => ReaderStream::new(ZstdEncoder::new(reader)).boxed(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project() -> ProjectContext {
        ProjectContext {
            scoping: Scoping {
                organization_id: relay_base_schema::organization::OrganizationId::new(1),
                project_id: relay_base_schema::project::ProjectId::new(42),
                project_key: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".parse().unwrap(),
                key_id: None,
            },
            upstream: None,
            retention: 90,
            resumable: true,
        }
    }

    #[tokio::test]
    async fn gzip_upload_encodes_body() {
        use axum::body::{Body, to_bytes};
        use std::io::Read;

        let data = Bytes::from_static(b"hello world");
        let length = data.len();
        let stream: ByteStream = futures::stream::iter([Ok(data.clone())]).boxed();
        let stream = BoundedStream::new(MeteredStream::new(stream, "upload"), length, length);
        let (mut upload, _rx) = UploadRequest::upload(
            project(),
            "/api/42/upload/key/".into(),
            UploadMode::Oneshot {
                length: Some(length),
            },
            encode_body(stream, HttpEncoding::Gzip),
            HttpEncoding::Gzip,
        );
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let mut builder =
            RequestBuilder::reqwest(client.patch("http://localhost/api/42/upload/key/"));
        upload.build(&mut builder).unwrap();
        let mut request = builder.finish().unwrap().0;
        assert!(upload.retry());
        assert_eq!(request.headers()[http::header::CONTENT_ENCODING], "gzip");
        let bytes = to_bytes(Body::new(request.body_mut().take().unwrap()), 1024)
            .await
            .unwrap();
        assert!(!upload.retry());
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(bytes.as_ref())
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn identity_upload_preserves_chunks() {
        use http_body_util::BodyExt;

        let chunks = [Bytes::from_static(b"hello "), Bytes::from_static(b"world")];
        let stream: ByteStream = futures::stream::iter(chunks.clone().map(Ok)).boxed();
        let stream = BoundedStream::new(MeteredStream::new(stream, "upload"), 11, 11);
        let (mut upload, _rx) = UploadRequest::upload(
            project(),
            "/api/42/upload/key/".into(),
            UploadMode::Oneshot { length: Some(11) },
            encode_body(stream, HttpEncoding::Identity),
            HttpEncoding::Identity,
        );
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let mut builder =
            RequestBuilder::reqwest(client.patch("http://localhost/api/42/upload/key/"));
        upload.build(&mut builder).unwrap();
        let mut request = builder.finish().unwrap().0;
        assert!(
            !request
                .headers()
                .contains_key(http::header::CONTENT_ENCODING)
        );
        assert!(upload.retry());
        let mut body = request.body_mut().take().unwrap().into_data_stream();
        for expected in chunks {
            let actual = body.next().await.unwrap().unwrap();
            assert_eq!(actual, expected);
            // Preserve the original buffers as well as their chunk boundaries.
            assert_eq!(actual.as_ptr(), expected.as_ptr());
        }
        assert!(body.next().await.is_none());
        assert!(!upload.retry());
    }

    /// Exercise the typed body and upstream request builder across two forwarding hops,
    /// followed by decoding before objectstore, without requiring network services.
    #[tokio::test]
    async fn compressed_upload_survives_forwarding_chain() {
        use axum::Router;
        use axum::body::{Body, to_bytes};
        use axum::extract::Request;
        use axum::http::StatusCode;
        use axum::routing::patch;
        use tower::ServiceExt;

        let data = b"hello world";
        let compressed = Bytes::from(zstd::encode_all(data.as_slice(), 0).unwrap());
        assert!(compressed.len() > data.len());
        let mut bytes = compressed.clone();
        let mut headers = http::HeaderMap::new();
        let app = Router::new().route(
            UPLOAD_PATCH_PATH,
            patch(|request: Request| async move {
                let (parts, body) = request.into_parts();
                (parts.headers, body)
            }),
        );
        let client = reqwest::Client::builder().no_proxy().build().unwrap();

        for _ in 0..2 {
            let stream: ByteStream = futures::stream::iter([Ok(bytes)]).boxed();
            let stream = BoundedStream::new(MeteredStream::new(stream, "upload"), 0, 1024);
            let (mut upload, _rx) = UploadRequest::upload(
                project(),
                "/api/42/upload/key/".into(),
                UploadMode::Resumable {
                    offset: 123,
                    chunk_length: data.len(),
                },
                Either::Right(stream),
                HttpEncoding::Zstd,
            );
            let mut builder =
                RequestBuilder::reqwest(client.patch("http://localhost/api/42/upload/key/"));
            upload.build(&mut builder).unwrap();
            let mut request = builder.finish().unwrap().0;
            assert_eq!(request.headers()[http::header::CONTENT_ENCODING], "zstd");
            assert_eq!(
                request.headers()[crate::constants::X_DECODED_CONTENT_LENGTH],
                "11"
            );
            assert_eq!(request.headers()[tus::UPLOAD_OFFSET], "123");
            assert!(!request.headers().contains_key(http::header::CONTENT_LENGTH));

            assert!(upload.retry());
            bytes = to_bytes(Body::new(request.body_mut().take().unwrap()), 1024)
                .await
                .unwrap();
            assert_eq!(bytes, compressed);
            assert!(!upload.retry());
            headers = request.headers().clone();
        }

        let mut request = Request::builder()
            .method(Method::PATCH)
            .uri("/api/42/upload/key/")
            .body(Body::from(bytes))
            .unwrap();
        *request.headers_mut() = headers;
        let response = app
            .layer(axum::middleware::from_fn(crate::middlewares::decompress))
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            !response
                .headers()
                .contains_key(http::header::CONTENT_ENCODING)
        );
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap(),
            data.as_slice()
        );
    }

    #[cfg(feature = "processing")]
    mod with_processing {
        use chrono::Utc;
        use relay_auth::SignatureError;
        use relay_base_schema::project::ProjectId;
        use relay_config::{Config, Credentials, OverridableConfig, UploadCredentials};

        use super::*;

        fn location() -> Location<Provisional> {
            Location {
                project_id: ProjectId::new(42),
                key: "upload-key".to_owned(),
                kind: Provisional::Oneshot { length: None },
                other: UploadParams::default(),
            }
        }

        fn config(
            relay_credentials: Credentials,
            credentials: Option<UploadCredentials>,
        ) -> ConfigSnapshot {
            let mut config = Config::from_json_value(serde_json::json!({
                "upload": {
                    "credentials": credentials,
                },
            }))
            .unwrap();
            config
                .apply_override(OverridableConfig {
                    id: Some(relay_credentials.id.to_string()),
                    secret_key: Some(relay_credentials.secret_key.to_string()),
                    public_key: Some(relay_credentials.public_key.to_string()),
                    ..Default::default()
                })
                .unwrap();
            config.current()
        }

        #[test]
        fn verify_legacy() {
            let relay_credentials = Credentials::generate();
            let (signing_key, verification_key) = relay_auth::generate_key_pair();

            let signing_config = config(relay_credentials.clone(), None);
            let verification_config = config(
                relay_credentials,
                Some(UploadCredentials {
                    signing_key,
                    verification_key,
                }),
            );

            let signed_location = location().try_sign(&signing_config).unwrap();

            assert!(
                signed_location
                    .verify(Utc::now(), &verification_config)
                    .is_err() // If a dedicated verification key is present, don't try the legacy one.
            );
        }

        #[test]
        fn verify_new() {
            let relay_credentials = Credentials::generate();
            let (signing_key, verification_key) = relay_auth::generate_key_pair();
            let config = config(
                relay_credentials,
                Some(UploadCredentials {
                    signing_key,
                    verification_key,
                }),
            );

            let signed_location = location().try_sign(&config).unwrap();

            assert!(signed_location.verify(Utc::now(), &config).is_ok());
        }

        #[test]
        fn verify_inverse() {
            let relay_credentials = Credentials::generate();
            let (signing_key, verification_key) = relay_auth::generate_key_pair();

            let signing_config = config(
                relay_credentials.clone(),
                Some(UploadCredentials {
                    signing_key,
                    verification_key,
                }),
            );

            let verification_config = config(relay_credentials, None);

            let signed_location = location().try_sign(&signing_config).unwrap();

            assert!(matches!(
                signed_location.verify(Utc::now(), &verification_config),
                Err(Error::InvalidSignature(SignatureError::Unverifiable))
            ));
        }
    }

    #[test]
    fn parse_location_incomplete() {
        let url = "signature=foo";

        let LocationQueryParams {
            upload_length,
            upload_id,
            ..
        } = serde_urlencoded::from_str(url).unwrap();

        assert!(Provisional::from_params(upload_length, upload_id.clone()).is_ok());
        assert!(Final::from_params(upload_length, upload_id).is_err());
    }

    #[test]
    fn parse_location_complete() {
        let json = r#"signature=foo&length=123"#;

        let LocationQueryParams {
            upload_length,
            upload_id,
            ..
        } = serde_urlencoded::from_str(json).unwrap();

        assert_eq!(
            Provisional::from_params(upload_length, upload_id.clone())
                .unwrap()
                .upload_length(),
            Some(123)
        );
        assert_eq!(
            Final::from_params(upload_length, upload_id.clone())
                .unwrap()
                .length,
            123
        );
    }

    #[test]
    fn parse_location_complete_with_upload_id() {
        let json = r#"signature=foo&length=123&upload_id=bar"#;

        let query_params: LocationQueryParams = serde_urlencoded::from_str(json).unwrap();
        insta::assert_debug_snapshot!(query_params, @r#"
        LocationQueryParams {
            upload_length: Some(
                123,
            ),
            upload_id: Some(
                "bar",
            ),
            upload_signature: "foo",
            other: UploadParams(
                {},
            ),
        }
        "#);
    }

    #[test]
    fn parse_location_with_other() {
        let json =
            r#"upload_signature=foo&upload_length=123&not_an_upload_param=123&upload_type=bar"#;

        let query_params: LocationQueryParams = serde_urlencoded::from_str(json).unwrap();
        insta::assert_debug_snapshot!(query_params, @r#"
        LocationQueryParams {
            upload_length: Some(
                123,
            ),
            upload_id: None,
            upload_signature: "foo",
            other: UploadParams(
                {
                    "upload_type": "bar",
                },
            ),
        }
        "#);
    }
}
