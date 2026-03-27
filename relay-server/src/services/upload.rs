//! Utilities for uploading large files.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use futures::stream::BoxStream;
use http::{HeaderValue, Method};
use relay_auth::Signature;
#[cfg(feature = "processing")]
use relay_auth::SignatureHeader;
use relay_base_schema::project::ProjectId;
use relay_config::Config;
use relay_quotas::Scoping;
use relay_system::{
    Addr, AsyncResponse, ConcurrentService, FromMessage, Interface, LoadShed, SendError, Sender,
    SimpleService,
};
use serde::Deserialize;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
#[cfg(feature = "processing")]
use uuid::Uuid;

use crate::http::{HttpError, RequestBuilder, Response};

#[cfg(feature = "processing")]
use crate::services::objectstore::{self, Objectstore};
use crate::services::upstream::{
    SendRequest, UpstreamRelay, UpstreamRequest, UpstreamRequestError,
};
use crate::utils::MeteredStream;
use crate::utils::{BoundedStream, tus};

/// The URL template for uploading bytes to a known location.
pub const UPLOAD_PATCH_PATH: &str = "/api/{project_id}/upload/{key}/";

/// An error that occurs during upload.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("send failed: {0}")]
    Send(#[from] RecvError),
    #[error("request failed: {0}")]
    UpstreamRequest(#[from] UpstreamRequestError),
    #[error("request timeout: {0}")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("upstream response: {0}")]
    Upstream(#[source] reqwest::Error),
    #[error("upstream provided invalid location: {0:?}")]
    InvalidLocation(Option<HeaderValue>),
    #[error("failed to sign location")]
    SigningFailed,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("service unavailable")]
    ServiceUnavailable(#[source] SendError),
    #[cfg(feature = "processing")]
    #[error("objectstore service: {0}")]
    Objectstore(#[from] objectstore::Error),
    #[error("loadshed")]
    LoadShed,
    #[error("internal error")]
    Internal(#[source] http::header::InvalidHeaderValue),
}

/// The message interface for this service.
pub enum Upload {
    /// Creates an upload resource.
    ///
    /// Returns the trusted identifier of the upload.
    Create(Create, Sender<Result<SignedLocation, Error>>),
    /// Upload a stream of bytes for a given location.
    ///
    /// The service also returns the signed location. This is redundant, but creates a simpler
    /// flow for the caller side.
    Upload(Stream, Sender<Result<SignedLocation, Error>>),
}

impl Interface for Upload {}

/// Request to create an upload resource.
pub struct Create {
    /// The project to create the upload for.
    pub scoping: Scoping,
    /// The size of the intended upload in bytes, as specified in the `Upload-Length` header.
    ///
    /// Trusted clients (i.e. PoP Relays) are allowed to omit the length (see `Upload-Defer-Length: 1`).
    pub length: Option<usize>,
}

/// The type used to stream a request body.
pub type ByteStream = BoxStream<'static, std::io::Result<Bytes>>;

/// A stream of bytes to be uploaded to objectstore or the upstream.
pub struct Stream {
    /// Time of arrival of the request.
    pub received: DateTime<Utc>,
    /// The organization & project that the stream belongs to.
    pub scoping: Scoping,
    /// The location to upload to.
    pub location: SignedLocation,
    /// The body to be uploaded to objectstore, with length validation.
    pub stream: BoundedStream<MeteredStream<ByteStream>>,
}

impl FromMessage<Create> for Upload {
    type Response = AsyncResponse<Result<SignedLocation, Error>>;

    fn from_message(message: Create, sender: Sender<Result<SignedLocation, Error>>) -> Self {
        Self::Create(message, sender)
    }
}

impl FromMessage<Stream> for Upload {
    type Response = AsyncResponse<Result<SignedLocation, Error>>;

    fn from_message(message: Stream, sender: Sender<Result<SignedLocation, Error>>) -> Self {
        Self::Upload(message, sender)
    }
}

/// Creates a new upload service.
pub fn create_service(
    config: &Arc<Config>,
    upstream: &Addr<UpstreamRelay>,
    #[cfg(feature = "processing")] objectstore: &Option<Addr<Objectstore>>,
) -> ConcurrentService<Service> {
    let backend = create_backend(
        config,
        upstream,
        #[cfg(feature = "processing")]
        objectstore,
    );
    let service = Service {
        timeout: Duration::from_secs(config.upload().timeout),
        backend,
    };
    ConcurrentService::new(service)
        .with_backlog_limit(0)
        .with_concurrency_limit(config.upload().max_concurrent_requests)
}

fn create_backend(
    config: &Arc<Config>,
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

#[derive(Debug, Clone)]
enum Backend {
    Upstream {
        addr: Addr<UpstreamRelay>,
    },
    #[cfg(feature = "processing")]
    Objectstore {
        addr: Addr<Objectstore>,
        config: Arc<Config>,
    },
}

impl Service {
    async fn create(&self, Create { scoping, length }: Create) -> Result<SignedLocation, Error> {
        match &self.backend {
            Backend::Upstream { addr } => {
                let (request, rx) = UploadRequest::create(scoping, length);
                addr.send(SendRequest(request));
                let response = rx.await??;
                SignedLocation::try_from_response(response)
            }
            #[cfg(feature = "processing")]
            Backend::Objectstore { addr: _, config } => {
                // We can create & sign a location right here, no need to query the objectstore service.
                let key = Uuid::now_v7().as_simple().to_string();
                Location {
                    project_id: scoping.project_id,
                    key,
                    length,
                }
                .try_sign(config)
            }
        }
    }

    async fn upload(&self, stream: Stream) -> Result<SignedLocation, Error> {
        match &self.backend {
            Backend::Upstream { addr } => {
                let (request, rx) = UploadRequest::upload(stream);
                addr.send(SendRequest(request));
                let response = rx.await??;
                SignedLocation::try_from_response(response)
            }
            #[cfg(feature = "processing")]
            Backend::Objectstore { addr, config } => {
                let Stream {
                    received,
                    scoping,
                    location,
                    stream,
                } = stream;
                let Location {
                    project_id,
                    key,
                    length,
                } = location.verify(received, config)?;

                debug_assert_eq!(scoping.project_id, project_id);
                debug_assert!(stream.length().is_none_or(|l| Some(l) == length));
                let byte_counter = stream.byte_counter();

                let key = addr
                    .send(objectstore::Stream {
                        organization_id: scoping.organization_id,
                        project_id,
                        key,
                        stream,
                    })
                    .await
                    .map_err(Error::ServiceUnavailable)??
                    .into_inner();
                let length = Some(byte_counter.get());

                Location {
                    project_id,
                    key,
                    length,
                }
                .try_sign(config)
            }
        }
    }

    async fn timeout<F: IntoFuture<Output = Result<SignedLocation, Error>>>(
        &self,
        future: F,
    ) -> Result<SignedLocation, Error> {
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
                sender.send(self.timeout(self.upload(stream)).await);
            }
        }
    }
}

impl LoadShed<Upload> for Service {
    fn handle_loadshed(&self, message: Upload) {
        match message {
            Upload::Create(_, tx) | Upload::Upload(_, tx) => tx.send(Err(Error::LoadShed)),
        }
    }
}

/// An identifier for the upload.
///
/// The location can be converted into a URI to be put in the `Location` HTTP header
/// used by the TUS protocol.
///
/// Calling [`Self::try_sign`] appends a `&signature=` query parameter that can later be used
/// to validate whether the URI (especially the length) has been tempered with.
#[derive(Debug)]
pub struct Location {
    /// Sentry project ID.
    pub project_id: ProjectId,
    /// Objectstore identifier.
    pub key: String,
    /// Value of the `Upload-Length` header. `None` if `Upload-Defer-Length: 1`.
    pub length: Option<usize>,
}

impl Location {
    fn as_uri(&self) -> String {
        let Location {
            project_id,
            key,
            length,
        } = self;
        match length {
            Some(length) => format!("/api/{project_id}/upload/{key}/?length={length}"),
            None => format!("/api/{project_id}/upload/{key}/"),
        }
    }

    #[cfg(feature = "processing")]
    fn try_sign(self, config: &Config) -> Result<SignedLocation, Error> {
        let uri = self.as_uri();
        let signature = config
            .credentials()
            .ok_or(Error::SigningFailed)?
            .secret_key
            .sign_with_header(
                uri.as_bytes(),
                &SignatureHeader {
                    timestamp: Some(Utc::now()),
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
    pub length: Option<usize>,
    pub signature: String,
}

/// A verifiable [`Location`] signed by this Relay or an upstream Relay.
#[derive(Debug)]
pub struct SignedLocation {
    location: Location,
    signature: Signature,
}

impl SignedLocation {
    /// Creates an unverified location from path and query params.
    ///
    /// Call `verify` to make sure the signature is correct.
    pub fn from_parts(
        project_id: ProjectId,
        key: String,
        length: Option<usize>,
        signature: String,
    ) -> Self {
        // TODO: forward compat: allow other query params?
        Self {
            location: Location {
                project_id,
                key,
                length,
            },
            signature: Signature(signature),
        }
    }

    /// Converts the location into an URI for future reference.
    pub fn into_header_value(self) -> Result<HeaderValue, Error> {
        HeaderValue::from_str(&self.as_uri()).map_err(Error::Internal)
    }

    fn as_uri(&self) -> String {
        let Self {
            location,
            signature,
        } = self;
        let mut uri = location.as_uri();
        uri.push(if location.length.is_some() { '&' } else { '?' }); // TODO: brittle.
        uri.push_str("signature=");
        uri.push_str(&signature.to_string());
        uri
    }

    /// Converts the signed location into a location object.
    ///
    /// Fails if the signature is outdated or incorrect.
    #[cfg(feature = "processing")]
    fn verify(self, received: DateTime<Utc>, config: &Config) -> Result<Location, Error> {
        let public_key = config.public_key().ok_or(Error::SigningFailed)?;
        let is_valid = self.signature.verify(
            self.location.as_uri().as_bytes(),
            public_key,
            received,
            chrono::Duration::seconds(config.upload().max_age),
        );
        match is_valid {
            true => Ok(self.location),
            false => Err(Error::InvalidSignature),
        }
    }

    fn try_from_response(response: Response) -> Result<Self, Error> {
        match response.0.error_for_status() {
            Ok(response) => {
                let header = response
                    .headers()
                    .get(hyper::header::LOCATION)
                    .ok_or(Error::InvalidLocation(None))?;
                let uri = header
                    .to_str()
                    .map_err(|_| Error::InvalidLocation(Some(header.clone())))?;
                Self::try_from_str(uri).ok_or(Error::InvalidLocation(Some(header.clone())))
            }
            Err(e) => Err(Error::Upstream(e)),
        }
    }

    fn try_from_str(uri: &str) -> Option<Self> {
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
        let LocationQueryParams { length, signature } = serde_urlencoded::from_str(query).ok()?;

        Some(Self::from_parts(project_id, key, length, signature))
    }
}

enum RequestKind {
    Create {
        length: Option<usize>,
    },
    Upload {
        location: SignedLocation,
        stream: Option<BoundedStream<MeteredStream<ByteStream>>>,
    },
}

/// An upstream request made to the `/upload` endpoint.
struct UploadRequest {
    scoping: Scoping,
    kind: RequestKind,
    sender: oneshot::Sender<Result<Response, UpstreamRequestError>>,
}

impl UploadRequest {
    fn create(
        scoping: Scoping,
        length: Option<usize>,
    ) -> (
        Self,
        oneshot::Receiver<Result<Response, UpstreamRequestError>>,
    ) {
        let (sender, rx) = oneshot::channel();

        (
            Self {
                scoping,
                kind: RequestKind::Create { length },
                sender,
            },
            rx,
        )
    }

    fn upload(
        stream: Stream,
    ) -> (
        Self,
        oneshot::Receiver<Result<Response, UpstreamRequestError>>,
    ) {
        let (sender, rx) = oneshot::channel();
        let Stream {
            scoping,
            received: _,
            location,
            stream,
        } = stream;

        (
            Self {
                scoping,
                kind: RequestKind::Upload {
                    location,
                    stream: Some(stream),
                },
                sender,
            },
            rx,
        )
    }

    /// Returns the length of the upload, if known.
    fn length(&self) -> Option<usize> {
        match &self.kind {
            RequestKind::Create { length } => *length,
            RequestKind::Upload { stream, .. } => {
                debug_assert!(stream.is_some());
                stream.as_ref().and_then(BoundedStream::length)
            }
        }
    }
}

impl fmt::Debug for UploadRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadRequest")
            .field("scoping", &self.scoping)
            .finish()
    }
}

impl UpstreamRequest for UploadRequest {
    fn method(&self) -> Method {
        match self.kind {
            RequestKind::Create { .. } => Method::POST,
            RequestKind::Upload { .. } => Method::PATCH,
        }
    }

    fn path(&self) -> std::borrow::Cow<'_, str> {
        let project_id = self.scoping.project_id;
        match &self.kind {
            RequestKind::Create { .. } => format!("/api/{project_id}/upload/"),
            RequestKind::Upload { location, .. } => location.as_uri(),
        }
        .into()
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
        false
    }

    fn intercept_status_errors(&self) -> bool {
        false // same as ForwardRequest
    }

    fn set_relay_id(&self) -> bool {
        true // needed for trusted requests with `Upload-Defer-Length: 1`
    }

    fn build(&mut self, builder: &mut RequestBuilder) -> Result<(), HttpError> {
        let upload_length = self.length();
        if let RequestKind::Upload { stream, .. } = &mut self.kind {
            let Some(body) = stream.take() else {
                relay_log::error!("upload request was retried or never initialized");
                return Err(HttpError::Misconfigured);
            };
            tus::add_upload_headers(builder);
            builder.body(reqwest::Body::wrap_stream(body));
        } else {
            tus::add_creation_headers(upload_length, builder);
        }

        let project_key = self.scoping.project_key;
        builder.header("X-Sentry-Auth", format!("Sentry sentry_key={project_key}"));
        builder.timeout(Duration::MAX); // rely on service timeout to cancel requests

        Ok(())
    }
}
