//! Utilities for uploading large files.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
#[cfg(feature = "processing")]
use chrono::Utc;
use futures::stream::BoxStream;
use http::{HeaderValue, Method, StatusCode};
use relay_auth::Signature;
#[cfg(feature = "processing")]
use relay_auth::SignatureHeader;
use relay_base_schema::organization::OrganizationId;
use relay_base_schema::project::ProjectId;
use relay_config::Config;
use relay_quotas::Scoping;
use relay_system::{
    Addr, AsyncResponse, ConcurrentService, FromMessage, Interface, LoadShed, Sender, SimpleService,
};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use uuid::Uuid;

use crate::http::{HttpError, RequestBuilder, Response};

use crate::services::objectstore::ObjectstoreKey;
#[cfg(feature = "processing")]
use crate::services::objectstore::{self, Objectstore};
use crate::services::upstream::{
    SendRequest, UpstreamRelay, UpstreamRequest, UpstreamRequestError,
};
use crate::utils::{BoundedStream, tus};

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
    Upstream(StatusCode),
    #[error("upstream provided invalid location")]
    InvalidLocation,
    #[error("failed to sign location")]
    SigningFailed,
    #[error("service unavailable")]
    ServiceUnavailable,
    #[cfg(feature = "processing")]
    #[error("objectstore service: {0}")]
    Objectstore(#[from] objectstore::Error),
    #[error("loadshed")]
    LoadShed,
    #[error("internal error")]
    Internal,
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

/// A stream of bytes to be uploaded to objectstore or the upstream.
pub struct Stream {
    /// The organization & project that the stream belongs to.
    pub scoping: Scoping,
    /// The location to upload to.
    pub location: SignedLocation,
    /// The body to be uploaded to objectstore, with length validation.
    pub stream: BoundedStream<BoxStream<'static, std::io::Result<Bytes>>>,
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
    let service = create_service_inner(
        config,
        upstream,
        #[cfg(feature = "processing")]
        objectstore,
    );
    ConcurrentService::new(service)
        .with_backlog_limit(0)
        .with_concurrency_limit(config.upload().max_concurrent_requests)
}

fn create_service_inner(
    config: &Arc<Config>,
    upstream: &Addr<UpstreamRelay>,
    #[cfg(feature = "processing")] objectstore: &Option<Addr<Objectstore>>,
) -> Service {
    #[cfg(feature = "processing")]
    if let Some(addr) = objectstore.as_ref() {
        return Service::Objectstore {
            addr: addr.clone(),
            config: config.clone(),
        };
    }
    Service::Upstream {
        addr: upstream.clone(),
        timeout: Duration::from_secs(config.upload().timeout),
    }
}

/// A dispatcher for uploading large files.
///
/// Uploads go to either the upstream relay or objectstore.
#[derive(Debug, Clone)]
pub enum Service {
    Upstream {
        addr: Addr<UpstreamRelay>,
        timeout: Duration,
    },
    #[cfg(feature = "processing")]
    Objectstore {
        addr: Addr<Objectstore>,
        config: Arc<Config>,
    },
}

impl Service {
    async fn create(&self, Create { scoping, length }: Create) -> Result<SignedLocation, Error> {
        match self {
            Self::Upstream { addr, timeout } => {
                let (request, rx) = UploadRequest::create(scoping, length);
                addr.send(SendRequest(request));
                let response = tokio::time::timeout(*timeout, rx).await???;
                SignedLocation::try_from_response(response)
            }
            Self::Objectstore { addr: _, config } => {
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
        match self {
            Service::Upstream { addr, timeout } => {
                let (request, rx) = UploadRequest::upload(stream);
                addr.send(SendRequest(request));
                // We're already passing `timeout` to the reqwest library, but we also want to
                // limit the time spent waiting for the upstream service:
                let response = tokio::time::timeout(*timeout, rx).await???;
                SignedLocation::try_from_response(response)
            }
            #[cfg(feature = "processing")]
            Service::Objectstore { addr, config } => {
                let Stream {
                    scoping,
                    location,
                    stream,
                } = stream;
                let Location {
                    project_id,
                    key,
                    length,
                } = location.verify()?;

                debug_assert_eq!(scoping.project_id, project_id);
                debug_assert_eq!(stream.length(), length);
                let byte_counter = stream.byte_counter();

                let key = addr
                    .send(objectstore::Stream {
                        organization_id: scoping.organization_id,
                        project_id,
                        key,
                        stream,
                    })
                    .await
                    .map_err(|_send_error| Error::ServiceUnavailable)??
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
}

impl SimpleService for Service {
    type Interface = Upload;

    async fn handle_message(&self, message: Upload) {
        match message {
            Upload::Create(create, sender) => {
                sender.send(self.create(create).await);
            }
            Upload::Upload(stream, sender) => {
                sender.send(self.upload(stream).await);
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

/// A verifiable [`Location`] signed by this Relay or an upstream Relay.
#[derive(Debug)]
pub struct SignedLocation {
    location: Location,
    signature: Signature,
}

impl SignedLocation {
    /// Converts the location into an URI for future reference.
    pub fn into_header_value(self) -> Result<HeaderValue, Error> {
        let Self {
            location,
            signature,
        } = self;

        let mut uri = location.as_uri();
        uri.push(if location.length.is_some() { '&' } else { '?' }); // TODO: brittle.
        uri.push_str("signature=");
        uri.push_str(&signature.to_string());

        HeaderValue::from_str(&uri).map_err(|_| Error::Internal)
    }

    /// Converts the signed location into a location object.
    ///
    /// Fails if the signature is outdated or incorrect.
    fn verify(self) -> Result<Location, Error> {
        Ok(self.location) // TODO: actually verify
    }

    fn try_from_response(response: Response) -> Result<Self, Error> {
        match response.status() {
            status if status.is_success() => {
                let header = response
                    .headers()
                    .get(hyper::header::LOCATION)
                    .ok_or(Error::InvalidLocation)?;
                let uri = header.to_str().map_err(|_| Error::InvalidLocation)?;
                Self::try_from_str(uri).ok_or(Error::InvalidLocation)
            }
            status => Err(Error::Upstream(status)),
        }
    }

    fn try_from_str(uri: &str) -> Option<Self> {
        // Parse path segments: /api/{project_id}/upload/{key}/
        let path = uri.split('?').next().unwrap_or(uri);
        let mut segments = path.split('/').filter(|s| !s.is_empty());
        expect(&mut segments, "api")?;
        let project_id = segments.next()?;
        expect(&mut segments, "upload")?;
        let key = segments.next()?;
        if !(key.len() == 32 && key.bytes().all(|c| c.is_ascii_hexdigit())) {
            return None;
        }
        let project_id: ProjectId = project_id.parse().ok()?;

        // Parse query parameters: length and signature.
        let query = uri.split('?').nth(1)?;
        let mut length = None;
        let mut signature = None;
        for param in query.split('&') {
            if let Some(v) = param.strip_prefix("length=") {
                length = Some(v.parse().ok()?);
            } else if let Some(v) = param.strip_prefix("signature=") {
                signature = Some(Signature(v.to_owned()));
            }
        }
        let signature = signature?;

        Some(SignedLocation {
            location: Location {
                project_id,
                key: key.to_owned(),
                length,
            },
            signature,
        })
    }
}

fn expect<'a, I: Iterator<Item = &'a str>>(it: &mut I, expected_value: &str) -> Option<()> {
    (it.next()? == expected_value).then_some(())
}

enum RequestKind {
    Create {
        length: Option<usize>,
    },
    Upload {
        location: SignedLocation,
        stream: Option<BoundedStream<BoxStream<'static, std::io::Result<Bytes>>>>,
    },
}

/// An upstream request made to the `/upload` endpoint.
struct UploadRequest {
    scoping: Scoping,
    timeout: Option<Duration>,
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
            location,
            stream,
        } = stream;

        (
            Self {
                scoping,
                timeout: None, // will be set by `configure()`
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
            RequestKind::Upload { stream, .. } => stream.as_ref().and_then(BoundedStream::length),
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
            RequestKind::Upload { .. } => Method::PATCH, // TODO: allow method
        }
    }

    fn path(&self) -> std::borrow::Cow<'_, str> {
        let project_id = self.scoping.project_id;
        match &self.kind {
            RequestKind::Create { .. } => format!("/api/{project_id}/upload/").into(),
            RequestKind::Upload { location, .. } => location.location.key.as_str().into(),
        }
    }

    fn route(&self) -> &'static str {
        "upload"
    }

    fn configure(&mut self, config: &Config) {
        self.timeout = Some(Duration::from_secs(config.upload().timeout));
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
        let body = match &mut self.kind {
            RequestKind::Create { .. } => None,
            RequestKind::Upload { stream, .. } => match stream.take() {
                Some(body) => Some(body),
                None => {
                    relay_log::error!("upload request was retried or never initialized");
                    return Err(HttpError::Misconfigured);
                }
            },
        };

        let project_key = self.scoping.project_key;
        builder.header("X-Sentry-Auth", format!("Sentry sentry_key={project_key}"));
        for (key, value) in tus::request_headers(self.length()) {
            let Some(key) = key else { continue };
            builder.header(key, value);
        }

        debug_assert!(
            self.timeout.is_some(),
            "timeout should be set by UpstreamRequest::configure()"
        );
        if let Some(timeout) = self.timeout {
            builder.timeout(timeout);
        }

        builder.body(reqwest::Body::wrap_stream(body));

        Ok(())
    }
}
