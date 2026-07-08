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
use futures::stream::BoxStream;
use http::{HeaderValue, Method};
use relay_auth::Signature;
use relay_auth::SignatureError;
#[cfg(feature = "processing")]
use relay_auth::SignatureHeader;
use relay_base_schema::project::ProjectId;
use relay_config::{Config, HttpEncoding, UpstreamDescriptor};
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
    #[cfg(feature = "processing")]
    #[error(transparent)]
    InvalidUploadId(#[from] objectstore_types::multipart::InvalidUploadId),
    #[error("serializing location failed: {0}")]
    SerializeFailed(#[from] serde_urlencoded::ser::Error),
    #[error("failed to sign location")]
    SigningFailed,
    #[error("invalid signature: {0}")]
    InvalidSignature(#[from] SignatureError),
    #[error("objectstore service unavailable: {0}")]
    ObjectstoreServiceUnavailable(#[source] SendError),
    #[cfg(feature = "processing")]
    #[error("objectstore service: {0}")]
    Objectstore(#[from] objectstore::Error),
    #[error("loadshed")]
    LoadShed,
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
            Error::InvalidUploadId(_) => "invalid_upload_id",
            Error::SigningFailed => "signing_failed",
            Error::SerializeFailed(_) => "serialize_failed",
            Error::InvalidSignature(_) => "invalid_signature",
            Error::ObjectstoreServiceUnavailable(_) => "service_unavailable",
            #[cfg(feature = "processing")]
            Error::Objectstore(_) => "objectstore_error",
            Error::LoadShed => "load_shed",
            Error::Internal(_) => "internal",
        }
    }
}

/// The message interface for this service.
pub enum Upload {
    /// Creates an upload resource.
    ///
    /// Returns the trusted identifier of the upload.
    Create(Create, InstrumentedSender<Provisional>),
    /// Upload a stream of bytes for a given location.
    ///
    /// The service also returns the signed location. This is redundant, but creates a simpler
    /// flow for the caller side.
    Upload(Stream, InstrumentedSender<Final>),
}

impl Interface for Upload {}

/// Project information necessary for uploading.
#[derive(Debug, Clone)]
pub struct ProjectContext {
    /// The organization and project identifiers.
    pub scoping: Scoping,
    /// Where to send the request.
    pub upstream: Option<UpstreamDescriptor>,
}

/// Request to create an upload resource.
pub struct Create {
    /// The project to create the upload for.
    pub project: ProjectContext,
    /// The size of the intended upload in bytes, as specified in the `Upload-Length` header.
    ///
    /// Trusted clients (i.e. PoP Relays) are allowed to omit the length (see `Upload-Defer-Length: 1`).
    pub length: Option<usize>,
    /// The attachment type of the upload.
    pub attachment_type: Option<AttachmentType>,
}

/// The type used to stream a request body.
pub type ByteStream = BoxStream<'static, std::io::Result<Bytes>>;

/// A stream of bytes to be uploaded to objectstore or the upstream.
pub struct Stream {
    /// Time of arrival of the request.
    pub received: DateTime<Utc>,
    /// The project to create the upload for.
    pub project: ProjectContext,
    /// The location to upload to.
    pub location: SignedLocation<Provisional>,
    /// The body to be uploaded to objectstore, with length validation.
    pub stream: BoundedStream<MeteredStream<ByteStream>>,
}

impl FromMessage<Create> for Upload {
    type Response = AsyncResponse<Result<SignedLocation<Provisional>, Error>>;

    fn from_message(
        message: Create,
        sender: Sender<Result<SignedLocation<Provisional>, Error>>,
    ) -> Self {
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
    type Response = AsyncResponse<Result<SignedLocation<Final>, Error>>;

    fn from_message(message: Stream, sender: Sender<Result<SignedLocation<Final>, Error>>) -> Self {
        Self::Upload(
            message,
            InstrumentedSender {
                metric: RelayCounters::UploadUpload,
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
pub struct InstrumentedSender<L: UploadLength> {
    metric: RelayCounters,
    inner: Sender<Result<SignedLocation<L>, Error>>,
}

impl<L: UploadLength> InstrumentedSender<L> {
    fn send(self, result: Result<SignedLocation<L>, Error>) {
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
    ) -> Result<SignedLocation<Provisional>, Error> {
        match &self.backend {
            Backend::Upstream { addr } => {
                let (request, rx) = UploadRequest::create(project, length, attachment_type);
                addr.send(SendRequest(request));
                let response = rx.await??;
                SignedLocation::try_from_response(response)
            }
            #[cfg(feature = "processing")]
            Backend::Objectstore { addr, config } => {
                use crate::services::objectstore::UploadRef;

                let key = Uuid::now_v7().as_simple().to_string();
                let Scoping {
                    organization_id,
                    project_id,
                    ..
                } = project.scoping;

                let UploadRef { key, upload_id } = addr
                    .send(objectstore::Create {
                        organization_id,
                        project_id,
                        key,
                    })
                    .await
                    .map_err(Error::ObjectstoreServiceUnavailable)??;

                Location {
                    project_id: project.scoping.project_id,
                    key,
                    length: Provisional(length),
                    upload_id: upload_id.map(|s| s.to_string()),
                    other: Default::default(),
                }
                .try_sign(config)
            }
        }
    }

    async fn upload(&self, stream: Stream) -> Result<SignedLocation<Final>, Error> {
        let Stream {
            #[cfg_attr(not(feature = "processing"), expect(unused))]
            received,
            project,
            location,
            stream,
        } = stream;
        match &self.backend {
            Backend::Upstream { addr } => {
                let (request, rx) = UploadRequest::upload(project, location.try_to_uri()?, stream);
                addr.send(SendRequest(request));
                let response = rx.await??;
                SignedLocation::try_from_response(response)
            }
            #[cfg(feature = "processing")]
            Backend::Objectstore { addr, config } => {
                use crate::services::objectstore::UploadRef;

                let Location {
                    project_id,
                    key,
                    length,
                    upload_id,
                    other,
                } = location.verify(received, config)?;

                let scoping = project.scoping;
                debug_assert_eq!(scoping.project_id, project_id);
                debug_assert!(stream.length().is_none_or(|l| Some(l) == length.value()));
                let byte_counter = stream.byte_counter();

                let upload_ref = UploadRef::new(key, upload_id)?;
                let key = addr
                    .send(objectstore::Stream {
                        organization_id: scoping.organization_id,
                        project_id,
                        upload_ref,
                        stream,
                    })
                    .await
                    .map_err(Error::ObjectstoreServiceUnavailable)??
                    .into_inner();
                let length = Final(byte_counter.get());

                Location {
                    project_id,
                    key,
                    length,
                    upload_id: None,
                    other,
                }
                .try_sign(config)
            }
        }
    }

    async fn timeout<L, F>(&self, future: F) -> Result<SignedLocation<L>, Error>
    where
        L: UploadLength,
        F: IntoFuture<Output = Result<SignedLocation<L>, Error>>,
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
                sender.send(self.timeout(self.upload(stream)).await);
            }
        }
    }
}

impl LoadShed<Upload> for Service {
    fn handle_loadshed(&self, message: Upload) {
        match message {
            Upload::Create(_, tx) => tx.send(Err(Error::LoadShed)),
            Upload::Upload(_, tx) => tx.send(Err(Error::LoadShed)),
        }
    }
}

/// An interface for known or unknown upload lengths.
///
/// This allows code sharing between [`Provisional`] and [`Final`] upload locations.
pub trait UploadLength: for<'de> Deserialize<'de> {
    fn value(&self) -> Option<usize>;
}

/// A provisional upload length which may or may not yet be known.
///
/// See also [`Final`].
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(transparent)]
pub struct Provisional(Option<usize>);

impl UploadLength for Provisional {
    fn value(&self) -> Option<usize> {
        self.0
    }
}

/// A final upload length that represents the actual amount of bytes uploaded to objectstore.
///
/// See also [`Provisional`].
#[derive(Debug, Clone, Copy, Deserialize)]
pub struct Final(usize);

impl Final {
    /// Get the value.
    pub fn into_inner(self) -> usize {
        self.0
    }
}

impl UploadLength for Final {
    fn value(&self) -> Option<usize> {
        Some(self.0)
    }
}

/// An identifier for the upload.
///
/// The location can be converted into a URI to be put in the `Location` HTTP header
/// used by the TUS protocol.
///
/// Calling [`Self::try_sign`] appends an `&upload_signature=` query parameter that can later be used
/// to validate whether the URI (especially the length) has been tampered with.
#[derive(Debug)]
pub struct Location<L> {
    /// Sentry project ID.
    pub project_id: ProjectId,
    /// Objectstore identifier.
    pub key: String,
    /// Value of the `Upload-Length` header. `None` if `Upload-Defer-Length: 1`.
    pub length: L,
    /// Identifies the upload in case the created location has a multipart upload assigned to it.
    pub upload_id: Option<String>,
    pub other: UploadParams,
}

impl<L: UploadLength> Location<L> {
    fn try_to_uri(&self) -> Result<String, Error> {
        let Location {
            project_id,
            key,
            length,
            upload_id,
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
            upload_length: length.value(),
            upload_id: upload_id.as_deref(),
            other,
        };
        let query = serde_urlencoded::to_string(params)?;
        match query.as_str() {
            "" => Ok(format!("/api/{project_id}/upload/{key}/")),
            _ => Ok(format!("/api/{project_id}/upload/{key}/?{query}")),
        }
    }

    #[cfg(feature = "processing")]
    fn try_sign(self, config: &Config) -> Result<SignedLocation<L>, Error> {
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
#[serde(bound = "L: UploadLength")]
pub struct LocationQueryParams<L: UploadLength> {
    #[serde(alias = "length")]
    pub upload_length: L,
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
pub struct SignedLocation<L: UploadLength> {
    location: Location<L>,
    signature: Signature,
}

impl<L: UploadLength> SignedLocation<L> {
    /// Creates an unverified location from path and query params.
    ///
    /// Call `verify` to make sure the signature is correct.
    pub fn from_parts(
        project_id: ProjectId,
        key: String,
        length: L,
        upload_id: Option<String>,
        signature: String,
        other: UploadParams,
    ) -> Self {
        Self {
            location: Location {
                project_id,
                key,
                length,
                upload_id,
                other,
            },
            signature: Signature(signature),
        }
    }

    /// Converts the location into an URI for future reference.
    pub fn into_header_value(self) -> Result<HeaderValue, Error> {
        HeaderValue::from_str(&self.try_to_uri()?).map_err(Error::Internal)
    }

    fn try_to_uri(&self) -> Result<String, Error> {
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
    pub fn verify(self, received: DateTime<Utc>, config: &Config) -> Result<Location<L>, Error> {
        let mut result = Err(SignatureError::Unverifiable);
        let location = self.location.try_to_uri()?;
        let max_age = chrono::Duration::seconds(config.upload().max_age);

        if let Some(public_key) = &config
            .upload()
            .credentials
            .as_ref()
            .map(|c| &c.verification_key)
        {
            result = self
                .signature
                .verify(location.as_bytes(), public_key, received, max_age);
        }

        // For the transition phase, check the general purpose signature even when there is a
        // special-purpose upload key, because the URL may have been signed by an old instance.
        // This can be simplified after the rollout.
        if matches!(&result, Err(SignatureError::Unverifiable))
            && let Some(public_key) = config.credentials().map(|c| &c.public_key)
        {
            result = self
                .signature
                .verify(location.as_bytes(), public_key, received, max_age);
        }

        result?;

        Ok(self.location)
    }
}

impl<L> SignedLocation<L>
where
    L: UploadLength,
    LocationQueryParams<L>: for<'de> Deserialize<'de>,
{
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

        Some(Self::from_parts(
            project_id,
            key,
            upload_length,
            upload_id,
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
        stream: TakeOnce<BoundedStream<MeteredStream<ByteStream>>>,
        encoding: HttpEncoding,
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
        stream: BoundedStream<MeteredStream<ByteStream>>,
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
                    stream: TakeOnce::new(stream),
                    encoding: HttpEncoding::Zstd, // just a default, will be overwritten by .configure()
                },
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
        }
    }

    fn path(&self) -> Cow<'_, str> {
        let project_id = self.project.scoping.project_id;
        match &self.kind {
            RequestKind::Create { .. } => Cow::Owned(format!("/api/{project_id}/upload/")),
            RequestKind::Upload { uri, .. } => Cow::Borrowed(uri),
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
                stream,
                encoding,
            } => {
                let Some(body) = RetryableStream::new(stream.clone()) else {
                    relay_log::error!("upload request stream was already consumed");
                    return Err(HttpError::Misconfigured);
                };
                tus::add_upload_headers(builder);

                let body = encode_body(body, *encoding);
                builder.content_encoding(*encoding);

                builder.body(reqwest::Body::wrap_stream(body));
            }
        };

        let project_key = self.project.scoping.project_key;
        builder.header("X-Sentry-Auth", format!("Sentry sentry_key={project_key}"));
        builder.timeout(Duration::MAX); // rely on service timeout to cancel requests

        Ok(())
    }

    fn configure(&mut self, config: &Config) {
        if let RequestKind::Upload { encoding, .. } = &mut self.kind {
            *encoding = config.http_encoding();
        }
    }
}

fn encode_body<S>(stream: S, encoding: HttpEncoding) -> ByteStream
where
    S: futures::Stream<Item = std::io::Result<Bytes>> + Send + 'static,
{
    let reader = BufReader::new(StreamReader::new(stream));
    match encoding {
        HttpEncoding::Identity => ReaderStream::new(reader).boxed(),
        HttpEncoding::Deflate => ReaderStream::new(DeflateEncoder::new(reader)).boxed(),
        HttpEncoding::Gzip => ReaderStream::new(GzipEncoder::new(reader)).boxed(),
        HttpEncoding::Br => ReaderStream::new(BrotliEncoder::new(reader)).boxed(),
        HttpEncoding::Zstd => ReaderStream::new(ZstdEncoder::new(reader)).boxed(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                length: Provisional(Some(123)),
                upload_id: None,
                other: UploadParams::default(),
            }
        }

        fn config(
            relay_credentials: Credentials,
            credentials: Option<UploadCredentials>,
        ) -> Config {
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
            config
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
                    .is_ok()
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

        // Can only parse provisional:
        let provisional: LocationQueryParams<Provisional> =
            serde_urlencoded::from_str(url).unwrap();
        assert!(provisional.upload_length.0.is_none());
        assert!(serde_urlencoded::from_str::<LocationQueryParams::<Final>>(url).is_err());
    }

    #[test]
    fn parse_location_complete() {
        let json = r#"signature=foo&length=123"#;

        let provisional: LocationQueryParams<Provisional> =
            serde_urlencoded::from_str(json).unwrap();
        assert_eq!(provisional.upload_length.0, Some(123));
        let full: LocationQueryParams<Final> = serde_urlencoded::from_str(json).unwrap();
        assert_eq!(full.upload_length.0, 123);
    }

    #[test]
    fn parse_location_with_other() {
        let json =
            r#"upload_signature=foo&upload_length=123&not_an_upload_param=123&upload_type=bar"#;

        let provisional: LocationQueryParams<Provisional> =
            serde_urlencoded::from_str(json).unwrap();
        insta::assert_debug_snapshot!(provisional, @r#"
        LocationQueryParams {
            upload_length: Provisional(
                Some(
                    123,
                ),
            ),
            upload_signature: "foo",
            other: UploadParams(
                {
                    "upload_type": "bar",
                },
            ),
        }
        "#);
        let full: LocationQueryParams<Final> = serde_urlencoded::from_str(json).unwrap();
        insta::assert_debug_snapshot!(full, @r#"
        LocationQueryParams {
            upload_length: Final(
                123,
            ),
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
