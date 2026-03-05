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
use relay_base_schema::project::ProjectId;
use relay_config::Config;
use relay_quotas::Scoping;
use relay_system::{
    Addr, AsyncResponse, ConcurrentService, FromMessage, Interface, LoadShed, Sender, SimpleService,
};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;

use crate::http::{HttpError, RequestBuilder, Response};

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
    /// Upload a stream of bytes for a project.
    ///
    /// Returns the trusted identifier of the upload.
    Stream(Stream, Sender<Result<SignedLocation, Error>>),
}

impl Interface for Upload {}

/// A stream of bytes to be uploaded to objectstore or the upstream.
pub struct Stream {
    /// The organization and project the stream belongs to.
    pub scoping: Scoping,
    /// The body to be uploaded to objectstore, with length validation.
    pub stream: BoundedStream<BoxStream<'static, std::io::Result<Bytes>>>,
}

impl FromMessage<Stream> for Upload {
    type Response = AsyncResponse<Result<SignedLocation, Error>>;

    fn from_message(message: Stream, sender: Sender<Result<SignedLocation, Error>>) -> Self {
        Self::Stream(message, sender)
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
    async fn upload(&self, stream: Stream) -> Result<SignedLocation, Error> {
        match self {
            Service::Upstream { addr, timeout } => {
                let (request, rx) = UploadRequest::create(stream);
                addr.send(SendRequest(request));
                // We're already passing `timeout` to the reqwest library, but we also want to
                // limit the time spent waiting for the upstream service:
                let response = tokio::time::timeout(*timeout, rx).await???;
                SignedLocation::try_from_response(response)
            }
            #[cfg(feature = "processing")]
            Service::Objectstore { addr, config } => {
                let project_id = stream.scoping.project_id;
                let byte_counter = stream.stream.byte_counter();
                let key = addr
                    .send(stream)
                    .await
                    .map_err(|_send_error| Error::ServiceUnavailable)??
                    .into_inner();
                let length = byte_counter.get();

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
        let Upload::Stream(stream, tx) = message;
        tx.send(self.upload(stream).await)
    }
}

impl LoadShed<Upload> for Service {
    fn handle_loadshed(&self, Upload::Stream(_, tx): Upload) {
        tx.send(Err(Error::LoadShed));
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
    pub project_id: ProjectId,
    pub key: String,
    pub length: usize,
}

impl Location {
    fn as_uri(&self) -> String {
        let Location {
            project_id,
            key,
            length,
        } = self;
        format!("/api/{project_id}/upload/{key}/?length={length}")
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

        Ok(SignedLocation::Local {
            location: self,
            signature,
        })
    }
}

/// A verifiable [`Location`] signed by this Relay or an upstream Relay.
#[derive(Debug)]
pub enum SignedLocation {
    FromUpstream(HeaderValue),
    Local {
        location: Location,
        signature: Signature,
    },
}

impl SignedLocation {
    /// Converts the location into an URI for future reference.
    pub fn into_header_value(self) -> Result<HeaderValue, Error> {
        let header = match self {
            SignedLocation::FromUpstream(value) => value,
            SignedLocation::Local {
                location,
                signature,
            } => {
                let mut uri = location.as_uri();
                uri.push_str("&signature=");
                uri.push_str(&signature.to_string());
                HeaderValue::from_str(&uri).map_err(|_| Error::Internal)?
            }
        };
        Ok(header)
    }

    fn try_from_response(response: Response) -> Result<Self, Error> {
        match response.status() {
            status if status.is_success() => {
                let location = response
                    .headers()
                    .get(hyper::header::LOCATION)
                    .ok_or(Error::InvalidLocation)?;
                Ok(Self::FromUpstream(location.clone()))
            }
            status => Err(Error::Upstream(status)),
        }
    }
}

/// An upstream request made to the `/upload` endpoint.
struct UploadRequest {
    scoping: Scoping,
    timeout: Option<Duration>,
    body: Option<BoundedStream<BoxStream<'static, std::io::Result<Bytes>>>>,
    sender: oneshot::Sender<Result<Response, UpstreamRequestError>>,
}

impl UploadRequest {
    fn create(
        stream: Stream,
    ) -> (
        Self,
        oneshot::Receiver<Result<Response, UpstreamRequestError>>,
    ) {
        let (sender, rx) = oneshot::channel();
        let Stream { scoping, stream } = stream;

        (
            Self {
                scoping,
                timeout: None, // will be set by `configure()`
                body: Some(stream),
                sender,
            },
            rx,
        )
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
        Method::POST
    }

    fn path(&self) -> std::borrow::Cow<'_, str> {
        let project_id = self.scoping.project_id;
        format!("/api/{project_id}/upload/").into()
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
        let Some(body) = self.body.take() else {
            relay_log::error!("upload request was retried or never initialized");
            return Err(HttpError::Misconfigured);
        };

        let project_key = self.scoping.project_key;
        builder.header("X-Sentry-Auth", format!("Sentry sentry_key={project_key}"));
        let upload_length = (body.lower_bound == body.upper_bound).then_some(body.lower_bound);
        for (key, value) in tus::request_headers(upload_length) {
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
