//! Utilities for uploading large files.

use std::fmt;

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
use relay_system::Addr;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;

use crate::http::{HttpError, RequestBuilder, Response};
use crate::service::ServiceState;
#[cfg(feature = "processing")]
use crate::services::upload::{Error as ServiceError, Upload};
use crate::services::upstream::{
    SendRequest, UpstreamRelay, UpstreamRequest, UpstreamRequestError,
};
use crate::utils::{ExactStream, tus};

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
    #[cfg_attr(not(feature = "processing"), expect(unused))]
    #[error("failed to sign location")]
    SigningFailed,
    #[cfg_attr(not(feature = "processing"), expect(unused))]
    #[error("service unavailable")]
    ServiceUnavailable,
    #[cfg(feature = "processing")]
    #[error("upload service: {0}")]
    UploadService(ServiceError),
    #[error("internal error")]
    Internal,
}

/// A stream of bytes to be uploaded to objectstore or the upstream.
pub struct Stream {
    /// The organization and project the stream belongs to.
    pub scoping: Scoping,
    /// The body to be uploaded to objectstore, with length validation.
    pub stream: ExactStream<BoxStream<'static, std::io::Result<Bytes>>>,
}

/// A dispatcher for uploading large files.
///
/// Uploads go to either the upstream relay or objectstore.
pub enum Sink {
    Upstream(Addr<UpstreamRelay>),
    #[cfg(feature = "processing")]
    Upload(Addr<Upload>),
}

impl Sink {
    /// Creates a new upload dispatcher.
    pub fn new(state: &ServiceState) -> Self {
        #[cfg(feature = "processing")]
        if let Some(addr) = state.upload() {
            return Self::Upload(addr.clone());
        }
        Self::Upstream(state.upstream_relay().clone())
    }

    /// Uploads a given stream and returns the upload's identifier upon success.
    pub async fn upload(&self, config: &Config, stream: Stream) -> Result<SignedLocation, Error> {
        match self {
            Sink::Upstream(addr) => {
                let (request, response_channel) = UploadRequest::create(stream);
                addr.send(SendRequest(request));
                let response =
                    tokio::time::timeout(config.http_timeout(), response_channel).await???;
                SignedLocation::try_from_response(response)
            }
            #[cfg(feature = "processing")]
            Sink::Upload(addr) => {
                let project_id = stream.scoping.project_id;
                let length = stream.stream.expected_length();
                let key = addr
                    .send(stream)
                    .await
                    .map_err(|_send_error| Error::ServiceUnavailable)?
                    .map_err(Error::UploadService)?
                    .into_inner();

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
    #[cfg_attr(not(feature = "processing"), expect(unused))]
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
    body: Option<ExactStream<BoxStream<'static, std::io::Result<Bytes>>>>,
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
        for (key, value) in tus::request_headers(body.expected_length()) {
            let Some(key) = key else { continue };
            builder.header(key, value);
        }

        builder.body(reqwest::Body::wrap_stream(body));

        Ok(())
    }
}
