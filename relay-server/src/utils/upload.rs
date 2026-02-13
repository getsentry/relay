//! Utilities for uploading large files.

use axum::response::IntoResponse;
use bytes::Bytes;
use futures::stream::BoxStream;
use hyper::http::{Method, StatusCode};
use relay_auth::Signature;
use relay_base_schema::project::ProjectId;
use relay_config::Config;
use relay_quotas::Scoping;
use relay_system::Addr;

use crate::service::ServiceState;
use crate::services::upload::{Upload, UploadKey};
use crate::services::upstream::UpstreamRelay;
use crate::utils::{ExactStream, ForwardError, ForwardRequest, ForwardResponse};

/// An error that occurs during upload.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("forwarding failed: {0}")]
    Forward(#[from] ForwardError),
    #[error("upstream response: {0}")]
    Upstream(StatusCode),
    #[error("upstream provided invalid location")]
    InvalidLocation,
    #[error("failed to sign location")]
    SigningFailed,
    #[error("internal server error")]
    Internal,

    #[error("service unavailable")]
    ServiceUnavailable,
    #[error("upload service")]
    UploadService,
}

/// A stream of bytes to be uploaded to objectstore or the upstream.
pub struct Stream {
    /// The organization and project the stream belongs to.
    pub scoping: Scoping,
    /// The body to be uploaded to objectstore, with length validation.
    pub stream: ExactStream<BoxStream<'static, std::io::Result<Bytes>>>,
}

/// An dispatcher for uploading large files.
///
/// Uploads go to either the upstream relay or objectstore.
pub enum Sink {
    Upstream(Addr<UpstreamRelay>),
    Upload(Addr<Upload>),
}

impl Sink {
    fn new(state: &ServiceState) -> Self {
        if let Some(addr) = state.upload() {
            Self::Upload(addr.clone())
        } else {
            Self::Upstream(state.upstream_relay().clone())
        }
    }

    async fn upload(&self, config: &Config, stream: Stream) -> Result<SignedLocation, Error> {
        match self {
            Sink::Upstream(addr) => {
                let Stream { scoping, stream } = stream;
                let project_id = scoping.project_id;
                let path = format!("/api/{project_id}/upload/");
                let response = ForwardRequest::builder(Method::POST, path)
                    .with_body(axum::body::Body::from_stream(stream))
                    .send_to(addr)
                    .await?;
                SignedLocation::try_from_response(response)
            }
            Sink::Upload(addr) => {
                let project_id = stream.scoping.project_id;
                let length = stream.stream.expected_length();
                let key = addr
                    .send(stream)
                    .await
                    .map_err(|_send_error| Error::ServiceUnavailable)?
                    .map_err(|_| Error::UploadService)?;

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
pub struct Location {
    project_id: ProjectId,
    key: UploadKey,
    length: usize,
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

    fn try_sign(self, config: &Config) -> Result<SignedLocation, Error> {
        let uri = self.as_uri();
        let signature = config
            .credentials()
            .ok_or(Error::SigningFailed)?
            .secret_key
            .sign(uri.as_bytes());

        Ok(SignedLocation::Local {
            location: self,
            signature,
        })
    }
}

pub enum SignedLocation {
    FromUpstream(String),
    Local {
        location: Location,
        signature: Signature,
    },
}

impl SignedLocation {
    fn try_from_response(response: ForwardResponse) -> Result<Self, Error> {
        let response = response.into_response();
        match response.status() {
            status if status.is_success() => {
                let location = response
                    .headers()
                    .get(hyper::header::LOCATION)
                    .ok_or(Error::InvalidLocation)?;
                let location = location.to_str().map_err(|_| Error::InvalidLocation)?;
                Ok(Self::FromUpstream(location.to_owned()))
            }
            status => Err(Error::Upstream(status)),
        }
    }

    fn into_string(self) -> String {
        match self {
            SignedLocation::FromUpstream(value) => value,
            SignedLocation::Local {
                location,
                signature,
            } => {
                let mut uri = location.as_uri();
                uri.push_str("&signature=");
                uri.push_str(&signature.to_string());
                uri
            }
        }
    }
}
