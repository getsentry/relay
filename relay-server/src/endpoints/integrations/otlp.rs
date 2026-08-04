use axum::extract::DefaultBodyLimit;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, post};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse;
use relay_config::Config;
use relay_dynamic_config::Feature;

use crate::endpoints::common;
use crate::envelope::ContentType;
use crate::extractors::IntegrationBuilderRejection;
use crate::extractors::{IntegrationBuilder, RawContentType};
use crate::integrations::{LogsIntegration, OtelFormat, SpansIntegration};
use crate::managed::Rejected;
use crate::service::ServiceState;

/// All routes configured for the OTLP integration.
///
/// The integration currently supports the following endpoints:
///  - V1 Traces
///  - V1 Logs
pub fn routes(config: &Config) -> axum::Router<ServiceState> {
    axum::Router::new()
        .route("/v1/traces", traces::route(config))
        .route("/v1/traces/", traces::route(config))
        .route("/v1/logs", logs::route(config))
        .route("/v1/logs/", logs::route(config))
}

mod traces {
    use super::*;

    async fn handle(
        content_type: RawContentType,
        state: ServiceState,
        builder: Result<IntegrationBuilder, IntegrationBuilderRejection>,
    ) -> Result<SuccessResponse<ExportTraceServiceResponse>, OtlpError> {
        let format = match content_type.as_ref().parse::<ContentType>() {
            Ok(ContentType::Json) => OtelFormat::Json,
            Ok(ContentType::Protobuf) => OtelFormat::Protobuf,
            _ => return Err(OtlpError::unsupported_content_type(content_type)),
        };

        let envelope = builder
            .map_err(|err| OtlpError::from_err(format, err))?
            .with_type(SpansIntegration::OtelV1 { format })
            .build();

        common::handle_envelope(&state, envelope)
            .await
            .map_err(Rejected::into_inner)
            .and_then(|r| r.check_rate_limits())
            .map_err(|err| OtlpError::from_err(format, err))?;

        Ok(SuccessResponse::new(format))
    }

    pub fn route(config: &Config) -> MethodRouter<ServiceState> {
        post(handle).route_layer(DefaultBodyLimit::max(config.max_container_size()))
    }
}

mod logs {
    use super::*;

    async fn handle(
        content_type: RawContentType,
        state: ServiceState,
        builder: Result<IntegrationBuilder, IntegrationBuilderRejection>,
    ) -> Result<SuccessResponse<ExportLogsServiceResponse>, OtlpError> {
        let format = match content_type.as_ref().parse::<ContentType>() {
            Ok(ContentType::Json) => OtelFormat::Json,
            Ok(ContentType::Protobuf) => OtelFormat::Protobuf,
            _ => return Err(OtlpError::unsupported_content_type(content_type)),
        };

        let envelope = builder
            .map_err(|err| OtlpError::from_err(format, err))?
            .with_type(LogsIntegration::OtelV1 { format })
            .with_required_feature(Feature::OurLogsIngestion)
            .build();

        common::handle_envelope(&state, envelope)
            .await
            .map_err(Rejected::into_inner)
            .and_then(|r| r.check_rate_limits())
            .map_err(|err| OtlpError::from_err(format, err))?;

        Ok(SuccessResponse::new(format))
    }

    pub fn route(config: &Config) -> MethodRouter<ServiceState> {
        post(handle).route_layer(DefaultBodyLimit::max(config.max_container_size()))
    }
}

struct SuccessResponse<T> {
    format: OtelFormat,
    _proto: std::marker::PhantomData<T>,
}

impl<T> SuccessResponse<T> {
    fn new(format: OtelFormat) -> Self {
        Self {
            format,
            _proto: std::marker::PhantomData,
        }
    }
}

impl<T> IntoResponse for SuccessResponse<T>
where
    T: prost::Message,
    T: Default,
{
    fn into_response(self) -> Response {
        match self.format {
            OtelFormat::Json => {
                let empty = serde_json::Value::Object(Default::default());
                axum::Json(empty).into_response()
            }
            OtelFormat::Protobuf => Protobuf(T::default()).into_response(),
        }
    }
}

/// Protobuf-compatible subset of `google.rpc.Status`.
#[derive(Clone, PartialEq, Eq, serde::Serialize, prost::Message)]
#[serde(rename_all = "camelCase")]
struct OtlpStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

#[repr(i32)]
#[derive(Clone, Copy, Debug)]
enum GrpcStatusCode {
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    Unauthenticated = 16,
}

impl GrpcStatusCode {
    fn from_http_status(status: StatusCode) -> Self {
        match status {
            StatusCode::BAD_REQUEST | StatusCode::UNSUPPORTED_MEDIA_TYPE => Self::InvalidArgument,
            StatusCode::UNAUTHORIZED => Self::Unauthenticated,
            StatusCode::FORBIDDEN => Self::PermissionDenied,
            StatusCode::NOT_FOUND => Self::NotFound,
            StatusCode::PAYLOAD_TOO_LARGE | StatusCode::TOO_MANY_REQUESTS => {
                Self::ResourceExhausted
            }
            StatusCode::NOT_IMPLEMENTED => Self::Unimplemented,
            StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE => Self::Unavailable,
            StatusCode::GATEWAY_TIMEOUT => Self::DeadlineExceeded,
            status if status.is_server_error() => Self::Internal,
            _ => Self::Unknown,
        }
    }
}
struct OtlpError {
    format: OtelFormat,
    status: StatusCode,
    headers: header::HeaderMap,
    message: String,
}

impl OtlpError {
    fn unsupported_content_type(content_type: RawContentType) -> Self {
        Self {
            format: OtelFormat::Json,
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            headers: Default::default(),
            message: format!("unsupported content type: {content_type}"),
        }
    }

    fn from_err<E>(format: OtelFormat, err: E) -> Self
    where
        E: IntoResponse,
        E: std::fmt::Display,
    {
        let message = err.to_string();
        let (parts, _) = err.into_response().into_parts();

        Self {
            format,
            status: parts.status,
            headers: parts.headers,
            message,
        }
    }
}

impl IntoResponse for OtlpError {
    fn into_response(mut self) -> Response {
        let status = OtlpStatus {
            code: GrpcStatusCode::from_http_status(self.status) as i32,
            message: self.message,
        };

        let mut response = match self.format {
            OtelFormat::Json => axum::Json(status).into_response(),
            OtelFormat::Protobuf => Protobuf(status).into_response(),
        };

        *response.status_mut() = self.status;

        self.headers.remove(header::CONTENT_LENGTH);
        self.headers.remove(header::CONTENT_TYPE);
        response.headers_mut().extend(self.headers);

        response
    }
}

struct Protobuf<T>(T);

impl<T> IntoResponse for Protobuf<T>
where
    T: prost::Message,
{
    fn into_response(self) -> Response {
        let body = self.0.encode_to_vec();
        let mut res = axum::body::Body::from(body).into_response();
        res.headers_mut().insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/x-protobuf"),
        );
        res
    }
}
