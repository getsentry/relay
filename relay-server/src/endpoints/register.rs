use axum::http::HeaderMap;
use axum::response::IntoResponse;
use bytes::Bytes;

use crate::service::ServiceState;
use crate::services::upstream::Method;
use crate::utils::ForwardRequest;

/// Forwards a challenge request to the upstream.
pub async fn challenge(state: ServiceState, headers: HeaderMap, body: Bytes) -> impl IntoResponse {
    ForwardRequest::builder(Method::POST, "/api/0/relays/register/challenge/")
        .with_name("forward-register")
        .with_headers(headers)
        .with_body(body)
        .with_config(state.config())
        .send_to(state.upstream_relay())
        .await
}

/// Forwards a challenge response to the upstream.
pub async fn response(state: ServiceState, headers: HeaderMap, body: Bytes) -> impl IntoResponse {
    ForwardRequest::builder(Method::POST, "/api/0/relays/register/response/")
        .with_name("forward-register")
        .with_headers(headers)
        .with_body(body)
        .with_config(state.config())
        .send_to(state.upstream_relay())
        .await
}
