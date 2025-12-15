use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use relay_auth::{RegisterRequest, RegisterResponse};
use tokio::time::error::Elapsed;

use crate::service::ServiceState;
use crate::services::upstream::{SendQuery, UpstreamQuery, UpstreamRequestError};

#[derive(Debug, thiserror::Error)]
#[error("error while forwarding challenge: {0}")]
enum RegisterError {
    Upstream(#[from] UpstreamRequestError),
    SendError(#[from] relay_system::SendError),
    Timeout(#[from] Elapsed),
}

impl IntoResponse for RegisterError {
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::Upstream(upstream) => {
                if let Some(status) = upstream.status_code()
                    && status.is_client_error()
                {
                    return status.into_response();
                }

                StatusCode::BAD_GATEWAY.into_response()
            }
            Self::SendError(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            Self::Timeout(_) => StatusCode::REQUEST_TIMEOUT.into_response(),
        }
    }
}

async fn handle<T: UpstreamQuery + 'static>(
    state: ServiceState,
    query: T,
) -> Result<Json<T::Response>, RegisterError> {
    let response = tokio::time::timeout(
        state.config().http_timeout(),
        state.upstream_relay().send(SendQuery(query)),
    )
    .await???;

    Ok(Json(response))
}

/// Forwards a challenge request to the upstream.
pub async fn challenge(
    state: ServiceState,
    Json(body): Json<RegisterRequest>,
) -> impl IntoResponse {
    handle(state, body).await
}

/// Forwards a challenge response to the upstream.
pub async fn response(
    state: ServiceState,
    Json(body): Json<RegisterResponse>,
) -> impl IntoResponse {
    handle(state, body).await
}
