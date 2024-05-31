use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use bytes::Bytes;

use crate::service::ServiceState;

pub async fn handle(state: ServiceState, Path(project_id): Path<u64>) -> impl IntoResponse {
    // TODO: Check if the feature is enabled.
    // let config_enabled = project_state.has_feature(Feature::RemoteConfig);

    let url = format!(
        "https://sentry.io/api/0/relays/{}/configuration/",
        project_id
    );

    // TODO: Set "HTTP_X_SENTRY_RELAY_ID" header in Sentry request.
    if let Ok(response) = reqwest::get(url).await {
        let status = response.status();

        if let Ok(bytes) = response.bytes().await {
            return (
                status,
                [("ETag", "1"), ("Cache-Control", "public, max-age=3600")],
                bytes,
            );
        }
    }

    (
        StatusCode::NOT_FOUND,
        [("ETag", "1"), ("Cache-Control", "no-store")],
        Bytes::new(),
    )
}
