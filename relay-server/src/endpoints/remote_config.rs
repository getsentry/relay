use axum::http::StatusCode;
use axum::response::IntoResponse;
use google_cloud_storage::client::Client;
use google_cloud_storage::client::ClientConfig;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;

use crate::service::ServiceState;

pub async fn handle(state: ServiceState) -> impl IntoResponse {
    // Configs are tied to the DSN: configuration/<dsn-public-key>/<environment>
    // For now we default to production, environment scoping will be handled later.
    let maybe_key = state
        .config()
        .public_key()
        .map(|v| format!("configuration/{}/production", v));

    // Is the DSN ever null? Why is it optional?
    if let Some(key) = maybe_key {
        if let Some(response) = fetch_from_public_bucket(key).await {
            return (StatusCode::OK, axum::Json(response));
        }
    }
    (StatusCode::NOT_FOUND, axum::Json(Vec::new()))
}

// Half-implementation of what needs to happen to support private bucket access. In reality we'll
// want to create a signed-url and make an HTTP request to a CDN URL. However, this form of CDN
// access has not been configured for Sentry yet so we fallback to plain bucket access. This
// doesn't have a latency impact but does have a COGS impact. This state of affair should be
// sufficient for a proof of concept or internal testing but should not remain for the private
// alpha.
async fn fetch_from_private_bucket(key: String) -> Option<Vec<u8>> {
    // TODO: Log if credentials do not exist?
    // TODO: Should this be in some global and passed into this function?
    let config = ClientConfig::default().with_auth().await.ok()?;
    let client = Client::new(config);

    // TODO: Log if an error is encountered.
    // TODO: Stream response to client rather than waiting for the download to complete.
    client
        .download_object(
            &GetObjectRequest {
                bucket: "remote-config-cdn-private".to_string(),
                object: key,
                ..Default::default()
            },
            &Range::default(),
        )
        .await
        .ok()
}

// Half-implementation of what needs to happen to support public bucket access. In reality we'll
// make an HTTP request to a public CDN URL. The same implications for private bucket access apply
// here. No latency impact. Slight COGS impact. Unwise to ship to customers.
async fn fetch_from_public_bucket(key: String) -> Option<Vec<u8>> {
    // TODO: This isn't a CDN url. The CDN url is an ip-address that would need to be
    // configured per PoP region.
    let url = format!(
        "https://storage.googleapis.com/remote-config-cdn-testing/{}",
        key
    );

    // TODO: Log if an error is encountered.
    // TODO: Stream response to client rather than waiting for the download to complete.
    match reqwest::get(url).await {
        Ok(response) => match response.bytes().await {
            Ok(bytes) => Some(bytes.to_vec()),
            Err(_) => None,
        },
        Err(_) => None,
    }
}
