use axum::http::StatusCode;
use axum::response::IntoResponse;
use google_cloud_storage::client::Client;
use google_cloud_storage::client::ClientConfig;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;

use crate::service::ServiceState;

pub async fn handle(state: ServiceState) -> impl IntoResponse {
    // This is temporarily placed here. I have no idea if we'll want to cache this client
    // instance in some way.
    //
    // This should never fail so I'm unwrapping. If it does fail what would the appropriate
    // fallback handling be? The product doesn't function without a client connection.
    let config = ClientConfig::default().with_auth().await.unwrap();
    let client = Client::new(config);

    // Configs are tied to the DSN: configuration/<dsn-public-key>/<environment>
    // For now we default to production, environment scoping will be handled later.
    let filepath = state
        .config()
        .public_key()
        .map(|v| format!("configuration/{}/production", v));

    // Is the DSN ever null? Why is it optional?
    if let Some(fp) = filepath {
        // This is temporary!
        //
        // As a proof of concept we'll reach out to GCS for our internal testing.
        //
        // In the real production app we won't reach out to GCS directly. We'll proxy Cloud CDN.
        // We'll likely still need a gcp client to create a signed url. Unless the bucket
        // is public in which case we'll have a configured url per PoP region. I'm not totally
        // sure at the moment how private bucket cdn-url signing works. Area of active
        // exploration. But the docs say it works!
        //
        // Errors are ignored! Maybe we should log them?
        if let Ok(data) = client
            .download_object(
                &GetObjectRequest {
                    bucket: "bucket".to_string(),
                    object: fp,
                    ..Default::default()
                },
                &Range::default(),
            )
            .await
        {
            return (StatusCode::OK, axum::Json(data));
        }
    }
    return (StatusCode::NOT_FOUND, axum::Json(Vec::new()));
}
