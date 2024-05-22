use axum::http::StatusCode;
use axum::response::IntoResponse;
use google_cloud_storage::client::Client;
use google_cloud_storage::client::ClientConfig;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};
use google_cloud_storage::http::Error;
use google_cloud_storage::sign::SignBy;
use google_cloud_storage::sign::SignedURLMethod;
use google_cloud_storage::sign::SignedURLOptions;

use crate::service::ServiceState;

pub async fn handle(state: ServiceState) -> impl IntoResponse {
    // This is psuedo-code.  I have no idea how we want to handle this (manual credentials,
    // cached client, whatever)
    let config = ClientConfig::default().with_auth().await.unwrap();
    let client = Client::new(config);

    // Configs are tied to the DSN: configuration/<dsn-public-key>/environment
    // For now we default to production, environment scoping will be handled later.
    let filepath = state
        .config()
        .public_key()
        .map(|v| format!("configuration/{}/production", v));

    // Is the DSN ever null? Why is it optional?
    if let Some(fp) = filepath {
        // TODO: Should handle errors here!
        let data = client
            .download_object(
                &GetObjectRequest {
                    bucket: "bucket".to_string(),
                    object: fp,
                    ..Default::default()
                },
                &Range::default(),
            )
            .await
            .unwrap();

        return (StatusCode::OK, axum::Json(data));
    } else {
        // Presumably gcs returns &[u8] so what should we return here?
        return (StatusCode::NOT_FOUND, axum::Json("Not found"));
    }
}
