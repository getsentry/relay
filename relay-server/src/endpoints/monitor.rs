use axum::extract::{DefaultBodyLimit, Path, Query};
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{on, MethodFilter, MethodRouter};
use axum::{Json, RequestExt};
use bytes::Bytes;
use relay_config::Config;
use relay_event_schema::protocol::EventId;
use relay_monitors::{CheckIn, CheckInStatus};
use serde::Deserialize;
use uuid::Uuid;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::{RawContentType, RequestMeta};
use crate::service::ServiceState;

#[derive(Debug, Deserialize)]
struct MonitorPath {
    monitor_slug: String,
}

#[derive(Debug, Deserialize)]
struct MonitorQuery {
    status: CheckInStatus,
    check_in_id: Option<Uuid>,
    environment: Option<String>,
    duration: Option<f64>,
}

async fn handle<B>(
    state: ServiceState,
    content_type: RawContentType,
    meta: RequestMeta,
    Path(path): Path<MonitorPath>,
    request: Request<B>,
) -> axum::response::Result<impl IntoResponse>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send + Into<Bytes>,
    B::Error: Into<axum::BoxError>,
{
    let check_in = if content_type.as_ref().starts_with("application/json") {
        let Json(mut check_in): Json<CheckIn> = request.extract().await?;
        check_in.monitor_slug = path.monitor_slug;
        check_in
    } else {
        let Query(query): Query<MonitorQuery> = request.extract().await?;
        CheckIn {
            check_in_id: query.check_in_id.unwrap_or_default(),
            monitor_slug: path.monitor_slug,
            status: query.status,
            environment: query.environment,
            duration: query.duration,
            monitor_config: None,
            contexts: None,
        }
    };

    let json = serde_json::to_vec(&check_in).map_err(BadStoreRequest::InvalidJson)?;

    let mut envelope = Envelope::from_request(Some(EventId::new()), meta);
    let mut item = Item::new(ItemType::CheckIn);
    item.set_payload(ContentType::Json, json);
    envelope.add_item(item);

    // Never respond with a 429
    match common::handle_envelope(&state, envelope).await {
        Ok(_) | Err(BadStoreRequest::RateLimited(_)) => (),
        Err(error) => return Err(error.into()),
    };

    // Event will be proccessed by Sentry, respond with a 202
    Ok(StatusCode::ACCEPTED)
}

pub fn route<B>(config: &Config) -> MethodRouter<ServiceState, B>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send + Into<Bytes>,
    B::Error: Into<axum::BoxError>,
{
    on(MethodFilter::GET | MethodFilter::POST, handle)
        .route_layer(DefaultBodyLimit::max(config.max_event_size()))
}
