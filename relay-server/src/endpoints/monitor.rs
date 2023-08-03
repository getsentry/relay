use axum::extract::{DefaultBodyLimit, FromRequest, Path, Query};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{on, MethodFilter, MethodRouter};
use relay_common::Uuid;
use relay_config::Config;
use relay_general::protocol::EventId;
use relay_monitors::{CheckIn, CheckInStatus};
use serde::Deserialize;

use crate::endpoints::common::{self, BadStoreRequest};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
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

#[derive(Debug, FromRequest)]
#[from_request(state(ServiceState))]
struct MonitorParams {
    meta: RequestMeta,
    #[from_request(via(Path))]
    path: MonitorPath,
    #[from_request(via(Query))]
    query: MonitorQuery,
}

impl MonitorParams {
    fn extract_envelope(self) -> Result<Box<Envelope>, BadStoreRequest> {
        let Self { meta, path, query } = self;

        let mut envelope = Envelope::from_request(Some(EventId::new()), meta);

        let mut item = Item::new(ItemType::CheckIn);
        item.set_payload(
            ContentType::Json,
            serde_json::to_vec(&CheckIn {
                check_in_id: query.check_in_id.unwrap_or_default(),
                monitor_slug: path.monitor_slug,
                status: query.status,
                environment: query.environment,
                duration: query.duration,
                monitor_config: None,
                contexts: None,
            })
            .map_err(BadStoreRequest::InvalidJson)?,
        );
        envelope.add_item(item);

        Ok(envelope)
    }
}

async fn handle(
    state: ServiceState,
    params: MonitorParams,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let envelope = params.extract_envelope()?;

    // Never respond with a 429
    match common::handle_envelope(&state, envelope).await {
        Ok(_) | Err(BadStoreRequest::RateLimited(_)) => (),
        Err(error) => return Err(error),
    };

    // Event will be proccessed by Sentry, respond with a 202
    Ok(StatusCode::ACCEPTED)
}

pub fn route<B>(config: &Config) -> MethodRouter<ServiceState, B>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<axum::BoxError>,
{
    on(MethodFilter::GET | MethodFilter::POST, handle)
        .route_layer(DefaultBodyLimit::max(config.max_event_size()))
}
