use axum::extract::{DefaultBodyLimit, FromRequest, Path, Query};
use axum::response::IntoResponse;
use axum::routing::{post, MethodRouter};
use relay_common::Uuid;
use relay_config::Config;
use relay_general::protocol::EventId;
use relay_monitors::{CheckIn, CheckInStatus};
use serde::Deserialize;

use crate::endpoints::common::{self, BadStoreRequest, TextResponse};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::service::ServiceState;

#[derive(Debug, Deserialize)]
struct CronQuery {
    status: CheckInStatus,
    check_in_id: Option<Uuid>,
    environment: Option<String>,
    duration: Option<f64>,
}

#[derive(Debug, FromRequest)]
#[from_request(state(ServiceState))]
struct CronParams {
    meta: RequestMeta,
    monitor_slug: Path<String>,
    #[from_request(via(Query))]
    query: CronQuery,
}

impl CronParams {
    fn extract_envelope(self) -> Result<Box<Envelope>, BadStoreRequest> {
        let Self {
            meta,
            monitor_slug,
            query,
        } = self;

        let mut envelope = Envelope::from_request(Some(EventId::new()), meta);

        let mut item = Item::new(ItemType::CheckIn);
        item.set_payload(
            ContentType::Json,
            serde_json::to_vec(&CheckIn {
                check_in_id: query.check_in_id.unwrap_or_default(),
                monitor_slug: monitor_slug.0,
                status: query.status,
                environment: query.environment,
                duration: query.duration,
                monitor_config: None,
            })
            .map_err(BadStoreRequest::InvalidJson)?,
        );
        envelope.add_item(item);

        Ok(envelope)
    }
}

async fn handle(
    state: ServiceState,
    params: CronParams,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let envelope = params.extract_envelope()?;

    // Never respond with a 429
    match common::handle_envelope(&state, envelope).await {
        Ok(_) | Err(BadStoreRequest::RateLimited(_)) => (),
        Err(error) => return Err(error),
    };

    // What do we want to return?
    Ok(TextResponse(None))
}

pub fn route<B>(config: &Config) -> MethodRouter<ServiceState, B>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<axum::BoxError>,
{
    post(handle).route_layer(DefaultBodyLimit::max(config.max_attachments_size()))
}
