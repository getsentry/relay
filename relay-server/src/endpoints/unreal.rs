use axum::extract::{DefaultBodyLimit, FromRequest, Query};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use bytes::Bytes;
use relay_config::Config;
use relay_event_schema::protocol::EventId;
use serde::Deserialize;

use crate::constants::UNREAL_USER_HEADER;
use crate::endpoints::common::{self, BadStoreRequest, TextResponse};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::middlewares;
use crate::service::ServiceState;
use crate::services::outcome::DiscardItemType;
use crate::utils;

#[derive(Debug, Deserialize)]
struct UnrealQuery {
    #[serde(rename = "UserID")]
    user_id: Option<String>,
}

#[derive(Debug, FromRequest)]
#[from_request(state(ServiceState))]
struct UnrealParams {
    meta: RequestMeta,
    #[from_request(via(Query))]
    query: UnrealQuery,
    data: Bytes,
}

impl UnrealParams {
    fn extract_envelope(self, state: &ServiceState) -> Result<Box<Envelope>, BadStoreRequest> {
        let Self { meta, query, data } = self;

        if data.is_empty() {
            return Err(BadStoreRequest::EmptyBody);
        }

        let mut envelope = Envelope::from_request(Some(EventId::new()), meta);

        let global_config = state.global_config_handle().current();
        let project_id = envelope.meta().project_id().map(|p| p.value()).unwrap_or(0);

        match utils::is_rolled_out(
            project_id,
            global_config.options.unreal_report_expansion_rollout_rate,
        )
        .is_keep()
        {
            true => {
                // FIXME: Add more fine-grained BadStoreRequest.
                for mut item in utils::expand_unreal(data, state.config())
                    .map_err(|_| BadStoreRequest::Overflow(DiscardItemType::UnrealReport))?
                    .attachments
                {
                    item.set_unreal_expanded();
                    envelope.add_item(item);
                }
            }
            false => {
                let mut item = Item::new(ItemType::UnrealReport);
                item.set_payload(ContentType::OctetStream, data);
                envelope.add_item(item);
            }
        }

        if let Some(user_id) = query.user_id {
            envelope.set_header(UNREAL_USER_HEADER, user_id);
        }

        Ok(envelope)
    }
}

async fn handle(
    state: ServiceState,
    params: UnrealParams,
) -> Result<impl IntoResponse, BadStoreRequest> {
    let envelope = params.extract_envelope(&state)?;
    let id = envelope.event_id();

    // Never respond with a 429 since clients often retry these
    match common::handle_envelope(&state, envelope)
        .await
        .map_err(|err| err.into_inner())
    {
        Ok(_) | Err(BadStoreRequest::RateLimited(_)) => (),
        Err(error) => return Err(error),
    };

    // The return here is only useful for consistency because the UE4 crash reporter doesn't
    // care about it.
    Ok(TextResponse(id))
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle)
        .route_layer(DefaultBodyLimit::max(config.max_attachments_size()))
        .route_layer(axum::middleware::from_fn(middlewares::content_length))
}
