use axum::extract::{DefaultBodyLimit, FromRequest, Query};
use axum::response::IntoResponse;
use axum::routing::{MethodRouter, post};
use bytes::Bytes;
use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_event_schema::protocol::EventId;
use serde::Deserialize;

use crate::constants::UNREAL_USER_HEADER;
use crate::endpoints::common::{self, BadStoreRequest, TextResponse};
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::middlewares;
use crate::service::ServiceState;
use crate::services::outcome::{DiscardItemType, DiscardReason};
use crate::services::processor::ProcessingError;
use crate::utils::{self, extract_items};

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
    async fn extract_envelope(
        self,
        state: &ServiceState,
    ) -> Result<Box<Envelope>, BadStoreRequest> {
        let Self { meta, query, data } = self;

        if data.is_empty() {
            return Err(BadStoreRequest::EmptyBody);
        }

        let public_key = meta.public_key();
        let mut envelope = Envelope::from_request(Some(EventId::new()), meta);

        if let Some(user_id) = query.user_id {
            envelope.set_header(UNREAL_USER_HEADER, user_id);
        }

        let global_config = state.global_config_handle().current();
        let project_id = envelope.meta().project_id().map(|p| p.value()).unwrap_or(0);
        let endpoint_expansion_rolled_out = utils::is_rolled_out(
            project_id,
            global_config.options.unreal_report_expansion_rollout_rate,
        )
        .is_keep();

        if endpoint_expansion_rolled_out {
            let project = state
                .project_cache_handle()
                .ready(public_key, state.config().query_timeout())
                .await
                .ok_or(BadStoreRequest::ProjectUnavailable)?;

            let project_config = project
                .state()
                .clone()
                .enabled()
                .ok_or(BadStoreRequest::EventRejected(DiscardReason::ProjectId))?;

            if project_config.has_feature(Feature::UnrealEndpointExpansion) {
                for mut item in
                    extract_items(data, state.config()).map_err(|error| match error {
                        ProcessingError::PayloadTooLarge(_) => {
                            BadStoreRequest::Overflow(DiscardItemType::UnrealReport)
                        }
                        _ => BadStoreRequest::InvalidUnrealReport,
                    })?
                {
                    item.set_unreal_expanded(true);
                    envelope.add_item(item);
                }
                return Ok(envelope);
            }
        }

        let mut item = Item::new(ItemType::UnrealReport);
        item.set_payload(ContentType::OctetStream, data);
        envelope.add_item(item);

        Ok(envelope)
    }
}

async fn handle(
    state: ServiceState,
    params: UnrealParams,
) -> axum::response::Result<impl IntoResponse> {
    let envelope = params.extract_envelope(&state).await?;
    let id = envelope.event_id();

    // Never respond with a 429 since clients often retry these
    common::handle_envelope(&state, envelope)
        .await?
        .silence_rate_limits();

    // The return here is only useful for consistency because the UE4 crash reporter doesn't
    // care about it.
    Ok(TextResponse(id))
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle)
        .route_layer(DefaultBodyLimit::max(config.max_attachments_size()))
        .route_layer(axum::middleware::from_fn(middlewares::content_length))
}
