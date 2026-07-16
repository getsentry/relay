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
use crate::endpoints::minidump::is_gpu_crash_item;
use crate::envelope::{ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::middlewares;
use crate::service::ServiceState;
use crate::services::outcome::{DiscardItemType, DiscardReason};
use crate::services::processor::ProcessingError;
use crate::services::projects::project::ProjectState;
use crate::statsd::RelayCounters;
use crate::utils::extract_items;

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
    /// Returns the envelope and whether the org may split off a GPU crash event.
    async fn extract_envelope(
        self,
        state: &ServiceState,
    ) -> Result<(Box<Envelope>, bool), BadStoreRequest> {
        let Self { meta, query, data } = self;

        if data.is_empty() {
            return Err(BadStoreRequest::EmptyBody);
        }

        let public_key = meta.public_key();
        let mut envelope = Envelope::from_request(Some(EventId::new()), meta);

        if let Some(user_id) = query.user_id {
            envelope.set_header(UNREAL_USER_HEADER, user_id);
        }

        let global_config = state.global_config_handle().current().unwrap_or_default();

        if global_config.options.endpoint_fetch_config_enabled {
            // Ensure that we really make it here.
            relay_statsd::metric!(counter(RelayCounters::UnrealEndpointExpansion) += 1);

            let project = state
                .project_cache_handle()
                .ready(public_key, state.config().query_timeout())
                .await
                .ok_or(BadStoreRequest::ProjectUnavailable)?;

            let project_config = match project.state() {
                ProjectState::Enabled(info) => Some(info.clone()),
                // Note: In Proxy mode we should never make it here since the endpoint_fetch_config_enabled
                // check should already fail.
                ProjectState::Dummy => None,
                ProjectState::Disabled | ProjectState::Pending => {
                    return Err(BadStoreRequest::EventRejected(DiscardReason::ProjectId));
                }
            };

            if let Some(project_config) = project_config
                && project_config.has_feature(Feature::UnrealEndpointExpansion)
            {
                for mut item in
                    extract_items(data, state.config()).map_err(|error| match error {
                        ProcessingError::PayloadTooLarge(_) => {
                            BadStoreRequest::ItemTooLarge(DiscardItemType::UnrealReport)
                        }
                        _ => BadStoreRequest::InvalidUnrealReport,
                    })?
                {
                    item.set_unreal_expanded(true);
                    envelope.add_item(item);
                }
                let gpu_crash_split = project_config.has_feature(Feature::NvGpuCrashSplit);
                return Ok((envelope, gpu_crash_split));
            }
        }

        let mut item = Item::new(ItemType::UnrealReport);
        item.set_payload(ContentType::OctetStream, data);
        envelope.add_item(item);

        Ok((envelope, false))
    }
}

async fn handle(
    state: ServiceState,
    params: UnrealParams,
) -> axum::response::Result<impl IntoResponse> {
    let (mut envelope, gpu_crash_split) = params.extract_envelope(&state).await?;
    let id = envelope.event_id();

    // Gated on the org feature: only opted-in orgs split the GPU crash off the
    // expanded report into a second (billed) event.
    if gpu_crash_split {
        let scope_event = envelope
            .get_item_by(|item| item.ty() == &ItemType::Event)
            .cloned();
        let gpu_items = envelope.take_items_by(is_gpu_crash_item);
        if !gpu_items.is_empty() {
            let gpu_event_id = EventId::new();
            let mut gpu_envelope =
                Envelope::from_request(Some(gpu_event_id), envelope.meta().clone());
            if let Some(scope_event) = scope_event {
                gpu_envelope.add_item(scope_event);
            }
            for item in gpu_items {
                gpu_envelope.add_item(item);
            }

            if let Ok(handled) = common::handle_envelope(&state, gpu_envelope).await {
                handled.ignore_rate_limits();
            }
        }
    }

    // Never respond with a 429 since clients often retry these
    common::handle_envelope(&state, envelope)
        .await?
        .ignore_rate_limits();

    // The return here is only useful for consistency because the UE4 crash reporter doesn't
    // care about it.
    Ok(TextResponse(id))
}

pub fn route(config: &Config) -> MethodRouter<ServiceState> {
    post(handle)
        .route_layer(DefaultBodyLimit::max(config.max_attachments_size()))
        .route_layer(axum::middleware::from_fn(middlewares::content_length))
}
