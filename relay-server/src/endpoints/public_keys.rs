use std::collections::HashMap;

use axum::response::IntoResponse;
use futures::future;

use crate::endpoints::common::ServiceUnavailable;
use crate::extractors::SignedJson;
use crate::service::ServiceState;
use crate::services::relays::{GetRelay, GetRelays, GetRelaysResponse};

/// Handles the Relay public keys endpoint.
///
/// Note that this has nothing to do with Sentry public keys, which refer to the public key portion
/// of a DSN used for authenticating event submission. This endpoint is for Relay's public keys,
/// which authenticate entire Relays.
pub async fn handle(
    state: ServiceState,
    body: SignedJson<GetRelays>,
) -> Result<impl IntoResponse, ServiceUnavailable> {
    let relay_cache = &state.relay_cache();

    let relay_ids = body.inner.relay_ids.into_iter();
    let futures = relay_ids.map(|relay_id| {
        let inner = relay_cache.send(GetRelay { relay_id });
        async move { (relay_id, inner.await) }
    });

    let mut relays = HashMap::new();
    for (relay_id, result) in future::join_all(futures).await {
        let relay_info = result?;
        relays.insert(relay_id, relay_info);
    }

    Ok(axum::Json(GetRelaysResponse { relays }))
}
