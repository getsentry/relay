use std::collections::HashMap;

use axum::response::Result;
use axum::routing::post;
use axum::{Json, Router};
use futures::future;

use crate::actors::relays::{GetRelay, GetRelays, GetRelaysResponse, RelayCache};
use crate::extractors::SignedJson;

async fn get_public_keys(body: SignedJson<GetRelays>) -> Result<Json<GetRelaysResponse>> {
    let relay_cache = RelayCache::from_registry();

    let relay_ids = body.inner.relay_ids.into_iter();
    let futures = relay_ids.map(|relay_id| {
        let inner = relay_cache.send(GetRelay { relay_id });
        async move { (relay_id, inner.await) }
    });

    let mut relays = HashMap::new();
    for (relay_id, result) in future::join_all(futures).await {
        let relay_info = result.map_err(|_| ())?; // TODO(ja): Error handling
        relays.insert(relay_id, relay_info);
    }

    Ok(Json(GetRelaysResponse { relays }))
}

/// Registers the Relay public keys endpoint.
///
/// Note that this has nothing to do with Sentry public keys, which refer to the public key portion
/// of a DSN used for authenticating event submission. This endpoint is for Relay's public keys,
/// which authenticate entire Relays.
pub fn routes<S>() -> Router<S> {
    // r.name("relay-publickeys");
    Router::new()
        .route("/api/0/relays/publickeys/", post(get_public_keys))
        .with_state(())
}
