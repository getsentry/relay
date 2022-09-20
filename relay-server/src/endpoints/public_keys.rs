use std::collections::HashMap;

use actix_web::{actix::*, Error, Json};
use futures::{future, FutureExt, TryFutureExt};

use crate::actors::relays::{GetRelay, GetRelays, GetRelaysResult, RelayCache};
use crate::extractors::SignedJson;
use crate::service::ServiceApp;

fn get_public_keys(body: SignedJson<GetRelays>) -> ResponseFuture<Json<GetRelaysResult>, Error> {
    let future = async move {
        let relay_cache = RelayCache::from_registry();

        let relay_ids = body.inner.relay_ids.into_iter();
        let futures = relay_ids.map(|relay_id| {
            let inner = relay_cache.send(GetRelay { relay_id });
            async move { (relay_id, inner.await) }
        });

        let mut relays = HashMap::new();
        for (relay_id, result) in future::join_all(futures).await {
            let relay_info = result.map_err(|_| Error::from(MailboxError::Closed))?;
            relays.insert(relay_id, relay_info);
        }

        Ok(Json(GetRelaysResult { relays }))
    };

    Box::new(future.boxed().compat())
}

/// Registers the Relay public keys endpoint.
///
/// Note that this has nothing to do with Sentry public keys, which refer to the public key portion
/// of a DSN used for authenticating event submission. This endpoint is for Relay's public keys,
/// which authenticate entire Relays.
pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/publickeys/", |r| {
        r.name("relay-publickeys");
        r.post().with(get_public_keys);
    })
}
