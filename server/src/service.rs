use std::sync::Arc;

use actix_web::{http, server, App, Json};
use failure::ResultExt;
use uuid::Uuid;

use smith_aorta::PublicKeyEventAction;
use smith_config::Config;
use smith_trove::Trove;

use errors::{Error, ErrorKind};
use extractors::{BadStoreRequest, StoreRequest};

/// Response returned from the store route.
#[derive(Serialize)]
pub struct StoreResponse {
    /// The ID of the stored event
    id: Uuid,
}

fn store(request: StoreRequest) -> Result<Json<StoreResponse>, BadStoreRequest> {
    let public_key = request.auth().public_key();
    let trove_state = request.trove_state();
    let project_state = trove_state.get_or_create_project_state(request.project_id());

    // TODO: Set this ID into the event somehow
    let event_id = request.event().id.unwrap_or_else(|| Uuid::new_v4());

    match project_state.get_public_key_event_action(public_key) {
        PublicKeyEventAction::Send => {
            // TODO: Send right away
        }
        PublicKeyEventAction::Queue => {
            // TODO: We don't have a queue yet
        }
        PublicKeyEventAction::Discard => {
            // We simply swallow the event here.
            // Not sure what to do with the response in this case
            // TODO: Return something
        }
    }

    Ok(Json(StoreResponse { id: event_id }))
}

/// Given a relay config spawns the server and lets it run until it stops.
///
/// This not only spawning the server but also a governed trove in the
/// background.  Effectively this boots the server.
pub fn run(config: Config) -> Result<(), Error> {
    let trove = Arc::new(Trove::new(config.make_aorta_config()));
    let state = trove.state();
    trove.govern().context(ErrorKind::TroveGovernSpawnFailed)?;

    info!("spawning http listener");
    server::new(move || {
        App::with_state(state.clone()).route("/api/{project}/store/", http::Method::POST, store)
    }).bind(config.listen_addr())
        .context(ErrorKind::BindFailed)?
        .run();

    trove.abdicate().context(ErrorKind::TroveGovernSpawnFailed)?;
    info!("relay shutdown complete");

    Ok(())
}
