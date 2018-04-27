use std::env;
use std::sync::Arc;

use actix_web::{http, server, App, Json};
use failure::ResultExt;
use sentry_types::protocol::latest::Event;
use uuid::Uuid;

use smith_aorta::{AortaChangeset, PublicKeyEventAction};
use smith_config::Config;
use smith_trove::Trove;

use errors::{ServerError, ServerErrorKind};
use extractors::{BadProjectRequest, StoreRequest};
use middlewares::{CaptureSentryError, ForceJson};

#[derive(Serialize)]
struct StoreResponse {
    /// The ID of the stored event
    id: Uuid,
}

#[derive(Serialize)]
struct StoreChangeset {
    public_key: String,
    event: Event<'static>,
}

impl AortaChangeset for StoreChangeset {
    fn aorta_changeset_type(&self) -> &'static str {
        "store"
    }
}

fn store(mut request: StoreRequest) -> Result<Json<StoreResponse>, BadProjectRequest> {
    let trove_state = request.trove_state();
    let project_id = request.project_id();
    let public_key = request.auth().public_key().to_string();

    let mut event = request.take_payload().expect("Should not happen");
    let event_id = *event.id.get_or_insert_with(Uuid::new_v4);

    let project_state = trove_state.get_or_create_project_state(project_id);
    match project_state.get_public_key_event_action(&public_key) {
        // TODO: Implement an event queue in `TroveState`
        PublicKeyEventAction::Send | PublicKeyEventAction::Queue => trove_state
            .request_manager()
            .add_changeset(project_id, StoreChangeset { public_key, event }),
        PublicKeyEventAction::Discard => {
            debug!("Discarded event {} for project {}", event_id, project_id)
        }
    }

    Ok(Json(StoreResponse { id: event_id }))
}

fn dump_spawn_infos<H: server::HttpHandler>(config: &Config, server: &server::HttpServer<H>) {
    info!(
        "launching relay with config {}",
        config.filename().display()
    );
    info!("  relay id: {}", config.relay_id());
    info!("  public key: {}", config.public_key());
    info!("  log level: {}", config.log_level_filter());
    for addr in server.addrs() {
        info!("  listening on: http://{}/", addr);
    }
}

/// Given a relay config spawns the server and lets it run until it stops.
///
/// This not only spawning the server but also a governed trove in the
/// background.  Effectively this boots the server.
pub fn run(config: Config) -> Result<(), ServerError> {
    let trove = Arc::new(Trove::new(config.make_aorta_config()));
    let state = trove.state();
    trove
        .govern()
        .context(ServerErrorKind::TroveGovernSpawnFailed)?;

    let mut server = server::new(move || {
        App::with_state(state.clone())
            .middleware(CaptureSentryError)
            .resource("/api/{project}/store/", |r| {
                r.middleware(ForceJson);
                r.method(http::Method::POST).with(store);
            })
    });

    let mut listening = false;

    #[cfg(not(windows))]
    {
        use std::net::TcpListener;
        use std::os::unix::io::FromRawFd;

        if let Some(fd) = env::var("LISTEN_FD").ok().and_then(|fd| fd.parse().ok()) {
            server = server.listen(unsafe { TcpListener::from_raw_fd(fd) });
            listening = true;
        }
    }

    if !listening {
        server = server
            .bind(config.listen_addr())
            .context(ServerErrorKind::BindFailed)?;
    }

    dump_spawn_infos(&config, &server);
    info!("spawning http listener");
    server.run();

    trove
        .abdicate()
        .context(ServerErrorKind::TroveGovernSpawnFailed)?;
    info!("relay shutdown complete");

    Ok(())
}
