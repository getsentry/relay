use std::env;
use std::sync::Arc;

use actix_web::{http, server, App};
use failure::ResultExt;

use smith_config::Config;
use smith_trove::{Trove, TroveState};

use endpoints;
use errors::{ServerError, ServerErrorKind};
use middlewares::{CaptureSentryError, ForceJson};

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

fn make_app(state: Arc<TroveState>) -> App<Arc<TroveState>> {
    App::with_state(state)
        .middleware(CaptureSentryError)
        .resource("/api/{project}/store/", |r| {
            r.middleware(ForceJson);
            r.method(http::Method::POST).with(endpoints::store);
        })
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

    let mut server = server::new(move || make_app(state.clone()));

    let fd_bound = {
        #[cfg(not(windows))]
        {
            use std::net::TcpListener;
            use std::os::unix::io::FromRawFd;

            if let Some(fd) = env::var("LISTEN_FD").ok().and_then(|fd| fd.parse().ok()) {
                server = server.listen(unsafe { TcpListener::from_raw_fd(fd) });
                true
            } else {
                false
            }
        }
        #[cfg(windows)]
        {
            false
        }
    };

    if !fd_bound {
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
