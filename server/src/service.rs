use parking_lot::Mutex;

use futures::future::Future;
use futures::sync::oneshot;

use hyper::Body;
use hyper::header::{ContentLength, ContentType};
use hyper::server::{const_service, service_fn, Http, Response};

use errors::{Error, ErrorKind};
use failure::ResultExt;
use ctrlc;

use smith_config::Config;
use smith_trove::Trove;

static TEXT: &'static str = "Doing absolutely nothing so far!";

fn dump_spawn_infos(config: &Config) {
    info!(
        "launching relay with config {}",
        config.filename().display()
    );
    info!("  relay id: {}", config.relay_id());
    info!("  public key: {}", config.public_key());
    info!("  listening on http://{}/", config.listen_addr());
}

pub fn run(config: &Config) -> Result<(), Error> {
    dump_spawn_infos(config);

    let trove = Trove::new(config.make_aorta_config());
    trove.govern().context(ErrorKind::TroveGovernSpawnFailed)?;

    let hello = const_service(service_fn(move |_req| {
        Ok(Response::<Body>::new()
            .with_header(ContentLength(TEXT.len() as u64))
            .with_header(ContentType::plaintext())
            .with_body(TEXT))
    }));

    let server = Http::new()
        .bind(&config.listen_addr(), hello)
        .context(ErrorKind::BindFailed)?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let shutdown_tx = Mutex::new(Some(shutdown_tx));

    ctrlc::set_handler(move || {
        if let Some(tx) = shutdown_tx.lock().take() {
            info!("received shutdown signal");
            tx.send(()).ok();
        }
    }).expect("failed to set SIGINT/SIGTERM handler");

    server
        .run_until(shutdown_rx.map(|_| ()).map_err(|_| ()))
        .context(ErrorKind::ListenFailed)?;

    trove.abdicate().context(ErrorKind::TroveGovernSpawnFailed)?;

    info!("relay shut down");

    Ok(())
}
