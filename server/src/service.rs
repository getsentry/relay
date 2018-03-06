use parking_lot::Mutex;

use futures::future::{self, Future};
use futures::sync::oneshot;
use hyper::{Body, Error as HyperError};
use hyper::header::{ContentLength, ContentType};
use hyper::server::{Http, Request, Response, Service};
use regex::Regex;
use failure::ResultExt;
use ctrlc;

use errors::{Error, ErrorKind};

use smith_config::Config;
use smith_trove::Trove;
use smith_common::ProjectId;

static TEXT: &'static str = "Doing absolutely nothing so far!";

struct ProxyService;


impl Service for ProxyService {
    type Request = Request;
    type Response = Response;
    type Error = HyperError;
    type Future = Box<Future<Item=Self::Response, Error=Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        lazy_static! {
            static ref SUBMIT_URL: Regex = Regex::new(r"^/api/(\d+)/store/$").unwrap();
        }

        if let Some(m) = SUBMIT_URL.captures(req.path()) {
            if let Ok(project_id) = m[1].parse::<ProjectId>() {
                println!("{}", project_id);
            }
        }

        Box::new(future::ok(
            Response::new()
                .with_header(ContentLength(TEXT.len() as u64))
                .with_header(ContentType::plaintext())
                .with_body(TEXT)
        ))
    }
}

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

    let server = Http::new()
        .bind(&config.listen_addr(), || Ok(ProxyService))
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
