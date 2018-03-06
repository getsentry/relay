use std::panic;
use std::sync::Arc;

use parking_lot::Mutex;
use futures::future::{self, Future};
use futures::sync::oneshot;
use hyper::{Error as HyperError, StatusCode};
use hyper::header::{ContentLength, ContentType};
use hyper::server::{Http, Request, Response, Service};
use regex::Regex;
use failure::ResultExt;
use ctrlc;

use errors::{Error, ErrorKind};
use utils::make_error_response;

use smith_config::Config;
use smith_aorta::ApiErrorResponse;
use smith_trove::{Trove, TroveContext};
use smith_common::ProjectId;

static TEXT: &'static str = "Doing absolutely nothing so far!";

struct ProxyService {
    ctx: Arc<TroveContext>,
}

impl Service for ProxyService {
    type Request = Request;
    type Response = Response;
    type Error = HyperError;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        panic::catch_unwind(panic::AssertUnwindSafe(|| -> Self::Future {
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
                    .with_body(TEXT),
            ))
        })).unwrap_or_else(|_| {
            make_error_response(
                StatusCode::InternalServerError,
                ApiErrorResponse::with_detail(
                    "The server encountered a fatal internal server error",
                ),
            )
        })
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

    let trove = Arc::new(Trove::new(config.make_aorta_config()));
    trove.govern().context(ErrorKind::TroveGovernSpawnFailed)?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let shutdown_rx = shutdown_rx.shared();
    let shutdown_tx = Mutex::new(Some(shutdown_tx));

    ctrlc::set_handler(move || {
        if let Some(tx) = shutdown_tx.lock().take() {
            info!("received shutdown signal");
            tx.send(()).ok();
        }
    }).expect("failed to set SIGINT/SIGTERM handler");

    // the service itself has a landing pad but in addition we also create one
    // for the server entirely in case we encounter a bad panic somewhere.
    loop {
        info!("spawning http listener");

        let trove = trove.clone();
        let shutdown_rx = shutdown_rx.clone();
        if panic::catch_unwind(panic::AssertUnwindSafe(|| -> Result<(), Error> {
            // we need to do a slightly shitty dance here to get the handle
            // from the server so we can create a trove context with the same
            // handle as we have on the server process.  It might also make
            // sense to actually spawn a thread here with a separate core but
            // for now we can just share it.
            let ctx = Arc::new(Mutex::new(None::<Arc<TroveContext>>));
            let ctx_inner = ctx.clone();
            let server = Http::new()
                .bind(&config.listen_addr(), move || {
                    Ok(ProxyService {
                        ctx: ctx_inner.lock().as_ref().unwrap().clone(),
                    })
                })
                .context(ErrorKind::BindFailed)?;
            *ctx.lock() = Some(trove.new_context(server.handle()));

            server
                .run_until(shutdown_rx.map(|_| ()).map_err(|_| ()))
                .context(ErrorKind::ListenFailed)?;
            Ok(())
        })).is_ok()
        {
            break;
        }
        warn!("tearning down http listener for respawn because of uncontained panic");
    }

    trove.abdicate().context(ErrorKind::TroveGovernSpawnFailed)?;

    info!("relay shut down");

    Ok(())
}
