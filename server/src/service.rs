use hyper::Body;
use hyper::header::{ContentLength, ContentType};
use hyper::server::{const_service, service_fn, Http, Response};

use errors::{Error, ErrorKind};
use failure::ResultExt;

use smith_config::Config;
use smith_trove::Trove;

static TEXT: &'static str = "Doing absolutely nothing so far!";

pub fn run(config: &Config) -> Result<(), Error> {
    println!("spawning with config: {}", config.filename().display());
    println!("  agent id: {}", config.agent_id());
    println!("  public key: {}", config.public_key());
    println!("  listening on {}", config.listen_addr());

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

    server.run().context(ErrorKind::ListenFailed)?;
    trove.abdicate().context(ErrorKind::TroveGovernSpawnFailed)?;

    Ok(())
}
