use hyper::Body;
use hyper::header::{ContentLength, ContentType};
use hyper::server::{const_service, service_fn, Http, Response};

use errors::{Error, ErrorKind};
use failure::ResultExt;

use smith_config::Config;

static TEXT: &'static str = "Doing absolutely nothing so far!";

pub fn run(config: &Config) -> Result<(), Error> {
    println!("spawning with config: {}", config.filename().display());
    println!("  agent id: {}", config.agent_id());
    println!("  public key: {}", config.public_key());

    let addr = ([127, 0, 0, 1], 3000).into();

    let hello = const_service(service_fn(|_req| {
        Ok(Response::<Body>::new()
            .with_header(ContentLength(TEXT.len() as u64))
            .with_header(ContentType::plaintext())
            .with_body(""))
    }));

    let server = Http::new()
        .bind(&addr, hello)
        .context(ErrorKind::BindFailed)?;
    Ok(server.run().context(ErrorKind::ListenFailed)?)
}
