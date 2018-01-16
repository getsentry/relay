use hyper::Body;
use hyper::header::{ContentLength, ContentType};
use hyper::server::{Http, Response, const_service, service_fn};

use errors::Error;
static TEXT: &'static str = "Doing absolutely nothing so far!";

pub fn run() -> Result<(), Error> {
    let addr = ([127, 0, 0, 1], 3000).into();

    let hello = const_service(service_fn(|_req|{
        Ok(Response::<Body>::new()
            .with_header(ContentLength(TEXT.len() as u64))
            .with_header(ContentType::plaintext())
            .with_body(""))
    }));

    let server = Http::new().bind(&addr, hello)?;
    Ok(server.run()?)
}
