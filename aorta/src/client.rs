use std::io::{self, Write};

use futures::{Future, Stream};
use hyper::Client;
use hyper_tls::HttpsConnector;

use tokio_core::reactor::Handle;

pub fn test_req(handle: Handle) {
    let client = Client::configure()
        .connector(HttpsConnector::new(4, &handle).unwrap())
        .build(&handle);
    let uri = "https://httpbin.org/ip".parse().unwrap();
    let work = client.get(uri).and_then(|res| {
        println!("Response: {}", res.status());

        res.body().for_each(|chunk| {
            io::stdout()
                .write_all(&chunk)
                .map(|_| ())
                .map_err(From::from)
        })
    });
    handle.spawn(work.map_err(|err| println!("error: {}", err)));
}
