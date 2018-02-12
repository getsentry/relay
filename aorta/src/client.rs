use std::io::{self, Write};

use futures::{Future, Stream};
use hyper::Client;
use tokio_core::reactor::Handle;

pub fn test_req(handle: Handle) {
    let client = Client::configure().keep_alive(true).build(&handle);
    let uri = "http://httpbin.org/ip".parse().unwrap();
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
