use futures::future::{self, Future};
use hyper::{Error as HyperError, StatusCode};
use hyper::server::Response;
use hyper::header::{ContentLength, ContentType};
use serde_json;

use smith_trove::ApiErrorResponse;

pub fn make_error_response(
    status_code: StatusCode,
    err: ApiErrorResponse,
) -> Box<Future<Item = Response, Error = HyperError>> {
    // this cannot fail unless someone changes the error type. Don't change the error type.
    let body = serde_json::to_vec(&err).unwrap();
    Box::new(future::ok(
        Response::new()
            .with_status(status_code)
            .with_header(ContentType::json())
            .with_header(ContentLength(body.len() as u64))
            .with_body(body),
    ))
}
