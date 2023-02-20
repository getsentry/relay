use actix_web::{error::PayloadError, HttpRequest};
use bytes::Bytes;

use crate::extractors::{Decoder, SharedPayload};
use crate::utils;

/// Reads the body of a Relay endpoint request.
///
/// If the body exceeds the given `limit` during streaming or decompression, an error is returned.
pub async fn request_body<S>(req: &HttpRequest<S>, limit: usize) -> Result<Bytes, PayloadError> {
    if let Some(length) = utils::get_content_length(req) {
        if length > limit {
            return Err(PayloadError::Overflow);
        }
    }

    let mut payload = SharedPayload::get(req);
    let mut decoder = Decoder::new(req, limit);

    while let Some(encoded) = payload.chunk().await? {
        if decoder.decode(&encoded)? {
            return Err(PayloadError::Overflow);
        }
    }

    Ok(decoder.finish()?)
}

// /// Future that resolves to a complete store endpoint body.
// pub struct RequestBody {
//     err: Option<PayloadError>,
//     stream: Option<(SharedPayload, Decoder)>,
// }

// impl RequestBody {
//     /// Create `ForwardBody` for request.
//     pub fn new<S>(req: &HttpRequest<S>, limit: usize) -> Self {
//         if let Some(length) = utils::get_content_length(req) {
//             if length > limit {
//                 return RequestBody {
//                     stream: None,
//                     err: Some(PayloadError::Overflow),
//                 };
//             }
//         }

//         RequestBody {
//             stream: Some((SharedPayload::get(req), Decoder::new(req, limit))),
//             err: None,
//         }
//     }
// }

// impl Future for RequestBody {
//     type Item = Bytes;
//     type Error = PayloadError;

//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         if let Some(err) = self.err.take() {
//             return Err(err);
//         }

//         if let Some((ref mut payload, ref mut decoder)) = self.stream {
//             loop {
//                 return match payload.poll()? {
//                     Async::Ready(Some(encoded)) => {
//                         if decoder.decode(encoded)? {
//                             Err(PayloadError::Overflow)
//                         } else {
//                             continue;
//                         }
//                     }
//                     Async::Ready(None) => Ok(Async::Ready(decoder.finish()?)),
//                     Async::NotReady => Ok(Async::NotReady),
//                 };
//             }
//         }

//         panic!("cannot be used second time")
//     }
// }
