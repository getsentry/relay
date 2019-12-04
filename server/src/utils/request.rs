use actix_web::{http::header, HttpMessage};

// Resolve the content length from HTTP request headers.
pub fn get_content_length<T>(req: &T) -> Option<usize>
where
    T: HttpMessage,
{
    req.headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse().ok())
}
