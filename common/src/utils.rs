use hyper::Uri;
use url::Url;

/// Takes a Url and coverts it into a hyper uri.
pub fn url_to_hyper_uri(url: Url) -> Uri {
    url.to_string().parse().unwrap()
}
