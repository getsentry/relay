use std::convert::Infallible;
use std::net::SocketAddr;

use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::request::Parts;
use axum::http::HeaderMap;

#[derive(Debug)]
pub struct ForwardedFor(String);

impl ForwardedFor {
    /// The defacto standard header for identifying the originating IP address of a client, [`X-Forwarded-For`].
    ///
    /// [`X-Forwarded-For`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-For)
    const FORWARDED_HEADER: &'static str = "X-Forwarded-For";
    /// Sentry's custom forwarded for header.
    ///
    /// Clients or proxies can use `X-Sentry-Forwarded-For` when they cannot control the value of
    /// the [`X-Forwarded-For`](Self::FORWARDED_HEADER) header.
    ///
    /// The Sentry SaaS infrastructure sets this header.
    const SENTRY_FORWARDED_HEADER: &'static str = "X-Sentry-Forwarded-For";
    /// Vercel forwards the client ip in its own [`X-Vercel-Forwarded-For`] header.
    ///
    /// [`X-Vercel-Forwarded-For`](https://vercel.com/docs/concepts/edge-network/headers#x-vercel-forwarded-for)
    const VERCEL_FORWARDED_HEADER: &'static str = "X-Vercel-Forwarded-For";
    /// Cloudflare forwards the client ip in its own [`CF-Connecting-IP`] header.
    ///
    /// [`CF-Connecting-IP`](https://developers.cloudflare.com/fundamentals/reference/http-request-headers/#cf-connecting-ip)
    const CLOUDFLARE_FORWARDED_HEADER: &'static str = "CF-Connecting-IP";

    /// Extracts the clients ip from a [`HeaderMap`].
    ///
    /// The function prioritizes more specific vendor headers over the more generic/common headers
    /// to allow clients to override and modify headers even when they do not have control over
    /// reverse proxies.
    ///
    /// First match wins in order:
    /// - [`Self::CLOUDFLARE_FORWARDED_HEADER`], highest priority since users may use Cloudflare
    /// infront of Vercel, it is generally the first layer.
    /// - [`Self::VERCEL_FORWARDED_HEADER`]
    /// - [`Self::SENTRY_FORWARDED_HEADER`]
    /// - [`Self::FORWARDED_HEADER`].
    fn get_forwarded_for_ip(header_map: &HeaderMap) -> Option<&str> {
        // List of headers to check from highest to lowest priority.
        let headers = [
            Self::CLOUDFLARE_FORWARDED_HEADER,
            Self::VERCEL_FORWARDED_HEADER,
            Self::SENTRY_FORWARDED_HEADER,
            Self::FORWARDED_HEADER,
        ];

        headers
            .into_iter()
            .flat_map(|header| header_map.get(header))
            .flat_map(|value| value.to_str().ok())
            .find(|value| !value.is_empty())
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for ForwardedFor {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<ForwardedFor> for String {
    fn from(forwarded: ForwardedFor) -> Self {
        forwarded.into_inner()
    }
}

#[axum::async_trait]
impl<S> FromRequestParts<S> for ForwardedFor
where
    S: Send + Sync,
{
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let peer_addr = ConnectInfo::<SocketAddr>::from_request_parts(parts, state)
            .await
            .map(|ConnectInfo(peer)| peer.ip().to_string())
            .ok();

        let forwarded = Self::get_forwarded_for_ip(&parts.headers);

        Ok(ForwardedFor(match (forwarded, peer_addr) {
            (None, None) => String::new(),
            (None, Some(peer_addr)) => peer_addr,
            (Some(forwarded), None) => forwarded.to_owned(),
            (Some(forwarded), Some(peer_addr)) => format!("{forwarded}, {peer_addr}"),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn test_prefer_vercel_forwarded() {
        let vercel_ip = "192.158.1.38";
        let other_ip = "111.222.3.44";

        let mut headermap = HeaderMap::default();
        headermap.insert(
            ForwardedFor::VERCEL_FORWARDED_HEADER,
            HeaderValue::from_str(vercel_ip).unwrap(),
        );
        headermap.insert(
            ForwardedFor::FORWARDED_HEADER,
            HeaderValue::from_str(other_ip).unwrap(),
        );

        let forwarded = ForwardedFor::get_forwarded_for_ip(&headermap);

        assert_eq!(forwarded, Some(vercel_ip));
    }

    #[test]
    fn test_prefer_cf_forwarded() {
        let cf_ip = "192.158.1.38";
        let other_ip = "111.222.3.44";

        let mut headermap = HeaderMap::default();
        headermap.insert(
            ForwardedFor::CLOUDFLARE_FORWARDED_HEADER,
            HeaderValue::from_str(cf_ip).unwrap(),
        );
        headermap.insert(
            ForwardedFor::FORWARDED_HEADER,
            HeaderValue::from_str(other_ip).unwrap(),
        );

        let forwarded = ForwardedFor::get_forwarded_for_ip(&headermap);

        assert_eq!(forwarded, Some(cf_ip));
    }

    #[test]
    fn test_prefer_sentry_forwarded() {
        let sentry_ip = "192.158.1.38";
        let other_ip = "111.222.3.44";

        let mut headermap = HeaderMap::default();
        headermap.insert(
            ForwardedFor::SENTRY_FORWARDED_HEADER,
            HeaderValue::from_str(sentry_ip).unwrap(),
        );
        headermap.insert(
            ForwardedFor::FORWARDED_HEADER,
            HeaderValue::from_str(other_ip).unwrap(),
        );

        let forwarded = ForwardedFor::get_forwarded_for_ip(&headermap);

        assert_eq!(forwarded, Some(sentry_ip));
    }

    /// If there's no vercel or sentry header then use the normal `X-Forwarded-For`-header.
    #[test]
    fn test_fall_back_on_forwarded_for_header() {
        let other_ip = "111.222.3.44";

        let mut headermap = HeaderMap::default();
        headermap.insert(
            ForwardedFor::FORWARDED_HEADER,
            HeaderValue::from_str(other_ip).unwrap(),
        );

        let forwarded = ForwardedFor::get_forwarded_for_ip(&headermap);

        assert_eq!(forwarded, Some(other_ip));
    }

    #[test]
    fn test_get_none_if_empty_header() {
        let mut headermap = HeaderMap::default();
        headermap.insert(
            ForwardedFor::FORWARDED_HEADER,
            HeaderValue::from_str("").unwrap(),
        );

        let forwarded = ForwardedFor::get_forwarded_for_ip(&headermap);
        assert!(forwarded.is_none());
    }

    #[test]
    fn test_get_none_if_invalid_header() {
        let other_ip = "111.222.3.44";

        let mut headermap = HeaderMap::default();
        headermap.insert("X-Invalid-Header", HeaderValue::from_str(other_ip).unwrap());

        let forwarded = ForwardedFor::get_forwarded_for_ip(&headermap);
        assert!(forwarded.is_none());
    }
}
