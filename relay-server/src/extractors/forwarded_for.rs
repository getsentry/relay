use std::convert::Infallible;
use std::net::SocketAddr;

use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::request::Parts;
use axum::http::HeaderMap;

#[derive(Debug)]
pub struct ForwardedFor(String);

impl ForwardedFor {
    const FORWARDED_HEADER: &str = "X-Forwarded-For";
    const VERCEL_FORWARDED_HEADER: &str = "X-Vercel-Forwarded-For";
    const SENTRY_FORWARDED_HEADER: &str = "X-Sentry-Forwarded-For";

    /// We prefer the SENTRY_FORWARDED_HEADER over the standard header because our infrastructure
    /// puts the contents of the incoming FORWARDED_HEADER into SENTRY_FORWARDED_HEADER for security
    /// reasons. This also is a way for users who can't mutate the regular FORWARDED_HEADER to still
    /// tell Sentry about forwarded ip.
    ///
    /// In Vercel environments the FORWARDED_HEADER value could be overwritten (which also makes the
    /// SENTRY_FORWARDED_HEADER value inaccruate). This leads us to rely on the
    /// VERCEL_FORWARDED_HEADER as the source of truth instead.
    /// `https://vercel.com/docs/concepts/edge-network/headers#x-vercel-forwarded-for`
    fn get_forwarded_for_ip(header_map: &HeaderMap) -> &str {
        header_map
            .get(Self::VERCEL_FORWARDED_HEADER)
            .or_else(|| header_map.get(Self::SENTRY_FORWARDED_HEADER))
            .or_else(|| header_map.get(Self::FORWARDED_HEADER))
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
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
            .unwrap_or_default();

        let forwarded = Self::get_forwarded_for_ip(&parts.headers);

        Ok(ForwardedFor(if forwarded.is_empty() {
            peer_addr
        } else if peer_addr.is_empty() {
            forwarded.to_string()
        } else {
            format!("{forwarded}, {peer_addr}")
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

        assert_eq!(forwarded, vercel_ip);
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

        assert_eq!(forwarded, sentry_ip);
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

        assert_eq!(forwarded, other_ip);
    }

    #[test]
    fn test_get_empty_string_if_invalid_header() {
        let other_ip = "111.222.3.44";

        let mut headermap = HeaderMap::default();
        headermap.insert("X-Invalid-Header", HeaderValue::from_str(other_ip).unwrap());

        let forwarded = ForwardedFor::get_forwarded_for_ip(&headermap);
        assert!(forwarded.is_empty());
    }
}
