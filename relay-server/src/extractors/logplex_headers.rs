use std::convert::Infallible;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{HeaderMap, header};
use serde::{Deserialize, Serialize};

/// Header values longer than this are ignored.
///
/// The values Logplex sends are all well below this limit.
const MAX_HEADER_VALUE_LENGTH: usize = 256;

/// The metadata headers Heroku's Logplex sends with every HTTPS log drain request.
///
/// The headers are stored on the integration item, see [`Item::logplex_headers`](crate::envelope::Item::logplex_headers).
///
/// See: <https://devcenter.heroku.com/articles/log-drains#https-drains>.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogplexHeaders {
    /// The number of messages in the request body, `Logplex-Msg-Count`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub msg_count: Option<u32>,
    /// The unique identifier of the request, `Logplex-Frame-Id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frame_id: Option<String>,
    /// The token of the drain sending the request, `Logplex-Drain-Token`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain_token: Option<String>,
    /// The version of Logplex sending the request, `User-Agent`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
}

impl LogplexHeaders {
    const MSG_COUNT: &str = "Logplex-Msg-Count";
    const FRAME_ID: &str = "Logplex-Frame-Id";
    const DRAIN_TOKEN: &str = "Logplex-Drain-Token";

    fn from_headers(headers: &HeaderMap) -> Self {
        let get = |name| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.is_empty() && value.len() <= MAX_HEADER_VALUE_LENGTH)
        };

        Self {
            msg_count: get(Self::MSG_COUNT).and_then(|value| value.parse().ok()),
            frame_id: get(Self::FRAME_ID).map(str::to_owned),
            drain_token: get(Self::DRAIN_TOKEN).map(str::to_owned),
            user_agent: get(header::USER_AGENT.as_str()).map(str::to_owned),
        }
    }

    /// Returns the headers which are added to every log.
    pub fn as_log_headers(&self) -> relay_ourlogs::LogplexHeaders<'_> {
        relay_ourlogs::LogplexHeaders {
            frame_id: self.frame_id.as_deref(),
            drain_token: self.drain_token.as_deref(),
            user_agent: self.user_agent.as_deref(),
        }
    }
}

impl<S: Send + Sync> FromRequestParts<S> for LogplexHeaders {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        Ok(Self::from_headers(&parts.headers))
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;
    use crate::envelope::{Item, ItemType};

    #[test]
    fn test_from_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("Logplex-Msg-Count", HeaderValue::from_static("2"));
        headers.insert("Logplex-Frame-Id", HeaderValue::from_static("abc123"));
        headers.insert("Logplex-Drain-Token", HeaderValue::from_static("d.123"));
        headers.insert("User-Agent", HeaderValue::from_static("Logplex/v72"));

        let logplex = LogplexHeaders::from_headers(&headers);
        assert_eq!(
            logplex,
            LogplexHeaders {
                msg_count: Some(2),
                frame_id: Some("abc123".to_owned()),
                drain_token: Some("d.123".to_owned()),
                user_agent: Some("Logplex/v72".to_owned()),
            }
        );
        assert_eq!(
            logplex.as_log_headers(),
            relay_ourlogs::LogplexHeaders {
                frame_id: Some("abc123"),
                drain_token: Some("d.123"),
                user_agent: Some("Logplex/v72"),
            }
        );
    }

    #[test]
    fn test_from_headers_missing_and_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert("Logplex-Msg-Count", HeaderValue::from_static("-1"));
        headers.insert("Logplex-Frame-Id", HeaderValue::from_static(""));
        headers.insert(
            "Logplex-Drain-Token",
            HeaderValue::from_str(&"x".repeat(MAX_HEADER_VALUE_LENGTH + 1)).unwrap(),
        );
        headers.insert("User-Agent", HeaderValue::from_bytes(b"\xff").unwrap());

        assert_eq!(
            LogplexHeaders::from_headers(&headers),
            LogplexHeaders::default()
        );
    }

    #[test]
    fn test_item_round_trip() {
        let logplex = LogplexHeaders {
            msg_count: Some(2),
            frame_id: Some("abc123".to_owned()),
            drain_token: None,
            user_agent: Some("Logplex/v72".to_owned()),
        };

        let mut item = Item::new(ItemType::Integration);
        assert_eq!(item.logplex_headers(), None);

        item.set_logplex_headers(logplex.clone());
        assert_eq!(item.logplex_headers(), Some(logplex));
    }
}
