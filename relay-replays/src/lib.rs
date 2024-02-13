//! Session replay protocol and processing for Sentry.
//!
//! [Session Replay] provides a video-like reproduction of user interactions on a site or web app.
//! All user interactions — including page visits, mouse movements, clicks, and scrolls — are
//! captured, helping developers connect the dots between a known issue and how a user experienced
//! it in the UI.
//!
//! [session replay]: https://docs.sentry.io/product/session-replay/

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![warn(missing_docs)]

pub mod recording;
mod transform;
pub mod video;
