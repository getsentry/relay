//! Implements event filtering.
//!
//! Events may be filtered base on the following configurable criteria.
//!
//! * localhost (filter events originating from the local machine)
//! * browser extensions (filter events caused by known problematic browser extensions)
//! * web crawlers (filter events sent by user agents known to be web crawlers)
//! * legacy browsers (filter events originating from legacy browsers, can be configured)
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

use std::net::IpAddr;

use relay_general::protocol::Event;

pub mod browser_extensions;
pub mod client_ips;
pub mod csp;
pub mod error_messages;
pub mod legacy_browsers;
pub mod localhost;
pub mod web_crawlers;

mod common;
mod config;
mod releases;

#[cfg(test)]
mod testutils;

pub use crate::common::*;
pub use crate::config::*;
pub use crate::csp::matches_any_origin;

/// Checks whether an event should be filtered for a particular configuration.
///
/// If the event should be filtered, the `Err` returned contains a filter reason.
/// The reason is the message returned by the first filter that didn't pass.
pub fn should_filter(
    event: &Event,
    client_ip: Option<IpAddr>,
    config: &FiltersConfig,
) -> Result<(), FilterStatKey> {
    // NB: The order of applying filters should not matter as they are additive. Still, be careful
    // when making changes to this order.

    csp::should_filter(event, &config.csp)?;
    client_ips::should_filter(client_ip, &config.client_ips)?;
    releases::should_filter(event, &config.releases)?;
    error_messages::should_filter(event, &config.error_messages)?;
    localhost::should_filter(event, &config.localhost)?;
    browser_extensions::should_filter(event, &config.browser_extensions)?;
    legacy_browsers::should_filter(event, &config.legacy_browsers)?;
    web_crawlers::should_filter(event, &config.web_crawlers)?;

    Ok(())
}
