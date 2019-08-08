//! Implements event filtering.
//!
//! Events may be filtered base on the following configurable criteria.
//!
//! * localhost ( filter events originating from the local machine)
//! * browser extensions ( filter events caused by known problematic browser extensions)
//! * web crawlers ( filter events sent by user agents known to be web crawlers)
//! * legacy browsers ( filter events originating from legacy browsers, can be configured)
//!

use crate::protocol::Event;

mod browser_extensions;
mod client_ip;
mod config;
mod csp;
mod error_message;
mod legacy_browsers;
mod localhost;
mod release;
mod web_crawlers;

#[cfg(test)]
mod test_utils;

pub use config::*;
use std::net::IpAddr;

/// Groups all filter related information extracted from a (ProjectConfig)[server.actors.ProjectConfig].
pub struct GlobalFilterConfig<'a> {
    pub filters: &'a FiltersConfig,
    pub csp_disallowed_sources: &'a [String],
    pub black_listed_ips: &'a [String],
    pub filtered_releases: &'a [String],
    pub filtered_error_messages: &'a [String],
}

/// Checks whether an event should be filtered for a particular configuration.
///
/// If the event should be filter, the `Err` returned contains a filter reason.
/// The reason is the message returned by the first filter that didn't pass.
pub fn should_filter(
    event: &Event,
    client_ip: &Option<IpAddr>,
    config: GlobalFilterConfig,
) -> Result<(), String> {
    // NB: The order of applying filters should not matter as they are additive. Still, be careful
    // when making changes to this order.

    csp::should_filter(event, config.csp_disallowed_sources)?;
    client_ip::should_filter(client_ip, config.black_listed_ips)?;
    release::should_filter(event, config.filtered_releases)?;
    error_message::should_filter(event, config.filtered_error_messages)?;
    localhost::should_filter(event, &config.filters.localhost)?;
    browser_extensions::should_filter(event, &config.filters.browser_extensions)?;
    legacy_browsers::should_filter(event, &config.filters.legacy_browsers)?;
    web_crawlers::should_filter(event, &config.filters.web_crawlers)?;

    Ok(())
}
