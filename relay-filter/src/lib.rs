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

use std::net::IpAddr;

use relay_protocol::Getter;

pub mod browser_extensions;
pub mod client_ips;
pub mod csp;
pub mod error_messages;
pub mod generic;
pub mod legacy_browsers;
pub mod localhost;
pub mod transaction_name;
pub mod web_crawlers;

mod common;
mod config;
mod interface;
mod releases;

#[cfg(test)]
mod testutils;

pub use crate::common::*;
pub use crate::config::*;
pub use crate::csp::matches_any_origin;
pub use crate::generic::are_generic_filters_supported;
pub use interface::Filterable;

/// Checks whether an event should be filtered for a particular configuration.
///
/// If the event should be filtered, the `Err` returned contains a filter reason.
/// The reason is the message returned by the first filter that didn't pass.
pub fn should_filter<F: Filterable + Getter>(
    item: &F,
    client_ip: Option<IpAddr>,
    config: &ProjectFiltersConfig,
    global_config: Option<&GenericFiltersConfig>,
) -> Result<(), FilterStatKey> {
    // In order to maintain backwards compatibility, we still want to run the old matching logic,
    // but we will try to match generic filters first, since the goal is to eventually fade out
    // the normal filters except for the ones that have complex conditions.
    generic::should_filter(item, &config.generic, global_config)?;

    // The order of applying filters should not matter as they are additive. Still, be careful
    // when making changes to this order.
    csp::should_filter(item, &config.csp)?;
    client_ips::should_filter(client_ip, &config.client_ips)?;
    releases::should_filter(item, &config.releases)?;
    error_messages::should_filter(item, &config.error_messages)?;
    localhost::should_filter(item, &config.localhost)?;
    browser_extensions::should_filter(item, &config.browser_extensions)?;
    legacy_browsers::should_filter(item, &config.legacy_browsers)?;
    web_crawlers::should_filter(item, &config.web_crawlers)?;
    transaction_name::should_filter(item, &config.ignore_transactions)?;

    Ok(())
}
