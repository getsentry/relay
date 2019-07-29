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
mod config;
mod legacy_browsers;
mod localhost;
mod utils;
mod web_crawlers;

#[cfg(test)]
mod test_utils;

pub use config::*;

/// Checks whether an event should be filtered for a particular configuration.
///
/// If the event should be filter, the `Err` returned contains a filter reason.
/// The reason is the message returned by the first filter that didn't pass.
pub fn should_filter(event: &Event, config: &FiltersConfig) -> Result<(), String> {
    // NB: The order of applying filters should not matter as they are additive. Still, be careful
    // when making changes to this order.

    localhost::should_filter(event, &config.localhost)?;
    browser_extensions::should_filter(event, &config.browser_extensions)?;
    legacy_browsers::should_filter(event, &config.legacy_browsers)?;
    web_crawlers::should_filter(event, &config.web_crawlers)?;

    Ok(())
}
