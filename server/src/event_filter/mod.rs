//! Implements event filtering
//!
//! Events may be filtered base on the following configurable criteria.
//!
//! * localhost ( filter events originating from the local machine)
//! * browser extensions ( filter events caused by known problematic browser extensions)
//! * web crawlers ( filter events sent by user agents known to be web crawlers)
//! * legacy browsers ( filter events originating from legacy browsers, can be configured)
//!

use semaphore_general::protocol::Event;

use crate::actors::project::FiltersConfig;

mod browser_extensions;
mod legacy_browsers;
mod localhost;
mod util;
mod web_crawlers;

/// Checks whether an event should be filtered (for a particular configuration)
/// If the event should be filter the Err returned contains a filter reason.
/// The reason is the message returned by the first filter that didn't pass.
pub fn should_filter(event: &Event, config: &FiltersConfig) -> Result<(), String> {
    localhost::should_filter(event, &config.localhost)?;
    browser_extensions::should_filter(event, &config.browser_extensions)?;
    web_crawlers::should_filter(event, &config.web_crawlers)?;
    legacy_browsers::should_filter(event, &config.legacy_browsers)?;
    return Ok(());
}
