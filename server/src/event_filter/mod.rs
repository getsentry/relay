mod browser_extensions;
mod localhost;

use crate::actors::project::FiltersConfig;
use semaphore_general::protocol::Event;

pub fn should_filter(event: Option<&Event>, config: &FiltersConfig) -> Result<(), String> {
    if let Some(ref event) = event {
        localhost::localhost_filter(event, &config.localhost)?;
        browser_extensions::browser_extensions_filter(event, &config.browser_extensions)?;
    }
    return Ok(());
}
