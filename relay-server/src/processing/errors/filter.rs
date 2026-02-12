use crate::managed::Managed;
use crate::processing::errors::errors::SentryError as _;
use crate::processing::errors::{ExpandedError, Result};
use crate::processing::{self, Context};
use crate::services::processor::ProcessingError;

/// Runs inbound filters on the [`ExpandedError`].
pub fn filter(error: &Managed<ExpandedError>, ctx: Context<'_>) -> Result<()> {
    let _ = processing::utils::event::filter(&error.headers, error.error.event(), ctx)
        .map_err(ProcessingError::EventFiltered)?;

    Ok(())
}
