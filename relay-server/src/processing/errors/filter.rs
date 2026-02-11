use crate::managed::Managed;
use crate::processing::errors::{Error, ExpandedError, Result};
use crate::processing::utils::event::FiltersStatus;
use crate::processing::{self, Context};
use crate::services::processor::ProcessingError;

/// Runs inbound filters on the [`ExpandedError`] and returns the [`FiltersStatus`].
pub fn filter(error: &Managed<ExpandedError>, ctx: Context<'_>) -> Result<FiltersStatus> {
    let e = error.kind.as_ref();

    processing::utils::event::filter(&error.headers, e.event, ctx)
        .map_err(ProcessingError::EventFiltered)
        .map_err(Error::from)
}
