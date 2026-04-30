use crate::managed::Managed;
use crate::processing::spans::{Error, Result, SerializedSpans};

/// Validates that there are no mixed span items and no duplicated integrations and span containers.
///
/// Currently it is only allowed to send a single span container in an envelope.
/// Since all spans of the same envelope must belong to the same trace (due to the dynamic sampling
/// context on the envelope), they also should be collapsed by SDKs into a single container.
///
/// Integrations are only created within the same Relay and must not create multiple items at the moment.
///
/// Mixed span items are not supported/allowed.
pub fn invalid(spans: &Managed<SerializedSpans>) -> Result<()> {
    if !spans.invalid.is_empty() {
        return Err(Error::DuplicateItem);
    }

    Ok(())
}
