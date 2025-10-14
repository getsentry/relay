use crate::managed::Managed;
use crate::processing::spans::{Error, Result, SerializedSpans};

/// Validates that there is only a single spans container processed at a time.
///
/// Currently it is only allowed to send a single span container in an envelope.
/// Since all spans of the same envelope must belong to the same trace (due to the dynamic sampling
/// context on the envelope), they also should be collapsed by SDKs into a single container.
///
/// Once we lift the requirement of a single trace per envelope, we may want to also consider
/// lifting this restriction.
pub fn container(spans: &Managed<SerializedSpans>) -> Result<()> {
    // It's fine if there was no container, as we still accept OTel spans.
    if spans.spans.len() > 1 {
        return Err(Error::DuplicateContainer);
    }

    Ok(())
}
