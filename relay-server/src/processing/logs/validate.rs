use crate::processing::logs::{Error, SerializedLogs};
use crate::processing::{Managed, Rejected};

/// Validates that there is only a single log container processed at a time.
///
/// The `Log` item must always be sent as an `ItemContainer`, currently it is not allowed to
/// send multiple containers for logs.
///
/// This restriction may be lifted in the future.
///
/// This limit mostly exists to incentivise SDKs to batch multiple logs into a single container,
/// technically it can be removed without issues.
pub fn container(logs: &Managed<SerializedLogs>) -> Result<(), Rejected<Error>> {
    // It's fine if there was no log container, as we still accept OTel logs.
    if logs.logs.len() > 1 {
        return Err(logs.reject_err(Error::DuplicateContainer));
    }

    Ok(())
}
