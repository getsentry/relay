use crate::processing::logs::EinsLog;
use crate::processing::{Context, if_processing};
use crate::services::processor::ProcessingError;

/// Validates that there is only a single log container processed at a time.
///
/// The `Log` item must always be sent as an `ItemContainer`, currently it is not allowed to
/// send multiple containers for logs.
///
/// This restriction may be lifted in the future, this is why this validation only happens
/// when processing is enabled, allowing it to be changed easily in the future.
///
/// This limit mostly exists to incentivise SDKs to batch multiple logs into a single container,
/// technically it can be removed without issues.
pub fn container(logs: &EinsLog, ctx: Context<'_>) -> Result<(), ProcessingError> {
    if_processing!(ctx, {
        use crate::envelope::ItemType;

        // It's fine if there was no log container, as we still accept otel logs.
        if logs.logs.len() > 1 {
            return Err(ProcessingError::DuplicateItem(ItemType::Log));
        }
    });

    Ok(())
}
