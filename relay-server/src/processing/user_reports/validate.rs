use crate::processing::user_reports::{Error, SerializedUserReports};

/// Checks that the user reports contain an event ID.
pub fn validate(reports: &SerializedUserReports) -> Result<(), Error> {
    if reports.headers.event_id().is_none() {
        return Err(Error::NoEventId);
    }

    Ok(())
}
