use crate::processing::user_reports::{Error, SerializedUserReports};
use crate::statsd::RelayCounters;
use crate::utils::client_name_tag;

/// Checks that the user reports contain an event ID.
pub fn validate(reports: &SerializedUserReports) -> Result<(), Error> {
    let has_event_id = reports.headers.event_id().is_some();

    // Temporary counter to figure out which SDKs are sending user reports without event IDs.
    relay_statsd::metric!(
        counter(RelayCounters::UserReport) += 1,
        sdk = client_name_tag(reports.headers.meta().client_name()),
        has_event_id = has_event_id.to_string(),
    );

    if !has_event_id {
        return Err(Error::NoEventId);
    }

    Ok(())
}
