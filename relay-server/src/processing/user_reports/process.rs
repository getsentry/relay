use bytes::Bytes;
use relay_event_schema::protocol::UserReport;

use crate::envelope::{ContentType, Item};
use crate::managed::Managed;
use crate::managed::RecordKeeper;
use crate::processing::user_reports::SerializedUserReports;
use crate::services::processor::ProcessingError;

pub fn process(reports: &mut Managed<SerializedUserReports>) {
    reports.modify(|reports, records| {
        process_user_reports(&mut reports.reports, records);
    })
}

/// Validates and normalizes all user report items in the envelope.
///
/// User feedback items are removed from the envelope if they contain invalid JSON or if the
/// JSON violates the schema (basic type validation). Otherwise, their normalized representation
/// is written back into the item.
pub fn process_user_reports(user_reports: &mut Vec<Item>, records: &mut RecordKeeper<'_>) {
    for mut user_report in std::mem::take(user_reports) {
        let data = match process_user_report(user_report.payload()) {
            Ok(data) => data,
            Err(err) => {
                records.reject_err(err, user_report);
                continue;
            }
        };
        user_report.set_payload(ContentType::Json, data);
        user_reports.push(user_report);
    }
}

fn process_user_report(user_report: Bytes) -> Result<Bytes, ProcessingError> {
    // There is a customer SDK which sends invalid reports with a trailing `\n`,
    // strip it here, even if they update/fix their SDK there will still be many old
    // versions with the broken SDK out there.
    let user_report = trim_whitespaces(&user_report);

    let report =
        serde_json::from_slice::<UserReport>(user_report).map_err(ProcessingError::InvalidJson)?;

    serde_json::to_string(&report)
        .map(Bytes::from)
        .map_err(ProcessingError::SerializeFailed)
}

fn trim_whitespaces(data: &[u8]) -> &[u8] {
    let Some(from) = data.iter().position(|x| !x.is_ascii_whitespace()) else {
        return &[];
    };
    let Some(to) = data.iter().rposition(|x| !x.is_ascii_whitespace()) else {
        return &[];
    };
    &data[from..to + 1]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trim_whitespaces() {
        assert_eq!(trim_whitespaces(b""), b"");
        assert_eq!(trim_whitespaces(b" \n\r "), b"");
        assert_eq!(trim_whitespaces(b" \nx\r "), b"x");
        assert_eq!(trim_whitespaces(b" {foo: bar} "), b"{foo: bar}");
        assert_eq!(trim_whitespaces(b"{ foo: bar}"), b"{ foo: bar}");
    }
}
