use crate::managed::Managed;
use crate::processing;
use crate::processing::user_reports::SerializedUserReports;

pub fn process(reports: &mut Managed<SerializedUserReports>) {
    reports.modify(|reports, records| {
        processing::utils::user_reports::process_user_reports(&mut reports.reports, records);
    })
}
