use crate::Envelope;
use crate::managed::{Managed, Rejected};
use crate::processing::user_reports::{SerializedUserReports, UserReportsOutput};
use crate::processing::{Forward, ForwardContext};

impl Forward for UserReportsOutput {
    fn serialize_envelope(
        self,
        _: ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        let Self(reports) = self;
        let envelope = reports.map(|SerializedUserReports { headers, reports }, _| {
            Envelope::from_parts(headers, reports.into())
        });
        Ok(envelope)
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: crate::processing::StoreHandle<'_>,
        _: ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        use crate::services::store::StoreUserReport;

        let Self(reports) = self;
        let Some(event_id) = reports.headers.event_id() else {
            return Err(reports.reject_err(super::Error::NoEventId).map(drop));
        };

        for report in reports.split(|report| report.reports) {
            let store_report = report.map(|report, _| StoreUserReport { event_id, report });
            s.send_to_store(store_report);
        }

        Ok(())
    }
}
