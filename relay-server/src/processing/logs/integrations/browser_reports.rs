use chrono::{DateTime, Utc};
use relay_event_normalization::{browser_report, csp, nel};
use relay_event_schema::protocol::{CspViolationReport, NetworkReportRaw, OurLog};
use relay_protocol::DeserializableAnnotated;
use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::value::RawValue;

use crate::envelope::EnvelopeHeaders;
use crate::processing::logs::{Error, Result, Settings};
use crate::services::outcome::DiscardReason;

/// Expands browser reports into the [`OurLog`] format.
///
/// The payload is either a batch of reports sent through the Reporting API, or a single legacy CSP
/// report sent via the `report-uri` directive. Every report is converted into a log. CSP and NEL
/// reports have dedicated conversions, all other report types are converted generically.
///
/// Invalid reports are skipped.
pub fn expand<F>(payload: &[u8], headers: &EnvelopeHeaders, mut produce: F) -> Result<Settings>
where
    F: FnMut(OurLog),
{
    let received_at = headers.meta().received_at();

    let mut convert = |report: &[u8]| match convert_report(report, received_at) {
        Ok(Some(log)) => produce(log),
        Ok(None) => {}
        Err(error) => relay_log::debug!(
            error = &error as &dyn std::error::Error,
            "skipping invalid browser report"
        ),
    };

    match serde_json::from_slice::<Vec<&RawValue>>(payload) {
        Ok(reports) => reports
            .into_iter()
            .for_each(|report| convert(report.get().as_bytes())),
        Err(_) => {
            // Legacy CSP reports are sent as a single JSON object. Validate that the payload is
            // JSON, so we can reject garbage payloads with a proper outcome.
            serde_json::from_slice::<&RawValue>(payload)
                .map_err(|_| Error::Invalid(DiscardReason::InvalidJson))?;
            convert(payload);
        }
    }

    Ok(Settings {
        infer_user_agent: true,
        infer_ip: false,
    })
}

/// Converts a single JSON encoded browser report into a log.
fn convert_report(report: &[u8], received_at: DateTime<Utc>) -> serde_json::Result<Option<OurLog>> {
    #[derive(Deserialize)]
    struct ReportType {
        #[serde(default, rename = "type")]
        ty: Option<String>,
        #[serde(default, rename = "csp-report")]
        csp_report: Option<IgnoredAny>,
    }

    let ReportType { ty, csp_report } = serde_json::from_slice(report)?;

    if csp_report.is_some() {
        return csp_log(report, received_at);
    }

    match ty.as_deref() {
        Some("csp-violation") => csp_log(report, received_at),
        Some("network-error") => {
            let DeserializableAnnotated(report) =
                serde_json::from_slice::<DeserializableAnnotated<NetworkReportRaw>>(report)?;
            Ok(nel::create_log(report, received_at))
        }
        Some(_) => browser_report::create_log(report, received_at).map(Some),
        None => Ok(None),
    }
}

fn csp_log(report: &[u8], received_at: DateTime<Utc>) -> serde_json::Result<Option<OurLog>> {
    let report = CspViolationReport::from_json(report)?;
    Ok(report.map(|report| csp::create_log(report, received_at)))
}
