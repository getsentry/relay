use std::fmt;

use chrono::{DateTime, Utc};
use relay_event_normalization::{browser_report, csp, integrity, nel};
use relay_event_schema::protocol::{
    CspViolationReport, IntegrityReportRaw, NetworkReportRaw, OurLog,
};
use relay_protocol::DeserializableAnnotated;
use serde::de::{IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

use crate::envelope::EnvelopeHeaders;
use crate::processing::logs::{Error, Result, Settings};
use crate::services::outcome::DiscardReason;

/// Expands browser reports into the [`OurLog`] format.
///
/// The payload is either a batch of reports sent through the Reporting API, or a single legacy CSP
/// report sent via the `report-uri` directive. Every report is converted into a log. CSP, NEL and
/// integrity reports have dedicated conversions, all other report types are converted generically.
///
/// Like log containers, the payload may spend at most `max_ops` deserialization operations. This
/// bounds the amount of logs expanded from a single dense batch. Payloads exceeding the budget are
/// rejected as a whole, before any report is converted.
///
/// Invalid reports are skipped.
pub fn expand<F>(
    payload: &[u8],
    headers: &EnvelopeHeaders,
    max_ops: usize,
    mut produce: F,
) -> Result<Settings>
where
    F: FnMut(OurLog),
{
    check_budget(payload, max_ops)?;

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

/// Validates that `payload` is JSON and fits into the `max_ops` deserialization budget.
///
/// Every value in the payload, including nested values, is charged to the budget, the same way as
/// for log containers.
fn check_budget(payload: &[u8], max_ops: usize) -> Result<()> {
    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    relay_serialization::serde::deserialize::<Visit, _>(&mut deserializer, max_ops).map_err(
        |err| {
            relay_log::debug!("failed to parse browser reports: {err}");

            if err.is_limit_exceeded() {
                Error::Invalid(DiscardReason::RequestTooLarge)
            } else {
                Error::Invalid(DiscardReason::InvalidJson)
            }
        },
    )?;
    Ok(())
}

/// Visits every value of a JSON document without retaining it.
///
/// Unlike [`IgnoredAny`], this visits all nested values through the deserializer, so that they are
/// charged to the operation budget.
struct Visit;

impl<'de> Deserialize<'de> for Visit {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        deserializer.deserialize_any(Visit)
    }
}

impl<'de> Visitor<'de> for Visit {
    type Value = Visit;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_bool<E>(self, _: bool) -> std::result::Result<Self, E> {
        Ok(Visit)
    }

    fn visit_i64<E>(self, _: i64) -> std::result::Result<Self, E> {
        Ok(Visit)
    }

    fn visit_u64<E>(self, _: u64) -> std::result::Result<Self, E> {
        Ok(Visit)
    }

    fn visit_f64<E>(self, _: f64) -> std::result::Result<Self, E> {
        Ok(Visit)
    }

    fn visit_str<E>(self, _: &str) -> std::result::Result<Self, E> {
        Ok(Visit)
    }

    fn visit_unit<E>(self) -> std::result::Result<Self, E> {
        Ok(Visit)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Self, A::Error> {
        while seq.next_element::<Visit>()?.is_some() {}
        Ok(Visit)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Self, A::Error> {
        while map.next_entry::<Visit, Visit>()?.is_some() {}
        Ok(Visit)
    }
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
        Some("integrity-violation") => {
            let DeserializableAnnotated(report) =
                serde_json::from_slice::<DeserializableAnnotated<IntegrityReportRaw>>(report)?;
            Ok(integrity::create_log(report, received_at))
        }
        Some(_) => browser_report::create_log(report, received_at).map(Some),
        None => Ok(None),
    }
}

fn csp_log(report: &[u8], received_at: DateTime<Utc>) -> serde_json::Result<Option<OurLog>> {
    let report = CspViolationReport::from_json(report)?;
    Ok(report.map(|report| csp::create_log(report, received_at)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const BATCH: &[u8] = br#"[
        {"type": "deprecation", "age": 0, "url": "https://example.com", "body": {"id": "a", "message": "a"}},
        {"type": "deprecation", "age": 0, "url": "https://example.com", "body": {"id": "b", "message": "b"}}
    ]"#;

    #[test]
    fn test_check_budget_within_limit() {
        assert!(check_budget(BATCH, 1_000).is_ok());
    }

    #[test]
    fn test_check_budget_exceeded() {
        assert!(matches!(
            check_budget(BATCH, 10),
            Err(Error::Invalid(DiscardReason::RequestTooLarge))
        ));
    }

    #[test]
    fn test_check_budget_charges_nested_values() {
        // Nested values must be charged, a flat payload of the same report count fits a budget
        // which a nested one exceeds.
        let flat = br#"[{"type": "a"}, {"type": "b"}]"#;
        let nested =
            br#"[{"type": "a", "body": {"x": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}}, {"type": "b"}]"#;
        assert!(check_budget(flat, 15).is_ok());
        assert!(check_budget(nested, 15).is_err());
    }

    #[test]
    fn test_check_budget_invalid_json() {
        assert!(matches!(
            check_budget(b"not json", 1_000),
            Err(Error::Invalid(DiscardReason::InvalidJson))
        ));
    }
}
