use bytes::Bytes;
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::UserReport;
use relay_quotas::DataCategory;

use crate::envelope::{ContentType, Item};
use crate::managed::{Managed, RecordKeeper, Rejected};
use crate::processing::errors::errors::{self, ErrorKind, SentryError as _};
use crate::processing::errors::{Error, ExpandedError, Flags, Result, SerializedError};
use crate::processing::utils::event::EventFullyNormalized;
use crate::processing::{self, Context};
use crate::services::processor::ProcessingError;

pub fn expand(error: Managed<SerializedError>, ctx: Context<'_>) -> Managed<ExpandedError> {
    error.map(|error, records| do_expand(error, ctx, records).unwrap())
}

fn do_expand(
    mut error: SerializedError,
    ctx: Context<'_>,
    records: &mut RecordKeeper<'_>,
) -> Result<ExpandedError> {
    let is_trusted = error.headers.meta().request_trust().is_trusted();

    let ctx = errors::Context {
        envelope: &error.headers,
        processing: ctx,
    };

    records.lenient(DataCategory::Attachment);
    records.lenient(DataCategory::AttachmentItem);
    records.lenient(DataCategory::UserReportV2);

    // TODO: support the "only expand errors in processing" usecase
    let parsed = ErrorKind::try_expand(&mut error.items, ctx)
        .unwrap()
        .unwrap();

    // TODO: think about forward compatibility with the remaining `items`.
    // TODO: event size limit(s), maybe in serialize?

    Ok(ExpandedError {
        headers: error.headers,
        flags: Flags {
            fully_normalized: EventFullyNormalized(is_trusted && parsed.fully_normalized),
        },
        metrics: Default::default(),
        error: parsed.error,
    })
}

pub fn process(
    error: &mut Managed<ExpandedError>,
    ctx: Context<'_>,
) -> Result<(), Rejected<Error>> {
    error.try_modify(|error, records| {
        records.lenient(DataCategory::Attachment);
        records.lenient(DataCategory::AttachmentItem);
        records.lenient(DataCategory::UserReportV2);

        error.error.process(errors::Context {
            envelope: &error.headers,
            processing: ctx,
        })?;

        if let Some(user_reports) = error.error.as_ref_mut().user_reports {
            process_user_reports(user_reports, records);
        }

        Ok::<_, Error>(())
    })
}

pub fn finalize(
    error: &mut Managed<ExpandedError>,
    ctx: Context<'_>,
) -> Result<(), Rejected<Error>> {
    error.try_modify(|error, _| {
        let e = error.error.as_ref_mut();

        processing::utils::event::finalize(
            &error.headers,
            e.event,
            e.attachments.iter(),
            &mut error.metrics,
            ctx.config,
        )?;

        Ok::<_, Error>(())
    })
}

pub fn normalize(
    error: &mut Managed<ExpandedError>,
    geoip_lookup: &GeoIpLookup,
    ctx: Context<'_>,
) -> Result<(), Rejected<Error>> {
    let scoping = error.scoping();

    error.try_modify(|error, _| {
        let e = error.error.as_ref_mut();

        error.flags.fully_normalized = processing::utils::event::normalize(
            &error.headers,
            e.event,
            error.flags.fully_normalized,
            scoping.project_id,
            ctx,
            geoip_lookup,
        )?;

        Ok::<_, Error>(())
    })
}

pub fn scrub(error: &mut Managed<ExpandedError>, ctx: Context<'_>) -> Result<(), Rejected<Error>> {
    error.try_modify(|error, _| {
        let e = error.error.as_ref_mut();

        processing::utils::event::scrub(e.event, ctx.project_info)?;
        processing::utils::attachments::scrub(e.attachments.iter_mut(), ctx.project_info);

        Ok::<_, Error>(())
    })
}

/// Validates and normalizes all user report items in the envelope.
///
/// User feedback items are removed from the envelope if they contain invalid JSON or if the
/// JSON violates the schema (basic type validation). Otherwise, their normalized representation
/// is written back into the item.
fn process_user_reports(user_reports: &mut Vec<Item>, records: &mut RecordKeeper<'_>) {
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

fn process_user_report(user_report: Bytes) -> Result<Bytes> {
    // There is a customer SDK which sends invalid reports with a trailing `\n`,
    // strip it here, even if they update/fix their SDK there will still be many old
    // versions with the broken SDK out there.
    let user_report = trim_whitespaces(&user_report);

    let report =
        serde_json::from_slice::<UserReport>(user_report).map_err(ProcessingError::InvalidJson)?;

    serde_json::to_string(&report)
        .map(Bytes::from)
        .map_err(ProcessingError::SerializeFailed)
        .map_err(Into::into)
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
