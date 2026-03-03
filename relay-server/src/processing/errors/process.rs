use bytes::Bytes;
use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::UserReport;
use relay_quotas::DataCategory;

use crate::envelope::{ContentType, Item};
use crate::managed::{Managed, RecordKeeper, Rejected};
use crate::processing::errors::errors::{self, ErrorKind, SentryError as _};
use crate::processing::errors::{Error, ExpandedError, Result, SerializedError};
use crate::processing::utils::event::EventFullyNormalized;
use crate::processing::{self, Context};
use crate::services::processor::ProcessingError;

/// Expands an (error) event envelope.
///
/// Identifies the underlying type of error/event and expands it into an event and its parts.
///
/// For example an crash report attachment may be expanded into an error event, multiple other
/// attachments and some user feedback.
pub fn expand(
    error: Managed<SerializedError>,
    ctx: Context<'_>,
) -> Result<Managed<ExpandedError>, Rejected<Error>> {
    error.try_map(|error, records| do_expand(error, ctx, records))
}

fn do_expand(
    mut error: SerializedError,
    ctx: Context<'_>,
    records: &mut RecordKeeper<'_>,
) -> Result<ExpandedError> {
    let is_trusted = error.headers.meta().request_trust().is_trusted();

    // Certain attachment types are dissolved into different types (Nintendo Switch),
    // or are created from other types (unreal).
    records.lenient(DataCategory::Attachment);
    records.lenient(DataCategory::AttachmentItem);
    // User feedback is extracted from unreal reports.
    records.lenient(DataCategory::UserReportV2);

    let Some(parsed) = ErrorKind::try_expand(
        &mut error.items,
        errors::Context {
            envelope: &error.headers,
            processing: ctx,
        },
    )?
    else {
        return Err(ProcessingError::NoEventPayload.into());
    };

    // All unprocessed items which would create an event but were not turned into an event are
    // duplicates.
    //
    // E.g. if you send an envelope with two error events, you end up here.
    //
    // It is reasonable to just reject such envelopes, this is ported over from a prior
    // implementation of the error processing pipeline.
    for item in error.items.extract_if(.., |item| item.creates_event()) {
        records.reject_err(ProcessingError::DuplicateItem(item.ty().clone()), item);
    }

    if ctx.is_processing() {
        // In processing, there are cannot be any leftover events due to forward compatibility.
        for item in std::mem::take(&mut error.items) {
            records.reject_err(ProcessingError::UnsupportedItem, item);
        }
    }

    Ok(ExpandedError {
        headers: error.headers,
        fully_normalized: EventFullyNormalized(is_trusted && parsed.fully_normalized),
        metrics: parsed.metrics,
        event: parsed.event,
        attachments: parsed.attachments,
        user_reports: parsed.user_reports,
        data: parsed.error,
        other: error.items,
    })
}

pub fn process(error: &mut Managed<ExpandedError>) -> Result<(), Rejected<Error>> {
    error.try_modify(|error, records| {
        process_user_reports(&mut error.user_reports, records);

        Ok::<_, Error>(())
    })
}

pub fn finalize(
    error: &mut Managed<ExpandedError>,
    ctx: Context<'_>,
) -> Result<(), Rejected<Error>> {
    error.try_modify(|error, _| {
        processing::utils::event::finalize(
            &error.headers,
            &mut error.event,
            error.attachments.iter(),
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
        error.fully_normalized = processing::utils::event::normalize(
            &error.headers,
            &mut error.event,
            error.fully_normalized,
            scoping.project_id,
            ctx,
            geoip_lookup,
        )?;

        Ok::<_, Error>(())
    })
}

pub fn scrub(error: &mut Managed<ExpandedError>, ctx: Context<'_>) -> Result<(), Rejected<Error>> {
    error.try_modify(|error, _| {
        processing::utils::event::scrub(&mut error.event, ctx.project_info)?;
        processing::utils::attachments::scrub(error.attachments.iter_mut(), ctx.project_info);
        if let Some(minidump) = error.data.minidump_mut() {
            processing::utils::attachments::scrub(std::iter::once(minidump), ctx.project_info);
        }

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
