use relay_event_normalization::GeoIpLookup;

use crate::managed::{Managed, RecordKeeper, Rejected};
use crate::processing::errors::types::GenericError;
use crate::processing::errors::{Error, ExpandedError, Result, SerializedError};
use crate::processing::utils::event::EventFullyNormalized;
use crate::processing::{self, Context};

pub fn expand(error: Managed<SerializedError>) -> Managed<ExpandedError> {
    error.map(|error, records| do_expand(error, records).unwrap())
}

fn do_expand(mut error: SerializedError, _: &mut RecordKeeper<'_>) -> Result<ExpandedError> {
    // let mut event: Option<Annotated<Event>> = None;
    // for x in [&EventExtract as &dyn Foobar] {
    //     // Only attempt to parse an event, if we don't already have one.
    //     if event.is_none() {
    //         event = x.try_parse(&mut error.items)?;
    //     }
    //
    //     // Remove all duplicates, even if we parsed an event there may be more duplicates of the
    //     // same type in the envelope, which need to be removed.
    //     for item in error.items.drain_filter(|item| x.matches(item)) {
    //         // TODO: reject duplicate
    //         records.reject_err(None, item);
    //     }
    // }

    let mut kind;

    if let Some(error) = GenericError::try_parse(&mut error.items)? {
        kind = error.into();
    } else {
        todo!()
    }

    Ok(ExpandedError {
        headers: error.headers,
        // TODO: grab this from the event item header and check against meta is_from_trusted()
        fully_normalized: EventFullyNormalized(false),
        metrics: Default::default(),
        kind,
    })
}

pub fn process(mut error: &mut Managed<ExpandedError>) {}

pub fn finalize(
    error: &mut Managed<ExpandedError>,
    ctx: Context<'_>,
) -> Result<(), Rejected<Error>> {
    error.try_modify(|error, _| {
        let e = error.kind.as_ref_mut();

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
        let e = error.kind.as_ref_mut();

        error.fully_normalized = processing::utils::event::normalize(
            &error.headers,
            e.event,
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
        let e = error.kind.as_ref_mut();

        processing::utils::event::scrub(e.event, ctx.project_info)?;
        processing::utils::attachments::scrub(e.attachments.iter_mut(), ctx.project_info);

        Ok::<_, Error>(())
    })
}
