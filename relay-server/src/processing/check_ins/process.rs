use crate::managed::{Managed, RecordKeeper, Rejected};
use crate::processing::Processor;
use crate::processing::check_ins::{CheckInsProcessor, Error, ExpandedCheckIn, SerializedCheckIns};
use crate::services::outcome::{DiscardReason, Outcome};
use relay_monitors::ProcessCheckInError;

/// Normalizes all check-ins using the [`relay_monitors`] module.
///
/// Individual, invalid check-ins will be discarded.
pub fn normalize(check_in: &mut Managed<ExpandedCheckIn>) -> Result<(), Rejected<Error>> {
    check_in.try_modify(|c, _| {
        relay_monitors::normalize(&mut c.check_in).inspect_err(|err| {
            relay_log::debug!(
                error = err as &dyn std::error::Error,
                "dropped invalid monitor check-in"
            )
        })?;

        Ok::<_, Error>(())
    })
}

pub fn expand(
    input: Managed<SerializedCheckIns>,
) -> Result<Managed<ExpandedCheckIn>, Rejected<<CheckInsProcessor as Processor>::Error>> {
    input.try_map(expand_check_in)
}

/// Deserializes a SerializedCheckIns into a single ExpandedCheckIn.  Extra checkins items are
/// discarded as invalid outcomes.
fn expand_check_in(
    sc: SerializedCheckIns,
    record_keeper: &mut RecordKeeper<'_>,
) -> Result<ExpandedCheckIn, Error> {
    let SerializedCheckIns { headers, check_ins } = sc;

    let mut check_ins = check_ins.into_iter();

    // We know that we have at least one check-in, due to guards in prepare_envelope.  Still,
    // rather return an error than call an unwrap.
    let Some(item) = check_ins.next() else {
        return Err(Error::Processing(ProcessCheckInError::MissingCheckIn));
    };
    for extra in check_ins {
        record_keeper.reject_err(Outcome::Invalid(DiscardReason::DuplicateItem), extra);
    }
    let check_in = serde_json::from_slice(&item.payload())
        .map_err(ProcessCheckInError::from)
        .map_err(Error::from)?;

    Ok(ExpandedCheckIn {
        headers,
        check_in,
        item,
    })
}
