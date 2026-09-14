use crate::managed::{Managed, RecordKeeper, Rejected};
use crate::processing::Processor;
use crate::processing::check_ins::{CheckInsProcessor, Error, ExpandedCheckIn, SerializedCheckIn};
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
    input: Managed<SerializedCheckIn>,
) -> Result<Managed<ExpandedCheckIn>, Rejected<<CheckInsProcessor as Processor>::Error>> {
    input.try_map(expand_check_in)
}

/// Deserializes a SerializedCheckIns into a single ExpandedCheckIn.  Extra checkins items are
/// discarded as invalid outcomes.
fn expand_check_in(
    sc: SerializedCheckIn,
    _record_keeper: &mut RecordKeeper<'_>,
) -> Result<ExpandedCheckIn, Error> {
    let SerializedCheckIn {
        headers,
        check_in: item,
    } = sc;

    let check_in = serde_json::from_slice(&item.payload())
        .map_err(ProcessCheckInError::from)
        .map_err(Error::from)?;

    Ok(ExpandedCheckIn {
        headers,
        check_in,
        item,
    })
}
