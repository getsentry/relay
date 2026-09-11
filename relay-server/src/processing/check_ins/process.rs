use crate::managed::{Managed, RecordKeeper, Rejected};
use crate::processing::check_ins::{Error, ExpandedCheckIn, SerializedCheckIns};
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

/// Deserializes a SerializedCheckIns into a single ExpandedCheckIn.  Extra checkins items are
/// discarded as invalid outcomes.
pub fn expand_check_in(
    sc: SerializedCheckIns,
    record_keeper: &mut RecordKeeper<'_>,
) -> Result<ExpandedCheckIn, Error> {
    let SerializedCheckIns { headers, check_ins } = sc;

    let mut check_ins = check_ins.into_iter();

    // We know that we have at least one check-in, due to guards.
    let item = check_ins.next().unwrap();
    for extra in check_ins {
        record_keeper.reject_err(Outcome::Invalid(DiscardReason::TooManyItems), extra);
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
