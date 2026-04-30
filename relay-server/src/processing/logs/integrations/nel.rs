use relay_event_normalization::nel;
use relay_event_schema::protocol::OurLog;
use relay_protocol::DeserializableAnnotated;

use crate::envelope::EnvelopeHeaders;
use crate::processing::logs::{Error, Result, Settings};
use crate::services::outcome::DiscardReason;

/// Expands OTeL logs into the [`OurLog`] format.
pub fn expand<F>(payload: &[u8], headers: &EnvelopeHeaders, produce: F) -> Result<Settings>
where
    F: FnMut(OurLog),
{
    let received_at = headers.meta().received_at();

    serde_json::from_slice::<Vec<_>>(payload)
        .map_err(|_| Error::Invalid(DiscardReason::InvalidJson))?
        .into_iter()
        .filter_map(|DeserializableAnnotated(nel)| nel::create_log(nel, received_at))
        .for_each(produce);

    Ok(Settings {
        infer_user_agent: true,
        infer_ip: false,
    })
}
