use relay_event_normalization::nel;
use relay_event_schema::protocol::OurLog;
use relay_protocol::DeserializableAnnotated;

use crate::envelope::EnvelopeHeaders;
use crate::processing::logs::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands OTeL logs into the [`OurLog`] format.
pub fn expand2(
    payload: &[u8],
    headers: &EnvelopeHeaders,
) -> Result<Box<dyn Iterator<Item = OurLog>>> {
    let received_at = headers.meta().received_at();

    Ok(Box::new(
        serde_json::from_slice::<Vec<_>>(payload)
            .map_err(|_| Error::Invalid(DiscardReason::InvalidJson))?
            .into_iter()
            .filter_map(move |DeserializableAnnotated(nel)| nel::create_log(nel, received_at)),
    ))
}
