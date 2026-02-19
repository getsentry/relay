use crate::Envelope;
use crate::envelope::EnvelopeHeaders;
use crate::managed::{Counted, Managed, Quantities};
use crate::processing::CountRateLimited;
use crate::processing::transactions::Error;
use crate::processing::transactions::types::ExpandedProfile;

/// A standalone profile, which is no longer attached to a transaction as the transaction was
/// dropped by dynamic sampling.
#[derive(Debug)]
pub struct StandaloneProfile {
    pub headers: EnvelopeHeaders,
    pub profile: ExpandedProfile,
}

impl StandaloneProfile {
    pub fn serialize_envelope(self) -> Box<Envelope> {
        let mut item = self.profile.serialize_item();
        // This profile is now standalone, as dynamic sampling dropped the transaction.
        item.set_sampled(false);

        Envelope::from_parts(self.headers, smallvec::smallvec![item])
    }
}

impl Counted for StandaloneProfile {
    fn quantities(&self) -> Quantities {
        self.profile.quantities()
    }
}

impl CountRateLimited for Managed<Box<StandaloneProfile>> {
    type Error = Error;
}
