use crate::Envelope;
use crate::managed::{Counted, Managed, ManagedEnvelope, Rejected};
use crate::processing::{self, Context, Forward, Output};

#[derive(Debug, thiserror::Error)]
pub enum Error {}

/// A processor for Replays.
pub struct ReplaysProcessor {}

// TODO: Q: We seems to use both `processing::Processor` and `Processor` ask about the preference.
impl processing::Processor for ReplaysProcessor {
    type UnitOfWork = SerializedReplays;
    type Output = ReplaysOutput;
    type Error = Error;

    fn prepare_envelope(
        &self,
        _envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        todo!()
    }

    async fn process(
        &self,
        _work: Managed<Self::UnitOfWork>,
        _ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        todo!()
    }
}

#[derive(Debug)]
pub struct ReplaysOutput();

impl Forward for ReplaysOutput {
    fn serialize_envelope(
        self,
        _ctx: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        todo!()
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        _s: processing::StoreHandle<'_>,
        _ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        todo!()
    }
}

#[derive(Debug)]
pub struct SerializedReplays {}

impl Counted for SerializedReplays {
    fn quantities(&self) -> crate::managed::Quantities {
        todo!()
    }
}
