use relay_system::Addr;

use crate::envelope::{EnvelopeHeaders, Item, ItemType};
use crate::managed::{Counted, Managed, ManagedEnvelope, Quantities, Rejected};
use crate::processing::{self, Context, Nothing, Output};
use crate::services::outcome::TrackOutcome;

mod process;

// TODO: Not sure there is a cleaner way to do this.
#[derive(Debug, thiserror::Error)]
pub enum Error {}

/// A processor for Client-Reports.
pub struct ClientReportsProcessor {
    aggregator: Addr<TrackOutcome>,
}

impl ClientReportsProcessor {
    /// Creates a new [`Self`].
    pub fn new(aggregator: Addr<TrackOutcome>) -> Self {
        Self { aggregator }
    }
}

impl processing::Processor for ClientReportsProcessor {
    type UnitOfWork = SerializedClientReport;
    type Output = Nothing;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let headers = envelope.envelope().headers().clone();

        let client_reports = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::ClientReport))
            .into_vec();

        let work = SerializedClientReport {
            headers,
            client_reports,
        };
        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        mut client_reports: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        process::process_client_reports(
            &mut client_reports,
            ctx.config,
            ctx.project_info,
            &self.aggregator,
        );

        Ok(Output::empty())
    }
}

/// Client-Reports in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedClientReport {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// A list of client-reports waiting to be processed.
    ///
    /// All items contained here must be client-reports.
    client_reports: Vec<Item>,
}

impl Counted for SerializedClientReport {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![]
    }
}
