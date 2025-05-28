use smallvec::SmallVec;

use crate::envelope::{Item, ItemType, Items};
use crate::processing::{self, Context, Counted, Managed, Output, Quantities};
use crate::services::processor::ProcessingError;
use crate::utils::ManagedEnvelope;

pub struct LogsProcessor {}

impl LogsProcessor {
    pub fn new() -> Self {
        Self {}
    }
}

impl processing::Processor for LogsProcessor {
    type UnitOfWork = EinsLog;
    type Output = ();
    type Error = ProcessingError;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let otel_logs = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::OtelLog));
        let logs = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Log));

        // if !logs_items.is_empty() {
        //     grouped_envelopes.push((
        //         ProcessingGroup::Log,
        //         Envelope::from_parts(headers.clone(), logs_items),
        //     ))
        // }

        let work = EinsLog { otel_logs, logs };
        Some(Managed::from_envelope(envelope, work))
    }

    fn process(
        &self,
        work: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<()>, ProcessingError> {
        // ourlog::filter(
        //     managed_envelope,
        //     &self.inner.config,
        //     &project_info,
        //     &self.inner.global_config.current(),
        // );
        //
        // self.enforce_quotas(
        //     managed_envelope,
        //     Annotated::empty(),
        //     &mut extracted_metrics,
        //     &project_info,
        //     &rate_limits,
        // )
        // .await?;
        //
        // if_processing!(self.inner.config, {
        //     ourlog::process(managed_envelope, &project_info)?;
        // });

        Ok(Output::just(()))
    }
}

struct EinsLog {
    /// Otel Logs are not sent in containers, an envelope is very likely to contain multiple otel logs.
    otel_logs: Items,
    /// Logs are sent in item containers, there is specified limit of a single container per
    /// envelope.
    ///
    /// But at this point this has not yet been validated.
    // TODO: validation, that there is only a single log item (for now).
    logs: Items,
}

impl Counted for EinsLog {
    fn quantities(&self) -> Quantities {
        todo!()
    }
}
