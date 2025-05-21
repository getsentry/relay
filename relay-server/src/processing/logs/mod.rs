use smallvec::SmallVec;

use crate::envelope::{Item, ItemType, Items};
use crate::processing;
use crate::services::processor::ProcessingError;
use crate::utils::ManagedEnvelope;

pub struct LogsProcessor {}

impl processing::Processor for LogsProcessor {
    type UnitOfWork = EinsLog;
    type Result = ();
    type Error = ProcessingError;

    fn prepare(&self, envelope: &mut ManagedEnvelope) -> UnitOfWork {
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

        EinsLog { otel_logs, logs }
    }

    fn process(&self, work: UnitOfWork) -> Result<(), ProcessingError> {
        todo!()
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
