use relay_event_normalization::nel;
use relay_event_schema::protocol::{OurLog, OurLogLevel, Timestamp, TraceId};
use relay_protocol::Annotated;

use crate::{extractors::RequestMeta, utils::ManagedEnvelope};

pub fn convert_to_logs(envelope: &mut ManagedEnvelope) {
    let items = envelope.take_items_by(|item| item.ty() == &ItemType::Nel);

    log_from_nel_item(item, envelope.meta()).inspect_err(|error| {
        relay_log::error!(error = error as &dyn Error, "failed to extract NEL report");
    });
}

fn log_from_nel_item(item: Item, meta: &RequestMeta) -> Result<ExtractedEvent, ProcessingError> {
    let len = item.len();
    let data: &[u8] = &item.payload();

    // Try to get the raw network report.
    let report = Annotated::from_json_bytes(data).map_err(NetworkReportError::InvalidJson);

    match report {
        // If the incoming payload could be converted into the raw network error, try
        // to use it to normalize the event.
        Ok(report) => nel::create_log(report, meta.received_at()),
        Err(err) => {
            todo!();
            // // logged in extract_event
            // relay_log::configure_scope(|scope| {
            //     scope.set_extra("payload", String::from_utf8_lossy(data).into());
            // });
            // return Err(ProcessingError::InvalidNelReport(err));
        }
    }

    Ok((Annotated::new(event), len))
}
