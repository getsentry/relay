use relay_event_normalization::nel;
use relay_event_schema::protocol::{NetworkReportError, OurLog};
use relay_protocol::Annotated;

use crate::envelope::{ContainerItems, Item, ItemContainer, ItemType, WithHeader};
use crate::extractors::RequestMeta;
use crate::managed::ManagedEnvelope;
use crate::services::processor::ProcessingError;

pub fn convert_to_logs(envelope: &mut ManagedEnvelope) {
    let items = envelope
        .envelope_mut()
        .take_items_by(|item| item.ty() == ItemType::Nel);
    let mut logs = ContainerItems::new();

    for item in items {
        match log_from_nel_item(&item, envelope.meta()) {
            Ok(Some(log)) => logs.push(WithHeader::just(log)),
            Ok(None) => {}
            Err(error) => {
                let mut payload = item.payload();
                relay_log::with_scope(
                    move |scope| {
                        payload.truncate(200_000);
                        scope.add_attachment(relay_log::protocol::Attachment {
                            buffer: payload.into(),
                            filename: "payload.json".to_owned(),
                            content_type: Some("application/json".to_owned()),
                            ty: None,
                        })
                    },
                    || {
                        relay_log::error!(
                            error = &error as &dyn std::error::Error,
                            "failed to parse NEL report"
                        )
                    },
                );
            }
        };
    }

    if logs.is_empty() {
        return;
    }

    let mut item = Item::new(ItemType::Log);
    if let Ok(()) = ItemContainer::from(logs).write_to(&mut item) {
        envelope.envelope_mut().add_item(item);
    }
}

fn log_from_nel_item(
    item: &Item,
    meta: &RequestMeta,
) -> Result<Option<Annotated<OurLog>>, ProcessingError> {
    let payload = item.payload();

    let report = Annotated::from_json_bytes(&payload)
        .map_err(NetworkReportError::InvalidJson)
        .map_err(ProcessingError::InvalidNelReport)?;
    let Some(log) = nel::create_log(report, meta.received_at()) else {
        return Ok(None);
    };

    Ok(Some(Annotated::new(log)))
}
