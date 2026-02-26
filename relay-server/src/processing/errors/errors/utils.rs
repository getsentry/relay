use relay_base_schema::events::EventType;
use relay_event_schema::protocol::{Event, Metrics};
use relay_protocol::Annotated;

use crate::envelope::{Item, ItemType};
use crate::processing::errors::Result;
use crate::processing::errors::errors::Context;
use crate::services::outcome::DiscardItemType;
use crate::services::processor::ProcessingError;

pub fn take_item_by<F>(items: &mut Vec<Item>, f: F) -> Option<Item>
where
    F: FnMut(&Item) -> bool,
{
    let index = items.iter().position(f);
    index.map(|index| items.swap_remove(index))
}

pub fn take_item_of_type(items: &mut Vec<Item>, ty: ItemType) -> Option<Item> {
    take_item_by(items, |item| item.ty() == &ty)
}

pub fn take_items_by<T, F>(items: &mut Vec<Item>, mut f: F) -> T
where
    F: FnMut(&Item) -> bool,
    T: FromIterator<Item>,
{
    items.extract_if(.., |item| f(item)).collect()
}

pub fn take_items_of_type<T>(items: &mut Vec<Item>, ty: ItemType) -> T
where
    T: FromIterator<Item>,
{
    take_items_by(items, |item| item.ty() == &ty)
}

pub fn event_from_json_payload(
    item: Item,
    event_type: impl Into<Option<EventType>>,
    metrics: &mut Metrics,
    ctx: Context<'_>,
) -> Result<Annotated<Event>> {
    event_from_json(&item.payload(), event_type, metrics, ctx)
}

pub fn event_from_json(
    payload: &[u8],
    event_type: impl Into<Option<EventType>>,
    metrics: &mut Metrics,
    ctx: Context<'_>,
) -> Result<Annotated<Event>> {
    let event_type = event_type.into();

    if payload.len() > ctx.processing.config.max_event_size() {
        return Err(ProcessingError::PayloadTooLarge(DiscardItemType::Event).into());
    }

    let mut event =
        Annotated::<Event>::from_json_bytes(payload).map_err(ProcessingError::InvalidJson)?;

    if let Some(event_value) = event.value_mut()
        && let Some(event_type) = event_type
    {
        event_value.ty.set_value(Some(event_type));
    }

    metrics.bytes_ingested_event = Annotated::new(payload.len() as u64);

    Ok(event)
}

pub fn try_take_parsed_event(
    items: &mut Vec<Item>,
    metrics: &mut Metrics,
    ctx: Context<'_>,
) -> Result<Annotated<Event>> {
    let Some(item) = take_item_of_type(items, ItemType::Event) else {
        return Ok(Annotated::empty());
    };
    event_from_json(&item.payload(), None, metrics, ctx)
}
