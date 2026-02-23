use relay_base_schema::events::EventType;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{Item, ItemType};
use crate::processing::errors::{Error, Result};

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
) -> Result<Annotated<Event>> {
    event_from_json(&item.payload(), event_type)
}

pub fn event_from_json(
    payload: &[u8],
    event_type: impl Into<Option<EventType>>,
) -> Result<Annotated<Event>> {
    let mut event = Annotated::<Event>::from_json_bytes(payload).map_err(Error::InvalidJson)?;

    if let Some(event_value) = event.value_mut()
        && let Some(event_type) = event_type.into()
    {
        event_value.ty.set_value(Some(event_type));
    }

    Ok(event)
}
