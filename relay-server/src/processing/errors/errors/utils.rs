use relay_base_schema::events::EventType;
use relay_event_schema::protocol::Event;
use relay_protocol::Annotated;

use crate::envelope::{Item, ItemType, Items};
use crate::processing::errors::{Error, Result};

pub fn take_item_by(items: &mut Items, f: F) -> Option<Item>
where
    F: FnMut(&Item) -> bool,
{
    let index = items.iter().position(f);
    index.map(|index| items.swap_remove(index))
}

pub fn take_item_of_type(items: &mut Items, ty: ItemType) -> Option<Item> {
    take_item_by(items, |item| item.ty() == &ty)
}

pub fn event_from_json_payload(
    item: Item,
    event_type: Option<EventType>,
) -> Result<Annotated<Event>> {
    let mut event =
        Annotated::<Event>::from_json_bytes(&item.payload()).map_err(Error::InvalidJson)?;

    if let Some(event_value) = event.value_mut()
        && event_type.is_some()
    {
        event_value.ty.set_value(event_type);
    }

    Ok(event)
}
