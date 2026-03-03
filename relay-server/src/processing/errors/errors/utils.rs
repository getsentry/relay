use relay_base_schema::events::EventType;
use relay_event_schema::protocol::{Event, Metrics};
use relay_protocol::Annotated;

use crate::envelope::{AttachmentType, Item, ItemType};
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

/// Attempts to parse an event from an [`ItemType::Event`].
///
/// Takes the event item from the `items` vector and returns the parsed event,
/// if there is no event item, returns [`Annotated::empty`].
pub fn take_parsed_event(
    items: &mut Vec<Item>,
    metrics: &mut Metrics,
    ctx: Context<'_>,
) -> Result<Annotated<Event>> {
    let Some(item) = take_item_of_type(items, ItemType::Event) else {
        return Ok(Annotated::empty());
    };
    event_from_json(&item.payload(), None, metrics, ctx)
}

/// Attempts to create an event from event creating attachments.
///
/// An event can be assembled from [`AttachmentType::EventPayload`] and
/// two [`AttachmentType::Breadcrumbs`] attachment items.
///
/// Returns [`Annotated::empty`] if `items` does not contain the necessary attachments.
pub fn take_event_from_attachments(
    items: &mut Vec<Item>,
    metrics: &mut Metrics,
    ctx: Context<'_>,
) -> Result<Annotated<Event>> {
    let ev = take_item_by(items, |item| {
        item.attachment_type() == Some(AttachmentType::EventPayload)
    });
    let b1 = take_item_by(items, |item| {
        item.attachment_type() == Some(AttachmentType::Breadcrumbs)
    });
    let b2 = take_item_by(items, |item| {
        item.attachment_type() == Some(AttachmentType::Breadcrumbs)
    });

    if ev.is_none() && b1.is_none() || b2.is_none() {
        return Ok(Annotated::empty());
    }

    let (event, len) = crate::services::processor::event::event_from_attachments(
        ctx.processing.config,
        ev,
        b1,
        b2,
    )?;
    metrics.bytes_ingested_event = Annotated::new(len as u64);

    Ok(event)
}

/// Attempts to parse an event from [`ItemType::FormData`].
///
/// Takes the element from the `items` vector and returns the parsed event,
/// if there is no form data item in the `items` vector, returns [`Annotated::empty`].
pub fn take_event_from_formdata(
    items: &mut Vec<Item>,
    metrics: &mut Metrics,
) -> Result<Annotated<Event>> {
    let Some(form_data) = take_item_of_type(items, ItemType::FormData) else {
        return Ok(Annotated::empty());
    };

    let event = {
        let mut value = serde_json::Value::Object(Default::default());
        crate::services::processor::event::merge_formdata(&mut value, &form_data);
        Annotated::deserialize_with_meta(value).map_err(ProcessingError::InvalidJson)
    }?;
    metrics.bytes_ingested_event = Annotated::new(form_data.len() as u64);

    Ok(event)
}

/// Attempts to parse an event from a vector of items.
///
/// Tries multiple sources for events:
/// 1. [`take_parsed_event`]
/// 2. [`take_event_from_formdata`]
/// 3. [`take_event_from_attachments`]
///
/// Used when parsing crashes which have multiple additional ways of supplying additional event
/// data.
///
/// Returns [`Annotated::empty`] if no event is found.
pub fn take_event_from_crash_items(
    items: &mut Vec<Item>,
    metrics: &mut Metrics,
    ctx: Context<'_>,
) -> Result<Annotated<Event>> {
    let event = take_parsed_event(items, metrics, ctx)?;
    if event.0.is_some() {
        return Ok(event);
    }

    let event = take_event_from_formdata(items, metrics)?;
    if event.0.is_some() {
        return Ok(event);
    }

    let event = take_event_from_attachments(items, metrics, ctx)?;
    if event.0.is_some() {
        return Ok(event);
    }

    Ok(Annotated::empty())
}
