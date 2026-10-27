use relay_base_schema::events::EventType;
use relay_event_schema::protocol::{Event, Metrics};
use relay_protocol::Annotated;

use crate::envelope::{AttachmentItem, AttachmentType, EventItem, FormDataItem, Item, ItemPayload};
use crate::processing::errors::Result;
use crate::processing::errors::errors::Context;
use crate::services::outcome::DiscardItemType;
use crate::services::processor::ProcessingError;

mod attachment;
mod formdata;

macro_rules! if_not_processing {
    ($ctx:expr, $if_true:block) => {
        match $ctx {
            #[cfg(not(feature = "processing"))]
            ctx if !ctx.processing.is_processing() => $if_true,
            _ => {}
        }
    };
    ($ctx:expr, $if_true:block else $if_false:block) => {
        match $ctx {
            #[cfg(not(feature = "processing"))]
            _ => $if_true
            #[cfg(feature = "processing")]
            ctx => {
                if !ctx.processing.is_processing() $if_true else $if_false
            }
        }
    };
}
pub(crate) use if_not_processing;

macro_rules! if_processing {
    ($ctx:expr, $if_true:block) => {
        match $ctx {
            #[cfg(feature = "processing")]
            ctx if ctx.processing.is_processing() => $if_true,
            _ => {}
        }
    };
}
pub(crate) use if_processing;

#[cfg(feature = "processing")]
pub fn take_item_by<T, F>(items: &mut Vec<T>, f: F) -> Option<T>
where
    F: FnMut(&T) -> bool,
{
    let index = items.iter().position(f);
    index.map(|index| items.swap_remove(index))
}

pub fn take_payload<T: ItemPayload>(items: &mut Vec<Item>) -> Option<T> {
    take_payload_if(items, |_| true)
}

pub fn take_payloads<T: ItemPayload>(items: &mut Vec<Item>) -> impl Iterator<Item = T> {
    items
        .extract_if(.., |item| T::from_item_ref(item).is_some())
        .map(|item| T::try_from(item).unwrap_or_else(|_| unreachable!("selected payload type")))
}

pub fn take_payload_if<T: ItemPayload>(
    items: &mut Vec<Item>,
    mut predicate: impl FnMut(&T) -> bool,
) -> Option<T> {
    let index = items
        .iter()
        .position(|item| T::from_item_ref(item).is_some_and(&mut predicate))?;
    Some(
        T::try_from(items.swap_remove(index))
            .unwrap_or_else(|_| unreachable!("selected payload type")),
    )
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

/// Attempts to parse an event from an [`EventItem`].
///
/// Takes the event item from the `items` vector and returns the parsed event,
/// if there is no event item, returns [`Annotated::empty`].
pub fn take_parsed_event(
    items: &mut Vec<Item>,
    metrics: &mut Metrics,
    ctx: Context<'_>,
) -> Result<Annotated<Event>> {
    let Some(item) = take_payload::<EventItem>(items) else {
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
    let ev = take_payload_if::<AttachmentItem>(items, |item| {
        item.attachment_type() == Some(AttachmentType::EventPayload)
    });
    let b1 = take_payload_if::<AttachmentItem>(items, |item| {
        item.attachment_type() == Some(AttachmentType::Breadcrumbs)
    });
    let b2 = take_payload_if::<AttachmentItem>(items, |item| {
        item.attachment_type() == Some(AttachmentType::Breadcrumbs)
    });

    if ev.is_none() && b1.is_none() {
        return Ok(Annotated::empty());
    }

    let (event, len) = attachment::event_from_attachments(ctx.processing.config, ev, b1, b2)?;
    metrics.bytes_ingested_event = Annotated::new(len as u64);

    Ok(event)
}

#[cfg(feature = "processing")]
pub fn take_event_from_typed_attachments(
    items: &mut Vec<AttachmentItem>,
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

    if ev.is_none() && b1.is_none() {
        return Ok(Annotated::empty());
    }

    let (event, len) = attachment::event_from_attachments(ctx.processing.config, ev, b1, b2)?;
    metrics.bytes_ingested_event = Annotated::new(len as u64);

    Ok(event)
}

/// Attempts to parse an event from [`FormDataItem`].
///
/// Takes the element from the `items` vector and returns the parsed event,
/// if there is no form data item in the `items` vector, returns [`Annotated::empty`].
pub fn take_event_from_formdata(
    items: &mut Vec<Item>,
    metrics: &mut Metrics,
) -> Result<Annotated<Event>> {
    let Some(form_data) = take_payload::<FormDataItem>(items) else {
        return Ok(Annotated::empty());
    };

    let event = {
        let mut value = serde_json::Value::Object(Default::default());
        formdata::merge_formdata(&mut value, &form_data)?;
        Annotated::deserialize_with_meta(value).map_err(ProcessingError::InvalidJson)
    }?;
    metrics.bytes_ingested_event = Annotated::new(form_data.len() as u64);

    Ok(event)
}

/// Attempts to parse an event from a vector of items.
///
/// Tries multiple sources for events:
/// 1. [`take_parsed_event`]
/// 2. [`take_event_from_attachments`]
/// 3. [`take_event_from_formdata`]
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
    // Remove other event creating items to make sure there are no leftovers.
    let remove_other_event_items = |items: &mut Vec<Item>| {
        items
            .extract_if(.., |item| match item {
                Item::FormData(_) => true,
                Item::Attachment(attachment) => matches!(
                    attachment.attachment_type(),
                    Some(AttachmentType::EventPayload | AttachmentType::Breadcrumbs)
                ),
                _ => false,
            })
            .for_each(drop);
    };

    let event = take_parsed_event(items, metrics, ctx)?;
    if event.0.is_some() {
        remove_other_event_items(items);
        return Ok(event);
    }

    let event = take_event_from_attachments(items, metrics, ctx)?;
    if event.0.is_some() {
        remove_other_event_items(items);
        return Ok(event);
    }

    let event = take_event_from_formdata(items, metrics)?;
    if event.0.is_some() {
        remove_other_event_items(items);
        return Ok(event);
    }

    Ok(Annotated::empty())
}
