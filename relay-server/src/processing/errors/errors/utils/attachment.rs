use relay_config::Config;
use relay_event_schema::protocol::{Breadcrumb, Event, Values};
use relay_protocol::{Annotated, Array, Object};

use crate::envelope::Item;
use crate::services::processor::ProcessingError;
use crate::utils::msgpack_deserializer;

pub fn event_from_attachments(
    config: &Config,
    event_item: Option<Item>,
    breadcrumbs_item1: Option<Item>,
    breadcrumbs_item2: Option<Item>,
) -> Result<(Annotated<Event>, usize), ProcessingError> {
    let len = event_item.as_ref().map_or(0, |item| item.len())
        + breadcrumbs_item1.as_ref().map_or(0, |item| item.len())
        + breadcrumbs_item2.as_ref().map_or(0, |item| item.len());

    let mut event = extract_attached_event(config, event_item)?;
    let mut breadcrumbs1 = parse_msgpack_breadcrumbs(config, breadcrumbs_item1)?;
    let mut breadcrumbs2 = parse_msgpack_breadcrumbs(config, breadcrumbs_item2)?;

    let timestamp1 = breadcrumbs1
        .iter()
        .rev()
        .find_map(|breadcrumb| breadcrumb.value().and_then(|b| b.timestamp.value()));

    let timestamp2 = breadcrumbs2
        .iter()
        .rev()
        .find_map(|breadcrumb| breadcrumb.value().and_then(|b| b.timestamp.value()));

    // Sort breadcrumbs by date. We presume that last timestamp from each row gives the
    // relative sequence of the whole sequence, i.e., we don't need to splice the sequences
    // to get the breadrumbs sorted.
    if timestamp1 > timestamp2 {
        std::mem::swap(&mut breadcrumbs1, &mut breadcrumbs2);
    }

    // Limit the total length of the breadcrumbs. We presume that if we have both
    // breadcrumbs with items one contains the maximum number of breadcrumbs allowed.
    let max_length = std::cmp::max(breadcrumbs1.len(), breadcrumbs2.len());

    breadcrumbs1.extend(breadcrumbs2);

    if breadcrumbs1.len() > max_length {
        // Keep only the last max_length elements from the vectors
        breadcrumbs1.drain(0..(breadcrumbs1.len() - max_length));
    }

    if !breadcrumbs1.is_empty() {
        event.get_or_insert_with(Event::default).breadcrumbs = Annotated::new(Values {
            values: Annotated::new(breadcrumbs1),
            other: Object::default(),
        });
    }

    Ok((event, len))
}

fn extract_attached_event(
    config: &Config,
    item: Option<Item>,
) -> Result<Annotated<Event>, ProcessingError> {
    let item = match item {
        Some(item) if !item.is_empty() => item,
        _ => return Ok(Annotated::new(Event::default())),
    };

    // Protect against blowing up during deserialization. Attachments can have a significantly
    // larger size than regular events and may cause significant processing delays.
    if item.len() > config.max_event_size() {
        return Err(ProcessingError::PayloadTooLarge(
            item.attachment_type()
                .map(|t| t.into())
                .unwrap_or_else(|| item.ty().into()),
        ));
    }

    let payload = item.payload();
    let deserializer = &mut msgpack_deserializer(payload.as_ref());
    Annotated::deserialize_with_meta(deserializer).map_err(ProcessingError::InvalidMsgpack)
}

fn parse_msgpack_breadcrumbs(
    config: &Config,
    item: Option<Item>,
) -> Result<Array<Breadcrumb>, ProcessingError> {
    let mut breadcrumbs = Array::new();
    let item = match item {
        Some(item) if !item.is_empty() => item,
        _ => return Ok(breadcrumbs),
    };

    // Validate that we do not exceed the maximum breadcrumb payload length. Breadcrumbs are
    // truncated to a maximum of 100 in event normalization, but this is to protect us from
    // blowing up during deserialization. As approximation, we use the maximum event payload
    // size as bound, which is roughly in the right ballpark.
    if item.len() > config.max_event_size() {
        return Err(ProcessingError::PayloadTooLarge(
            item.attachment_type()
                .map(|t| t.into())
                .unwrap_or_else(|| item.ty().into()),
        ));
    }

    let payload = item.payload();
    let mut deserializer = msgpack_deserializer(&payload);

    while !deserializer.get_ref().is_empty() {
        let breadcrumb = Annotated::deserialize_with_meta(&mut deserializer)?;
        breadcrumbs.push(breadcrumb);
    }

    Ok(breadcrumbs)
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeMap;

    use chrono::{DateTime, TimeZone, Utc};

    use crate::envelope::{ContentType, ItemType};

    use super::*;

    fn create_breadcrumbs_item(breadcrumbs: &[(Option<DateTime<Utc>>, &str)]) -> Item {
        let mut data = Vec::new();

        for (date, message) in breadcrumbs {
            let mut breadcrumb = BTreeMap::new();
            breadcrumb.insert("message", (*message).to_owned());
            if let Some(date) = date {
                breadcrumb.insert("timestamp", date.to_rfc3339());
            }

            rmp_serde::encode::write(&mut data, &breadcrumb).expect("write msgpack");
        }

        let mut item = Item::new(ItemType::Attachment);
        item.set_payload(ContentType::MsgPack, data);
        item
    }

    fn breadcrumbs_from_event(event: &Annotated<Event>) -> &Vec<Annotated<Breadcrumb>> {
        event
            .value()
            .unwrap()
            .breadcrumbs
            .value()
            .unwrap()
            .values
            .value()
            .unwrap()
    }

    #[test]
    fn test_breadcrumbs_file1() {
        let item = create_breadcrumbs_item(&[(None, "item1")]);

        // NOTE: using (Some, None) here:
        let result = event_from_attachments(&Config::default(), None, Some(item), None);

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);

        assert_eq!(breadcrumbs.len(), 1);
        let first_breadcrumb_message = breadcrumbs[0].value().unwrap().message.value().unwrap();
        assert_eq!("item1", first_breadcrumb_message);
    }

    #[test]
    fn test_breadcrumbs_file2() {
        let item = create_breadcrumbs_item(&[(None, "item2")]);

        // NOTE: using (None, Some) here:
        let result = event_from_attachments(&Config::default(), None, None, Some(item));

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 1);

        let first_breadcrumb_message = breadcrumbs[0].value().unwrap().message.value().unwrap();
        assert_eq!("item2", first_breadcrumb_message);
    }

    #[test]
    fn test_breadcrumbs_truncation() {
        let item1 = create_breadcrumbs_item(&[(None, "crumb1")]);
        let item2 = create_breadcrumbs_item(&[(None, "crumb2"), (None, "crumb3")]);

        let result = event_from_attachments(&Config::default(), None, Some(item1), Some(item2));

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 2);
    }

    #[test]
    fn test_breadcrumbs_order_with_none() {
        let d1 = Utc.with_ymd_and_hms(2019, 10, 10, 12, 10, 10).unwrap();
        let d2 = Utc.with_ymd_and_hms(2019, 10, 11, 12, 10, 10).unwrap();

        let item1 = create_breadcrumbs_item(&[(None, "none"), (Some(d1), "d1")]);
        let item2 = create_breadcrumbs_item(&[(Some(d2), "d2")]);

        let result = event_from_attachments(&Config::default(), None, Some(item1), Some(item2));

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 2);

        assert_eq!(Some("d1"), breadcrumbs[0].value().unwrap().message.as_str());
        assert_eq!(Some("d2"), breadcrumbs[1].value().unwrap().message.as_str());
    }

    #[test]
    fn test_breadcrumbs_reversed_with_none() {
        let d1 = Utc.with_ymd_and_hms(2019, 10, 10, 12, 10, 10).unwrap();
        let d2 = Utc.with_ymd_and_hms(2019, 10, 11, 12, 10, 10).unwrap();

        let item1 = create_breadcrumbs_item(&[(Some(d2), "d2")]);
        let item2 = create_breadcrumbs_item(&[(None, "none"), (Some(d1), "d1")]);

        let result = event_from_attachments(&Config::default(), None, Some(item1), Some(item2));

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 2);

        assert_eq!(Some("d1"), breadcrumbs[0].value().unwrap().message.as_str());
        assert_eq!(Some("d2"), breadcrumbs[1].value().unwrap().message.as_str());
    }

    #[test]
    fn test_empty_breadcrumbs_item() {
        let item1 = create_breadcrumbs_item(&[]);
        let item2 = create_breadcrumbs_item(&[]);
        let item3 = create_breadcrumbs_item(&[]);

        let result =
            event_from_attachments(&Config::default(), Some(item1), Some(item2), Some(item3));

        // regression test to ensure we don't fail parsing an empty file
        result.expect("event_from_attachments");
    }

    fn deeply_nested_msgpack_event(depth: usize) -> Vec<u8> {
        let mut buf = Vec::with_capacity(depth + 8);
        buf.push(0x81); // fixmap with 1 entry
        buf.push(0xa1); // fixstr of length 1
        buf.push(b'a'); //   the key "a"
        buf.resize(buf.len() + depth, 0x91); // `depth` fixarrays of length 1 (one level each)
        buf.push(0x90); // innermost: fixarray of length 0
        buf
    }

    /// Reject event encoded as msgpack if it is too deeply nested.
    #[test]
    fn test_msgpack_deep_nesting_is_rejected() {
        // ~200 KB payload, comfortably under the 1 MiB max_event_size, but 200k levels deep.
        let payload = deeply_nested_msgpack_event(200_000);
        assert!(payload.len() < Config::default().max_event_size());

        let mut item = Item::new(ItemType::Attachment);
        item.set_payload(ContentType::MsgPack, payload);

        let result = extract_attached_event(&Config::default(), Some(item));

        assert!(
            matches!(result, Err(ProcessingError::InvalidMsgpack(_))),
            "expected the deeply nested payload to be rejected, got: {result:?}"
        );
    }
}
