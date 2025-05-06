//! Playstation related code.
//!
//! These functions are included only in the processing mode.

use relay_event_schema::protocol::{
    AppContext, Context, Contexts, DeviceContext, LenientString, OsContext, RuntimeContext, Tags,
};
use relay_event_schema::protocol::{Event, TagEntry};
use relay_prosperoconv::write_dump;
use relay_prosperoconv::{extract_data, ProsperoDump};
use relay_protocol::{Annotated, Object};

use crate::envelope::{AttachmentType, ContentType, Item, ItemType};
use crate::services::processor::metric;
use crate::services::processor::{ErrorGroup, EventFullyNormalized, ProcessingError};
use crate::statsd::RelayCounters;
use crate::utils::TypedEnvelope;

pub fn process(
    managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    event: &mut Annotated<Event>,
) -> Result<Option<EventFullyNormalized>, ProcessingError> {
    let envelope = &mut managed_envelope.envelope_mut();
    if let Some(item) = envelope.take_item_by(|item| {
        item.ty() == &ItemType::Attachment
            && item.attachment_type() == Some(&AttachmentType::Prosperodump)
    }) {
        let event = event.get_or_insert_with(Event::default);
        let data = extract_data(&item.payload()).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to extract data: {}", err))
        })?;
        let prospero_dump = ProsperoDump::parse(&data).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to parse dump: {}", err))
        })?;
        let minidump_buffer = write_dump(&prospero_dump).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to create minidump: {}", err))
        })?;
        update_sentry_event(event, &prospero_dump);

        let mut item = Item::new(ItemType::Attachment);
        item.set_filename("DO_NOT_USE");
        item.set_payload(ContentType::Minidump, minidump_buffer);
        item.set_attachment_type(AttachmentType::Minidump);
        envelope.add_item(item);

        for file in prospero_dump.files {
            let mut item = Item::new(ItemType::Attachment);
            item.set_filename(file.name);
            item.set_attachment_type(AttachmentType::Attachment);

            if let Some(content_type) = mime_guess::from_path(file.name)
                .first()
                .map(|m| ContentType::from(m.essence_str()))
            {
                item.set_payload(content_type, file.contents.to_owned());
            } else {
                item.set_payload_without_content_type(file.contents.to_owned());
            }
            envelope.add_item(item);
        }

        let mut console_log = prospero_dump.system_log.to_string();
        for log_line in prospero_dump.log_lines {
            console_log.push_str(log_line);
        }
        if !console_log.is_empty() {
            let mut item = Item::new(ItemType::Attachment);
            item.set_filename("console.log");
            item.set_payload(ContentType::Text, console_log.into_bytes());
            item.set_attachment_type(AttachmentType::Attachment);
            envelope.add_item(item);
        }

        metric!(counter(RelayCounters::PlaystationProcessing) += 1);
        return Ok(Some(EventFullyNormalized(false)));
    }
    Ok(None)
}

pub fn update_sentry_event(event: &mut Event, prospero: &ProsperoDump) {
    let contexts = event.contexts.get_or_insert_with(Contexts::default);

    let mut device_context = DeviceContext {
        name: prospero
            .system_name
            .map(|s| Annotated::new(s.to_string()))
            .unwrap_or_default(),
        arch: Annotated::new("x86_64".into()),
        model_id: prospero
            .hardware_id
            .clone()
            .map(Annotated::new)
            .unwrap_or_default(),
        other: [("manufacturer".into(), Annotated::new("Sony".into()))].into(),
        ..Default::default()
    };

    let mut os_context = OsContext {
        name: Annotated::new("PlayStation".into()),
        ..Default::default()
    };
    let mut runtime_context = RuntimeContext::default();

    if let Some(hardware_id) = prospero.hardware_id.clone() {
        event.server_name = Annotated::new(hardware_id);
    }

    // Elevate userdata values with sentry. prefix into the sentry event.
    // Set the release based on the "release" key in the userdata
    if let Some(release) = prospero
        .userdata
        .get("sentry.release")
        .or_else(|| prospero.userdata.get("release"))
    {
        event.release = Annotated::new(LenientString(release.clone().into_owned()));
    }
    if let Some(environment) = prospero.userdata.get("sentry.environment") {
        event.environment = Annotated::new(environment.clone().into_owned());
    }
    if let Some(username) = prospero.userdata.get("sentry.user.username") {
        event.user.get_or_insert_with(Default::default).username =
            Annotated::new(LenientString(username.to_string()));
    }
    if let Some(email) = prospero.userdata.get("sentry.user.email") {
        event.user.get_or_insert_with(Default::default).email = Annotated::new(email.to_string());
    }

    let tags = event.tags.value_mut().get_or_insert_with(Tags::default);
    macro_rules! add_tag {
        ($key:expr, $value:expr) => {
            tags.push(Annotated::new(TagEntry(
                Annotated::new($key.into()),
                Annotated::new($value),
            )));
        };
    }

    add_tag!("cpu_vendor", "Sony".into());
    add_tag!("os.name", "PlayStation".into());

    let platform = "PS5";
    runtime_context.name = Annotated::new(platform.into());
    device_context.model = Annotated::new(platform.into());

    add_tag!("cpu_brand", format!("{platform} CPU"));
    add_tag!("runtime.name", platform.into());

    if let Some(system_version) = &prospero.sdk_version {
        add_tag!("os", format!("PlayStation {system_version}"));
        add_tag!("runtime", system_version.into());
        add_tag!("runtime.version", system_version.into());

        os_context.version = Annotated::new(system_version.into());
        runtime_context.version = Annotated::new(system_version.into());
    }

    for (k, v) in &prospero.userdata {
        if let Some(context_field_pair) = k.strip_prefix("sentry.context.") {
            // Handle custom context data of the form: sentry.context.<name>.<field>
            if let Some((context_name, field_name)) = context_field_pair.split_once('.') {
                if let Context::Other(map) =
                    contexts.get_or_insert_with(context_name, || Context::Other(Object::new()))
                {
                    map.insert(field_name.into(), Annotated::new(v.to_string().into()));
                }
            }
        } else {
            let tag = k.strip_prefix("sentry.").unwrap_or(k);
            add_tag!(tag, v.to_string());
        }
    }

    if let Some(app_info) = &prospero.app_info {
        add_tag!("titleId", app_info.title_id.into());

        contexts.get_or_default::<AppContext>().app_version =
            Annotated::new(app_info.version.into());
    }

    contexts.add(device_context);
    contexts.add(os_context);
    contexts.add(runtime_context);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use relay_protocol::Value;

    #[test]
    fn test_event_release_setting() {
        let prospero = ProsperoDump {
            userdata: BTreeMap::from([("release", Cow::Borrowed("1.2.3.4"))]),
            ..Default::default()
        };

        let mut event = Event::default();
        update_sentry_event(&mut event, &prospero);
        assert_eq!(
            event.release,
            Annotated::new(LenientString("1.2.3.4".into()))
        );
    }

    #[test]
    fn test_event_field_mapping() {
        let prospero = ProsperoDump {
            userdata: BTreeMap::from([
                ("sentry.release", Cow::Borrowed("1.2.3.4")),
                ("sentry.environment", Cow::Borrowed("production")),
                ("sentry.user.username", Cow::Borrowed("janedoe")),
                ("sentry.user.email", Cow::Borrowed("janedoe@example.com")),
                ("santry.tag", Cow::Borrowed("other_value")),
                ("other_tag", Cow::Borrowed("other_value")),
            ]),
            ..Default::default()
        };

        let mut event = Event::default();
        update_sentry_event(&mut event, &prospero);

        // Check the event fields
        assert_eq!(
            event.release,
            Annotated::new(LenientString("1.2.3.4".into()))
        );
        assert_eq!(event.environment, Annotated::new("production".into()));

        if let Some(user) = event.user.0 {
            assert_eq!(
                user.username,
                Annotated::new(LenientString("janedoe".into()))
            );
            assert_eq!(user.email, Annotated::new("janedoe@example.com".into()));
        } else {
            panic!("User information not set in the event");
        }

        // Checks the tags are present
        let tags = event.tags.value().unwrap();
        assert_eq!(tags.get("release"), Some("1.2.3.4"));
        assert_eq!(tags.get("environment"), Some("production"));
        assert_eq!(tags.get("user.username"), Some("janedoe"));
        assert_eq!(tags.get("user.email"), Some("janedoe@example.com"));
        assert_eq!(tags.get("santry.tag"), Some("other_value"));
        assert_eq!(tags.get("other_tag"), Some("other_value"));

        // Check that the raw tags are not present
        assert!(!tags.0.contains("sentry.release"));
        assert!(!tags.0.contains("sentry.environment"));
        assert!(!tags.0.contains("sentry.user.username"));
        assert!(!tags.0.contains("sentry.user.email"));
    }

    #[test]
    fn test_event_context_mapping() {
        let prospero = ProsperoDump {
            userdata: BTreeMap::from([
                ("sentry.context.game.name", Cow::Borrowed("Foo")),
                ("sentry.context.game.level", Cow::Borrowed("Bar")),
                ("sentry.context.player.level", Cow::Borrowed("42")),
                ("regular_tag", Cow::Borrowed("regular_value")),
            ]),
            ..Default::default()
        };

        let mut event = Event::default();
        update_sentry_event(&mut event, &prospero);

        // Check the game context is correct
        if let Some(Context::Other(game_context)) = event.contexts.value().unwrap().get_key("game")
        {
            assert_eq!(
                game_context.get("name").unwrap(),
                &Annotated::new(Value::from("Foo"))
            );
            assert_eq!(
                game_context.get("level").unwrap(),
                &Annotated::new(Value::from("Bar"))
            );
        } else {
            panic!("Game context not found or has wrong type");
        }

        // Check that the player context is correct
        if let Some(Context::Other(player_context)) =
            event.contexts.value().unwrap().get_key("player")
        {
            assert_eq!(
                player_context.get("level").unwrap(),
                &Annotated::new(Value::from("42"))
            );
        } else {
            panic!("Device context not found or has wrong type");
        }

        // Check that the tag is unaffected
        let tags = event.tags.value().unwrap();
        assert!(tags.0.contains("regular_tag"));
    }

    #[test]
    fn test_cannot_overwrite_runtime_context() {
        let prospero = ProsperoDump {
            userdata: BTreeMap::from([
                ("sentry.context.runtime.name", Cow::Borrowed("Xbox")),
                ("sentry.context.runtime.version", Cow::Borrowed("0.0.0")),
            ]),
            sdk_version: Some("5.0.0".into()),
            ..Default::default()
        };

        let mut event = Event::default();
        update_sentry_event(&mut event, &prospero);

        // Ensure that the runtime context values are not overwritten
        if let Some(Context::Runtime(runtime)) = event.contexts.value().unwrap().get_key("runtime")
        {
            assert_eq!(runtime.name, Annotated::new("PS5".into()));
            assert_eq!(runtime.version, Annotated::new("5.0.0".into()));
        } else {
            panic!("Runtime context not found or has wrong type");
        }
    }
}
