//! Playstation related code.
//!
//! These functions are included only in the processing mode.

use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_event_schema::protocol::{
    AppContext, ClientSdkInfo, Context, Contexts, DeviceContext, LenientString, OsContext,
    RuntimeContext, Tags,
};
use relay_event_schema::protocol::{Event, TagEntry};
use relay_prosperoconv::{self, ProsperoDump};
use relay_protocol::{Annotated, Empty, Object};

use crate::envelope::{AttachmentType, ContentType, Item, ItemType};
use crate::managed::TypedEnvelope;
use crate::services::processor::metric;
use crate::services::processor::{ErrorGroup, EventFullyNormalized, ProcessingError};
use crate::services::projects::project::ProjectInfo;
use crate::statsd::RelayCounters;
use crate::utils;

/// Name of the custom tag in the UserData for Sentry event payloads.
const SENTRY_PAYLOAD_KEY: &str = "__sentry";

pub fn expand(
    managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    config: &Config,
    project_info: &ProjectInfo,
) -> Result<(), ProcessingError> {
    if !project_info.has_feature(Feature::PlaystationIngestion) {
        return Ok(());
    }
    let envelope = managed_envelope.envelope_mut();

    // Get instead of take as we want to keep the dump as an attachment
    if let Some(item) = envelope.get_item_by(|item| {
        item.ty() == &ItemType::Attachment
            && item.attachment_type() == Some(&AttachmentType::Prosperodump)
    }) {
        let data = relay_prosperoconv::extract_data(&item.payload()).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to extract data: {err}"))
        })?;
        let prospero_dump = ProsperoDump::parse(&data).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to parse dump: {err}"))
        })?;
        let minidump_buffer = relay_prosperoconv::write_dump(&prospero_dump).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to create minidump: {err}"))
        })?;

        if let Some(json) = prospero_dump.userdata.get(SENTRY_PAYLOAD_KEY) {
            let event = envelope.take_item_by(|item| item.ty() == &ItemType::Event);
            let event_item = merge_or_create_event_item(json, event);
            envelope.add_item(event_item);
        }

        add_attachments(envelope, prospero_dump, minidump_buffer);

        if let Err(offender) = utils::check_envelope_size_limits(config, envelope) {
            return Err(ProcessingError::PayloadTooLarge(offender));
        }
    }
    Ok(())
}

pub fn process(
    managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    event: &mut Annotated<Event>,
    project_info: &ProjectInfo,
) -> Result<Option<EventFullyNormalized>, ProcessingError> {
    if !project_info.has_feature(Feature::PlaystationIngestion) {
        return Ok(None);
    }
    let envelope = &mut managed_envelope.envelope_mut();

    if let Some(item) = envelope.get_item_by(|item| {
        item.ty() == &ItemType::Attachment
            && item.attachment_type() == Some(&AttachmentType::Prosperodump)
    }) {
        metric!(counter(RelayCounters::PlaystationProcessing) += 1);
        let event = event.get_or_insert_with(Event::default);

        // Currently we parse the dump here again, in order to set the contexts on the event
        // this can not be done in the expand function since we don't have an event at that point.
        // This is inline with how unreal reports are handled.
        let data = relay_prosperoconv::extract_data(&item.payload()).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to extract data: {err}"))
        })?;
        let prospero_dump = ProsperoDump::parse(&data).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to parse dump: {err}"))
        })?;

        // If "__sentry" is not a key in the userdata do the legacy extraction.
        // This should be removed once all customers migrated to the new format.
        if !&prospero_dump.userdata.contains_key(SENTRY_PAYLOAD_KEY) {
            legacy_userdata_extraction(event, &prospero_dump);
        }
        merge_playstation_context(event, &prospero_dump);

        // Remove the prosperodump attachment if the project is not configured to store it
        if !project_info
            .config
            .playstation_config
            .store_prosperodump
        {
            envelope.retain_items(|item| {
                !(item.ty() == &ItemType::Attachment
                    && item.attachment_type() == Some(&AttachmentType::Prosperodump))
            });
        }

        return Ok(Some(EventFullyNormalized(false)));
    }
    Ok(None)
}

fn add_attachments(
    envelope: &mut crate::Envelope,
    prospero_dump: ProsperoDump<'_>,
    minidump_buffer: Vec<u8>,
) {
    let mut item = Item::new(ItemType::Attachment);
    item.set_filename("generated_minidump.dmp");
    item.set_payload(ContentType::Minidump, minidump_buffer);
    item.set_attachment_type(AttachmentType::Minidump);
    envelope.add_item(item);

    for file in prospero_dump.files {
        let mut item = Item::new(ItemType::Attachment);
        item.set_filename(file.name);
        item.set_attachment_type(AttachmentType::Attachment);
        item.set_payload(infer_content_type(file.name), file.contents.to_owned());
        envelope.add_item(item);
    }

    let mut console_log = prospero_dump.system_log.into_owned();
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
}

fn legacy_userdata_extraction(event: &mut Event, prospero: &ProsperoDump) {
    let contexts = event.contexts.get_or_insert_with(Contexts::default);
    let tags = event.tags.value_mut().get_or_insert_with(Tags::default);
    macro_rules! add_tag {
        ($key:expr, $value:expr) => {
            tags.push(Annotated::new(TagEntry(
                Annotated::new($key.into()),
                Annotated::new($value.into()),
            )));
        };
    }

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
            Annotated::new(LenientString(username.clone().into_owned()));
    }
    if let Some(email) = prospero.userdata.get("sentry.user.email") {
        event.user.get_or_insert_with(Default::default).email =
            Annotated::new(email.clone().into_owned());
    }

    for (k, v) in &prospero.userdata {
        if let Some(context_field_pair) = k.strip_prefix("sentry.context.") {
            // Handle custom context data of the form: sentry.context.<name>.<field>
            if let Some((context_name, field_name)) = context_field_pair.split_once('.')
                && let Context::Other(map) =
                    contexts.get_or_insert_with(context_name, || Context::Other(Object::new()))
            {
                map.insert(
                    field_name.to_owned(),
                    Annotated::new(v.clone().into_owned().into()),
                );
            }
        } else {
            let tag = k.strip_prefix("sentry.").unwrap_or(k);
            add_tag!(tag, v.clone());
        }
    }

    // Set the tags here for now until we decide they should be set for the new format as well.
    let platform = "PS5";

    add_tag!("cpu_vendor", "Sony");
    add_tag!("os.name", "PlayStation");
    add_tag!("cpu_brand", format!("{platform} CPU"));
    add_tag!("runtime.name", platform);

    if let Some(system_version) = &prospero.sdk_version {
        add_tag!("os", format!("PlayStation {system_version}"));
        add_tag!("runtime", system_version);
        add_tag!("runtime.version", system_version);
    }

    if let Some(app_info) = &prospero.app_info {
        add_tag!("titleId", app_info.title_id);
    }
}

fn merge_playstation_context(event: &mut Event, prospero: &ProsperoDump) {
    let contexts = event.contexts.get_or_insert_with(Contexts::default);
    let platform = "PS5";

    let mut os_context_version = Annotated::default();
    let mut runtime_context_version = Annotated::default();
    if let Some(system_version) = &prospero.sdk_version {
        os_context_version = Annotated::new(system_version.to_owned());
        runtime_context_version = Annotated::new(system_version.to_owned());
    }

    if let Some(hardware_id) = prospero.hardware_id.clone()
        && event.server_name.is_empty()
    {
        event.server_name = Annotated::new(hardware_id);
    }

    if let Some(app_info) = &prospero.app_info
        && !contexts.contains::<AppContext>()
    {
        contexts.add(AppContext {
            app_version: Annotated::new(app_info.version.to_owned()),
            ..Default::default()
        });
    }

    if !contexts.contains::<DeviceContext>() {
        contexts.add(DeviceContext {
            name: prospero
                .system_name
                .map(|s| Annotated::new(s.to_owned()))
                .unwrap_or_default(),
            arch: Annotated::new("x86_64".to_owned()),
            model_id: prospero
                .hardware_id
                .clone()
                .map(Annotated::new)
                .unwrap_or_default(),
            other: [("manufacturer".to_owned(), Annotated::new("Sony".into()))].into(),
            model: Annotated::new(platform.to_owned()),
            ..Default::default()
        });
    }

    if !contexts.contains::<OsContext>() {
        contexts.add(OsContext {
            name: Annotated::new("PlayStation".to_owned()),
            version: os_context_version,
            ..Default::default()
        });
    }

    if !contexts.contains::<RuntimeContext>() {
        contexts.add(RuntimeContext {
            name: Annotated::new(platform.to_owned()),
            version: runtime_context_version,
            ..Default::default()
        });
    }

    event.client_sdk.get_or_insert_with(|| ClientSdkInfo {
        name: Annotated::new("sentry.playstation.devkit".to_owned()),
        version: Annotated::new("0.0.1".to_owned()),
        ..Default::default()
    });
}

fn infer_content_type(filename: &str) -> ContentType {
    // Since we only receive a limited selection of files through this mechanism this simple logic
    // should be enough.
    let extension = filename.rsplit('.').next().map(str::to_lowercase);
    match extension.as_deref() {
        Some("txt") => ContentType::Text,
        Some("json") => ContentType::Json,
        Some("xml") => ContentType::Xml,
        _ => ContentType::OctetStream,
    }
}

fn create_item_with_json_payload(payload: &str) -> Item {
    let mut item = Item::new(ItemType::Event);
    item.set_payload(ContentType::Json, payload.to_owned());
    item
}

fn merge_or_create_event_item(json: &str, event: Option<Item>) -> Item {
    if let Some(item) = event {
        let event =
            serde_json::from_slice::<serde_json::Value>(&item.payload()).unwrap_or_default();
        let mut base_event = serde_json::from_str::<serde_json::Value>(json).unwrap_or_default();

        utils::merge_values(&mut base_event, event);

        if let Ok(merged_json) = serde_json::to_string(&base_event) {
            create_item_with_json_payload(&merged_json)
        } else {
            item
        }
    } else {
        create_item_with_json_payload(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use insta::{assert_debug_snapshot, assert_json_snapshot};
    use relay_protocol::SerializableAnnotated;

    #[test]
    fn test_event_release_setting() {
        let prospero = ProsperoDump {
            userdata: BTreeMap::from([("release", Cow::Borrowed("1.2.3.4"))]),
            ..Default::default()
        };

        let mut event = Event::default();
        legacy_userdata_extraction(&mut event, &prospero);
        assert_eq!(
            event.release,
            Annotated::new(LenientString("1.2.3.4".to_owned()))
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

        legacy_userdata_extraction(&mut event, &prospero);

        // Check the event fields
        assert_eq!(
            event.release,
            Annotated::new(LenientString("1.2.3.4".to_owned()))
        );
        assert_eq!(event.environment, Annotated::new("production".to_owned()));

        assert_json_snapshot!(SerializableAnnotated(&(event.user)), @r#"
        {
          "email": "janedoe@example.com",
          "username": "janedoe"
        }
        "#);

        assert_debug_snapshot!(event.tags.value().unwrap(), @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "other_tag",
                        "other_value",
                    ),
                    TagEntry(
                        "santry.tag",
                        "other_value",
                    ),
                    TagEntry(
                        "environment",
                        "production",
                    ),
                    TagEntry(
                        "release",
                        "1.2.3.4",
                    ),
                    TagEntry(
                        "user.email",
                        "janedoe@example.com",
                    ),
                    TagEntry(
                        "user.username",
                        "janedoe",
                    ),
                    TagEntry(
                        "cpu_vendor",
                        "Sony",
                    ),
                    TagEntry(
                        "os.name",
                        "PlayStation",
                    ),
                    TagEntry(
                        "cpu_brand",
                        "PS5 CPU",
                    ),
                    TagEntry(
                        "runtime.name",
                        "PS5",
                    ),
                ],
            ),
        )
        "#);
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
        legacy_userdata_extraction(&mut event, &prospero);

        // Check the game context is correct
        assert_debug_snapshot!(event.contexts.value().unwrap().get_key("game").unwrap(), @r#"
        Other(
            {
                "level": String(
                    "Bar",
                ),
                "name": String(
                    "Foo",
                ),
            },
        )
        "#);

        // Check that the player context is correct
        assert_debug_snapshot!(event.contexts.value().unwrap().get_key("player").unwrap(), @r#"
        Other(
            {
                "level": String(
                    "42",
                ),
            },
        )
        "#);

        // Check that the tag is unaffected
        let tags = event.tags.value().unwrap();
        assert!(tags.0.contains("regular_tag"));
    }

    #[test]
    fn test_playstation_context_overwrites_userdata_context() {
        let prospero = ProsperoDump {
            userdata: BTreeMap::from([
                ("sentry.context.runtime.name", Cow::Borrowed("Xbox")),
                ("sentry.context.runtime.version", Cow::Borrowed("0.0.0")),
            ]),
            sdk_version: Some("5.0.0".to_owned()),
            ..Default::default()
        };

        let mut event = Event::default();
        legacy_userdata_extraction(&mut event, &prospero);
        merge_playstation_context(&mut event, &prospero);

        assert_debug_snapshot!(event.contexts.value().unwrap().get_key("runtime").unwrap(), @r#"
        Runtime(
            RuntimeContext {
                runtime: ~,
                name: "PS5",
                version: "5.0.0",
                build: ~,
                raw_description: ~,
                other: {},
            },
        )
        "#);
    }

    #[test]
    fn test_merge_playstation_context() {
        let prospero = ProsperoDump {
            ..Default::default()
        };
        let mut event = Event::default();
        merge_playstation_context(&mut event, &prospero);

        assert_json_snapshot!(SerializableAnnotated(&event.contexts), @r#"
        {
          "device": {
            "model": "PS5",
            "arch": "x86_64",
            "manufacturer": "Sony",
            "type": "device"
          },
          "os": {
            "name": "PlayStation",
            "type": "os"
          },
          "runtime": {
            "name": "PS5",
            "type": "runtime"
          }
        }
        "#);
    }

    #[test]
    fn test_merge_playstation_context_does_not_overwrite() {
        let prospero = ProsperoDump {
            ..Default::default()
        };
        let mut event = Event::default();
        let contexts = event.contexts.get_or_insert_with(Contexts::default);

        contexts.add(RuntimeContext::default());
        contexts.add(OsContext::default());
        contexts.add(DeviceContext::default());

        merge_playstation_context(&mut event, &prospero);

        // Should be all default since the presence of the default contexts blocks the merge:
        assert_json_snapshot!(SerializableAnnotated(&event.contexts), @r#"
        {
          "device": {
            "type": "device"
          },
          "os": {
            "type": "os"
          },
          "runtime": {
            "type": "runtime"
          }
        }
        "#);
    }
}
