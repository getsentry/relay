//! Playstation related code.
//!
//! These functions are included only in the processing mode.

use relay_config::Config;
use relay_dynamic_config::Feature;
use relay_event_schema::protocol::{
    AppContext, Context, Contexts, DeviceContext, LenientString, OsContext, RuntimeContext, Tags,
};
use relay_event_schema::protocol::{Event, TagEntry};
use relay_prosperoconv::{self, ProsperoDump};
use relay_protocol::{Annotated, Object};

use crate::envelope::{AttachmentType, ContentType, Item, ItemType};
use crate::services::processor::metric;
use crate::services::processor::{ErrorGroup, EventFullyNormalized, ProcessingError};
use crate::services::projects::project::ProjectInfo;
use crate::statsd::RelayCounters;
use crate::utils;
use crate::utils::TypedEnvelope;

pub fn process(
    managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    event: &mut Annotated<Event>,
    config: &Config,
    project_info: &ProjectInfo,
) -> Result<Option<EventFullyNormalized>, ProcessingError> {
    let envelope = &mut managed_envelope.envelope_mut();

    if !project_info.has_feature(Feature::PlaystationIngestion) {
        return Ok(None);
    }

    // Get instead of take as we want to keep the dump as an attachment
    if let Some(item) = envelope.get_item_by(|item| {
        item.ty() == &ItemType::Attachment
            && item.attachment_type() == Some(&AttachmentType::Prosperodump)
    }) {
        metric!(counter(RelayCounters::PlaystationProcessing) += 1);

        let event = event.get_or_insert_with(Event::default);
        let data = relay_prosperoconv::extract_data(&item.payload()).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to extract data: {err}"))
        })?;
        let prospero_dump = ProsperoDump::parse(&data).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to parse dump: {err}"))
        })?;
        let minidump_buffer = relay_prosperoconv::write_dump(&prospero_dump).map_err(|err| {
            ProcessingError::InvalidPlaystationDump(format!("Failed to create minidump: {err}"))
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

        if let Err(offender) = utils::check_envelope_size_limits(config, envelope) {
            return Err(ProcessingError::PayloadTooLarge(offender));
        } else {
            return Ok(Some(EventFullyNormalized(false)));
        }
    }
    Ok(None)
}

pub fn update_sentry_event(event: &mut Event, prospero: &ProsperoDump) {
    let contexts = event.contexts.get_or_insert_with(Contexts::default);

    let mut device_context = DeviceContext {
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
        ..Default::default()
    };

    let mut os_context = OsContext {
        name: Annotated::new("PlayStation".to_owned()),
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
            Annotated::new(LenientString(username.clone().into_owned()));
    }
    if let Some(email) = prospero.userdata.get("sentry.user.email") {
        event.user.get_or_insert_with(Default::default).email =
            Annotated::new(email.clone().into_owned());
    }

    let tags = event.tags.value_mut().get_or_insert_with(Tags::default);
    macro_rules! add_tag {
        ($key:expr, $value:expr) => {
            tags.push(Annotated::new(TagEntry(
                Annotated::new($key.into()),
                Annotated::new($value.into()),
            )));
        };
    }

    add_tag!("cpu_vendor", "Sony");
    add_tag!("os.name", "PlayStation");

    let platform = "PS5";
    runtime_context.name = Annotated::new(platform.to_owned());
    device_context.model = Annotated::new(platform.to_owned());

    add_tag!("cpu_brand", format!("{platform} CPU"));
    add_tag!("runtime.name", platform);

    if let Some(system_version) = &prospero.sdk_version {
        add_tag!("os", format!("PlayStation {system_version}"));
        add_tag!("runtime", system_version);
        add_tag!("runtime.version", system_version);

        os_context.version = Annotated::new(system_version.to_owned());
        runtime_context.version = Annotated::new(system_version.to_owned());
    }

    for (k, v) in &prospero.userdata {
        if let Some(context_field_pair) = k.strip_prefix("sentry.context.") {
            // Handle custom context data of the form: sentry.context.<name>.<field>
            if let Some((context_name, field_name)) = context_field_pair.split_once('.') {
                if let Context::Other(map) =
                    contexts.get_or_insert_with(context_name, || Context::Other(Object::new()))
                {
                    map.insert(
                        field_name.to_owned(),
                        Annotated::new(v.clone().into_owned().into()),
                    );
                }
            }
        } else {
            let tag = k.strip_prefix("sentry.").unwrap_or(k);
            add_tag!(tag, v.clone());
        }
    }

    if let Some(app_info) = &prospero.app_info {
        add_tag!("titleId", app_info.title_id);

        contexts.get_or_default::<AppContext>().app_version =
            Annotated::new(app_info.version.to_owned());
    }

    contexts.add(device_context);
    contexts.add(os_context);
    contexts.add(runtime_context);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use insta::assert_debug_snapshot;

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
        update_sentry_event(&mut event, &prospero);

        // Check the event fields
        assert_eq!(
            event.release,
            Annotated::new(LenientString("1.2.3.4".to_owned()))
        );
        assert_eq!(event.environment, Annotated::new("production".to_owned()));

        assert_debug_snapshot!(event.user, @r#"
        User {
            id: ~,
            email: "janedoe@example.com",
            ip_address: ~,
            username: LenientString(
                "janedoe",
            ),
            name: ~,
            sentry_user: ~,
            geo: ~,
            segment: ~,
            data: ~,
            other: {},
        }
        "#);

        assert_debug_snapshot!(event.tags.value().unwrap(), @r#"
        Tags(
            PairList(
                [
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
        update_sentry_event(&mut event, &prospero);

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
    fn test_cannot_overwrite_runtime_context() {
        let prospero = ProsperoDump {
            userdata: BTreeMap::from([
                ("sentry.context.runtime.name", Cow::Borrowed("Xbox")),
                ("sentry.context.runtime.version", Cow::Borrowed("0.0.0")),
            ]),
            sdk_version: Some("5.0.0".to_owned()),
            ..Default::default()
        };

        let mut event = Event::default();
        update_sentry_event(&mut event, &prospero);

        // Ensure that the runtime context values are not overwritten
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
}
