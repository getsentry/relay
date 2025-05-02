//! Playstation related code.
//!
//! These functions are included only in the processing mode.

use prosperoconv::write_dump;
use prosperoconv::{extract_data, ProsperoDump};

use relay_event_schema::protocol::{
    AppContext, Context, Contexts, DeviceContext, LenientString, OsContext, RuntimeContext, Tags,
};
use relay_event_schema::protocol::{Event, TagEntry};
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
    }

    Ok(Some(EventFullyNormalized(false)))
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
                Annotated::new($key),
                Annotated::new($value),
            )));
        };
    }

    add_tag!("cpu_vendor".into(), "Sony".into());
    add_tag!("os.name".into(), "PlayStation".into());

    let platform = "PS5";
    runtime_context.name = Annotated::new(platform.into());
    device_context.model = Annotated::new(platform.into());

    add_tag!("cpu_brand".into(), format!("{platform} CPU").into());
    add_tag!("runtime.name".into(), platform.into());

    if let Some(system_version) = &prospero.sdk_version {
        add_tag!("os".into(), format!("PlayStation {system_version}"));
        add_tag!("runtime".into(), system_version.into());
        add_tag!("runtime.version".into(), system_version.into());

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
            add_tag!(tag.into(), v.to_string());
        }
    }

    if let Some(app_info) = &prospero.app_info {
        add_tag!("titleId".into(), app_info.title_id.into());

        contexts.get_or_default::<AppContext>().app_version =
            Annotated::new(app_info.version.into());
    }

    contexts.add(device_context);
    contexts.add(os_context);
    contexts.add(runtime_context);
}
