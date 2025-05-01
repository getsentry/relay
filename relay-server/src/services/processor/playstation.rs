//! Playstation related code.
//!
//! These functions are included only in the processing mode.

// TODO: CFG all this so that CI is happy.
use prosperoconv::ProsperoDump;

use relay_dynamic_config::Feature;
use relay_event_schema::protocol::{
    AppContext, Context, Contexts, DeviceContext, LenientString, OsContext, RuntimeContext, Tags,
};
use relay_event_schema::protocol::{Event, TagEntry};
use relay_protocol::{Annotated, Object};

use crate::envelope::{AttachmentType, ItemType};
use crate::services::processor::metric;
use crate::services::processor::{ErrorGroup, EventFullyNormalized, ProcessingError};
use crate::services::projects::project::ProjectInfo;
use crate::statsd::RelayCounters;
use crate::utils::TypedEnvelope;

pub fn expand(
    managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    project_info: &ProjectInfo,
) -> Result<(), ProcessingError> {
    let envelope = &mut managed_envelope.envelope_mut();
    if !project_info.has_feature(Feature::PlaystationIngestion) {
        return Ok(());
    }

    if let Some(_item) = envelope.take_item_by(|item| {
        item.ty() == &ItemType::Attachment
            && item.attachment_type() == Some(&AttachmentType::Prosperodump)
    }) {
        // TODO: Add the expand logic here

        metric!(counter(RelayCounters::PlaystationProcessing) += 1);
    }

    Ok(())
}

pub fn process(
    _managed_envelope: &mut TypedEnvelope<ErrorGroup>,
    _event: &mut Annotated<Event>,
) -> Result<Option<EventFullyNormalized>, ProcessingError> {
    // TODO: Add the processing logic here.
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
