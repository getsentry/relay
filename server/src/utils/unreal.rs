use symbolic::unreal::{
    Unreal4Context, Unreal4Crash, Unreal4Error, Unreal4FileType, Unreal4LogEntry,
};

use semaphore_general::protocol::{
    AsPair, Breadcrumb, Context, Contexts, DeviceContext, Event, EventId, GpuContext,
    LenientString, LogEntry, OsContext, TagEntry, Tags, User, UserReport, Values,
};
use semaphore_general::types::{Annotated, Array, Value};

use crate::constants::{ITEM_NAME_BREADCRUMBS1, ITEM_NAME_BREADCRUMBS2, ITEM_NAME_EVENT};
use crate::envelope::{AttachmentType, ContentType, Envelope, Item, ItemType};

/// Maximum number of unreal logs to parse for breadcrumbs.
const MAX_NUM_UNREAL_LOGS: usize = 40;

/// Envelope header used to store the UE4 user id.
pub const UNREAL_USER_HEADER: &str = "unreal_user_id";

/// Expands Unreal 4 items inside an envelope.
///
/// If the envelope does NOT contain an `UnrealReport` item, it doesn't do anything. If the envelope
/// contains an `UnrealReport` item, it removes it from the envelope and inserts new items for each
/// of its contents.
///
/// After this, the `EventProcessor` should be able to process the envelope the same way it
/// processes any other envelopes.
pub fn expand_unreal_envelope(
    unreal_item: Item,
    envelope: &mut Envelope,
) -> Result<(), Unreal4Error> {
    let payload = unreal_item.payload();
    let crash = Unreal4Crash::parse(&payload)?;

    for file in crash.files() {
        let (content_type, attachment_type) = match file.ty() {
            Unreal4FileType::Minidump => (ContentType::OctetStream, AttachmentType::Minidump),
            Unreal4FileType::AppleCrashReport => {
                (ContentType::OctetStream, AttachmentType::AppleCrashReport)
            }
            Unreal4FileType::Log => (ContentType::OctetStream, AttachmentType::UnrealLogs),
            Unreal4FileType::Config => (ContentType::OctetStream, AttachmentType::Attachment),
            Unreal4FileType::Context => (ContentType::OctetStream, AttachmentType::UnrealContext),
            Unreal4FileType::Unknown => match file.name() {
                self::ITEM_NAME_EVENT => (ContentType::MsgPack, AttachmentType::MsgpackEvent),
                self::ITEM_NAME_BREADCRUMBS1 => (ContentType::MsgPack, AttachmentType::Breadcrumbs),
                self::ITEM_NAME_BREADCRUMBS2 => (ContentType::MsgPack, AttachmentType::Breadcrumbs),
                _ => (ContentType::OctetStream, AttachmentType::Attachment),
            },
        };

        let mut item = Item::new(ItemType::Attachment);
        item.set_name(file.name());
        item.set_filename(file.name());
        item.set_payload(content_type, file.data());
        item.set_attachment_type(attachment_type);
        envelope.add_item(item);
    }

    Ok(())
}

fn merge_unreal_user_info(event: &mut Event, user_info: &str) {
    let mut parts = user_info.split('|');

    if let Some(user_id) = parts.next() {
        let user = event.user.value_mut().get_or_insert_with(User::default);
        user.id = Annotated::new(LenientString(user_id.to_owned()));
    }

    if let Some(epic_account_id) = parts.next() {
        let tags = event.tags.value_mut().get_or_insert_with(Tags::default);
        tags.push(Annotated::new(TagEntry(
            Annotated::new("epic_account_id".to_string()),
            Annotated::new(epic_account_id.to_string()),
        )));
    }

    if let Some(machine_id) = parts.next() {
        let tags = event.tags.value_mut().get_or_insert_with(Tags::default);
        tags.push(Annotated::new(TagEntry::from_pair((
            Annotated::new("machine_id".to_string()),
            Annotated::new(machine_id.to_string()),
        ))));
    }
}

/// Merge an unreal logs object into event breadcrumbs.
fn merge_unreal_logs(event: &mut Event, data: &[u8]) -> Result<(), Unreal4Error> {
    let logs = Unreal4LogEntry::parse(data, MAX_NUM_UNREAL_LOGS)?;

    let breadcrumbs = event
        .breadcrumbs
        .value_mut()
        .get_or_insert_with(Values::default)
        .values
        .value_mut()
        .get_or_insert_with(Array::default);

    for log in logs {
        breadcrumbs.push(Annotated::new(Breadcrumb {
            timestamp: Annotated::from(log.timestamp),
            category: Annotated::from(log.component),
            message: Annotated::new(log.message),
            ..Breadcrumb::default()
        }));
    }

    Ok(())
}

/// Parses `UserReport` from unreal user information.
fn get_unreal_user_report(event_id: EventId, context: &mut Unreal4Context) -> Option<Item> {
    let runtime_props = context.runtime_properties.as_mut()?;
    let user_description = runtime_props.user_description.take()?;

    let user_name = runtime_props
        .username
        .clone()
        .unwrap_or_else(|| "unknown".to_owned());

    let user_report = UserReport {
        email: None,
        comments: user_description,
        event_id,
        name: Some(user_name),
    };

    let json = serde_json::to_string(&user_report).ok()?;

    let mut item = Item::new(ItemType::UserReport);
    item.set_payload(ContentType::Json, json);
    Some(item)
}

/// Merges an unreal context object into an event
fn merge_unreal_context(event: &mut Event, context: Unreal4Context) {
    let mut runtime_props = match context.runtime_properties {
        Some(runtime_props) => runtime_props,
        None => return,
    };

    if let Some(msg) = runtime_props.error_message.take() {
        event
            .logentry
            .get_or_insert_with(LogEntry::default)
            .formatted = Annotated::new(msg);
    }

    if let Some(username) = runtime_props.username.take() {
        event
            .user
            .get_or_insert_with(User::default)
            .username
            .set_value(Some(username));
    }

    let contexts = event.contexts.get_or_insert_with(Contexts::default);

    if let Some(memory_physical) = runtime_props.memory_stats_total_physical.take() {
        let device_context = contexts.get_or_insert_with(DeviceContext::default_key(), || {
            Context::Device(Box::new(DeviceContext::default()))
        });

        if let Context::Device(device_context) = device_context {
            device_context.memory_size = Annotated::new(memory_physical);
        }
    }

    if let Some(os_major) = runtime_props.misc_os_version_major.take() {
        let os_context = contexts.get_or_insert_with(OsContext::default_key(), || {
            Context::Os(Box::new(OsContext::default()))
        });

        if let Context::Os(os_context) = os_context {
            os_context.name = Annotated::new(os_major);
        }
    }

    if let Some(gpu_brand) = runtime_props.misc_primary_gpu_brand.take() {
        let gpu_context = contexts.get_or_insert_with(GpuContext::default_key(), || {
            Context::Gpu(Box::new(GpuContext::default()))
        });

        if let Context::Gpu(gpu_context) = gpu_context {
            gpu_context.insert("name".to_owned(), Annotated::new(Value::String(gpu_brand)));
        }
    }

    // modules not used just remove it from runtime props
    runtime_props.modules.take();

    let props = serde_json::to_string(&runtime_props).and_then(|p| serde_json::from_str(&p));
    if let Ok(Value::Object(props)) = props {
        contexts.add_at_index("unreal", Context::Other(props));
    }
}

pub fn process_unreal_envelope(
    event_opt: &mut Option<Annotated<Event>>,
    envelope: &mut Envelope,
) -> Result<(), Unreal4Error> {
    let user_header = envelope
        .get_header(UNREAL_USER_HEADER)
        .and_then(Value::as_str);
    let context_item =
        envelope.get_item_by(|item| item.attachment_type() == Some(AttachmentType::UnrealContext));
    let logs_item =
        envelope.get_item_by(|item| item.attachment_type() == Some(AttachmentType::UnrealLogs));

    // Early exit if there is no information.
    if user_header.is_none() && context_item.is_none() && logs_item.is_none() {
        return Ok(());
    }

    // If we have UE4 info, ensure an event is there to fill.
    let annotated = event_opt.get_or_insert_with(Annotated::empty);
    let event = annotated.get_or_insert_with(Event::default);

    if let Some(user_info) = user_header {
        merge_unreal_user_info(event, user_info);
    }

    if let Some(logs_item) = logs_item {
        merge_unreal_logs(event, &logs_item.payload())?;
    }

    if let Some(context_item) = context_item {
        let mut context = Unreal4Context::parse(&context_item.payload())?;
        if let Some(report) = get_unreal_user_report(envelope.event_id(), &mut context) {
            envelope.add_item(report);
        }
        merge_unreal_context(event, context);
    }

    Ok(())
}
