use symbolic::unreal::{
    Unreal4Context, Unreal4Crash, Unreal4Error, Unreal4FileType, Unreal4LogEntry,
};

use relay_general::protocol::{
    AsPair, Breadcrumb, ClientSdkInfo, Context, Contexts, DeviceContext, Event, EventId,
    GpuContext, LenientString, LogEntry, Message, OsContext, TagEntry, Tags, Timestamp, User,
    UserReport, Values,
};
use relay_general::types::{self, Annotated, Array, Object, Value};

use crate::constants::{
    ITEM_NAME_BREADCRUMBS1, ITEM_NAME_BREADCRUMBS2, ITEM_NAME_EVENT, UNREAL_USER_HEADER,
};
use crate::envelope::{AttachmentType, ContentType, Envelope, Item, ItemType};

/// Maximum number of unreal logs to parse for breadcrumbs.
const MAX_NUM_UNREAL_LOGS: usize = 40;

/// Name of the custom XML tag in Unreal GameData for Sentry event payloads.
const SENTRY_PAYLOAD_KEY: &str = "__sentry";

/// Client SDK name used for the event payload to identify the UE4 crash reporter.
const CLIENT_SDK_NAME: &str = "unreal.crashreporter";

fn get_event_item(data: &[u8]) -> Result<Option<Item>, Unreal4Error> {
    let mut context = Unreal4Context::parse(data)?;
    let json = match context.game_data.remove(SENTRY_PAYLOAD_KEY) {
        Some(json) if !json.is_empty() => json,
        _ => return Ok(None),
    };

    relay_log::trace!("adding event payload from unreal context");
    let mut item = Item::new(ItemType::Event);
    item.set_payload(ContentType::Json, json);
    Ok(Some(item))
}

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

    let mut has_event = envelope
        .get_item_by(|item| item.ty() == ItemType::Event)
        .is_some();

    for file in crash.files() {
        let (content_type, attachment_type) = match file.ty() {
            Unreal4FileType::Minidump => (ContentType::Minidump, AttachmentType::Minidump),
            Unreal4FileType::AppleCrashReport => {
                (ContentType::Text, AttachmentType::AppleCrashReport)
            }
            Unreal4FileType::Log => (ContentType::Text, AttachmentType::UnrealLogs),
            Unreal4FileType::Config => (ContentType::OctetStream, AttachmentType::Attachment),
            Unreal4FileType::Context => (ContentType::Xml, AttachmentType::UnrealContext),
            _ => match file.name() {
                self::ITEM_NAME_EVENT => (ContentType::MsgPack, AttachmentType::EventPayload),
                self::ITEM_NAME_BREADCRUMBS1 => (ContentType::MsgPack, AttachmentType::Breadcrumbs),
                self::ITEM_NAME_BREADCRUMBS2 => (ContentType::MsgPack, AttachmentType::Breadcrumbs),
                _ => (ContentType::OctetStream, AttachmentType::Attachment),
            },
        };

        if !has_event && attachment_type == AttachmentType::UnrealContext {
            if let Some(event_item) = get_event_item(file.data())? {
                envelope.add_item(event_item);
                has_event = true;
            }
        }

        let mut item = Item::new(ItemType::Attachment);
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
            timestamp: Annotated::from(log.timestamp.map(Timestamp)),
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
        email: "".to_owned(),
        comments: user_description,
        event_id,
        name: user_name,
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
            .formatted = Annotated::new(Message::from(msg));
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

    // OS information is likely overwritten by Minidump processing later.
    if let Some(os_major) = runtime_props.misc_os_version_major.take() {
        let os_context = contexts.get_or_insert_with(OsContext::default_key(), || {
            Context::Os(Box::new(OsContext::default()))
        });

        if let Context::Os(os_context) = os_context {
            os_context.name = Annotated::new(os_major);
        }
    }

    // Clear this property. It's a duplicate of misc_primary_gpu_brand and only exists because
    // somebody made a typo once while mapping the GPU data from unrealcontext in symbolic, and we
    // tried to fix this in a backwards-compatible way by retaining both properties.
    #[allow(deprecated)]
    {
        runtime_props.misc_primary_cpu_brand = None;
    }

    if let Some(gpu_brand) = runtime_props.misc_primary_gpu_brand.take() {
        let gpu_context = contexts.get_or_insert_with(GpuContext::default_key(), || {
            Context::Gpu(Box::new(GpuContext::default()))
        });

        if let Context::Gpu(gpu_context) = gpu_context {
            gpu_context.name = Annotated::new(gpu_brand);
        }
    }

    // Modules are not used and later replaced with Modules from the Minidump or Apple Crash Report.
    runtime_props.modules.take();

    // Promote all game data (except the special `__sentry` key) into a context.
    if !context.game_data.is_empty() {
        let game_context = contexts.get_or_insert_with("game", || Context::Other(Object::new()));
        if let Context::Other(game_context) = game_context {
            let filtered_keys = context
                .game_data
                .into_iter()
                .filter(|(key, _)| key != SENTRY_PAYLOAD_KEY)
                .map(|(key, value)| (key, Annotated::new(Value::String(value))));

            game_context.extend(filtered_keys);
        }
    }

    // Add sdk information for analytics.
    event.client_sdk.get_or_insert_with(|| ClientSdkInfo {
        name: CLIENT_SDK_NAME.to_owned().into(),
        version: runtime_props
            .crash_reporter_client_version
            .take()
            .unwrap_or_else(|| "0.0.0".to_owned())
            .into(),
        ..ClientSdkInfo::default()
    });

    if let Ok(Some(Value::Object(props))) = types::to_value(&runtime_props) {
        let unreal_context =
            contexts.get_or_insert_with("unreal", || Context::Other(Object::new()));

        if let Context::Other(unreal_context) = unreal_context {
            unreal_context.extend(props);
        }
    }
}

pub fn process_unreal_envelope(
    event: &mut Annotated<Event>,
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

    // If we have UE4 info, ensure an event is there to fill. DO NOT fill if there is no unreal
    // information, or otherwise `EventProcessor::process` breaks.
    let event = event.get_or_insert_with(Event::default);

    if let Some(user_info) = user_header {
        merge_unreal_user_info(event, user_info);
    }

    if let Some(logs_item) = logs_item {
        merge_unreal_logs(event, &logs_item.payload())?;
    }

    if let Some(context_item) = context_item {
        let mut context = Unreal4Context::parse(&context_item.payload())?;

        // the `unwrap_or_default` here can produce an invalid user report if the envelope id
        // is indeed missing. This should not happen under normal circumstances since the EventId is
        // created statically.
        let event_id = envelope.event_id().unwrap_or_default();
        debug_assert!(!event_id.is_nil());

        if let Some(report) = get_unreal_user_report(event_id, &mut context) {
            envelope.add_item(report);
        }

        merge_unreal_context(event, context);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_unreal_context() {
        let raw_context = br##"<?xml version="1.0" encoding="UTF-8"?>
<FGenericCrashContext>
	<RuntimeProperties>
		<UserName>bruno</UserName>
		<MemoryStats.TotalPhysical>6896832512</MemoryStats.TotalPhysical>
		<Misc.OSVersionMajor>Windows 10</Misc.OSVersionMajor>
		<Misc.OSVersionMinor />
		<Misc.PrimaryGPUBrand>Parallels Display Adapter (WDDM)</Misc.PrimaryGPUBrand>
		<ErrorMessage>Access violation - code c0000005 (first/second chance not available)</ErrorMessage>

		<CrashVersion>3</CrashVersion>
		<Misc.Is64bitOperatingSystem>1</Misc.Is64bitOperatingSystem>
		<Misc.CPUVendor>GenuineIntel</Misc.CPUVendor>
		<Misc.CPUBrand>Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz</Misc.CPUBrand>
		<GameStateName />
		<MemoryStats.TotalVirtual>140737488224256</MemoryStats.TotalVirtual>
		<PlatformFullName>Win64 [Windows 10  64b]</PlatformFullName>
		<CrashReportClientVersion>1.0</CrashReportClientVersion>
		<Modules>\\Mac\Home\Desktop\WindowsNoEditor\YetAnother\Binaries\Win64\YetAnother.exe
\\Mac\Home\Desktop\WindowsNoEditor\Engine\Binaries\ThirdParty\PhysX3\Win64\VS2015\PxFoundationPROFILE_x64.dll</Modules>
	</RuntimeProperties>
	<PlatformProperties>
		<PlatformIsRunningWindows>1</PlatformIsRunningWindows>
		<PlatformCallbackResult>0</PlatformCallbackResult>
	</PlatformProperties>
</FGenericCrashContext>
"##;

        let context = Unreal4Context::parse(raw_context).unwrap();
        let mut event = Event::default();

        merge_unreal_context(&mut event, context);

        insta::assert_snapshot!(Annotated::new(event).to_json_pretty().unwrap());
    }

    #[test]
    fn test_merge_unreal_logs() {
        let logs = br##"Log file open, 10/29/18 17:56:37
[2018.10.29-16.56.38:332][  0]LogGameplayTags: Display: UGameplayTagsManager::DoneAddingNativeTags. DelegateIsBound: 0
[2018.10.29-16.56.39:332][  0]LogStats: UGameplayTagsManager::ConstructGameplayTagTree: ImportINI prefixes -  0.000 s
[2018.10.29-16.56.40:332][  0]LogStats: UGameplayTagsManager::ConstructGameplayTagTree: Construct from data asset -  0.000 s
[2018.10.29-16.56.41:332][  0]LogStats: UGameplayTagsManager::ConstructGameplayTagTree: ImportINI -  0.000 s"##;

        let mut event = Event::default();
        merge_unreal_logs(&mut event, logs).ok();

        insta::assert_snapshot!(Annotated::new(event).to_json_pretty().unwrap());
    }

    #[test]
    fn test_merge_unreal_context_event() {
        let bytes = include_bytes!("../../../tests/integration/fixtures/native/unreal_crash");
        let user_id = "ebff51ef3c4878627823eebd9ff40eb4|2e7d369327054a448be6c8d3601213cb|C52DC39D-DAF3-5E36-A8D3-BF5F53A5D38F";

        let crash = Unreal4Crash::parse(bytes).unwrap();
        let mut event = Event::default();

        merge_unreal_user_info(&mut event, user_id);
        merge_unreal_context(&mut event, crash.context().unwrap().unwrap());

        insta::assert_snapshot!(Annotated::new(event).to_json_pretty().unwrap());
    }
}
