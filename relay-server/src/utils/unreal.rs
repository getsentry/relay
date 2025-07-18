use bytes::Bytes;
use chrono::{TimeZone, Utc};
use relay_config::Config;
use relay_event_schema::protocol::{
    AsPair, Breadcrumb, ClientSdkInfo, Context, Contexts, DeviceContext, Event, EventId,
    GpuContext, LenientString, Level, LogEntry, Message, OsContext, TagEntry, Tags, Timestamp,
    User, UserReport, Values,
};
use relay_protocol::{Annotated, Array, Empty, Object, Value};
use symbolic_unreal::{
    Unreal4Context, Unreal4Crash, Unreal4Error, Unreal4FileType, Unreal4LogEntry,
};

use crate::constants::{
    ITEM_NAME_BREADCRUMBS1, ITEM_NAME_BREADCRUMBS2, ITEM_NAME_EVENT, UNREAL_USER_HEADER,
};
use crate::envelope::{AttachmentType, ContentType, Envelope, Item, ItemType};
use crate::services::processor::ProcessingError;

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
/// After this, the `EnvelopeProcessor` should be able to process the envelope the same way it
/// processes any other envelopes.
pub fn expand_unreal_envelope(
    unreal_item: Item,
    envelope: &mut Envelope,
    config: &Config,
) -> Result<(), ProcessingError> {
    let payload = unreal_item.payload();
    let crash = Unreal4Crash::parse_with_limit(&payload, config.max_envelope_size())?;

    let mut has_event = envelope
        .get_item_by(|item| item.ty() == &ItemType::Event)
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
        // TODO: This clones data. Update symbolic to allow moving the bytes out.
        item.set_payload(content_type, file.data().to_owned());
        item.set_attachment_type(attachment_type);
        envelope.add_item(item);
    }

    if let Err(offender) = super::check_envelope_size_limits(config, envelope) {
        return Err(ProcessingError::PayloadTooLarge(offender));
    }

    Ok(())
}

fn merge_unreal_user_info(event: &mut Event, user_info: &str) {
    let mut parts = user_info.split('|');

    if let Some(user_id) = parts.next().filter(|id| !id.is_empty()) {
        let user = event.user.value_mut().get_or_insert_with(User::default);
        user.id = Annotated::new(LenientString(user_id.to_owned()));
    }

    if let Some(epic_account_id) = parts.next().filter(|id| !id.is_empty()) {
        let tags = event.tags.value_mut().get_or_insert_with(Tags::default);
        tags.push(Annotated::new(TagEntry(
            Annotated::new("epic_account_id".to_owned()),
            Annotated::new(epic_account_id.to_owned()),
        )));
    }

    if let Some(machine_id) = parts.next().filter(|id| !id.is_empty()) {
        let tags = event.tags.value_mut().get_or_insert_with(Tags::default);
        tags.push(Annotated::new(TagEntry::from_pair((
            Annotated::new("machine_id".to_owned()),
            Annotated::new(machine_id.to_owned()),
        ))));
    }
}

/// Merge an unreal logs object into event breadcrumbs.
fn merge_unreal_logs(
    event: &mut Event,
    logs: impl IntoIterator<Item = Bytes>,
) -> Result<(), Unreal4Error> {
    let breadcrumbs = event
        .breadcrumbs
        .value_mut()
        .get_or_insert_with(Values::default)
        .values
        .value_mut()
        .get_or_insert_with(Array::default);

    for breadcrumb in logs
        .into_iter()
        .flat_map(parse_unreal_logs)
        .take(MAX_NUM_UNREAL_LOGS)
    {
        breadcrumbs.push(Annotated::new(breadcrumb?));
    }

    Ok(())
}

/// Parses unreal logs into breadcrumbs.
fn parse_unreal_logs(data: Bytes) -> impl Iterator<Item = Result<Breadcrumb, Unreal4Error>> {
    let (logs, err) = match Unreal4LogEntry::parse(&data, MAX_NUM_UNREAL_LOGS) {
        Ok(logs) => (logs, None),
        Err(err) => (Vec::new(), Some(Err(err))),
    };

    let logs = logs.into_iter().map(|log| {
        let timestamp = log
            .timestamp
            .and_then(|ts| {
                Utc.timestamp_opt(ts.unix_timestamp(), ts.nanosecond())
                    .latest()
            })
            .map(Timestamp);

        Ok(Breadcrumb {
            timestamp: Annotated::from(timestamp),
            category: Annotated::from(log.component),
            message: Annotated::new(log.message),
            ..Breadcrumb::default()
        })
    });

    err.into_iter().chain(logs)
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
        let logentry = event.logentry.get_or_insert_with(LogEntry::default);
        if logentry.formatted.is_empty() {
            logentry.formatted = Annotated::new(Message::from(msg));
        }
    }

    if let Some(username) = runtime_props.username.take() {
        let user = event.user.get_or_insert_with(User::default);
        if user.username.is_empty() {
            user.username.set_value(Some(username.into()));
        }
    }

    if let Some(login_id) = &runtime_props.login_id {
        let id = event.user.get_or_insert_with(User::default).id.value_mut();

        if id.as_ref().is_none_or(|s| s.is_empty()) {
            *id = Some(login_id.clone().into());
        }
    }

    let contexts = event.contexts.get_or_insert_with(Contexts::default);

    if let Some(memory_physical) = runtime_props.memory_stats_total_physical.take() {
        let device_context = contexts.get_or_default::<DeviceContext>();
        if device_context.memory_size.is_empty() {
            device_context.memory_size = Annotated::new(memory_physical);
        }
    }

    // OS information is likely overwritten by Minidump processing later.
    if let Some(os_major) = runtime_props.misc_os_version_major.take() {
        let os_context = contexts.get_or_default::<OsContext>();
        if os_context.raw_description.is_empty() {
            os_context.raw_description = Annotated::new(os_major);
        }
    }

    // See https://github.com/EpicGames/UnrealEngine/blob/5.3.2-release/Engine/Source/Runtime/RHI/Private/DynamicRHI.cpp#L368-L376
    if let Some(adapter_name) = context.engine_data.get("RHI.AdapterName") {
        let gpu_context = contexts.get_or_default::<GpuContext>();
        if gpu_context.name.is_empty() {
            gpu_context.name = Annotated::new(adapter_name.into());
        }
    } else if let Some(gpu_brand) = runtime_props.misc_primary_gpu_brand.take() {
        let gpu_context = contexts.get_or_default::<GpuContext>();
        if gpu_context.name.is_empty() {
            gpu_context.name = Annotated::new(gpu_brand);
        }
    }

    if let Some(device_id) = context.engine_data.get("RHI.DeviceId") {
        let gpu_context = contexts.get_or_default::<GpuContext>();
        if gpu_context.id.is_empty() {
            gpu_context.id = Annotated::new(Value::String(device_id.into()));
        }
    }

    if let Some(feature_level) = context.engine_data.get("RHI.FeatureLevel") {
        let gpu_context = contexts.get_or_default::<GpuContext>();
        if gpu_context.graphics_shader_level.is_empty() {
            gpu_context.graphics_shader_level = Annotated::new(feature_level.into());
        }
    }

    if let Some(vendor_name) = context.engine_data.get("RHI.GPUVendor") {
        let gpu_context = contexts.get_or_default::<GpuContext>();
        if gpu_context.vendor_name.is_empty() {
            gpu_context.vendor_name = Annotated::new(vendor_name.into());
        }
    }

    if let Some(driver_version) = context.engine_data.get("RHI.UserDriverVersion") {
        let gpu_context = contexts.get_or_default::<GpuContext>();
        if gpu_context.version.is_empty() {
            gpu_context.version = Annotated::new(driver_version.into());
        }
    }

    if let Some(rhi_name) = context.engine_data.get("RHI.RHIName") {
        let gpu_context = contexts.get_or_default::<GpuContext>();
        if gpu_context.api_type.is_empty() {
            gpu_context.api_type = Annotated::new(rhi_name.into());
        }
    }

    if event.level.is_empty() {
        if runtime_props.is_assert.unwrap_or(false) {
            event.level = Annotated::new(Level::Error)
        } else if runtime_props.is_ensure.unwrap_or(false) {
            event.level = Annotated::new(Level::Warning)
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

    if let Ok(Some(Value::Object(props))) = relay_protocol::to_value(&runtime_props) {
        let unreal_context =
            contexts.get_or_insert_with("unreal", || Context::Other(Object::new()));

        if let Context::Other(unreal_context) = unreal_context {
            unreal_context.extend(props);
        }
    }
}

/// Processes an unreal envelope.
///
/// This function returns either the processing error, or a boolean indicating
/// whether the envelope contained an unreal item.
pub fn process_unreal_envelope(
    event: &mut Annotated<Event>,
    envelope: &mut Envelope,
) -> Result<bool, Unreal4Error> {
    let user_header = envelope
        .get_header(UNREAL_USER_HEADER)
        .and_then(Value::as_str);
    let context_item =
        envelope.get_item_by(|item| item.attachment_type() == Some(&AttachmentType::UnrealContext));
    let mut logs_items = envelope
        .items()
        .filter(|item| item.attachment_type() == Some(&AttachmentType::UnrealLogs))
        .map(|item| item.payload())
        .peekable();

    // Early exit if there is no information.
    if user_header.is_none() && context_item.is_none() && logs_items.peek().is_none() {
        return Ok(false);
    }

    // If we have UE4 info, ensure an event is there to fill. DO NOT fill if there is no unreal
    // information, or otherwise `EnvelopeProcessorService::process` breaks.
    let event = event.get_or_insert_with(Event::default);

    if let Some(user_info) = user_header {
        merge_unreal_user_info(event, user_info);
    }

    merge_unreal_logs(event, logs_items)?;

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

    Ok(true)
}

#[cfg(test)]
mod tests {

    use relay_protocol::SerializableAnnotated;

    use super::*;

    fn get_context() -> Unreal4Context {
        let raw_context = br#"<?xml version="1.0" encoding="UTF-8"?>
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
	<EngineData>
		<MatchingDPStatus>WindowsNo errors</MatchingDPStatus>
		<RHI.IntegratedGPU>false</RHI.IntegratedGPU>
		<RHI.DriverDenylisted>false</RHI.DriverDenylisted>
		<RHI.D3DDebug>false</RHI.D3DDebug>
		<RHI.Breadcrumbs>true</RHI.Breadcrumbs>
		<RHI.DRED>true</RHI.DRED>
		<RHI.DREDMarkersOnly>false</RHI.DREDMarkersOnly>
		<RHI.DREDContext>true</RHI.DREDContext>
		<RHI.Aftermath>true</RHI.Aftermath>
		<RHI.RHIName>D3D12</RHI.RHIName>
		<RHI.AdapterName>NVIDIA GeForce RTX 4060 Laptop GPU</RHI.AdapterName>
		<RHI.UserDriverVersion>551.52</RHI.UserDriverVersion>
		<RHI.InternalDriverVersion>31.0.15.5152</RHI.InternalDriverVersion>
		<RHI.DriverDate>2-7-2024</RHI.DriverDate>
		<RHI.FeatureLevel>SM5</RHI.FeatureLevel>
		<RHI.GPUVendor>NVIDIA</RHI.GPUVendor>
		<RHI.DeviceId>28E0</RHI.DeviceId>
		<DeviceProfile.Name>Windows</DeviceProfile.Name>
		<Platform.AppHasFocus>true</Platform.AppHasFocus>
	</EngineData>
	<PlatformProperties>
		<PlatformIsRunningWindows>1</PlatformIsRunningWindows>
		<PlatformCallbackResult>0</PlatformCallbackResult>
	</PlatformProperties>
</FGenericCrashContext>
"#;

        Unreal4Context::parse(raw_context).unwrap()
    }

    fn get_context_with_login_id() -> Unreal4Context {
        let raw_context = br#"<?xml version="1.0" encoding="UTF-8"?>
<FGenericCrashContext>
	<RuntimeProperties>
        <LoginId>SOME_ID</LoginId>
	</RuntimeProperties>
	<EngineData>
	</EngineData>
	<PlatformProperties>
	</PlatformProperties>
</FGenericCrashContext>
"#;

        Unreal4Context::parse(raw_context).unwrap()
    }

    #[test]
    fn test_merge_unreal_context() {
        let context = get_context();
        let mut event = Event::default();

        merge_unreal_context(&mut event, context);

        insta::assert_snapshot!(Annotated::new(event).to_json_pretty().unwrap());
    }

    #[test]
    fn test_merge_unreal_context_is_assert_level_error() {
        let mut context = get_context();
        let runtime_props = context.runtime_properties.as_mut().unwrap();
        runtime_props.is_assert = Some(true);

        let mut event = Event::default();

        merge_unreal_context(&mut event, context);

        assert_eq!(event.level, Annotated::new(Level::Error));
    }

    #[test]
    fn test_merge_unreal_context_is_esure_level_warning() {
        let mut context = get_context();
        let runtime_props = context.runtime_properties.as_mut().unwrap();
        runtime_props.is_ensure = Some(true);

        let mut event = Event::default();

        merge_unreal_context(&mut event, context);

        assert_eq!(event.level, Annotated::new(Level::Warning));
    }

    #[test]
    fn test_merge_unreal_context_is_assert_is_user_defined() {
        let mut context = get_context();
        let runtime_props = context.runtime_properties.as_mut().unwrap();
        runtime_props.is_assert = Some(true);

        let mut event = Event {
            level: Annotated::new(Level::Warning),
            ..Default::default()
        };

        merge_unreal_context(&mut event, context);

        assert_eq!(event.level, Annotated::new(Level::Warning));
    }

    #[test]
    fn test_merge_unreal_logs() {
        let logs = Bytes::from_static(br#"Log file open, 10/29/18 17:56:37
[2018.10.29-16.56.38:332][  0]LogGameplayTags: Display: UGameplayTagsManager::DoneAddingNativeTags. DelegateIsBound: 0
[2018.10.29-16.56.39:332][  0]LogStats: UGameplayTagsManager::ConstructGameplayTagTree: ImportINI prefixes -  0.000 s
[2018.10.29-16.56.40:332][  0]LogStats: UGameplayTagsManager::ConstructGameplayTagTree: Construct from data asset -  0.000 s
[2018.10.29-16.56.41:332][  0]LogStats: UGameplayTagsManager::ConstructGameplayTagTree: ImportINI -  0.000 s"#);

        let mut event = Event::default();
        merge_unreal_logs(&mut event, Some(logs)).unwrap();

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

    #[test]
    fn test_merge_unreal_context_login_id() {
        let context = get_context_with_login_id();
        let mut event = Event::default();

        merge_unreal_context(&mut event, context);

        assert_eq!(
            event.user.0.unwrap().id.0.unwrap(),
            LenientString("SOME_ID".to_owned())
        );
    }

    #[test]
    fn test_merge_unreal_context_login_id_empty_header() {
        let user_id = "";

        let context = get_context_with_login_id();
        let mut event = Event::default();

        merge_unreal_user_info(&mut event, user_id);
        merge_unreal_context(&mut event, context);

        assert_eq!(
            event.user.0.unwrap().id.0.unwrap(),
            LenientString("SOME_ID".to_owned())
        );
    }

    #[test]
    fn test_merge_unreal_context_login_id_full_header() {
        let user_id = "ebff51ef3c4878627823eebd9ff40eb4|2e7d369327054a448be6c8d3601213cb|C52DC39D-DAF3-5E36-A8D3-BF5F53A5D38F";

        let context = get_context_with_login_id();
        let mut event = Event::default();

        merge_unreal_user_info(&mut event, user_id);
        merge_unreal_context(&mut event, context);

        assert_eq!(
            event.user.0.unwrap().id.0.unwrap(),
            LenientString("ebff51ef3c4878627823eebd9ff40eb4".to_owned())
        );
    }

    #[test]
    fn test_merge_unreal_context_is_input_preserved() {
        let context = get_context();

        let mut event = Event::default();

        let logentry = event.logentry.get_or_insert_with(LogEntry::default);
        logentry.formatted = Annotated::new("error_message".to_owned().into());

        let user = event.user.get_or_insert_with(User::default);
        user.username = Annotated::new("user_name".to_owned().into());

        let contexts = event.contexts.get_or_insert_with(Contexts::default);

        let device_context = contexts.get_or_default::<DeviceContext>();
        device_context.memory_size = Annotated::new(123456789);

        let os_context = contexts.get_or_default::<OsContext>();
        os_context.name = Annotated::new("os_name".to_owned());
        os_context.raw_description = Annotated::new("my own raw desc".to_owned());

        let gpu_context = contexts.get_or_default::<GpuContext>();
        gpu_context.name = Annotated::new("adapter_name".to_owned());
        gpu_context.id = Annotated::new(Value::String("device_id".to_owned()));
        gpu_context.graphics_shader_level = Annotated::new("feature_level".to_owned());
        gpu_context.vendor_name = Annotated::new("vendor_name".to_owned());
        gpu_context.version = Annotated::new("driver_version".to_owned());
        gpu_context.api_type = Annotated::new("rhi_name".to_owned());

        merge_unreal_context(&mut event, context);

        let event = Annotated::new(event);
        insta::assert_json_snapshot!(SerializableAnnotated(&event), @r###"
        {
          "logentry": {
            "formatted": "error_message"
          },
          "user": {
            "username": "user_name"
          },
          "contexts": {
            "device": {
              "memory_size": 123456789,
              "type": "device"
            },
            "gpu": {
              "name": "adapter_name",
              "version": "driver_version",
              "id": "device_id",
              "vendor_name": "vendor_name",
              "api_type": "rhi_name",
              "graphics_shader_level": "feature_level",
              "type": "gpu"
            },
            "os": {
              "name": "os_name",
              "raw_description": "my own raw desc",
              "type": "os"
            },
            "unreal": {
              "custom": {
                "CrashVersion": "3",
                "PlatformFullName": "Win64 [Windows 10  64b]"
              },
              "memory_stats_total_virtual": 140737488224256,
              "misc_cpu_brand": "Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz",
              "misc_cpu_vendor": "GenuineIntel",
              "misc_primary_gpu_brand": "Parallels Display Adapter (WDDM)"
            }
          },
          "sdk": {
            "name": "unreal.crashreporter",
            "version": "1.0"
          }
        }
        "###);
    }
}
