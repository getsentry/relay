use bytes::Bytes;
use chrono::{TimeZone, Utc};
use relay_config::ConfigSnapshot;
use relay_event_schema::protocol::{
    Addr, AsPair, Breadcrumb, ClientSdkInfo, Context, Contexts, DeviceContext, Event, EventId,
    Exception, Frame, GpuContext, JsonLenientString, LenientString, Level, LogEntry, Mechanism,
    Message, OsContext, RawStacktrace, Stacktrace, TagEntry, Tags, Timestamp, User, UserReport,
    Values,
};
use relay_protocol::{Annotated, Array, Empty, Object, Value};
use symbolic_unreal::{
    Unreal4Context, Unreal4ContextRuntimeProperties, Unreal4Crash, Unreal4Error, Unreal4FileType,
    Unreal4LogEntry,
};

use crate::constants::{
    ITEM_NAME_BREADCRUMBS1, ITEM_NAME_BREADCRUMBS2, ITEM_NAME_EVENT, SENTRY_CRASH_PAYLOAD_KEY,
};
use crate::envelope::{AttachmentType, ContentType, Item, ItemType, Items};
use crate::services::processor::ProcessingError;

/// Maximum number of unreal logs to parse for breadcrumbs.
const MAX_NUM_UNREAL_LOGS: usize = 40;

/// Maximum number of stack frames taken from the call stacks in the Unreal crash context.
const MAX_NUM_UNREAL_FRAMES: usize = 256;

/// Client SDK name used for the event payload to identify the UE4 crash reporter.
const CLIENT_SDK_NAME: &str = "unreal.crashreporter";

/// Extracts the items from an Unreal 4 crash report payload.
fn extract_items(payload: Bytes, config: &ConfigSnapshot) -> Result<Items, ProcessingError> {
    let mut items = Items::new();
    let crash = Unreal4Crash::parse_with_limit(&payload, config.max_envelope_size())?;

    for file in crash.files().take(config.max_attachment_count()) {
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

        let mut item = Item::new(ItemType::Attachment);
        item.set_filename(file.name());
        item.set_payload(content_type, file.into_bytes());
        item.set_attachment_type(attachment_type);
        items.push(item);
    }

    Ok(items)
}

/// Expands an Unreal 4 crash report payload and returns the expanded items.
pub fn expand_unreal(
    payload: Bytes,
    config: &ConfigSnapshot,
) -> Result<UnrealExpansion, ProcessingError> {
    let items = extract_items(payload, config)?;

    let mut context = items
        .iter()
        .find(|&item| matches!(item.attachment_type(), Some(AttachmentType::UnrealContext)))
        .map(|item| Unreal4Context::parse(&item.payload()))
        .transpose()?;

    let event = context.as_mut().and_then(take_event_item);

    Ok(UnrealExpansion {
        event,
        context,
        attachments: items,
    })
}

/// Expansion from an Unreal 4 report.
pub struct UnrealExpansion {
    /// The error event if the crash contained one.
    pub event: Option<Item>,
    /// The parsed unreal context.
    ///
    /// Note: the raw unreal context may still be in [`Self::attachments`].
    pub context: Option<Unreal4Context>,
    /// Files of the report as attachments.
    pub attachments: Items,
}

fn take_event_item(context: &mut Unreal4Context) -> Option<Item> {
    let json = context
        .game_data
        .remove(crate::constants::SENTRY_CRASH_PAYLOAD_KEY)?;

    if json.is_empty() {
        return None;
    }

    relay_log::trace!("adding event payload from unreal context");
    let mut item = Item::new(ItemType::Event);
    item.set_payload(ContentType::Json, json);
    Some(item)
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
                .filter(|(key, _)| key != SENTRY_CRASH_PAYLOAD_KEY)
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

/// Parses one line of the legacy `CallStack` into a frame.
///
/// Lines look like `Module!Function(...) [File:Line]`, where everything after the module is
/// optional. Unsymbolicated frames are written as `Module!UnknownFunction(0xOffset)`, and symbols
/// without debug information carry their offset, like `Module!Function(+0x8a)`.
///
/// Returns the frame and the module offset of an unsymbolicated frame.
fn parse_legacy_frame(line: &str) -> Option<(Frame, Option<u64>)> {
    let mut line = line.trim();
    if let Some((address, rest)) = line.split_once(' ')
        && address.starts_with("0x")
    {
        line = rest.trim_start();
    }

    if line.is_empty() {
        return None;
    }

    let (package, mut function) = line.split_once('!').unwrap_or((line, ""));

    let mut frame = Frame {
        package: Annotated::from(package.to_owned()),
        ..Frame::default()
    };

    if let Some((name, location)) = function.strip_suffix(']').and_then(|f| f.rsplit_once(" [")) {
        if let Some((filename, lineno)) = location.rsplit_once(':')
            && let Ok(lineno) = lineno.parse::<u64>()
        {
            function = name;
            frame.filename = Annotated::new(filename.into());
            frame.lineno = Annotated::new(lineno);
        } else if location.is_empty() || location == "UnknownFile" {
            function = name;
        }
    }

    // Symbols with an offset are the closest exported symbol, found without debug information.
    // They are often wrong and would group unrelated crashes together, so they are skipped.
    let function = function.trim_end();
    let mut offset = None;
    let is_symbol = match function.strip_suffix(')').and_then(|f| f.rsplit_once('(')) {
        Some((_, args)) if args.starts_with("+0x") => false,
        Some((_, args)) => match args.strip_prefix("0x") {
            Some(hex) => {
                offset = u64::from_str_radix(hex, 16).ok();
                false
            }
            None => true,
        },
        // The last line is cut off when the call stack exceeds the engine's buffer.
        None => function.matches('(').count() == function.matches(')').count(),
    };

    if is_symbol && !function.is_empty() && function != "UnknownFunction" {
        frame.function = Annotated::new(function.to_owned());
    }

    Some((frame, offset))
}

/// A frame of the portable `PCallStack`.
#[derive(Debug, PartialEq)]
struct PortableFrame<'a> {
    module: Option<&'a str>,
    offset: u64,
    address: u64,
}

/// Parses the portable `PCallStack`.
///
/// Each frame is written as `Module 0xImageBase + Offset`, older versions omit the module.
/// Returns `None` if the call stack cannot be fully parsed or contains placeholder addresses.
fn parse_portable_call_stack(call_stack: &str) -> Option<Vec<PortableFrame<'_>>> {
    let tokens: Vec<&str> = call_stack.split_whitespace().collect();
    let mut frames = Vec::new();
    let mut tokens = tokens.as_slice();

    while !tokens.is_empty() && frames.len() < MAX_NUM_UNREAL_FRAMES {
        let (module, base, offset, rest) = match tokens {
            [base, "+", offset, rest @ ..] => (None, base, offset, rest),
            [module, base, "+", offset, rest @ ..] => (Some(*module), base, offset, rest),
            _ => return None,
        };

        let base = u64::from_str_radix(base.strip_prefix("0x")?, 16).ok()?;
        let offset = u64::from_str_radix(offset, 16).ok()?;
        if base == 0xffff_ffff {
            return None;
        }

        frames.push(PortableFrame {
            module,
            offset,
            address: base.checked_add(offset)?,
        });
        tokens = rest;
    }

    Some(frames)
}

/// Returns whether a module of the legacy call stack is the module of the portable call stack.
///
/// The legacy call stack can differ in case, replace `-` with `_`, or add a version suffix, like
/// `libc.so.6` for `libc.so`.
fn is_same_module(legacy: &str, portable: &str) -> bool {
    let legacy = legacy.to_ascii_lowercase().replace('-', "_");
    let portable = portable.to_ascii_lowercase().replace('-', "_");

    legacy == portable
        || legacy
            .strip_prefix(&portable)
            .is_some_and(|s| s.starts_with('.'))
        || portable
            .strip_prefix(&legacy)
            .is_some_and(|s| s.starts_with('.'))
}

/// Returns whether the engine writes full image bases into the portable call stack.
///
/// Unreal Engine 4.25 and older truncate image bases to 32 bits.
fn has_full_image_bases(engine_version: Option<&str>) -> bool {
    let mut parts = engine_version.unwrap_or_default().split(['.', '-']);
    let major = parts.next().and_then(|part| part.parse::<u32>().ok());
    let minor = parts.next().and_then(|part| part.parse::<u32>().ok());

    match (major, minor) {
        (Some(major), Some(minor)) => (major, minor) >= (4, 26),
        _ => true,
    }
}

/// Creates an exception with the stack trace from the Unreal crash context.
///
/// Platforms that do not write a minidump, like Linux, still store the crashing call stack in the
/// crash context. Frames are taken from the legacy `CallStack`, and their instruction addresses
/// from the portable `PCallStack` if it has at least as many frames and both agree on every frame.
fn unreal_exception(runtime_props: &Unreal4ContextRuntimeProperties) -> Option<Exception> {
    let legacy: Vec<(Frame, Option<u64>)> = runtime_props
        .legacy_call_stack
        .as_deref()
        .unwrap_or_default()
        .lines()
        .filter_map(parse_legacy_frame)
        .take(MAX_NUM_UNREAL_FRAMES)
        .collect();

    let portable = runtime_props
        .portable_call_stack
        .as_deref()
        .filter(|_| has_full_image_bases(runtime_props.engine_version.as_deref()))
        .and_then(parse_portable_call_stack)
        .unwrap_or_default();

    // Unknown functions are written with the offset of their return address minus one.
    let stacks_match = legacy.len() <= portable.len()
        && legacy
            .iter()
            .zip(&portable)
            .all(|((frame, offset), portable)| {
                let same_module = match (frame.package.as_str(), portable.module) {
                    (Some(legacy), Some(portable)) => is_same_module(legacy, portable),
                    _ => false,
                };
                same_module
                    && offset.is_none_or(|offset| offset.checked_add(1) == Some(portable.offset))
            });

    let mut frames: Vec<Frame> = legacy.into_iter().map(|(frame, _)| frame).collect();

    if stacks_match {
        for (frame, portable) in frames.iter_mut().zip(&portable) {
            frame.instruction_addr = Annotated::new(Addr(portable.address));
        }

        // The legacy call stack can be shorter, for example it may omit system libraries.
        let missing = portable.get(frames.len()..).unwrap_or_default();
        frames.extend(missing.iter().map(|portable| Frame {
            package: Annotated::from(portable.module.map(str::to_owned)),
            instruction_addr: Annotated::new(Addr(portable.address)),
            ..Frame::default()
        }));
    }

    if frames.is_empty() {
        return None;
    }

    // Unreal lists the crashing frame first, Sentry expects it last.
    frames.reverse();

    let crash_type = runtime_props
        .crash_type
        .as_deref()
        .filter(|crash_type| !crash_type.is_empty());
    let is_ensure = runtime_props.is_ensure.unwrap_or(false) || crash_type == Some("Ensure");
    let is_stall = runtime_props
        .custom
        .get("IsStall")
        .is_some_and(|v| v == "true")
        || crash_type == Some("Stall");

    let ty = match crash_type {
        Some(crash_type) => crash_type,
        None if is_ensure => "Ensure",
        None if is_stall => "Stall",
        None if runtime_props.is_assert.unwrap_or(false) => "Assert",
        None => "Crash",
    };

    // These reports are sent while the application keeps running.
    let handled = is_ensure || is_stall || crash_type == Some("VerseRuntimeError");

    Some(Exception {
        ty: Annotated::new(ty.to_owned()),
        value: Annotated::from(runtime_props.error_message.clone().map(JsonLenientString)),
        stacktrace: Annotated::new(Stacktrace(RawStacktrace {
            frames: Annotated::new(frames.into_iter().map(Annotated::new).collect()),
            ..RawStacktrace::default()
        })),
        mechanism: Annotated::new(Mechanism {
            ty: Annotated::new("unreal".to_owned()),
            handled: Annotated::new(handled),
            synthetic: Annotated::new(true),
            ..Mechanism::default()
        }),
        ..Exception::default()
    })
}

/// Processes an unreal crash report.
///
/// The `user_header` should be extracted from the [`crate::constants::UNREAL_USER_HEADER`] envelope header.
///
/// With `context_stacktrace`, crashes without a minidump or Apple crash report get an exception
/// with the stack trace from the crash context.
pub fn process_unreal(
    context: Option<Unreal4Context>,
    event_id: EventId,
    event: &mut Annotated<Event>,
    attachments: &[Item],
    user_header: Option<&str>,
    context_stacktrace: bool,
) -> Result<Option<ProcessedUnrealReport>, Unreal4Error> {
    let mut logs_items = attachments
        .iter()
        .filter(|item| item.attachment_type() == Some(AttachmentType::UnrealLogs))
        .map(|item| item.payload())
        .peekable();

    // Early exit if there is no information.
    if context.is_none() && user_header.is_none() && logs_items.peek().is_none() {
        return Ok(None);
    }

    // If we have UE4 info, ensure an event is there to fill. DO NOT fill if there is no unreal
    // information, or otherwise `EnvelopeProcessorService::process` breaks.
    let event = event.get_or_insert_with(Event::default);

    if let Some(user_info) = user_header {
        merge_unreal_user_info(event, user_info);
    }

    merge_unreal_logs(event, logs_items)?;

    let mut user_reports = Items::new();
    if let Some(mut context) = context {
        if let Some(report) = get_unreal_user_report(event_id, &mut context) {
            user_reports.push(report);
        }

        // Minidumps and Apple crash reports are processed later and provide a better stack trace.
        let has_native_crash = attachments.iter().any(|item| {
            matches!(
                item.attachment_type(),
                Some(AttachmentType::Minidump | AttachmentType::AppleCrashReport)
            )
        });

        let exception = if context_stacktrace && !has_native_crash && event.exceptions.is_empty() {
            context
                .runtime_properties
                .as_ref()
                .and_then(unreal_exception)
        } else {
            None
        };

        if let Some(exception) = exception {
            event.exceptions = Annotated::new(Values::new(vec![Annotated::new(exception)]));
            // The frames are native, like those of a minidump.
            *event.platform.value_mut() = Some("native".to_owned());
        }

        merge_unreal_context(event, context);
    }

    // This matches the native processing, minidumps and other crash reports which do not have a
    // dedicated level set are assumed to be fatal.
    //
    // We update this at the very end where we can be sure all event data has already been merged into
    // the event (e.g. the unreal context which may set the level).
    event.level.get_or_insert_with(|| Level::Fatal);

    Ok(Some(ProcessedUnrealReport { user_reports }))
}

/// Result when processing an unreal report.
pub struct ProcessedUnrealReport {
    /// User reports contained in the report.
    pub user_reports: Items,
}

#[cfg(test)]
mod tests {

    use relay_config::Config;
    use relay_event_schema::protocol::NativeImagePath;
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
    fn test_extract_items_limits_attachment_count() {
        let bytes = include_bytes!("../../../tests/integration/fixtures/native/unreal_crash");
        let payload = Bytes::from_static(bytes);

        // Everything parses with default config:
        let config = Config::default().current();
        let items = extract_items(payload.clone(), &config).unwrap();
        assert_eq!(items.len(), 4);

        // Extraction honors the attachment limit:
        let config = Config::from_json_value(serde_json::json!({
            "limits": {
                "max_attachment_count": 3
            }
        }))
        .unwrap()
        .current();
        let items = extract_items(payload, &config).unwrap();
        assert_eq!(items.len(), 3);
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

    /// Crash context of an ensure on Linux, which does not write a minidump.
    ///
    /// Taken from the sample in https://github.com/getsentry/relay/issues/5192.
    fn get_linux_context() -> Unreal4Context {
        let raw_context = br#"<?xml version="1.0" encoding="UTF-8"?>
<FGenericCrashContext>
	<RuntimeProperties>
		<IsEnsure>true</IsEnsure>
		<IsStall>false</IsStall>
		<CrashType>Ensure</CrashType>
		<ErrorMessage>Ensure condition failed: false [File:../../Game/Source/Game/GameInstance.cpp] [Line: 112] Stack: 0x000000000b5a7402 Game-Linux-Test!UnknownFunction(0xb3a7401) 0x000000000898ce09 Game-Linux-Test!UnknownFunction(0x878ce08) 0x00007f0b3822a28b libc.so.6!__libc_start_main(+0x8a) 0x0000000004a71029 Game-Linux-Test!UnknownFunction(0x4871028)</ErrorMessage>
		<EngineVersion>5.4.4-35769+++Proto+Release</EngineVersion>
		<CallStack>Game-Linux-Test!UnknownFunction(0xb3a7401)
Game-Linux-Test!UnknownFunction(0x878ce08)
libc.so.6!__libc_start_main(+0x8a)
Game-Linux-Test!UnknownFunction(0x4871028)
</CallStack>
		<PCallStack>Game-Linux-Test 0x0000000000200000 + b3a7402 Game-Linux-Test 0x0000000000200000 + 878ce09 libc.so 0x00007f0b38200000 + 2a28b Game-Linux-Test 0x0000000000200000 + 4871029</PCallStack>
	</RuntimeProperties>
</FGenericCrashContext>
"#;

        Unreal4Context::parse(raw_context).unwrap()
    }

    fn get_exception(runtime_properties: &str) -> Option<Exception> {
        let raw_context = format!(
            "<FGenericCrashContext><RuntimeProperties>{runtime_properties}</RuntimeProperties></FGenericCrashContext>"
        );
        let context = Unreal4Context::parse(raw_context.as_bytes()).unwrap();
        unreal_exception(context.runtime_properties.as_ref()?)
    }

    fn process_with_attachments(attachments: &[Item]) -> Annotated<Event> {
        let mut event = Annotated::empty();
        process_unreal(
            Some(get_linux_context()),
            EventId::new(),
            &mut event,
            attachments,
            None,
            true,
        )
        .unwrap();
        event
    }

    fn process_runtime_properties(runtime_properties: &str) -> Event {
        let raw_context = format!(
            "<FGenericCrashContext><RuntimeProperties>{runtime_properties}</RuntimeProperties></FGenericCrashContext>"
        );
        let context = Unreal4Context::parse(raw_context.as_bytes()).unwrap();

        let mut event = Annotated::empty();
        process_unreal(Some(context), EventId::new(), &mut event, &[], None, true).unwrap();
        event.into_value().unwrap()
    }

    #[test]
    fn test_parse_legacy_frame() {
        let cases = [
            (
                "MyProject!AActor::BeginPlay()",
                (
                    Some("MyProject"),
                    Some("AActor::BeginPlay()"),
                    None,
                    None,
                    None,
                ),
            ),
            (
                r"MyProject!__scrt_common_main_seh() [f:\dd\vctools\crt\vcstartup\src\startup\exe_common.inl:288]",
                (
                    Some("MyProject"),
                    Some("__scrt_common_main_seh()"),
                    Some(r"f:\dd\vctools\crt\vcstartup\src\startup\exe_common.inl"),
                    Some(288),
                    None,
                ),
            ),
            (
                "Game!Foo::operator[]() [Foo.cpp:3]",
                (
                    Some("Game"),
                    Some("Foo::operator[]()"),
                    Some("Foo.cpp"),
                    Some(3),
                    None,
                ),
            ),
            (
                "Game!Foo(int) [clone .cold]",
                (
                    Some("Game"),
                    Some("Foo(int) [clone .cold]"),
                    None,
                    None,
                    None,
                ),
            ),
            (
                "Game!Foo() []",
                (Some("Game"), Some("Foo()"), None, None, None),
            ),
            (
                "0x02a3b4c5 Game!Foo()  [UnknownFile]",
                (Some("Game"), Some("Foo()"), None, None, None),
            ),
            ("kernel32", (Some("kernel32"), None, None, None, None)),
            (
                "Game!UnknownFunction(0xb3a7401)",
                (Some("Game"), None, None, None, Some(0xb3a7401)),
            ),
            (
                "Game!UnknownFunction(0x10) [Foo.cpp:3]",
                (Some("Game"), None, Some("Foo.cpp"), Some(3), Some(0x10)),
            ),
            (
                "libc.so.6!__libc_start_main(+0x8a)",
                (Some("libc.so.6"), None, None, None, None),
            ),
            (
                "Game!UnknownFunction(0x48",
                (Some("Game"), None, None, None, None),
            ),
        ];

        for (line, expected) in cases {
            let (frame, offset) = parse_legacy_frame(line).unwrap();
            let actual = (
                frame.package.as_str(),
                frame.function.as_str(),
                frame.filename.value().map(NativeImagePath::as_str),
                frame.lineno.value().copied(),
                offset,
            );
            assert_eq!(actual, expected, "{line}");
        }

        assert!(parse_legacy_frame("  ").is_none());
    }

    #[test]
    fn test_parse_portable_call_stack() {
        let addresses = |call_stack| {
            parse_portable_call_stack(call_stack).map(|frames| {
                frames
                    .iter()
                    .map(|f| (f.module, f.address))
                    .collect::<Vec<_>>()
            })
        };

        assert_eq!(
            addresses("Game 0x0000000000200000 + b3a7402 libc.so 0x00007f0b38200000 + 2a28b"),
            Some(vec![
                (Some("Game"), 0xb5a7402),
                (Some("libc.so"), 0x7f0b3822a28b)
            ])
        );
        assert_eq!(
            addresses("0x0000000000200000 + 10"),
            Some(vec![(None, 0x200010)])
        );

        // Placeholder addresses written by older engine versions.
        assert_eq!(addresses("0x00000000ffffffff + ffffffff"), None);
        assert_eq!(addresses("Game 0x200000 +"), None);
        assert_eq!(addresses("Game 200000 + 10"), None);
        assert_eq!(addresses("Game 0xffffffffffffffff + 1"), None);
    }

    #[test]
    fn test_is_same_module() {
        assert!(is_same_module("Game", "Game"));
        assert!(is_same_module("kernel32", "KERNEL32"));
        assert!(is_same_module("UnrealEditor_Core", "UnrealEditor-Core"));
        assert!(is_same_module("libc.so.6", "libc.so"));
        assert!(is_same_module("kernel32", "KERNEL32.DLL"));

        assert!(!is_same_module("UnrealEditor", "UnrealEditor-Core"));
        assert!(!is_same_module("Game", "GameServer"));
        assert!(!is_same_module("libc.so.6", "libm.so"));
    }

    #[test]
    fn test_has_full_image_bases() {
        assert!(has_full_image_bases(Some("5.4.4-35769+++Proto+Release")));
        assert!(has_full_image_bases(Some(
            "4.26.2-15973114+++UE4+Release-4.26"
        )));
        assert!(!has_full_image_bases(Some(
            "4.24.3-11590370+++UE4+Release-4.24"
        )));
        assert!(has_full_image_bases(Some("unknown")));
        assert!(has_full_image_bases(None));
    }

    #[test]
    fn test_process_unreal_stacktrace_without_minidump() {
        let event = process_with_attachments(&[]);
        let exceptions = &event.value().unwrap().exceptions;

        insta::assert_snapshot!(exceptions.to_json_pretty().unwrap(), @r#"
        {
          "values": [
            {
              "type": "Ensure",
              "value": "Ensure condition failed: false [File:../../Game/Source/Game/GameInstance.cpp] [Line: 112] Stack: 0x000000000b5a7402 Game-Linux-Test!UnknownFunction(0xb3a7401) 0x000000000898ce09 Game-Linux-Test!UnknownFunction(0x878ce08) 0x00007f0b3822a28b libc.so.6!__libc_start_main(+0x8a) 0x0000000004a71029 Game-Linux-Test!UnknownFunction(0x4871028)",
              "stacktrace": {
                "frames": [
                  {
                    "package": "Game-Linux-Test",
                    "instruction_addr": "0x4a71029"
                  },
                  {
                    "package": "libc.so.6",
                    "instruction_addr": "0x7f0b3822a28b"
                  },
                  {
                    "package": "Game-Linux-Test",
                    "instruction_addr": "0x898ce09"
                  },
                  {
                    "package": "Game-Linux-Test",
                    "instruction_addr": "0xb5a7402"
                  }
                ]
              },
              "mechanism": {
                "type": "unreal",
                "synthetic": true,
                "handled": true
              }
            }
          ]
        }
        "#);
    }

    #[test]
    fn test_process_unreal_stacktrace_native_event() {
        let event = process_with_attachments(&[]);
        let event = event.value().unwrap();

        assert_eq!(event.platform.as_str(), Some("native"));
        // The crash context of the sample is an ensure.
        assert_eq!(event.level.value(), Some(&Level::Warning));
    }

    #[test]
    fn test_process_unreal_stacktrace_disabled() {
        let mut event = Annotated::empty();
        process_unreal(
            Some(get_linux_context()),
            EventId::new(),
            &mut event,
            &[],
            None,
            false,
        )
        .unwrap();
        let event = event.value().unwrap();

        assert!(event.exceptions.is_empty());
        assert!(event.platform.is_empty());
        assert_eq!(event.level.value(), Some(&Level::Warning));
    }

    #[test]
    fn test_process_unreal_stacktrace_level() {
        let cases = [
            ("<IsAssert>true</IsAssert>", Level::Error),
            ("<IsEnsure>true</IsEnsure>", Level::Warning),
            ("<CrashType>Crash</CrashType>", Level::Fatal),
            ("<CrashType>Stall</CrashType>", Level::Fatal),
            ("", Level::Fatal),
        ];

        for (properties, level) in cases {
            let event = process_runtime_properties(&format!(
                "{properties}<CallStack>Game!Main()</CallStack>"
            ));
            assert_eq!(event.level.value(), Some(&level), "{properties}");
        }

        // A level sent by the client is kept.
        let mut event = Annotated::new(Event {
            level: Annotated::new(Level::Info),
            ..Event::default()
        });
        process_unreal(
            Some(get_linux_context()),
            EventId::new(),
            &mut event,
            &[],
            None,
            true,
        )
        .unwrap();
        let event = event.into_value().unwrap();
        assert!(!event.exceptions.is_empty());
        assert_eq!(event.level.value(), Some(&Level::Info));

        // Without a call stack, there is no exception.
        let event = process_runtime_properties("<CrashType>Crash</CrashType>");
        assert!(event.exceptions.is_empty());
    }

    #[test]
    fn test_process_unreal_stacktrace_with_native_crash() {
        for attachment_type in [AttachmentType::Minidump, AttachmentType::AppleCrashReport] {
            let mut item = Item::new(ItemType::Attachment);
            item.set_attachment_type(attachment_type);
            item.set_payload(ContentType::OctetStream, "");

            let event = process_with_attachments(&[item]);
            assert!(event.value().unwrap().exceptions.is_empty());
        }
    }

    #[test]
    fn test_process_unreal_stacktrace_keeps_exceptions() {
        let mut event = Annotated::new(Event {
            exceptions: Annotated::new(Values::new(vec![Annotated::new(Exception {
                ty: Annotated::new("ClientError".to_owned()),
                ..Exception::default()
            })])),
            ..Event::default()
        });

        process_unreal(
            Some(get_linux_context()),
            EventId::new(),
            &mut event,
            &[],
            None,
            true,
        )
        .unwrap();

        let exceptions = event.value().unwrap().exceptions.value().unwrap();
        let exceptions = exceptions.values.value().unwrap();
        assert_eq!(exceptions.len(), 1);
        assert_eq!(
            exceptions[0].value().unwrap().ty.as_str(),
            Some("ClientError")
        );
    }

    #[test]
    fn test_unreal_exception_addresses() {
        let cases = [
            // Only the portable call stack.
            (
                "",
                "Game 0x200000 + 10 libc.so 0x7f0000000000 + 20",
                "",
                vec![
                    (Some("libc.so"), Some(0x7f0000000020)),
                    (Some("Game"), Some(0x200010)),
                ],
            ),
            // The legacy call stack omits the last module.
            (
                "Game!Inner()",
                "Game 0x200000 + 10 kernel32 0x7f0000000000 + 20",
                "",
                vec![
                    (Some("kernel32"), Some(0x7f0000000020)),
                    (Some("Game"), Some(0x200010)),
                ],
            ),
            // Different modules.
            (
                "Game!Inner()\nGame!Outer()",
                "Game 0x200000 + 10 Other 0x300000 + 20",
                "",
                vec![(Some("Game"), None), (Some("Game"), None)],
            ),
            // The legacy call stack has more frames.
            (
                "Game!Inner()\nGame!Outer()",
                "Game 0x200000 + 10",
                "",
                vec![(Some("Game"), None), (Some("Game"), None)],
            ),
            // Offsets of unknown functions do not match.
            (
                "Game!UnknownFunction(0x100)\nGame!UnknownFunction(0x200)",
                "Game 0x1000 + 201 Game 0x1000 + 301",
                "",
                vec![(Some("Game"), None), (Some("Game"), None)],
            ),
            // Image bases are truncated by old engine versions.
            (
                "Game!Inner()",
                "Game 0x200000 + 10",
                "<EngineVersion>4.24.3-11590370+++UE4+Release-4.24</EngineVersion>",
                vec![(Some("Game"), None)],
            ),
        ];

        for (legacy, portable, properties, expected) in cases {
            let exception = get_exception(&format!(
                "<CallStack>{legacy}</CallStack><PCallStack>{portable}</PCallStack>{properties}"
            ))
            .unwrap();

            let frames = exception
                .stacktrace
                .value()
                .unwrap()
                .frames
                .value()
                .unwrap();
            let actual: Vec<_> = frames
                .iter()
                .map(|frame| {
                    let frame = frame.value().unwrap();
                    (
                        frame.package.as_str(),
                        frame.instruction_addr.value().map(|a| a.0),
                    )
                })
                .collect();

            assert_eq!(actual, expected, "{legacy:?} {portable:?}");
        }
    }

    #[test]
    fn test_unreal_exception_frame_limit() {
        let exception = get_exception(&format!(
            "<CallStack>{}</CallStack>",
            "Game!Main()\n".repeat(1000)
        ))
        .unwrap();
        let frames = exception
            .stacktrace
            .value()
            .unwrap()
            .frames
            .value()
            .unwrap();
        assert_eq!(frames.len(), MAX_NUM_UNREAL_FRAMES);
    }

    #[test]
    fn test_unreal_exception_crash_types() {
        let cases = [
            ("<CrashType>Stall</CrashType>", "Stall", true),
            ("<IsStall>true</IsStall>", "Stall", true),
            ("<CrashType>Ensure</CrashType>", "Ensure", true),
            ("<IsEnsure>true</IsEnsure>", "Ensure", true),
            (
                "<CrashType>VerseRuntimeError</CrashType>",
                "VerseRuntimeError",
                true,
            ),
            ("<IsAssert>true</IsAssert>", "Assert", false),
            ("<CrashType />", "Crash", false),
            ("<CrashType>GPUCrash</CrashType>", "GPUCrash", false),
        ];

        for (properties, ty, handled) in cases {
            let exception =
                get_exception(&format!("{properties}<CallStack>Game!Main()</CallStack>")).unwrap();

            assert_eq!(exception.ty.as_str(), Some(ty), "{properties}");
            let mechanism = exception.mechanism.value().unwrap();
            assert_eq!(mechanism.handled.value(), Some(&handled), "{properties}");
        }
    }

    #[test]
    fn test_unreal_exception_without_call_stack() {
        let runtime_properties = get_context().runtime_properties.unwrap();
        assert!(unreal_exception(&runtime_properties).is_none());
    }
}
