//! Computation and normalization of contexts from event data.

use std::collections::HashMap;

use once_cell::sync::Lazy;
use regex::Regex;
use relay_event_schema::protocol::{
    BrowserContext, Context, Cookies, OsContext, ResponseContext, RuntimeContext,
};
use relay_protocol::{Annotated, Empty, Value};

/// Environment.OSVersion (GetVersionEx) or RuntimeInformation.OSDescription on Windows
static OS_WINDOWS_REGEX1: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(Microsoft\s+)?Windows\s+(NT\s+)?(?P<version>\d+\.\d+\.(?P<build_number>\d+)).*$")
        .unwrap()
});
static OS_WINDOWS_REGEX2: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^Windows\s+\d+\s+\((?P<version>\d+\.\d+\.(?P<build_number>\d+)).*$").unwrap()
});

static OS_ANDROID_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^Android (OS )?(?P<version>\d+(\.\d+){0,2}) / API-(?P<api>(\d+))").unwrap()
});

/// Format sent by Unreal Engine on macOS
static OS_MACOS_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^Mac OS X (?P<version>\d+\.\d+\.\d+)( \((?P<build>[a-fA-F0-9]+)\))?$").unwrap()
});

/// Format sent by Unity on iOS
static OS_IOS_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^iOS (?P<version>\d+\.\d+\.\d+)").unwrap());

/// Format sent by Unity on iPadOS
static OS_IPADOS_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^iPadOS (?P<version>\d+\.\d+\.\d+)").unwrap());

/// Specific regex to parse Linux distros
static OS_LINUX_DISTRO_UNAME_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^Linux (?P<kernel_version>\d+\.\d+(\.\d+(\.[1-9]+)?)?) (?P<name>[a-zA-Z]+) (?P<version>\d+(\.\d+){0,2})").unwrap()
});

/// Environment.OSVersion or RuntimeInformation.OSDescription (uname) on Mono and CoreCLR on
/// macOS, iOS, Linux, etc.
static OS_UNAME_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(?P<name>[a-zA-Z]+) (?P<kernel_version>\d+\.\d+(\.\d+(\.[1-9]+)?)?)").unwrap()
});

/// Mono 5.4, .NET Core 2.0
static RUNTIME_DOTNET_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(?P<name>.*) (?P<version>\d+\.\d+(\.\d+){0,2}).*$").unwrap());

/// A hashmap that translates from the android model to the more human-friendly product-names.
/// E.g. NE2211 -> OnePlus 10 Pro 5G
static ANDROID_MODEL_NAMES: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let mut map = HashMap::new();
    // Note that windows paths with backslashes '\' won't work on unix systems.
    let android_str = include_str!("android_models.csv");

    let mut lines = android_str.lines();

    let header = lines.next().expect("CSV file should have a header");

    let header_fields: Vec<&str> = header.split(',').collect();
    let model_index = header_fields.iter().position(|&s| s.trim() == "Model");
    let product_name_index = header_fields
        .iter()
        .position(|&s| s.trim() == "Marketing Name");

    let (model_index, product_name_index) = match (model_index, product_name_index) {
        (Some(model_index), Some(product_name_index)) => (model_index, product_name_index),
        (_, _) => {
            relay_log::error!(
                "failed to find model and/or marketing name headers for android-model map",
            );

            return HashMap::new();
        }
    };

    for line in lines {
        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() > std::cmp::max(model_index, product_name_index) {
            map.insert(
                fields[model_index].trim(),
                fields[product_name_index].trim(),
            );
        }
    }
    map
});

fn normalize_runtime_context(runtime: &mut RuntimeContext) {
    if runtime.name.value().is_empty() && runtime.version.value().is_empty() {
        if let Some(raw_description) = runtime.raw_description.as_str() {
            if let Some(captures) = RUNTIME_DOTNET_REGEX.captures(raw_description) {
                runtime.name = captures.name("name").map(|m| m.as_str().to_owned()).into();
                runtime.version = captures
                    .name("version")
                    .map(|m| m.as_str().to_owned())
                    .into();
            }
        }
    }

    // RuntimeInformation.FrameworkDescription doesn't return a very useful value.
    // Example: ".NET Framework 4.7.3056.0"
    // Use release keys from registry sent as #build
    if let Some(name) = runtime.name.as_str() {
        if let Some(build) = runtime.build.as_str() {
            if name.starts_with(".NET Framework") {
                let version = match build {
                    "378389" => Some("4.5".to_owned()),
                    "378675" => Some("4.5.1".to_owned()),
                    "378758" => Some("4.5.1".to_owned()),
                    "379893" => Some("4.5.2".to_owned()),
                    "393295" => Some("4.6".to_owned()),
                    "393297" => Some("4.6".to_owned()),
                    "394254" => Some("4.6.1".to_owned()),
                    "394271" => Some("4.6.1".to_owned()),
                    "394802" => Some("4.6.2".to_owned()),
                    "394806" => Some("4.6.2".to_owned()),
                    "460798" => Some("4.7".to_owned()),
                    "460805" => Some("4.7".to_owned()),
                    "461308" => Some("4.7.1".to_owned()),
                    "461310" => Some("4.7.1".to_owned()),
                    "461808" => Some("4.7.2".to_owned()),
                    "461814" => Some("4.7.2".to_owned()),
                    "528040" => Some("4.8".to_owned()),
                    "528049" => Some("4.8".to_owned()),
                    "528209" => Some("4.8".to_owned()),
                    "528372" => Some("4.8".to_owned()),
                    "528449" => Some("4.8".to_owned()),
                    _ => None,
                };

                if let Some(version) = version {
                    runtime.version = version.into();
                }
            }
        }
    }

    // Calculation of the computed context for the runtime.
    // The equivalent calculation is done in `sentry` in `src/sentry/interfaces/contexts.py`.
    if runtime.runtime.value().is_none() {
        if let (Some(name), Some(version)) = (runtime.name.value(), runtime.version.value()) {
            runtime.runtime = Annotated::from(format!("{name} {version}"));
        }
    }
}

/// Parses the Windows build number from the description and maps it to a marketing name.
/// Source: <https://en.wikipedia.org/wiki/List_of_Microsoft_Windows_versions>.
/// Note: We cannot distinguish between Windows Server and PC versions, so we map to the PC versions
/// here.
fn get_windows_version(description: &str) -> Option<(&str, &str)> {
    let captures = OS_WINDOWS_REGEX1
        .captures(description)
        .or_else(|| OS_WINDOWS_REGEX2.captures(description))?;

    let full_version = captures.name("version")?.as_str();
    let build_number_str = captures.name("build_number")?.as_str();
    let build_number = build_number_str.parse::<u64>().ok()?;

    let version_name = match build_number {
        // Not considering versions below Windows XP
        2600..=3790 => "XP",
        6002 => "Vista",
        7601 => "7",
        9200 => "8",
        9600 => "8.1",
        10240..=19044 => "10",
        22000..=22999 => "11",
        // Fall back to raw version:
        _ => full_version,
    };

    Some((version_name, build_number_str))
}

/// Simple marketing names in the form `<OS> <version>`.
fn get_marketing_name(description: &str) -> Option<(&str, &str)> {
    let (name, version) = description.split_once(' ')?;
    let name = name.trim();
    let version = version.trim();

    // Validate if it looks like a reasonable name.
    if name.bytes().any(|c| !c.is_ascii_alphabetic()) {
        return None;
    }

    // Validate if it looks like a reasonable version.
    if version
        .bytes()
        .any(|c| !matches!(c, b'0'..=b'9' | b'.' | b'-'))
    {
        return None;
    }

    Some((name, version))
}

#[allow(dead_code)]
/// Returns the API version of an Android description.
///
/// TODO use this to add a tag `android.api` to the message (not yet 100% decided)
pub fn get_android_api_version(description: &str) -> Option<&str> {
    if let Some(captures) = OS_ANDROID_REGEX.captures(description) {
        captures.name("api").map(|m| m.as_str())
    } else {
        None
    }
}

fn normalize_os_context(os: &mut OsContext) {
    if os.name.value().is_some() || os.version.value().is_some() {
        compute_os_context(os);
        return;
    }

    if let Some(raw_description) = os.raw_description.as_str() {
        if let Some((version, build_number)) = get_windows_version(raw_description) {
            os.name = "Windows".to_owned().into();
            os.version = version.to_owned().into();
            if os.build.is_empty() {
                // Keep raw version as build
                os.build.set_value(Some(build_number.to_owned().into()));
            }
        } else if let Some(captures) = OS_MACOS_REGEX.captures(raw_description) {
            os.name = "macOS".to_owned().into();
            os.version = captures
                .name("version")
                .map(|m| m.as_str().to_owned())
                .into();
            os.build = captures
                .name("build")
                .map(|m| m.as_str().to_owned().into())
                .into();
        } else if let Some(captures) = OS_IOS_REGEX.captures(raw_description) {
            os.name = "iOS".to_owned().into();
            os.version = captures
                .name("version")
                .map(|m| m.as_str().to_owned())
                .into();
        } else if let Some(captures) = OS_IPADOS_REGEX.captures(raw_description) {
            os.name = "iPadOS".to_owned().into();
            os.version = captures
                .name("version")
                .map(|m| m.as_str().to_owned())
                .into();
        } else if let Some(captures) = OS_LINUX_DISTRO_UNAME_REGEX.captures(raw_description) {
            os.name = captures.name("name").map(|m| m.as_str().to_owned()).into();
            os.version = captures
                .name("version")
                .map(|m| m.as_str().to_owned())
                .into();
            os.kernel_version = captures
                .name("kernel_version")
                .map(|m| m.as_str().to_owned())
                .into();
        } else if let Some(captures) = OS_UNAME_REGEX.captures(raw_description) {
            os.name = captures.name("name").map(|m| m.as_str().to_owned()).into();
            os.kernel_version = captures
                .name("kernel_version")
                .map(|m| m.as_str().to_owned())
                .into();
        } else if let Some(captures) = OS_ANDROID_REGEX.captures(raw_description) {
            os.name = "Android".to_owned().into();
            os.version = captures
                .name("version")
                .map(|m| m.as_str().to_owned())
                .into();
        } else if raw_description == "Nintendo Switch" {
            os.name = "Nintendo OS".to_owned().into();
        } else if let Some((name, version)) = get_marketing_name(raw_description) {
            os.name = name.to_owned().into();
            os.version = version.to_owned().into();
        }
    }

    compute_os_context(os);
}

fn compute_os_context(os: &mut OsContext) {
    // Calculation of the computed context for the os.
    // The equivalent calculation is done in `sentry` in `src/sentry/interfaces/contexts.py`.
    if os.os.value().is_none() {
        let name = match (os.name.value(), os.version.value()) {
            (Some(name), Some(version)) => Some(format!("{name} {version}")),
            (Some(name), _) => Some(name.to_owned()),
            _ => None,
        };

        if let Some(name) = name {
            os.os = Annotated::new(name);
        }
    }
}

fn normalize_browser_context(browser: &mut BrowserContext) {
    // Calculation of the computed context for the browser.
    // The equivalent calculation is done in `sentry` in `src/sentry/interfaces/contexts.py`.
    if browser.browser.value().is_none() {
        let name = match (browser.name.value(), browser.version.value()) {
            (Some(name), Some(version)) => Some(format!("{name} {version}")),
            (Some(name), _) => Some(name.to_owned()),
            _ => None,
        };

        if let Some(name) = name {
            browser.browser = Annotated::new(name);
        }
    }
}

fn parse_raw_response_data(response: &ResponseContext) -> Option<(&'static str, Value)> {
    let raw = response.data.as_str()?;

    serde_json::from_str(raw)
        .ok()
        .map(|value| ("application/json", value))
}

fn normalize_response_data(response: &mut ResponseContext) {
    // Always derive the `inferred_content_type` from the response body, even if there is a
    // `Content-Type` header present. This value can technically be ingested (due to the schema) but
    // should always be overwritten in normalization. Only if inference fails, fall back to the
    // content type header.
    if let Some((content_type, parsed_data)) = parse_raw_response_data(response) {
        // Retain meta data on the body (e.g. trimming annotations) but remove anything on the
        // inferred content type.
        response.data.set_value(Some(parsed_data));
        response.inferred_content_type = Annotated::from(content_type.to_owned());
    } else {
        response.inferred_content_type = response
            .headers
            .value()
            .and_then(|headers| headers.get_header("Content-Type"))
            .map(|value| value.split(';').next().unwrap_or(value).to_owned())
            .into();
    }
}

fn normalize_response(response: &mut ResponseContext) {
    normalize_response_data(response);

    let headers = match response.headers.value_mut() {
        Some(headers) => headers,
        None => return,
    };

    if response.cookies.value().is_some() {
        headers.remove("Set-Cookie");
        return;
    }

    let cookie_header = match headers.get_header("Set-Cookie") {
        Some(header) => header,
        None => return,
    };

    if let Ok(new_cookies) = Cookies::parse(cookie_header) {
        response.cookies = Annotated::from(new_cookies);
        headers.remove("Set-Cookie");
    }
}

/// Normalizes the given context.
pub fn normalize_context(context: &mut Context) {
    match context {
        Context::Runtime(runtime) => normalize_runtime_context(runtime),
        Context::Os(os) => normalize_os_context(os),
        Context::Browser(browser) => normalize_browser_context(browser),
        Context::Response(response) => normalize_response(response),
        Context::Device(device) => {
            if let Some(product_name) = device
                .as_ref()
                .model
                .value()
                .and_then(|model| ANDROID_MODEL_NAMES.get(model.as_str()))
            {
                device.name.set_value(Some(product_name.to_string()))
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{Headers, LenientString, PairList};
    use relay_protocol::SerializableAnnotated;
    use similar_asserts::assert_eq;

    use super::*;

    macro_rules! assert_json_context {
        ($ctx:expr, $($tt:tt)*) => {
            insta::assert_json_snapshot!(SerializableAnnotated(&Annotated::new($ctx)), $($tt)*)

        };
    }

    #[test]
    fn test_get_product_name() {
        assert_eq!(
            ANDROID_MODEL_NAMES.get("NE2211").unwrap(),
            &"OnePlus 10 Pro 5G"
        );

        assert_eq!(
            ANDROID_MODEL_NAMES.get("MP04").unwrap(),
            &"A13 Pro Max 5G EEA"
        );

        assert_eq!(ANDROID_MODEL_NAMES.get("ZT216_7").unwrap(), &"zyrex");

        assert!(ANDROID_MODEL_NAMES.get("foobar").is_none());
    }

    #[test]
    fn test_dotnet_framework_48_without_build_id() {
        let mut runtime = RuntimeContext {
            raw_description: ".NET Framework 4.8.4250.0".to_owned().into(),
            ..RuntimeContext::default()
        };

        normalize_runtime_context(&mut runtime);
        assert_json_context!(runtime, @r###"
        {
          "runtime": ".NET Framework 4.8.4250.0",
          "name": ".NET Framework",
          "version": "4.8.4250.0",
          "raw_description": ".NET Framework 4.8.4250.0"
        }
        "###);
    }

    #[test]
    fn test_dotnet_framework_472() {
        let mut runtime = RuntimeContext {
            raw_description: ".NET Framework 4.7.3056.0".to_owned().into(),
            build: LenientString("461814".to_owned()).into(),
            ..RuntimeContext::default()
        };

        normalize_runtime_context(&mut runtime);
        assert_json_context!(runtime, @r###"
        {
          "runtime": ".NET Framework 4.7.2",
          "name": ".NET Framework",
          "version": "4.7.2",
          "build": "461814",
          "raw_description": ".NET Framework 4.7.3056.0"
        }
        "###);
    }

    #[test]
    fn test_dotnet_framework_future_version() {
        let mut runtime = RuntimeContext {
            raw_description: ".NET Framework 200.0".to_owned().into(),
            build: LenientString("999999".to_owned()).into(),
            ..RuntimeContext::default()
        };

        // Unmapped build number doesn't override version
        normalize_runtime_context(&mut runtime);
        assert_json_context!(runtime, @r###"
        {
          "runtime": ".NET Framework 200.0",
          "name": ".NET Framework",
          "version": "200.0",
          "build": "999999",
          "raw_description": ".NET Framework 200.0"
        }
        "###);
    }

    #[test]
    fn test_dotnet_native() {
        let mut runtime = RuntimeContext {
            raw_description: ".NET Native 2.0".to_owned().into(),
            ..RuntimeContext::default()
        };

        normalize_runtime_context(&mut runtime);
        assert_json_context!(runtime, @r###"
        {
          "runtime": ".NET Native 2.0",
          "name": ".NET Native",
          "version": "2.0",
          "raw_description": ".NET Native 2.0"
        }
        "###);
    }

    #[test]
    fn test_dotnet_core() {
        let mut runtime = RuntimeContext {
            raw_description: ".NET Core 2.0".to_owned().into(),
            ..RuntimeContext::default()
        };

        normalize_runtime_context(&mut runtime);
        assert_json_context!(runtime, @r###"
        {
          "runtime": ".NET Core 2.0",
          "name": ".NET Core",
          "version": "2.0",
          "raw_description": ".NET Core 2.0"
        }
        "###);
    }

    #[test]
    fn test_windows_7_or_server_2008() {
        // Environment.OSVersion on Windows 7 (CoreCLR 1.0+, .NET Framework 1.1+, Mono 1+)
        let mut os = OsContext {
            raw_description: "Microsoft Windows NT 6.1.7601 Service Pack 1"
                .to_owned()
                .into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Windows 7",
          "name": "Windows",
          "version": "7",
          "build": "7601",
          "raw_description": "Microsoft Windows NT 6.1.7601 Service Pack 1"
        }
        "###);
    }

    #[test]
    fn test_windows_8_or_server_2012_or_later() {
        // Environment.OSVersion on Windows 10 (CoreCLR 1.0+, .NET Framework 1.1+, Mono 1+)
        // *or later, due to GetVersionEx deprecated on Windows 8.1
        // It's a potentially really misleading API on newer platforms
        // Only used if RuntimeInformation.OSDescription is not available (old runtimes)
        let mut os = OsContext {
            raw_description: "Microsoft Windows NT 6.2.9200.0".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Windows 8",
          "name": "Windows",
          "version": "8",
          "build": "9200",
          "raw_description": "Microsoft Windows NT 6.2.9200.0"
        }
        "###);
    }

    #[test]
    fn test_windows_10() {
        // RuntimeInformation.OSDescription on Windows 10 (CoreCLR 2.0+, .NET
        // Framework 4.7.1+, Mono 5.4+)
        let mut os = OsContext {
            raw_description: "Microsoft Windows 10.0.16299".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Windows 10",
          "name": "Windows",
          "version": "10",
          "build": "16299",
          "raw_description": "Microsoft Windows 10.0.16299"
        }
        "###);
    }

    #[test]
    fn test_windows_11() {
        // https://github.com/getsentry/relay/issues/1201
        let mut os = OsContext {
            raw_description: "Microsoft Windows 10.0.22000".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Windows 11",
          "name": "Windows",
          "version": "11",
          "build": "22000",
          "raw_description": "Microsoft Windows 10.0.22000"
        }
        "###);
    }

    #[test]
    fn test_windows_11_future1() {
        // This is fictional as of today, but let's be explicit about the behavior we expect.
        let mut os = OsContext {
            raw_description: "Microsoft Windows 10.0.22001".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Windows 11",
          "name": "Windows",
          "version": "11",
          "build": "22001",
          "raw_description": "Microsoft Windows 10.0.22001"
        }
        "###);
    }

    #[test]
    fn test_windows_11_future2() {
        // This is fictional, but let's be explicit about the behavior we expect.
        let mut os = OsContext {
            raw_description: "Microsoft Windows 10.1.23456".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Windows 10.1.23456",
          "name": "Windows",
          "version": "10.1.23456",
          "build": "23456",
          "raw_description": "Microsoft Windows 10.1.23456"
        }
        "###);
    }

    #[test]
    fn test_macos_os_version() {
        // Environment.OSVersion on macOS (CoreCLR 1.0+, Mono 1+)
        let mut os = OsContext {
            raw_description: "Unix 17.5.0.0".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Unix",
          "name": "Unix",
          "kernel_version": "17.5.0",
          "raw_description": "Unix 17.5.0.0"
        }
        "###);
    }

    #[test]
    fn test_macos_runtime() {
        // RuntimeInformation.OSDescription on macOS (CoreCLR 2.0+, Mono 5.4+)
        let mut os = OsContext {
        raw_description: "Darwin 17.5.0 Darwin Kernel Version 17.5.0: Mon Mar  5 22:24:32 PST 2018; root:xnu-4570.51.1~1/RELEASE_X86_64".to_owned().into(),
        ..OsContext::default()
    };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Darwin",
          "name": "Darwin",
          "kernel_version": "17.5.0",
          "raw_description": "Darwin 17.5.0 Darwin Kernel Version 17.5.0: Mon Mar  5 22:24:32 PST 2018; root:xnu-4570.51.1~1/RELEASE_X86_64"
        }
        "###);
    }

    #[test]
    fn test_centos_os_version() {
        // Environment.OSVersion on CentOS 7 (CoreCLR 1.0+, Mono 1+)
        let mut os = OsContext {
            raw_description: "Unix 3.10.0.693".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Unix",
          "name": "Unix",
          "kernel_version": "3.10.0.693",
          "raw_description": "Unix 3.10.0.693"
        }
        "###);
    }

    #[test]
    fn test_centos_runtime_info() {
        // RuntimeInformation.OSDescription on CentOS 7 (CoreCLR 2.0+, Mono 5.4+)
        let mut os = OsContext {
            raw_description: "Linux 3.10.0-693.21.1.el7.x86_64 #1 SMP Wed Mar 7 19:03:37 UTC 2018"
                .to_owned()
                .into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Linux",
          "name": "Linux",
          "kernel_version": "3.10.0",
          "raw_description": "Linux 3.10.0-693.21.1.el7.x86_64 #1 SMP Wed Mar 7 19:03:37 UTC 2018"
        }
        "###);
    }

    #[test]
    fn test_wsl_ubuntu() {
        // RuntimeInformation.OSDescription on Windows Subsystem for Linux (Ubuntu)
        // (CoreCLR 2.0+, Mono 5.4+)
        let mut os = OsContext {
            raw_description: "Linux 4.4.0-43-Microsoft #1-Microsoft Wed Dec 31 14:42:53 PST 2014"
                .to_owned()
                .into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Linux",
          "name": "Linux",
          "kernel_version": "4.4.0",
          "raw_description": "Linux 4.4.0-43-Microsoft #1-Microsoft Wed Dec 31 14:42:53 PST 2014"
        }
        "###);
    }

    #[test]
    fn test_macos_with_build() {
        let mut os = OsContext {
            raw_description: "Mac OS X 10.14.2 (18C54)".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "macOS 10.14.2",
          "name": "macOS",
          "version": "10.14.2",
          "build": "18C54",
          "raw_description": "Mac OS X 10.14.2 (18C54)"
        }
        "###);
    }

    #[test]
    fn test_macos_without_build() {
        let mut os = OsContext {
            raw_description: "Mac OS X 10.14.2".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "macOS 10.14.2",
          "name": "macOS",
          "version": "10.14.2",
          "raw_description": "Mac OS X 10.14.2"
        }
        "###);
    }

    #[test]
    fn test_name_not_overwritten() {
        let mut os = OsContext {
            name: "Properly defined name".to_owned().into(),
            raw_description: "Linux 4.4.0".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Properly defined name",
          "name": "Properly defined name",
          "raw_description": "Linux 4.4.0"
        }
        "###);
    }

    #[test]
    fn test_version_not_overwritten() {
        let mut os = OsContext {
            version: "Properly defined version".to_owned().into(),
            raw_description: "Linux 4.4.0".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "version": "Properly defined version",
          "raw_description": "Linux 4.4.0"
        }
        "###);
    }

    #[test]
    fn test_no_name() {
        let mut os = OsContext::default();

        normalize_os_context(&mut os);
        assert_json_context!(os, @"{}");
    }

    #[test]
    fn test_unity_mac_os() {
        let mut os = OsContext {
            raw_description: "Mac OS X 10.16.0".to_owned().into(),
            ..OsContext::default()
        };
        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "macOS 10.16.0",
          "name": "macOS",
          "version": "10.16.0",
          "raw_description": "Mac OS X 10.16.0"
        }
        "###);
    }

    #[test]
    fn test_unity_ios() {
        let mut os = OsContext {
            raw_description: "iOS 17.5.1".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "iOS 17.5.1",
          "name": "iOS",
          "version": "17.5.1",
          "raw_description": "iOS 17.5.1"
        }
        "###);
    }

    #[test]
    fn test_unity_ipados() {
        let mut os = OsContext {
            raw_description: "iPadOS 17.5.1".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "iPadOS 17.5.1",
          "name": "iPadOS",
          "version": "17.5.1",
          "raw_description": "iPadOS 17.5.1"
        }
        "###);
    }

    //OS_WINDOWS_REGEX = r#"^(Microsoft )?Windows (NT )?(?P<version>\d+\.\d+\.\d+).*$"#;
    #[test]
    fn test_unity_windows_os() {
        let mut os = OsContext {
            raw_description: "Windows 10  (10.0.19042) 64bit".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Windows 10",
          "name": "Windows",
          "version": "10",
          "build": "19042",
          "raw_description": "Windows 10  (10.0.19042) 64bit"
        }
        "###);
    }

    #[test]
    fn test_unity_android_os() {
        let mut os = OsContext {
            raw_description: "Android OS 11 / API-30 (RP1A.201005.001/2107031736)"
                .to_owned()
                .into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Android 11",
          "name": "Android",
          "version": "11",
          "raw_description": "Android OS 11 / API-30 (RP1A.201005.001/2107031736)"
        }
        "###);
    }

    #[test]
    fn test_unity_android_api_version() {
        let description = "Android OS 11 / API-30 (RP1A.201005.001/2107031736)";
        assert_eq!(Some("30"), get_android_api_version(description));
    }

    #[test]
    fn test_unreal_windows_os() {
        let mut os = OsContext {
            raw_description: "Windows 10".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Windows 10",
          "name": "Windows",
          "version": "10",
          "raw_description": "Windows 10"
        }
        "###);
    }

    #[test]
    fn test_linux_5_11() {
        let mut os = OsContext {
            raw_description: "Linux 5.11 Ubuntu 20.04 64bit".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Ubuntu 20.04",
          "name": "Ubuntu",
          "version": "20.04",
          "kernel_version": "5.11",
          "raw_description": "Linux 5.11 Ubuntu 20.04 64bit"
        }
        "###);
    }

    #[test]
    fn test_unity_nintendo_switch() {
        // Format sent by Unity on Nintendo Switch
        let mut os = OsContext {
            raw_description: "Nintendo Switch".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Nintendo OS",
          "name": "Nintendo OS",
          "raw_description": "Nintendo Switch"
        }
        "###);
    }

    #[test]
    fn test_android_4_4_2() {
        let mut os = OsContext {
            raw_description: "Android OS 4.4.2 / API-19 (KOT49H/A536_S186_150813_ROW)"
                .to_owned()
                .into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Android 4.4.2",
          "name": "Android",
          "version": "4.4.2",
          "raw_description": "Android OS 4.4.2 / API-19 (KOT49H/A536_S186_150813_ROW)"
        }
        "###);
    }

    #[test]
    fn test_infer_json() {
        let mut response = ResponseContext {
            data: Annotated::from(Value::String(r#"{"foo":"bar"}"#.to_owned())),
            ..ResponseContext::default()
        };

        normalize_response(&mut response);
        assert_json_context!(response, @r###"
        {
          "data": {
            "foo": "bar"
          },
          "inferred_content_type": "application/json"
        }
        "###);
    }

    #[test]
    fn test_broken_json_with_fallback() {
        let mut response = ResponseContext {
            data: Annotated::from(Value::String(r#"{"foo":"b"#.to_owned())),
            headers: Annotated::from(Headers(PairList(vec![Annotated::new((
                Annotated::new("Content-Type".to_owned().into()),
                Annotated::new("text/plain; encoding=utf-8".to_owned().into()),
            ))]))),
            ..ResponseContext::default()
        };

        normalize_response(&mut response);
        assert_json_context!(response, @r###"
        {
          "headers": [
            [
              "Content-Type",
              "text/plain; encoding=utf-8"
            ]
          ],
          "data": "{\"foo\":\"b",
          "inferred_content_type": "text/plain"
        }
        "###);
    }

    #[test]
    fn test_broken_json_without_fallback() {
        let mut response = ResponseContext {
            data: Annotated::from(Value::String(r#"{"foo":"b"#.to_owned())),
            ..ResponseContext::default()
        };

        normalize_response(&mut response);
        assert_json_context!(response, @r###"
        {
          "data": "{\"foo\":\"b"
        }
        "###);
    }

    #[test]
    fn test_os_computed_context() {
        let mut os = OsContext {
            name: "Windows".to_owned().into(),
            version: "10".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Windows 10",
          "name": "Windows",
          "version": "10"
        }
        "###);
    }

    #[test]
    fn test_os_computed_context_missing_version() {
        let mut os = OsContext {
            name: "Windows".to_owned().into(),
            ..OsContext::default()
        };

        normalize_os_context(&mut os);
        assert_json_context!(os, @r###"
        {
          "os": "Windows",
          "name": "Windows"
        }
        "###);
    }

    #[test]
    fn test_runtime_computed_context() {
        let mut runtime = RuntimeContext {
            name: "Python".to_owned().into(),
            version: "3.9.0".to_owned().into(),
            ..RuntimeContext::default()
        };

        normalize_runtime_context(&mut runtime);
        assert_json_context!(runtime, @r###"
        {
          "runtime": "Python 3.9.0",
          "name": "Python",
          "version": "3.9.0"
        }
        "###);
    }

    #[test]
    fn test_runtime_computed_context_missing_version() {
        let mut runtime = RuntimeContext {
            name: "Python".to_owned().into(),
            ..RuntimeContext::default()
        };

        normalize_runtime_context(&mut runtime);
        assert_json_context!(runtime, @r###"
        {
          "name": "Python"
        }
        "###);
    }

    #[test]
    fn test_browser_computed_context() {
        let mut browser = BrowserContext {
            name: "Firefox".to_owned().into(),
            version: "89.0".to_owned().into(),
            ..BrowserContext::default()
        };

        normalize_browser_context(&mut browser);
        assert_json_context!(browser, @r###"
        {
          "browser": "Firefox 89.0",
          "name": "Firefox",
          "version": "89.0"
        }
        "###);
    }

    #[test]
    fn test_browser_computed_context_missing_version() {
        let mut browser = BrowserContext {
            name: "Firefox".to_owned().into(),
            ..BrowserContext::default()
        };

        normalize_browser_context(&mut browser);
        assert_json_context!(browser, @r###"
        {
          "browser": "Firefox",
          "name": "Firefox"
        }
        "###);
    }
}
