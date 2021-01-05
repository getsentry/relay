use regex::Regex;

use crate::protocol::{Context, OsContext, RuntimeContext};
use crate::types::Empty;

lazy_static::lazy_static! {
    /// Environment.OSVersion (GetVersionEx) or RuntimeInformation.OSDescription on Windows
    static ref OS_WINDOWS_REGEX: Regex = Regex::new(r#"^(Microsoft )?Windows (NT )?(?P<version>\d+\.\d+\.\d+).*$"#).unwrap();

    /// Format sent by Unreal Engine on macOS
    static ref OS_MACOS_REGEX: Regex = Regex::new(r#"^Mac OS X (?P<version>\d+\.\d+\.\d+)( \((?P<build>[a-fA-F0-9]+)\))?$"#).unwrap();

    /// Environment.OSVersion or RuntimeInformation.OSDescription (uname) on Mono and CoreCLR on
    /// macOS, iOS, Linux, etc.
    static ref OS_UNAME_REGEX: Regex = Regex::new(r#"^(?P<name>[a-zA-Z]+) (?P<version>\d+\.\d+\.\d+(\.[1-9]+)?).*$"#).unwrap();

    /// Mono 5.4, .NET Core 2.0
    static ref RUNTIME_DOTNET_REGEX: Regex = Regex::new(r#"^(?P<name>.*) (?P<version>\d+\.\d+(\.\d+){0,2}).*$"#).unwrap();
}

fn normalize_runtime_context(runtime: &mut RuntimeContext) {
    if runtime.name.value().is_empty() && runtime.version.value().is_empty() {
        if let Some(raw_description) = runtime.raw_description.as_str() {
            if let Some(captures) = RUNTIME_DOTNET_REGEX.captures(raw_description) {
                runtime.name = captures.name("name").map(|m| m.as_str().to_string()).into();
                runtime.version = captures
                    .name("version")
                    .map(|m| m.as_str().to_string())
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
                    "378389" => Some("4.5".to_string()),
                    "378675" => Some("4.5.1".to_string()),
                    "378758" => Some("4.5.1".to_string()),
                    "379893" => Some("4.5.2".to_string()),
                    "393295" => Some("4.6".to_string()),
                    "393297" => Some("4.6".to_string()),
                    "394254" => Some("4.6.1".to_string()),
                    "394271" => Some("4.6.1".to_string()),
                    "394802" => Some("4.6.2".to_string()),
                    "394806" => Some("4.6.2".to_string()),
                    "460798" => Some("4.7".to_string()),
                    "460805" => Some("4.7".to_string()),
                    "461308" => Some("4.7.1".to_string()),
                    "461310" => Some("4.7.1".to_string()),
                    "461808" => Some("4.7.2".to_string()),
                    "461814" => Some("4.7.2".to_string()),
                    "528040" => Some("4.8".to_string()),
                    "528049" => Some("4.8".to_string()),
                    "528209" => Some("4.8".to_string()),
                    "528372" => Some("4.8".to_string()),
                    _ => None,
                };

                if let Some(version) = version {
                    runtime.version = version.into();
                }
            }
        }
    }
}

fn normalize_os_context(os: &mut OsContext) {
    if os.name.value().is_some() || os.version.value().is_some() {
        return;
    }

    if let Some(raw_description) = os.raw_description.as_str() {
        if let Some(captures) = OS_WINDOWS_REGEX.captures(raw_description) {
            os.name = "Windows".to_string().into();
            os.version = captures
                .name("version")
                .map(|m| m.as_str().to_string())
                .into();
        } else if let Some(captures) = OS_MACOS_REGEX.captures(raw_description) {
            os.name = "macOS".to_string().into();
            os.version = captures
                .name("version")
                .map(|m| m.as_str().to_string())
                .into();
            os.build = captures
                .name("build")
                .map(|m| m.as_str().to_string().into())
                .into();
        } else if let Some(captures) = OS_UNAME_REGEX.captures(raw_description) {
            os.name = captures.name("name").map(|m| m.as_str().to_string()).into();
            os.kernel_version = captures
                .name("version")
                .map(|m| m.as_str().to_string())
                .into();
        }
    }
}

pub fn normalize_context(context: &mut Context) {
    match context {
        Context::Runtime(runtime) => normalize_runtime_context(runtime),
        Context::Os(os) => normalize_os_context(os),
        _ => (),
    }
}

#[cfg(test)]
use crate::protocol::LenientString;

#[test]
fn test_dotnet_framework_48_without_build_id() {
    let mut runtime = RuntimeContext {
        raw_description: ".NET Framework 4.8.4250.0".to_string().into(),
        ..RuntimeContext::default()
    };

    normalize_runtime_context(&mut runtime);
    assert_eq_dbg!(Some(".NET Framework"), runtime.name.as_str());
    assert_eq_dbg!(Some("4.8.4250.0"), runtime.version.as_str());
}

#[test]
fn test_dotnet_framework_472() {
    let mut runtime = RuntimeContext {
        raw_description: ".NET Framework 4.7.3056.0".to_string().into(),
        build: LenientString("461814".to_string()).into(),
        ..RuntimeContext::default()
    };

    normalize_runtime_context(&mut runtime);
    assert_eq_dbg!(Some(".NET Framework"), runtime.name.as_str());
    assert_eq_dbg!(Some("4.7.2"), runtime.version.as_str());
}

#[test]
fn test_dotnet_framework_future_version() {
    let mut runtime = RuntimeContext {
        raw_description: ".NET Framework 200.0".to_string().into(),
        build: LenientString("999999".to_string()).into(),
        ..RuntimeContext::default()
    };

    // Unmapped build number doesn't override version
    normalize_runtime_context(&mut runtime);
    assert_eq_dbg!(Some(".NET Framework"), runtime.name.as_str());
    assert_eq_dbg!(Some("200.0"), runtime.version.as_str());
}

#[test]
fn test_dotnet_native() {
    let mut runtime = RuntimeContext {
        raw_description: ".NET Native 2.0".to_string().into(),
        ..RuntimeContext::default()
    };

    normalize_runtime_context(&mut runtime);
    assert_eq_dbg!(Some(".NET Native"), runtime.name.as_str());
    assert_eq_dbg!(Some("2.0"), runtime.version.as_str());
}

#[test]
fn test_dotnet_core() {
    let mut runtime = RuntimeContext {
        raw_description: ".NET Core 2.0".to_string().into(),
        ..RuntimeContext::default()
    };

    normalize_runtime_context(&mut runtime);
    assert_eq_dbg!(Some(".NET Core"), runtime.name.as_str());
    assert_eq_dbg!(Some("2.0"), runtime.version.as_str());
}

#[test]
fn test_windows_7_or_server_2008() {
    // Environment.OSVersion on Windows 7 (CoreCLR 1.0+, .NET Framework 1.1+, Mono 1+)
    let mut os = OsContext {
        raw_description: "Microsoft Windows NT 6.1.7601 Service Pack 1"
            .to_string()
            .into(),
        ..OsContext::default()
    };

    normalize_os_context(&mut os);
    assert_eq_dbg!(Some("Windows"), os.name.as_str());
    assert_eq_dbg!(Some("6.1.7601"), os.version.as_str());
}

#[test]
fn test_windows_8_or_server_2012_or_later() {
    // Environment.OSVersion on Windows 10 (CoreCLR 1.0+, .NET Framework 1.1+, Mono 1+)
    // *or later, due to GetVersionEx deprecated on Windows 8.1
    // It's a potentially really misleading API on newer platforms
    // Only used if RuntimeInformation.OSDescription is not available (old runtimes)
    let mut os = OsContext {
        raw_description: "Microsoft Windows NT 6.2.9200.0".to_string().into(),
        ..OsContext::default()
    };

    normalize_os_context(&mut os);
    assert_eq_dbg!(Some("Windows"), os.name.as_str());
    assert_eq_dbg!(Some("6.2.9200"), os.version.as_str());
}

#[test]
fn test_windows_10() {
    // RuntimeInformation.OSDescription on Windows 10 (CoreCLR 2.0+, .NET
    // Framework 4.7.1+, Mono 5.4+)
    let mut os = OsContext {
        raw_description: "Microsoft Windows 10.0.16299".to_string().into(),
        ..OsContext::default()
    };

    normalize_os_context(&mut os);
    assert_eq_dbg!(Some("Windows"), os.name.as_str());
    assert_eq_dbg!(Some("10.0.16299"), os.version.as_str());
}

#[test]
fn test_macos_os_version() {
    // Environment.OSVersion on macOS (CoreCLR 1.0+, Mono 1+)
    let mut os = OsContext {
        raw_description: "Unix 17.5.0.0".to_string().into(),
        ..OsContext::default()
    };

    normalize_os_context(&mut os);
    assert_eq_dbg!(Some("Unix"), os.name.as_str());
    assert_eq_dbg!(Some("17.5.0"), os.kernel_version.as_str());
}

#[test]
fn test_macos_runtime() {
    // RuntimeInformation.OSDescription on macOS (CoreCLR 2.0+, Mono 5.4+)
    let mut os = OsContext {
        raw_description: "Darwin 17.5.0 Darwin Kernel Version 17.5.0: Mon Mar  5 22:24:32 PST 2018; root:xnu-4570.51.1~1/RELEASE_X86_64".to_string().into(),
        ..OsContext::default()
    };

    normalize_os_context(&mut os);
    assert_eq_dbg!(Some("Darwin"), os.name.as_str());
    assert_eq_dbg!(Some("17.5.0"), os.kernel_version.as_str());
}

#[test]
fn test_centos_os_version() {
    // Environment.OSVersion on CentOS 7 (CoreCLR 1.0+, Mono 1+)
    let mut os = OsContext {
        raw_description: "Unix 3.10.0.693".to_string().into(),
        ..OsContext::default()
    };

    normalize_os_context(&mut os);
    assert_eq_dbg!(Some("Unix"), os.name.as_str());
    assert_eq_dbg!(Some("3.10.0.693"), os.kernel_version.as_str());
}

#[test]
fn test_centos_runtime_info() {
    // RuntimeInformation.OSDescription on CentOS 7 (CoreCLR 2.0+, Mono 5.4+)
    let mut os = OsContext {
        raw_description: "Linux 3.10.0-693.21.1.el7.x86_64 #1 SMP Wed Mar 7 19:03:37 UTC 2018"
            .to_string()
            .into(),
        ..OsContext::default()
    };

    normalize_os_context(&mut os);
    assert_eq_dbg!(Some("Linux"), os.name.as_str());
    assert_eq_dbg!(Some("3.10.0"), os.kernel_version.as_str());
}

#[test]
fn test_wsl_ubuntu() {
    // RuntimeInformation.OSDescription on Windows Subsystem for Linux (Ubuntu)
    // (CoreCLR 2.0+, Mono 5.4+)
    let mut os = OsContext {
        raw_description: "Linux 4.4.0-43-Microsoft #1-Microsoft Wed Dec 31 14:42:53 PST 2014"
            .to_string()
            .into(),
        ..OsContext::default()
    };

    normalize_os_context(&mut os);
    assert_eq_dbg!(Some("Linux"), os.name.as_str());
    assert_eq_dbg!(Some("4.4.0"), os.kernel_version.as_str());
}

#[test]
fn test_macos_with_build() {
    let mut os = OsContext {
        raw_description: "Mac OS X 10.14.2 (18C54)".to_string().into(),
        ..OsContext::default()
    };

    normalize_os_context(&mut os);
    assert_eq_dbg!(Some("macOS"), os.name.as_str());
    assert_eq_dbg!(Some("10.14.2"), os.version.as_str());
    assert_eq_dbg!(Some("18C54"), os.build.as_str());
}

#[test]
fn test_macos_without_build() {
    let mut os = OsContext {
        raw_description: "Mac OS X 10.14.2".to_string().into(),
        ..OsContext::default()
    };

    normalize_os_context(&mut os);
    assert_eq_dbg!(Some("macOS"), os.name.as_str());
    assert_eq_dbg!(Some("10.14.2"), os.version.as_str());
    assert_eq_dbg!(None, os.build.value());
}

#[test]
fn test_name_not_overwritten() {
    let mut os = OsContext {
        name: "Properly defined name".to_string().into(),
        raw_description: "Linux 4.4.0".to_string().into(),
        ..OsContext::default()
    };

    normalize_os_context(&mut os);
    assert_eq_dbg!(Some("Properly defined name"), os.name.as_str());
}

#[test]
fn test_version_not_overwritten() {
    let mut os = OsContext {
        version: "Properly defined version".to_string().into(),
        raw_description: "Linux 4.4.0".to_string().into(),
        ..OsContext::default()
    };

    normalize_os_context(&mut os);
    assert_eq_dbg!(Some("Properly defined version"), os.version.as_str());
}

#[test]
fn test_no_name() {
    let mut os = OsContext::default();

    normalize_os_context(&mut os);
    assert_eq_dbg!(None, os.name.value());
    assert_eq_dbg!(None, os.version.value());
    assert_eq_dbg!(None, os.kernel_version.value());
    assert_eq_dbg!(None, os.raw_description.value());
}
