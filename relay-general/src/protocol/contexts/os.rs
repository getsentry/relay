use crate::protocol::LenientString;
use crate::store::user_agent::{get_version, is_known};
use crate::types::{Annotated, Object, Value};
use crate::user_agent::{parse_os, RawUserAgentInfo};

/// Operating system information.
///
/// OS context describes the operating system on which the event was created. In web contexts, this
/// is the operating system of the browser (generally pulled from the User-Agent string).
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct OsContext {
    /// Name of the operating system.
    pub name: Annotated<String>,

    /// Version of the operating system.
    pub version: Annotated<String>,

    /// Internal build number of the operating system.
    #[metastructure(pii = "maybe")]
    pub build: Annotated<LenientString>,

    /// Current kernel version.
    ///
    /// This is typically the entire output of the `uname` syscall.
    #[metastructure(pii = "maybe")]
    pub kernel_version: Annotated<String>,

    /// Indicator if the OS is rooted (mobile mostly).
    pub rooted: Annotated<bool>,

    /// Unprocessed operating system info.
    ///
    /// An unprocessed description string obtained by the operating system. For some well-known
    /// runtimes, Sentry will attempt to parse `name` and `version` from this string, if they are
    /// not explicitly given.
    #[metastructure(pii = "maybe")]
    pub raw_description: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl OsContext {
    /// The key under which an os context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "os"
    }

    pub fn from_client_hints(contexts: &RawUserAgentInfo) -> Option<OsContext> {
        let platform = contexts.sec_ch_ua_platform?;
        let version = contexts.sec_ch_ua_platform_version?;

        Some(OsContext {
            name: Annotated::new(platform.to_owned()),
            version: Annotated::new(version.to_owned()),
            ..Default::default()
        })
    }

    pub fn from_user_agent(user_agent: &str) -> Option<OsContext> {
        let os = parse_os(user_agent);

        if !is_known(os.family.as_str()) {
            return None;
        }

        Some(OsContext {
            name: Annotated::from(os.family),
            version: Annotated::from(get_version(&os.major, &os.minor, &os.patch)),
            ..OsContext::default()
        })
    }

    pub fn from_hints_or_ua(raw_contexts: &RawUserAgentInfo) -> Option<Self> {
        Self::from_client_hints(raw_contexts)
            .or_else(|| raw_contexts.user_agent.and_then(Self::from_user_agent))
    }
}

#[test]
fn test_os_context_roundtrip() {
    let json = r#"{
  "name": "iOS",
  "version": "11.4.2",
  "build": "FEEDFACE",
  "kernel_version": "17.4.0",
  "rooted": true,
  "raw_description": "iOS 11.4.2 FEEDFACE (17.4.0)",
  "other": "value",
  "type": "os"
}"#;
    use crate::protocol::Context;
    let context = Annotated::new(Context::Os(Box::new(OsContext {
        name: Annotated::new("iOS".to_string()),
        version: Annotated::new("11.4.2".to_string()),
        build: Annotated::new(LenientString("FEEDFACE".to_string())),
        kernel_version: Annotated::new("17.4.0".to_string()),
        rooted: Annotated::new(true),
        raw_description: Annotated::new("iOS 11.4.2 FEEDFACE (17.4.0)".to_string()),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    })));

    assert_eq!(context, Annotated::from_json(json).unwrap());
    assert_eq!(json, context.to_json_pretty().unwrap());
}
