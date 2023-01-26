use crate::protocol::{FromUserAgentInfo, LenientString};
use crate::store::user_agent::{get_version, is_known};
use crate::types::{Annotated, Object, Value};
use crate::user_agent::{parse_os, ClientHints};

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
}

impl FromUserAgentInfo for OsContext {
    fn from_client_hints(client_hints: &ClientHints) -> Option<Self> {
        let platform = client_hints.sec_ch_ua_platform?;
        let version = client_hints.sec_ch_ua_platform_version?;

        Some(Self {
            name: Annotated::new(platform.to_owned()),
            version: Annotated::new(version.to_owned()),
            ..Default::default()
        })
    }

    fn from_user_agent(user_agent: &str) -> Option<Self> {
        let os = parse_os(user_agent);

        if !is_known(&os.family) {
            return None;
        }

        Some(Self {
            name: Annotated::from(os.family),
            version: Annotated::from(get_version(&os.major, &os.minor, &os.patch)),
            ..OsContext::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Headers, PairList};
    use crate::user_agent::RawUserAgentInfo;

    #[test]
    fn test_choose_client_hints_for_os_context() {
        let headers = Headers({
            let headers = vec![
            Annotated::new((
                Annotated::new("user-agent".to_string().into()),
                Annotated::new(r#"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"#.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("SEC-CH-UA-PLATFORM".to_string().into()),
                Annotated::new(r#"macOS"#.to_string().into()), // no browser field here
            )),
            Annotated::new((
                Annotated::new("SEC-CH-UA-PLATFORM-VERSION".to_string().into()),
                Annotated::new("13.1.0".to_string().into()),
            )),
        ];
            PairList(headers)
        });

        let os = OsContext::from_hints_or_ua(&RawUserAgentInfo::new(&headers));

        assert_eq!(os.clone().unwrap().name.as_str().unwrap(), "macOS");
        assert_eq!(os.unwrap().version.as_str().unwrap(), "13.1.0");
    }

    #[test]
    fn test_fallback_on_ua_string_for_os() {
        let headers = Headers({
            let headers = vec![
            Annotated::new((
                Annotated::new("user-agent".to_string().into()),
                Annotated::new(r#"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"#.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("invalid header".to_string().into()),
                Annotated::new(r#"macOS"#.to_string().into()),
            )),
            Annotated::new((
                Annotated::new("SEC-CH-UA-PLATFORM-VERSION".to_string().into()),
                Annotated::new("13.1.0".to_string().into()),
            )),
        ];
            PairList(headers)
        });

        let os = OsContext::from_hints_or_ua(&RawUserAgentInfo::new(&headers));

        assert_eq!(os.clone().unwrap().name.as_str().unwrap(), "Mac OS X");
        assert_eq!(os.unwrap().version.as_str().unwrap(), "10.15.7");
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
}
