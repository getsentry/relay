use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;
use crate::protocol::LenientString;

/// Operating system information.
///
/// OS context describes the operating system on which the event was created. In web contexts, this
/// is the operating system of the browser (generally pulled from the User-Agent string).
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct OsContext {
    /// Computed field from `name` and `version`. Needed by the metrics extraction.
    pub os: Annotated<String>,

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

    /// Meta-data for the Linux Distribution.
    #[metastructure(pii = "maybe")]
    pub distribution_name: Annotated<String>,
    #[metastructure(pii = "maybe")]
    pub distribution_version: Annotated<String>,
    #[metastructure(pii = "maybe")]
    pub distribution_pretty_name: Annotated<String>,

    /// Unprocessed operating system info.
    ///
    /// An unprocessed description string obtained by the operating system. For some well-known
    /// runtimes, Sentry will attempt to parse `name` and `version` from this string, if they are
    /// not explicitly given.
    #[metastructure(pii = "maybe")]
    pub raw_description: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for OsContext {
    fn default_key() -> &'static str {
        "os"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Os(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Os(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Os(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Os(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_os_context_roundtrip() {
        let json = r#"{
  "os": "iOS 11.4.2",
  "name": "iOS",
  "version": "11.4.2",
  "build": "FEEDFACE",
  "kernel_version": "17.4.0",
  "rooted": true,
  "raw_description": "iOS 11.4.2 FEEDFACE (17.4.0)",
  "other": "value",
  "type": "os"
}"#;
        let context = Annotated::new(Context::Os(Box::new(OsContext {
            os: Annotated::new("iOS 11.4.2".to_owned()),
            name: Annotated::new("iOS".to_owned()),
            version: Annotated::new("11.4.2".to_owned()),
            build: Annotated::new(LenientString("FEEDFACE".to_owned())),
            kernel_version: Annotated::new("17.4.0".to_owned()),
            rooted: Annotated::new(true),
            raw_description: Annotated::new("iOS 11.4.2 FEEDFACE (17.4.0)".to_owned()),
            distribution_name: Annotated::empty(),
            distribution_version: Annotated::empty(),
            distribution_pretty_name: Annotated::empty(),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_owned(),
                    Annotated::new(Value::String("value".to_owned())),
                );
                map
            },
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }

    #[test]
    fn test_os_context_linux_roundtrip() {
        let json: &str = r#"{
  "os": "Linux 5.15.133",
  "name": "Linux",
  "version": "5.15.133",
  "build": "1-microsoft-standard-WSL2",
  "distribution_name": "ubuntu",
  "distribution_version": "22.04",
  "distribution_pretty_name": "Ubuntu 22.04.4 LTS",
  "type": "os"
}"#;
        let context = Annotated::new(Context::Os(Box::new(OsContext {
            os: Annotated::new("Linux 5.15.133".to_owned()),
            name: Annotated::new("Linux".to_owned()),
            version: Annotated::new("5.15.133".to_owned()),
            build: Annotated::new(LenientString("1-microsoft-standard-WSL2".to_owned())),
            kernel_version: Annotated::empty(),
            rooted: Annotated::empty(),
            raw_description: Annotated::empty(),
            distribution_name: Annotated::new("ubuntu".to_owned()),
            distribution_version: Annotated::new("22.04".to_owned()),
            distribution_pretty_name: Annotated::new("Ubuntu 22.04.4 LTS".to_owned()),
            other: Object::default(),
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
