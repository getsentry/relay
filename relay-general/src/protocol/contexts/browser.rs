use crate::store::user_agent::{get_version, is_known};
use crate::types::{Annotated, Object, Value};
use crate::user_agent::{parse_user_agent, RawUserAgentInfo};

/// Web browser information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct BrowserContext {
    /// Display name of the browser application.
    pub name: Annotated<String>,

    /// Version string of the browser.
    pub version: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl BrowserContext {
    /// The key under which a browser context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "browser"
    }

    pub fn from_client_hints(raw_contexts: &RawUserAgentInfo) -> Option<Self> {
        let browser = raw_contexts.sec_ch_ua?.to_owned();
        let version = raw_contexts.sec_ch_ua_full_version?.to_owned();

        Some(Self {
            name: Annotated::new(browser),
            version: Annotated::new(version),
            ..Default::default()
        })
    }

    pub fn from_user_agent(user_agent: &str) -> Option<Self> {
        let browser = parse_user_agent(user_agent);

        if !is_known(browser.family.as_str()) {
            return None;
        }

        Some(Self {
            name: Annotated::from(browser.family),
            version: Annotated::from(get_version(&browser.major, &browser.minor, &browser.patch)),
            ..BrowserContext::default()
        })
    }

    pub fn from_hints_or_ua(raw_contexts: &RawUserAgentInfo) -> Option<Self> {
        Self::from_client_hints(raw_contexts)
            .or_else(|| raw_contexts.user_agent.and_then(Self::from_user_agent))
    }
}
