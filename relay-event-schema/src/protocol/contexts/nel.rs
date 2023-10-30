#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Contains NEL report information.
///
/// Network Error Logging (NEL) is a browser feature that allows reporting of failed network
/// requests from the client side. See the following resources for more information:
///
/// - [W3C Editor's Draft](https://w3c.github.io/network-error-logging/)
/// - [MDN](https://developer.mozilla.org/en-US/docs/Web/HTTP/Network_Error_Logging)
/// W3C Editor's Draft: <https://w3c.github.io/network-error-logging/>
/// MDN: <https://developer.mozilla.org/en-US/docs/Web/HTTP/Network_Error_Logging>
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct NelContext {
    /// If request failed, the type of its network error. If request succeeded, "ok".
    pub error_type: Annotated<String>,
    /// Server IP where the requests was sent to.
    #[metastructure(pii = "true")]
    pub server_ip: Annotated<String>,
    /// The time between the start of the resource fetch and when it was completed or aborted.
    pub elapsed_time: Annotated<u64>,
    /// If request failed, the phase of its network error. If request succeeded, "application".
    pub phase: Annotated<String>,
    /// The sampling rate.
    pub sampling_fraction: Annotated<f64>,
    /// For forward compatibility.
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for NelContext {
    fn default_key() -> &'static str {
        "nel"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Nel(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Nel(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Nel(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Nel(Box::new(self))
    }
}
