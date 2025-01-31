use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Our Logs context.
///
/// The Sentry Logs context contains information about our logging product (ourlogs) for an event.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct OurLogsContext {
    /// Whether breadcrumbs are being deduplicated.
    pub deduplicated_breadcrumbs: Annotated<bool>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true)]
    pub other: Object<Value>,
}

impl super::DefaultContext for OurLogsContext {
    fn default_key() -> &'static str {
        "sentry_logs" // Ourlogs is an internal name, and 'logs' likely has conflicts with user contexts.
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::OurLogs(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::OurLogs(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::OurLogs(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::OurLogs(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_our_logs_context() {
        let json = r#"{
  "deduplicated_breadcrumbs": true,
  "type": "sentry_logs"
}"#;
        let context = Annotated::new(Context::OurLogs(Box::new(OurLogsContext {
            deduplicated_breadcrumbs: Annotated::new(true),
            ..OurLogsContext::default()
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
