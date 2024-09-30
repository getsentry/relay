use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Performance Score context.
///
/// The performance score context contains the version of the
/// profile used to calculate the performance score.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]

pub struct PerformanceScoreContext {
    /// The performance score profile version.
    pub score_profile_version: Annotated<String>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true")]
    pub other: Object<Value>,
}

impl super::DefaultContext for PerformanceScoreContext {
    fn default_key() -> &'static str {
        "performance_score"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::PerformanceScore(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::PerformanceScore(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::PerformanceScore(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::PerformanceScore(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_performance_score_context() {
        let json = r#"{
  "score_profile_version": "alpha",
  "type": "performancescore"
}"#;
        let context = Annotated::new(Context::PerformanceScore(Box::new(
            PerformanceScoreContext {
                score_profile_version: Annotated::new("alpha".to_string()),
                other: Object::default(),
            },
        )));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
