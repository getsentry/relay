use serde::{Deserialize, Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::RawStacktrace;
use crate::protocol::Stacktrace;
use crate::types::{
    Annotated, Empty, Error, FromValue, IntoValue, Object, SkipSerialization, Value,
};

/// Represents a thread id.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[serde(untagged)]
pub enum ThreadId {
    /// Integer representation of the thread id.
    Int(u64),
    /// String representation of the thread id.
    String(String),
}

impl FromValue for ThreadId {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), meta) => {
                Annotated(Some(ThreadId::String(value)), meta)
            }
            Annotated(Some(Value::U64(value)), meta) => Annotated(Some(ThreadId::Int(value)), meta),
            Annotated(Some(Value::I64(value)), meta) => {
                Annotated(Some(ThreadId::Int(value as u64)), meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("a thread id"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl IntoValue for ThreadId {
    fn into_value(self) -> Value {
        match self {
            ThreadId::String(value) => Value::String(value),
            ThreadId::Int(value) => Value::U64(value),
        }
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        match *self {
            ThreadId::String(ref value) => Serialize::serialize(value, s),
            ThreadId::Int(value) => Serialize::serialize(&value, s),
        }
    }
}

impl ProcessValue for ThreadId {}

impl Empty for ThreadId {
    #[inline]
    fn is_empty(&self) -> bool {
        match self {
            ThreadId::Int(_) => false,
            ThreadId::String(string) => string.is_empty(),
        }
    }
}

/// A process thread of an event.
///
/// The Threads Interface specifies threads that were running at the time an event happened. These threads can also contain stack traces.
///
/// An event may contain one or more threads in an attribute named `threads`.
///
/// The following example illustrates the threads part of the event payload and omits other attributes for simplicity.
///
/// ```json
/// {
///   "threads": {
///     "values": [
///       {
///         "id": "0",
///         "name": "main",
///         "crashed": true,
///         "stacktrace": {}
///       }
///     ]
///   }
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_thread", value_type = "Thread")]
pub struct Thread {
    /// The ID of the thread. Typically a number or numeric string.
    ///
    /// Needs to be unique among the threads. An exception can set the `thread_id` attribute to cross-reference this thread.
    #[metastructure(max_chars = "symbol")]
    pub id: Annotated<ThreadId>,

    /// Display name of this thread.
    #[metastructure(max_chars = "summary")]
    pub name: Annotated<String>,

    /// Stack trace containing frames of this exception.
    ///
    /// The thread that crashed with an exception should not have a stack trace, but instead, the `thread_id` attribute should be set on the exception and Sentry will connect the two.
    #[metastructure(skip_serialization = "empty")]
    pub stacktrace: Annotated<Stacktrace>,

    /// Optional unprocessed stack trace.
    #[metastructure(skip_serialization = "empty", omit_from_schema)]
    pub raw_stacktrace: Annotated<RawStacktrace>,

    /// A flag indicating whether the thread crashed. Defaults to `false`.
    pub crashed: Annotated<bool>,

    /// A flag indicating whether the thread was in the foreground. Defaults to `false`.
    pub current: Annotated<bool>,

    /// A flag indicating whether the thread was responsible for rendering the user interface.
    pub ui: Annotated<bool>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use crate::types::Map;

    use super::*;

    #[test]
    fn test_thread_id() {
        assert_eq!(
            ThreadId::String("testing".into()),
            Annotated::<ThreadId>::from_json("\"testing\"")
                .unwrap()
                .0
                .unwrap()
        );
        assert_eq!(
            ThreadId::String("42".into()),
            Annotated::<ThreadId>::from_json("\"42\"")
                .unwrap()
                .0
                .unwrap()
        );
        assert_eq!(
            ThreadId::Int(42),
            Annotated::<ThreadId>::from_json("42").unwrap().0.unwrap()
        );
    }

    #[test]
    fn test_thread_roundtrip() {
        // stack traces are tested separately
        let json = r#"{
  "id": 42,
  "name": "myname",
  "crashed": true,
  "current": true,
  "ui": true,
  "other": "value"
}"#;
        let thread = Annotated::new(Thread {
            id: Annotated::new(ThreadId::Int(42)),
            name: Annotated::new("myname".to_string()),
            stacktrace: Annotated::empty(),
            raw_stacktrace: Annotated::empty(),
            crashed: Annotated::new(true),
            current: Annotated::new(true),
            ui: Annotated::new(true),
            other: {
                let mut map = Map::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        });

        assert_eq!(thread, Annotated::from_json(json).unwrap());
        assert_eq!(json, thread.to_json_pretty().unwrap());
    }

    #[test]
    fn test_thread_default_values() {
        let json = "{}";
        let thread = Annotated::new(Thread::default());

        assert_eq!(thread, Annotated::from_json(json).unwrap());
        assert_eq!(json, thread.to_json_pretty().unwrap());
    }
}
