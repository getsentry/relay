use serde::{Deserialize, Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::Stacktrace;
use crate::types::{Annotated, Empty, Error, FromValue, Object, SkipSerialization, ToValue, Value};

/// Represents a thread id.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("thread id"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl ToValue for ThreadId {
    fn to_value(self) -> Value {
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
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_thread", value_type = "Thread")]
pub struct Thread {
    /// Identifier of this thread within the process (usually an integer).
    #[metastructure(max_chars = "symbol")]
    pub id: Annotated<ThreadId>,

    /// Display name of this thread.
    #[metastructure(max_chars = "summary")]
    pub name: Annotated<String>,

    /// Stack trace containing frames of this exception.
    pub stacktrace: Annotated<Stacktrace>,

    /// Optional unprocessed stack trace.
    pub raw_stacktrace: Annotated<Stacktrace>,

    /// Indicates that this thread requested the event (usually by crashing).
    pub crashed: Annotated<bool>,

    /// Indicates that the thread was not suspended when the event was created.
    pub current: Annotated<bool>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[test]
fn test_thread_id() {
    assert_eq_dbg!(
        ThreadId::String("testing".into()),
        Annotated::<ThreadId>::from_json("\"testing\"")
            .unwrap()
            .0
            .unwrap()
    );
    assert_eq_dbg!(
        ThreadId::String("42".into()),
        Annotated::<ThreadId>::from_json("\"42\"")
            .unwrap()
            .0
            .unwrap()
    );
    assert_eq_dbg!(
        ThreadId::Int(42),
        Annotated::<ThreadId>::from_json("42").unwrap().0.unwrap()
    );
}

#[test]
fn test_thread_roundtrip() {
    use crate::types::Map;

    // stack traces are tested separately
    let json = r#"{
  "id": 42,
  "name": "myname",
  "crashed": true,
  "current": true,
  "other": "value"
}"#;
    let thread = Annotated::new(Thread {
        id: Annotated::new(ThreadId::Int(42)),
        name: Annotated::new("myname".to_string()),
        stacktrace: Annotated::empty(),
        raw_stacktrace: Annotated::empty(),
        crashed: Annotated::new(true),
        current: Annotated::new(true),
        other: {
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(thread, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, thread.to_json_pretty().unwrap());
}

#[test]
fn test_thread_default_values() {
    let json = "{}";
    let thread = Annotated::new(Thread::default());

    assert_eq_dbg!(thread, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, thread.to_json_pretty().unwrap());
}
