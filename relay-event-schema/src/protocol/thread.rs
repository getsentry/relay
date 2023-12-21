#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{
    Annotated, Empty, Error, ErrorKind, FromValue, IntoValue, Object, SkipSerialization, Value,
};
use serde::{Deserialize, Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::{RawStacktrace, Stacktrace};

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

/// Possible lock types responsible for a thread's blocked state
#[derive(Debug, Copy, Clone, Eq, PartialEq, ProcessValue, Empty)]
#[cfg_attr(feature = "jsonschema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "jsonschema", schemars(rename_all = "lowercase"))]
pub enum LockReasonType {
    /// Thread is Runnable but holding a lock object (generic case).
    Locked = 1,
    /// Thread TimedWaiting in Object.wait() with a timeout.
    Waiting = 2,
    /// Thread TimedWaiting in Thread.sleep().
    Sleeping = 4,
    /// Thread Blocked on a monitor/shared lock.
    Blocked = 8,
    // This enum does not have a `fallback_variant` because we consider it unlikely to be extended. If it is,
    // The error added to `Meta` will tell us to update this enum.
}

impl LockReasonType {
    fn from_android_lock_reason_type(value: u64) -> Option<LockReasonType> {
        Some(match value {
            1 => LockReasonType::Locked,
            2 => LockReasonType::Waiting,
            4 => LockReasonType::Sleeping,
            8 => LockReasonType::Blocked,
            _ => return None,
        })
    }
}

impl FromValue for LockReasonType {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::U64(val)), mut meta) => {
                match LockReasonType::from_android_lock_reason_type(val) {
                    Some(value) => Annotated(Some(value), meta),
                    None => {
                        meta.add_error(ErrorKind::InvalidData);
                        meta.set_original_value(Some(val));
                        Annotated(None, meta)
                    }
                }
            }
            Annotated(Some(Value::I64(val)), mut meta) => {
                match LockReasonType::from_android_lock_reason_type(val as u64) {
                    Some(value) => Annotated(Some(value), meta),
                    None => {
                        meta.add_error(ErrorKind::InvalidData);
                        meta.set_original_value(Some(val));
                        Annotated(None, meta)
                    }
                }
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("lock reason type"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl IntoValue for LockReasonType {
    fn into_value(self) -> Value {
        Value::U64(self as u64)
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(&(*self as u64), s)
    }
}

/// Represents an instance of a held lock (java monitor object) in a thread.
#[derive(Clone, Debug, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct LockReason {
    /// Type of lock on the thread with available options being blocked, waiting, sleeping and locked.
    #[metastructure(field = "type", required = "true")]
    pub ty: Annotated<LockReasonType>,

    /// Address of the java monitor object.
    #[metastructure(skip_serialization = "empty")]
    pub address: Annotated<String>,

    /// Package name of the java monitor object.
    #[metastructure(skip_serialization = "empty")]
    pub package_name: Annotated<String>,

    /// Class name of the java monitor object.
    #[metastructure(skip_serialization = "empty")]
    pub class_name: Annotated<String>,

    /// Thread ID that's holding the lock.
    #[metastructure(skip_serialization = "empty")]
    pub thread_id: Annotated<ThreadId>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
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
    #[metastructure(max_chars = 256)]
    pub id: Annotated<ThreadId>,

    /// Display name of this thread.
    #[metastructure(max_chars = 1024)]
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
    pub main: Annotated<bool>,

    /// Thread state at the time of the crash.
    #[metastructure(skip_serialization = "empty")]
    pub state: Annotated<String>,

    /// Represents a collection of locks (java monitor objects) held by a thread.
    ///
    /// A map of lock object addresses and their respective lock reason/details.
    pub held_locks: Annotated<Object<LockReason>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[cfg(test)]
mod tests {
    use relay_protocol::Map;
    use similar_asserts::assert_eq;

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
  "main": true,
  "state": "RUNNABLE",
  "other": "value"
}"#;
        let thread = Annotated::new(Thread {
            id: Annotated::new(ThreadId::Int(42)),
            name: Annotated::new("myname".to_string()),
            stacktrace: Annotated::empty(),
            raw_stacktrace: Annotated::empty(),
            crashed: Annotated::new(true),
            current: Annotated::new(true),
            main: Annotated::new(true),
            state: Annotated::new("RUNNABLE".to_string()),
            held_locks: Annotated::empty(),
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

    #[test]
    fn test_thread_lock_reason_roundtrip() {
        // stack traces are tested separately
        let input = r#"{
  "id": 42,
  "name": "myname",
  "crashed": true,
  "current": true,
  "main": true,
  "state": "BLOCKED",
  "held_locks": {
    "0x07d7437b": {
      "type": 2,
      "package_name": "io.sentry.samples",
      "class_name": "MainActivity",
      "thread_id": 7
    },
    "0x0d3a2f0a": {
      "type": 1,
      "package_name": "android.database.sqlite",
      "class_name": "SQLiteConnection",
      "thread_id": 2
    }
  },
  "other": "value"
}"#;
        let thread = Annotated::new(Thread {
            id: Annotated::new(ThreadId::Int(42)),
            name: Annotated::new("myname".to_string()),
            stacktrace: Annotated::empty(),
            raw_stacktrace: Annotated::empty(),
            crashed: Annotated::new(true),
            current: Annotated::new(true),
            main: Annotated::new(true),
            state: Annotated::new("BLOCKED".to_string()),
            held_locks: {
                let mut locks = Object::new();
                locks.insert(
                    "0x07d7437b".to_string(),
                    Annotated::new(LockReason {
                        ty: Annotated::new(LockReasonType::Waiting),
                        address: Annotated::empty(),
                        package_name: Annotated::new("io.sentry.samples".to_string()),
                        class_name: Annotated::new("MainActivity".to_string()),
                        thread_id: Annotated::new(ThreadId::Int(7)),
                        other: Default::default(),
                    }),
                );
                locks.insert(
                    "0x0d3a2f0a".to_string(),
                    Annotated::new(LockReason {
                        ty: Annotated::new(LockReasonType::Locked),
                        address: Annotated::empty(),
                        package_name: Annotated::new("android.database.sqlite".to_string()),
                        class_name: Annotated::new("SQLiteConnection".to_string()),
                        thread_id: Annotated::new(ThreadId::Int(2)),
                        other: Default::default(),
                    }),
                );
                Annotated::new(locks)
            },
            other: {
                let mut map = Map::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        });

        assert_eq!(thread, Annotated::from_json(input).unwrap());

        assert_eq!(input, thread.to_json_pretty().unwrap());
    }
}
