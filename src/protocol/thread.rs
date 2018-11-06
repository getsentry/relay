use general_derive::{FromValue, ProcessValue, ToValue};

use super::*;

/// A process thread of an event.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
#[metastructure(process_func = "process_thread")]
pub struct Thread {
    /// Identifier of this thread within the process (usually an integer).
    pub id: Annotated<ThreadId>,

    /// Display name of this thread.
    #[metastructure(cap_size = "summary")]
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
fn test_thread_roundtrip() {
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
