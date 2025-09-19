use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Memory Info Context
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct MemoryInfoContext {
    /// Currently allocated memory in bytes.
    pub allocated_bytes: Annotated<u64>,

    /// Boolean indicating if memory was compacted.
    pub compacted: Annotated<bool>,

    /// Boolean indicating if concurrent garbage collection occurred.
    pub concurrent: Annotated<bool>,

    /// Number of objects awaiting finalization.
    pub finalization_pending_count: Annotated<u64>,

    /// Fragmented memory that cannot be used in bytes.
    pub fragmented_bytes: Annotated<u64>,

    /// Total heap size in bytes.
    pub heap_size_bytes: Annotated<u64>,

    /// Threshold for high memory load detection in bytes.
    pub high_memory_load_threshold_bytes: Annotated<u64>,

    /// GC generation index.
    pub index: Annotated<u64>,

    /// Current memory load in bytes.
    pub memory_load_bytes: Annotated<u64>,

    /// Array of GC pause durations in milliseconds.
    pub pause_durations: Annotated<Vec<Annotated<u64>>>,

    /// Percentage of time spent in GC pauses.
    pub pause_time_percentage: Annotated<f64>,

    /// Number of pinned objects in memory.
    pub pinned_objects_count: Annotated<u64>,

    /// Bytes promoted to higher generation.
    pub promoted_bytes: Annotated<u64>,

    /// Total memory allocated since start in bytes.
    pub total_allocated_bytes: Annotated<u64>,

    /// Total memory available to the application in bytes.
    pub total_available_memory_bytes: Annotated<u64>,

    /// Total committed virtual memory in bytes.
    pub total_committed_bytes: Annotated<u64>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for MemoryInfoContext {
    fn default_key() -> &'static str {
        "memory_info"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::MemoryInfo(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::MemoryInfo(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::MemoryInfo(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::MemoryInfo(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_memory_info_context_roundtrip() {
        let json = r#"{
  "allocated_bytes": 1048576,
  "compacted": true,
  "concurrent": true,
  "finalization_pending_count": 42,
  "fragmented_bytes": 2048,
  "heap_size_bytes": 3145728,
  "high_memory_load_threshold_bytes": 8388608,
  "index": 2,
  "memory_load_bytes": 5242880,
  "pause_durations": [
    10,
    5,
    3
  ],
  "pause_time_percentage": 25.5,
  "pinned_objects_count": 150,
  "promoted_bytes": 524288,
  "total_allocated_bytes": 9437184,
  "total_available_memory_bytes": 16777216,
  "total_committed_bytes": 12582912,
  "other": "value",
  "type": "memory_info"
}"#;
        let context = Annotated::new(Context::MemoryInfo(Box::new(MemoryInfoContext {
            allocated_bytes: Annotated::new(1048576),
            total_allocated_bytes: Annotated::new(9437184),
            heap_size_bytes: Annotated::new(3145728),
            pinned_objects_count: Annotated::new(150),
            pause_time_percentage: Annotated::new(25.5),
            compacted: Annotated::new(true),
            concurrent: Annotated::new(true),
            pause_durations: Annotated::new(vec![
                Annotated::new(10),
                Annotated::new(5),
                Annotated::new(3),
            ]),
            finalization_pending_count: Annotated::new(42),
            fragmented_bytes: Annotated::new(2048),
            high_memory_load_threshold_bytes: Annotated::new(8388608),
            index: Annotated::new(2),
            memory_load_bytes: Annotated::new(5242880),
            promoted_bytes: Annotated::new(524288),
            total_available_memory_bytes: Annotated::new(16777216),
            total_committed_bytes: Annotated::new(12582912),
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
    fn test_memory_info_context_minimal() {
        let json = r#"{
  "type": "memory_info"
}"#;
        let context = Annotated::new(Context::MemoryInfo(Box::default()));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
