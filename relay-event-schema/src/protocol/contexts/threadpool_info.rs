use crate::processor::ProcessValue;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

/// Thread pool info context.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct ThreadPoolInfoContext {
    /// Number of worker threads currently available in the thread pool.
    /// Worker threads are used for executing application code and handling CPU-bound tasks.
    pub available_worker_threads: Annotated<u64>,

    /// Number of completion port threads (also known as I/O completion port threads) currently available.
    /// These threads handle I/O operations and asynchronous callbacks.
    pub available_completion_port_threads: Annotated<u64>,

    /// Maximum number of worker threads the thread pool can have.
    /// This represents the upper limit for worker thread allocation.
    pub max_worker_threads: Annotated<u64>,

    /// Maximum number of completion port threads the thread pool can maintain.
    /// This sets the ceiling for I/O completion port thread allocation.
    pub max_completion_port_threads: Annotated<u64>,

    /// Minimum number of worker threads maintained by the thread pool.
    /// The thread pool will always keep at least this many worker threads active.
    pub min_worker_threads: Annotated<u64>,

    /// Minimum number of completion port threads maintained by the thread pool.
    /// This ensures a baseline number of threads are available for I/O operations.
    pub min_completion_port_threads: Annotated<u64>,

    #[metastructure(additional_properties, retain = true, pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for ThreadPoolInfoContext {
    fn default_key() -> &'static str {
        "threadpool_info"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::ThreadPoolInfo(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::ThreadPoolInfo(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::ThreadPoolInfo(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::ThreadPoolInfo(Box::new(self))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_threadpool_info_context_roundtrip() {
        let json = r#"{
  "available_worker_threads": 1022,
  "available_completion_port_threads": 1000,
  "max_worker_threads": 1023,
  "max_completion_port_threads": 1000,
  "min_worker_threads": 1,
  "min_completion_port_threads": 1,
  "unknown_key": [
    123
  ],
  "type": "threadpool_info"
}"#;

        let other = {
            let mut map = Object::new();
            map.insert(
                "unknown_key".to_owned(),
                Annotated::new(Value::Array(vec![Annotated::new(Value::I64(123))])),
            );
            map
        };
        let context = Annotated::new(Context::ThreadPoolInfo(Box::new(ThreadPoolInfoContext {
            available_worker_threads: Annotated::new(1022),
            available_completion_port_threads: Annotated::new(1000),
            max_worker_threads: Annotated::new(1023),
            max_completion_port_threads: Annotated::new(1000),
            min_worker_threads: Annotated::new(1),
            min_completion_port_threads: Annotated::new(1),
            other,
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
