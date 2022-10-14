mod app;
pub use app::*;
mod browser;
pub use browser::*;
mod device;
pub use device::*;
mod gpu;
pub use gpu::*;
mod monitor;
pub use monitor::*;
mod os;
pub use os::*;
mod reprocessing;
pub use reprocessing::*;
mod runtime;
pub use runtime::*;
mod trace;
pub use trace::*;

use crate::types::{Annotated, FromValue, Object, Value};

/// Operation type such as `db.statement` for database queries or `http` for external HTTP calls.
/// Tries to follow OpenCensus/OpenTracing's span types.
pub type OperationType = String;

/// A context describes environment info (e.g. device, os or browser).
#[derive(Clone, Debug, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_context")]
pub enum Context {
    /// Device information.
    Device(Box<DeviceContext>),
    /// Operating system information.
    Os(Box<OsContext>),
    /// Runtime information.
    Runtime(Box<RuntimeContext>),
    /// Application information.
    App(Box<AppContext>),
    /// Web browser information.
    Browser(Box<BrowserContext>),
    /// Information about device's GPU.
    Gpu(Box<GpuContext>),
    /// Information related to Tracing.
    Trace(Box<TraceContext>),
    /// Information related to Monitors feature.
    Monitor(Box<MonitorContext>),
    /// Auxilliary information for reprocessing.
    #[metastructure(omit_from_schema)]
    Reprocessing(Box<ReprocessingContext>),
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(fallback_variant)]
    Other(#[metastructure(pii = "true")] Object<Value>),
}

impl Context {
    /// Represents the key under which a particular context type will be inserted in a Contexts object
    ///
    /// See [`Contexts::add`]
    pub fn default_key(&self) -> Option<&'static str> {
        match &self {
            Context::Device(_) => Some(DeviceContext::default_key()),
            Context::Os(_) => Some(OsContext::default_key()),
            Context::Runtime(_) => Some(RuntimeContext::default_key()),
            Context::App(_) => Some(AppContext::default_key()),
            Context::Browser(_) => Some(BrowserContext::default_key()),
            Context::Reprocessing(_) => Some(ReprocessingContext::default_key()),
            Context::Gpu(_) => Some(GpuContext::default_key()),
            Context::Trace(_) => Some(TraceContext::default_key()),
            Context::Monitor(_) => Some(MonitorContext::default_key()),
            Context::Other(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct ContextInner(#[metastructure(bag_size = "large")] pub Context);

impl std::ops::Deref for ContextInner {
    type Target = Context;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ContextInner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Context> for ContextInner {
    fn from(c: Context) -> ContextInner {
        ContextInner(c)
    }
}

/// The Contexts Interface provides additional context data. Typically, this is data related to the
/// current user and the environment. For example, the device or application version. Its canonical
/// name is `contexts`.
///
/// The `contexts` type can be used to define arbitrary contextual data on the event. It accepts an
/// object of key/value pairs. The key is the “alias” of the context and can be freely chosen.
/// However, as per policy, it should match the type of the context unless there are two values for
/// a type. You can omit `type` if the key name is the type.
///
/// Unknown data for the contexts is rendered as a key/value list.
///
/// For more details about sending additional data with your event, see the [full documentation on
/// Additional Data](https://docs.sentry.io/enriching-error-data/additional-data/).
#[derive(Clone, Debug, PartialEq, Empty, IntoValue, ProcessValue, Default)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_contexts")]
pub struct Contexts(pub Object<ContextInner>);

impl Contexts {
    pub fn new() -> Contexts {
        Contexts(Object::<ContextInner>::new())
    }

    /// Adds a context to self under the default key for the Context
    pub fn add(&mut self, context: Context) {
        if let Some(key) = context.default_key() {
            self.insert(key.to_owned(), Annotated::new(ContextInner(context)));
        }
    }

    /// Returns the context at the specified key or constructs it if not present.
    pub fn get_or_insert_with<F, S>(&mut self, key: S, context_builder: F) -> &mut Context
    where
        F: FnOnce() -> Context,
        S: Into<String>,
    {
        &mut *self
            .entry(key.into())
            .or_insert_with(Annotated::empty)
            .value_mut()
            .get_or_insert_with(|| ContextInner(context_builder()))
    }

    pub fn get_context_mut<S>(&mut self, key: S) -> Option<&mut Context>
    where
        S: AsRef<str>,
    {
        Some(&mut self.get_mut(key.as_ref())?.value_mut().as_mut()?.0)
    }
}

impl std::ops::Deref for Contexts {
    type Target = Object<ContextInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Contexts {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromValue for Contexts {
    fn from_value(mut annotated: Annotated<Value>) -> Annotated<Self> {
        if let Annotated(Some(Value::Object(ref mut items)), _) = annotated {
            for (key, value) in items.iter_mut() {
                if let Annotated(Some(Value::Object(ref mut items)), _) = value {
                    if !items.contains_key("type") {
                        items.insert(
                            "type".to_string(),
                            Annotated::new(Value::String(key.to_string())),
                        );
                    }
                }
            }
        }
        FromValue::from_value(annotated).map_value(Contexts)
    }
}

#[cfg(test)]
mod tests {
    use crate::processor::{ProcessingState, Processor};
    use crate::protocol::Event;
    use crate::types::{Map, Meta, ProcessingResult};

    use super::*;

    #[test]
    fn test_other_context_roundtrip() {
        let json = r#"{"other":"value","type":"mytype"}"#;
        let context = Annotated::new(Context::Other({
            let mut map = Map::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map.insert(
                "type".to_string(),
                Annotated::new(Value::String("mytype".to_string())),
            );
            map
        }));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json().unwrap());
    }

    #[test]
    fn test_untagged_context_deserialize() {
        let json = r#"{"os": {"name": "Linux"}}"#;

        let os_context = Annotated::new(ContextInner(Context::Os(Box::new(OsContext {
            name: Annotated::new("Linux".to_string()),
            ..Default::default()
        }))));
        let mut map = Object::new();
        map.insert("os".to_string(), os_context);
        let contexts = Annotated::new(Contexts(map));

        assert_eq!(contexts, Annotated::from_json(json).unwrap());
    }

    #[test]
    fn test_multiple_contexts_roundtrip() {
        let json =
            r#"{"os":{"name":"Linux","type":"os"},"runtime":{"name":"rustc","type":"runtime"}}"#;

        let os_context = Annotated::new(ContextInner(Context::Os(Box::new(OsContext {
            name: Annotated::new("Linux".to_string()),
            ..Default::default()
        }))));

        let runtime_context =
            Annotated::new(ContextInner(Context::Runtime(Box::new(RuntimeContext {
                name: Annotated::new("rustc".to_string()),
                ..Default::default()
            }))));

        let mut map = Object::new();
        map.insert("os".to_string(), os_context);
        map.insert("runtime".to_string(), runtime_context);
        let contexts = Annotated::new(Contexts(map));

        assert_eq!(contexts, Annotated::from_json(json).unwrap());
        assert_eq!(json, contexts.to_json().unwrap());
    }

    #[test]
    fn test_context_processing() {
        let mut event = Annotated::new(Event {
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "runtime".to_owned(),
                    Annotated::new(ContextInner(Context::Runtime(Box::new(RuntimeContext {
                        name: Annotated::new("php".to_owned()),
                        version: Annotated::new("7.1.20-1+ubuntu16.04.1+deb.sury.org+1".to_owned()),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            ..Default::default()
        });

        struct FooProcessor {
            called: bool,
        }

        impl Processor for FooProcessor {
            #[inline]
            fn process_context(
                &mut self,
                _value: &mut Context,
                _meta: &mut Meta,
                _state: &ProcessingState<'_>,
            ) -> ProcessingResult {
                self.called = true;
                Ok(())
            }
        }

        let mut processor = FooProcessor { called: false };
        crate::processor::process_value(&mut event, &mut processor, ProcessingState::root())
            .unwrap();
        assert!(processor.called);
    }
}
