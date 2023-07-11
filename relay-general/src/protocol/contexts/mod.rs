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
mod profile;
pub use profile::*;
mod replay;
pub use replay::*;
mod reprocessing;
pub use reprocessing::*;
mod response;
pub use response::*;
mod runtime;
pub use runtime::*;
mod trace;
pub use trace::*;
mod otel;
pub use otel::*;
mod cloud_resource;
pub use cloud_resource::*;

use crate::types::{Annotated, FromValue, Object, Value};
use crate::user_agent::{ClientHints, RawUserAgentInfo};

/// Operation type such as `db.statement` for database queries or `http` for external HTTP calls.
/// Tries to follow OpenCensus/OpenTracing's span types.
pub type OperationType = String;

/// Origin type such as `auto.http`.
/// Follows the pattern described in the [develop docs](https://develop.sentry.dev/sdk/performance/trace-origin/).
pub type OriginType = String;

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
    /// Information related to Profiling.
    Profile(Box<ProfileContext>),
    /// Information related to Replay.
    Replay(Box<ReplayContext>),
    /// Information related to Monitors feature.
    Monitor(Box<MonitorContext>),
    /// Auxilliary information for reprocessing.
    #[metastructure(omit_from_schema)]
    Reprocessing(Box<ReprocessingContext>),
    /// Response information.
    Response(Box<ResponseContext>),
    /// OpenTelemetry information.
    Otel(Box<OtelContext>),
    /// Cloud resource information.
    CloudResource(Box<CloudResourceContext>),
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(fallback_variant)]
    Other(#[metastructure(pii = "true")] Object<Value>),
}

/// Trait to get the Context both from the user agent string and also the new client hints.
/// With an automatically derived function which tries to first get the context from client hints,
/// if that fails it tries for the user agent string.
pub trait FromUserAgentInfo: Sized {
    fn parse_client_hints(client_hints: &ClientHints<&str>) -> Option<Self>;
    fn parse_user_agent(user_agent: &str) -> Option<Self>;

    fn from_hints_or_ua(raw_info: &RawUserAgentInfo<&str>) -> Option<Self> {
        Self::parse_client_hints(&raw_info.client_hints)
            .or_else(|| raw_info.user_agent.and_then(Self::parse_user_agent))
    }
}

#[derive(Clone, Debug, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct ContextInner(#[metastructure(bag_size = "large")] pub Context);

impl From<Context> for ContextInner {
    fn from(c: Context) -> ContextInner {
        ContextInner(c)
    }
}

/// The Contexts interface provides additional context data. Typically, this is data related to the
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
    /// Creates an empty contexts map.
    pub fn new() -> Contexts {
        Contexts(Object::new())
    }

    /// Inserts a context under the default key for the context.
    pub fn add<C>(&mut self, context: C)
    where
        C: DefaultContext,
    {
        self.insert(C::default_key().to_owned(), context.into_context());
    }

    /// Inserts a context under a custom given key.
    ///
    /// By convention, every typed context has a default key. Use [`add`](Self::add) to insert such
    /// contexts, instead.
    pub fn insert(&mut self, key: String, context: Context) {
        self.0.insert(key, Annotated::new(ContextInner(context)));
    }

    /// Returns `true` if a matching context resides in the map at its default key.
    pub fn contains<C>(&self) -> bool
    where
        C: DefaultContext,
    {
        // Use `get` to perform a type check.
        self.get::<C>().is_some()
    }

    /// Returns `true` if a context with the provided key is present in the map.
    ///
    /// By convention, every typed context has a default key. Use [`contains`](Self::contains) to
    /// check such contexts, instead.
    pub fn contains_key<S>(&self, key: S) -> bool
    where
        S: AsRef<str>,
    {
        self.0.contains_key(key.as_ref())
    }

    /// Returns the context at its default key or constructs it if not present.
    pub fn get_or_default<C>(&mut self) -> &mut C
    where
        C: DefaultContext,
    {
        if !self.contains::<C>() {
            self.add(C::default());
        }

        self.get_mut().unwrap()
    }

    /// Returns the context at the specified key or constructs it if not present.
    ///
    /// By convention, every typed context has a default key. Use
    /// [`get_or_default`](Self::get_or_default) to insert such contexts, instead.
    pub fn get_or_insert_with<F, S>(&mut self, key: S, context_builder: F) -> &mut Context
    where
        F: FnOnce() -> Context,
        S: Into<String>,
    {
        &mut self
            .0
            .entry(key.into())
            .or_insert_with(Annotated::empty)
            .value_mut()
            .get_or_insert_with(|| ContextInner(context_builder()))
            .0
    }

    /// Returns a reference to the default context by type.
    pub fn get<C>(&self) -> Option<&C>
    where
        C: DefaultContext,
    {
        C::cast(self.get_key(C::default_key())?)
    }

    /// Returns a mutable reference to the default context by type.
    pub fn get_mut<C>(&mut self) -> Option<&mut C>
    where
        C: DefaultContext,
    {
        C::cast_mut(self.get_key_mut(C::default_key())?)
    }

    /// Returns a reference to the context specified by `key`.
    ///
    /// By convention, every typed context has a default key. Use [`get`](Self::get) to retrieve
    /// such contexts, instead.
    pub fn get_key<S>(&self, key: S) -> Option<&Context>
    where
        S: AsRef<str>,
    {
        Some(&self.0.get(key.as_ref())?.value().as_ref()?.0)
    }

    /// Returns a mutable reference to the context specified by `key`.
    ///
    /// By convention, every typed context has a default key. Use [`get_mut`](Self::get_mut) to
    /// retrieve such contexts, instead.
    pub fn get_key_mut<S>(&mut self, key: S) -> Option<&mut Context>
    where
        S: AsRef<str>,
    {
        Some(&mut self.0.get_mut(key.as_ref())?.value_mut().as_mut()?.0)
    }

    /// Removes a context from the map, returning the context it was previously in the map.
    ///
    /// Returns `Some` if a matching context was removed from the default key. If the context at the
    /// default key does not have a matching type, it is removed but `None` is returned.
    pub fn remove<C>(&mut self) -> Option<C>
    where
        C: DefaultContext,
    {
        let context = self.remove_key(C::default_key())?;
        C::from_context(context)
    }

    /// Removes a context from the map, returning the context it was previously in the map.
    ///
    /// By convention, every typed context has a default key. Use [`remove`](Self::remove) to
    /// retrieve such contexts, instead.
    pub fn remove_key<S>(&mut self, key: S) -> Option<Context>
    where
        S: AsRef<str>,
    {
        let inner = self.0.remove(key.as_ref())?;
        Some(inner.into_value()?.0)
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

/// A well-known context in the [`Contexts`] interface.
///
/// These contexts have a [default key](Self::default_key) in the contexts map and can be
/// constructed as an empty default value.
pub trait DefaultContext: Default {
    /// The default key at which this context resides in [`Contexts`].
    fn default_key() -> &'static str;

    /// Converts this context type from a generic context type.
    ///
    /// Returns `Some` if the context is of this type. Otherwise, returns `None`.
    fn from_context(context: Context) -> Option<Self>;

    /// Casts a reference to this context type from a generic context type.
    ///
    /// Returns `Some` if the context is of this type. Otherwise, returns `None`.
    fn cast(context: &Context) -> Option<&Self>;

    /// Casts a mutable reference to this context type from a generic context type.
    ///
    /// Returns `Some` if the context is of this type. Otherwise, returns `None`.
    fn cast_mut(context: &mut Context) -> Option<&mut Self>;

    /// Boxes this context type in the generic context wrapper.
    ///
    /// Returns `Some` if the context is of this type. Otherwise, returns `None`.
    fn into_context(self) -> Context;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::{ProcessingState, Processor};
    use crate::protocol::Event;
    use crate::types::{Map, Meta, ProcessingResult};

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

        let mut map = Contexts::new();
        map.add(OsContext {
            name: Annotated::new("Linux".to_string()),
            ..Default::default()
        });

        assert_eq!(Annotated::new(map), Annotated::from_json(json).unwrap());
    }

    #[test]
    fn test_multiple_contexts_roundtrip() {
        let json =
            r#"{"os":{"name":"Linux","type":"os"},"runtime":{"name":"rustc","type":"runtime"}}"#;

        let mut map = Contexts::new();
        map.add(OsContext {
            name: Annotated::new("Linux".to_string()),
            ..Default::default()
        });
        map.add(RuntimeContext {
            name: Annotated::new("rustc".to_string()),
            ..Default::default()
        });

        let contexts = Annotated::new(map);
        assert_eq!(contexts, Annotated::from_json(json).unwrap());
        assert_eq!(json, contexts.to_json().unwrap());
    }

    #[test]
    fn test_context_processing() {
        let mut event = Annotated::new(Event {
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(RuntimeContext {
                    name: Annotated::new("php".to_owned()),
                    version: Annotated::new("7.1.20-1+ubuntu16.04.1+deb.sury.org+1".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
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
