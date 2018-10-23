use std::collections::BTreeMap;

use serde::ser::{SerializeMap, Serializer};
use uuid::Uuid;

use meta::{Annotated, Meta, MetaTree, Value};
use processor::{MetaStructure, ProcessingState, Processor, SerializeMetaStructurePayload};

/// Alias for typed arrays.
pub type Array<T> = Vec<Annotated<T>>;
/// Alias for typed objects.
pub type Object<T> = BTreeMap<String, Annotated<T>>;

#[derive(Clone, Debug)]
pub struct Values<T> {
    /// The values of the collection.
    pub values: Annotated<Array<T>>,
    /// Additional arbitrary fields for forwards compatibility.
    pub other: Object<Value>,
}

impl<T: MetaStructure> MetaStructure for Values<T> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Array(items)), meta) => Annotated(
                Some(Values {
                    values: Annotated(
                        Some(items.into_iter().map(MetaStructure::from_value).collect()),
                        meta,
                    ),
                    other: Default::default(),
                }),
                Meta::default(),
            ),
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(Some(Value::Object(mut obj)), mut meta) => {
                if let Some(values) = obj.remove("values") {
                    Annotated(
                        Some(Values {
                            values: MetaStructure::from_value(values),
                            other: obj,
                        }),
                        meta,
                    )
                } else {
                    meta.add_error("expected array or values".to_string());
                    Annotated(None, meta)
                }
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(_, mut meta) => {
                meta.add_error("expected array or values".to_string());
                Annotated(None, meta)
            }
        }
    }

    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => {
                let mut rv = value.other;
                rv.insert("values".to_string(), MetaStructure::to_value(value.values));
                Annotated(Some(Value::Object(rv)), meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        match value {
            &Annotated(Some(ref values), _) => {
                let mut map_ser = s.serialize_map(None)?;
                map_ser.serialize_key("values")?;
                map_ser.serialize_value(&SerializeMetaStructurePayload(&values.values))?;
                for (key, value) in values.other.iter() {
                    map_ser.serialize_key(key)?;
                    map_ser.serialize_value(&SerializeMetaStructurePayload(value))?;
                }
                map_ser.end()
            }
            &Annotated(None, _) => s.serialize_unit(),
        }
    }

    fn extract_meta_tree(value: &Annotated<Self>) -> MetaTree
    where
        Self: Sized,
    {
        let mut meta_tree = MetaTree {
            meta: value.1.clone(),
            children: Default::default(),
        };
        if let Some(ref value) = value.0 {
            let tree = MetaStructure::extract_meta_tree(&value.values);
            if !tree.is_empty() {
                meta_tree.children.insert("values".to_string(), tree);
            }
            for (key, value) in value.other.iter() {
                let tree = MetaStructure::extract_meta_tree(value);
                if !tree.is_empty() {
                    meta_tree.children.insert(key.to_string(), tree);
                }
            }
        }
        meta_tree
    }

    fn process<P: Processor>(
        value: Annotated<Self>,
        processor: &P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        // TODO: implement processing
        let _processor = processor;
        let _state = state;
        value
    }
}

#[derive(Debug, Clone, MetaStructure)]
#[metastructure(process_func = "process_event")]
pub struct Event {
    #[metastructure(field = "event_id")]
    pub id: Annotated<Uuid>,
    pub exceptions: Annotated<Array<Exception>>,
}

#[derive(Debug, Clone, MetaStructure)]
#[metastructure(process_func = "process_stacktrace")]
pub struct Stacktrace {
    pub frames: Annotated<Array<Frame>>,
}

#[derive(Debug, Clone, MetaStructure)]
#[metastructure(process_func = "process_frame")]
pub struct Frame {
    pub function: Annotated<String>,
}

#[derive(Debug, Clone, MetaStructure)]
#[metastructure(process_func = "process_exception")]
pub struct Exception {
    #[metastructure(field = "type", required = "true")]
    pub ty: Annotated<String>,
    #[metastructure(cap_size = "summary")]
    pub value: Annotated<String>,
    #[metastructure(cap_size = "symbol")]
    pub module: Annotated<String>,
    #[metastructure(legacy_alias = "sentry.interfaces.Stacktrace")]
    pub stacktrace: Annotated<Stacktrace>,
    pub raw_stacktrace: Annotated<Stacktrace>,
    #[metastructure(additional_properties)]
    pub other: BTreeMap<String, Annotated<Value>>,
}
