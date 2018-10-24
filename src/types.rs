use std::borrow::Cow;
use std::collections::BTreeMap;

use serde::ser::{SerializeMap, Serializer};

use meta::{Annotated, Meta, MetaTree, Value};
use processor::{
    FieldAttrs, MetaStructure, ProcessingState, Processor, SerializeMetaStructurePayload,
};

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
        let Annotated(value, meta) = value;
        if let Some(mut value) = value {
            const FIELD_ATTRS: FieldAttrs = FieldAttrs {
                name: Some("values"),
                required: false,
                cap_size: None,
                pii_kind: None,
            };
            value.values = MetaStructure::process(
                value.values,
                processor,
                state.enter_static("values", Some(Cow::Borrowed(&FIELD_ATTRS))),
            );
            value.other = value
                .other
                .into_iter()
                .map(|(key, value)| {
                    let value = MetaStructure::process(
                        value,
                        processor,
                        state.enter_borrowed(key.as_str(), None),
                    );
                    (key, value)
                }).collect();
            Annotated(Some(value), meta)
        } else {
            Annotated(None, meta)
        }
    }
}
