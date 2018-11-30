macro_rules! primitive_process_value {
    ($type:ident, $process_func:ident) => {
        impl crate::processor::ProcessValue for $type {
            fn process_value<P: crate::processor::Processor>(
                value: crate::types::Annotated<$type>,
                processor: &mut P,
                state: crate::processor::ProcessingState,
            ) -> Annotated<$type> {
                processor.$process_func(value, state)
            }
        }
    };
}

macro_rules! primitive_to_value {
    ($type:ident, $meta_type:ident) => {
        impl crate::types::ToValue for $type {
            fn to_value(value: Annotated<Self>) -> Annotated<Value> {
                match value {
                    Annotated(Some(value), meta) => Annotated(Some(Value::$meta_type(value)), meta),
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }

            fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: serde::Serializer,
            {
                serde::Serialize::serialize(self, s)
            }
        }
    };
}

macro_rules! numeric_meta_structure {
    ($type:ident, $meta_type:ident, $expectation:expr) => {
        impl crate::types::FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                value.and_then(|value| {
                    let number = match value {
                        Value::U64(x) => num_traits::cast::cast(x),
                        Value::I64(x) => num_traits::cast::cast(x),
                        Value::F64(x) => num_traits::cast::cast(x),
                        _ => None,
                    };

                    match number {
                        Some(x) => Annotated::new(x),
                        None => {
                            let mut meta = crate::types::Meta::default();
                            meta.add_unexpected_value_error($expectation, value);
                            Annotated(None, meta)
                        }
                    }
                })
            }
        }

        primitive_to_value!($type, $meta_type);
    };
}

macro_rules! primitive_meta_structure_through_string {
    ($type:ident, $expectation:expr) => {
        impl crate::types::FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                        Ok(value) => Annotated(Some(value), meta),
                        Err(err) => {
                            meta.add_error(err.to_string(), Some(Value::String(value.to_string())));
                            Annotated(None, meta)
                        }
                    },
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(Some(value), mut meta) => {
                        meta.add_unexpected_value_error($expectation, value);
                        Annotated(None, meta)
                    }
                }
            }
        }

        impl crate::types::ToValue for $type {
            fn to_value(value: Annotated<Self>) -> Annotated<Value> {
                match value {
                    Annotated(Some(value), meta) => {
                        Annotated(Some(Value::String(value.to_string())), meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }
            fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: serde::ser::Serializer,
            {
                serde::ser::Serialize::serialize(&self.to_string(), s)
            }
        }
    };
}

macro_rules! primitive_meta_structure {
    ($type:ident, $meta_type:ident, $expectation:expr) => {
        impl crate::types::FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::$meta_type(value)), meta) => Annotated(Some(value), meta),
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(Some(value), mut meta) => {
                        meta.add_unexpected_value_error($expectation, value);
                        Annotated(None, meta)
                    }
                }
            }
        }

        primitive_to_value!($type, $meta_type);
    };
}
