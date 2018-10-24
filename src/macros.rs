macro_rules! numeric_meta_structure {
    ($type:ident, $meta_type:ident, $expectation:expr) => {
        impl FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::U64(value)), meta) => {
                        Annotated(Some(value as $type), meta)
                    }
                    Annotated(Some(Value::I64(value)), meta) => {
                        Annotated(Some(value as $type), meta)
                    }
                    Annotated(Some(Value::F64(value)), meta) => {
                        Annotated(Some(value as $type), meta)
                    }
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(_, mut meta) => {
                        meta.add_error(format!("expected {}", $expectation));
                        Annotated(None, meta)
                    }
                }
            }
        }

        impl ToValue for $type {
            #[inline(always)]
            fn to_value(value: Annotated<Self>) -> Annotated<Value> {
                match value {
                    Annotated(Some(value), meta) => Annotated(Some(Value::$meta_type(value)), meta),
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }
            #[inline(always)]
            fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: Serializer,
            {
                let &Annotated(ref value, _) = value;
                Serialize::serialize(value, s)
            }
        }

        impl ProcessValue for $type {}
    };
}

macro_rules! primitive_meta_structure_through_string {
    ($type:ident, $expectation:expr) => {
        impl FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                        Ok(value) => Annotated(Some(value), meta),
                        Err(err) => {
                            meta.add_error(err.to_string());
                            Annotated(None, meta)
                        }
                    },
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(_, mut meta) => {
                        meta.add_error(format!("expected {}", $expectation));
                        Annotated(None, meta)
                    }
                }
            }
        }
        impl ToValue for $type {
            fn to_value(value: Annotated<Self>) -> Annotated<Value> {
                match value {
                    Annotated(Some(value), meta) => {
                        Annotated(Some(Value::String(value.to_string())), meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }
            fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: ::serde::ser::Serializer,
            {
                let &Annotated(ref value, _) = value;
                if let &Some(ref value) = value {
                    ::serde::ser::Serialize::serialize(&value.to_string(), s)
                } else {
                    ::serde::ser::Serialize::serialize(&(), s)
                }
            }
        }

        impl ProcessValue for $type {}
    };
}
