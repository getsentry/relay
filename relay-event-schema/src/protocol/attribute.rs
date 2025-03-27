use crate::processor::ProcessValue;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Value};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[metastructure(trim = false)]
pub struct Attribute {
    #[metastructure(field = "value", pii = "true", required = true)]
    value: Annotated<Value>,
}

#[derive(Clone, Debug, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub enum AttributeValue {
    String { value: Annotated<String> },
    Int { value: Annotated<i64> },
    Double { value: Annotated<f64> },
    Bool { value: Annotated<bool> },
}
