use relay_event_schema::protocol::{Attribute, Attributes, SpanData};
use relay_protocol::{Annotated, Object, Value};

/// An attribute collection which is keyed by an attribute key defined in [`relay_conventions`].
///
/// This exists as a common abstraction over [`SpanData`] and [`Attributes`]. Its purpose is to
/// allow modern attribute based normalizations also to apply to transaction based spans,
/// eliminating the need to duplicate code.
///
/// This largely mirrors the API of [`Attributes`] and provides it also for [`SpanData`].
pub trait AttributesLike {
    /// The attribute value.
    type Value: AttributeLike;

    /// Access this container through the underlying [`Object`].
    fn as_object(&self) -> &Object<Self::Value>;
    /// Access this container through the underlying [`Object`] mutably.
    fn as_object_mut(&mut self) -> &mut Object<Self::Value>;

    /// Checks whether this collection contains an attribute with the given `key`.
    fn contains_key(&self, key: &str) -> bool {
        self.as_object().contains_key(key)
    }

    /// Inserts an attribute with the given value into the collection.
    fn insert(&mut self, key: String, value: Annotated<Self::Value>) {
        self.as_object_mut().insert(key, value);
    }

    /// Removes a `key` from the attribute collection returning its value.
    fn remove(&mut self, key: &str) -> Option<Annotated<Self::Value>> {
        self.as_object_mut().remove(key)
    }
}

impl AttributesLike for SpanData {
    type Value = Value;

    fn as_object(&self) -> &Object<Self::Value> {
        &self.other
    }

    fn as_object_mut(&mut self) -> &mut Object<Self::Value> {
        &mut self.other
    }
}

impl AttributesLike for Attributes {
    type Value = Attribute;

    fn as_object(&self) -> &Object<Self::Value> {
        &self.0
    }

    fn as_object_mut(&mut self) -> &mut Object<Self::Value> {
        &mut self.0
    }
}

/// An attribute value stored in [`AttributesLike`].
///
/// This only allows read only accessors as mutating the attribute value may not be allowed.
/// For example [`Attribute`] has a `type` field which must match its `value field.
pub trait AttributeLike: Clone + From<String> + From<i64> + From<f64> {
    /// Returns a reference to the stored value in the attribute.
    fn as_value(&self) -> Option<&Value>;

    /// Returns the stored value as a `str`.
    ///
    /// Is `None` when the stored value is not a string.
    fn as_str(&self) -> Option<&str> {
        self.as_value().and_then(Value::as_str)
    }

    /// Returns the stored value as a `f64`.
    ///
    /// Is `None` when the stored value is not a float.
    fn as_f64(&self) -> Option<f64> {
        self.as_value().and_then(Value::as_f64)
    }
}

impl AttributeLike for Value {
    fn as_value(&self) -> Option<&Value> {
        Some(self)
    }
}

impl AttributeLike for Attribute {
    fn as_value(&self) -> Option<&Value> {
        self.value.value.value()
    }
}
