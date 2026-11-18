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

    /// Returns an attribute value.
    fn get_value(&self, key: &str) -> Option<&Value> {
        self.as_object()
            .get(key)
            .and_then(Annotated::value)
            .and_then(AttributeLike::as_value)
    }

    /// Returns an attribute value, preferring its current name over deprecated names.
    fn get_value_with_deprecations(&self, key: &str) -> Option<&Value> {
        relay_conventions::attribute_names(key).find_map(|name| self.get_value(name.as_ref()))
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_lookup<T: AttributesLike + Default>(
        pairs: &[(&str, &str)],
        key: &str,
        expected: Option<&str>,
    ) {
        let mut attributes = T::default();
        for (key, value) in pairs {
            attributes.insert(
                (*key).to_owned(),
                Annotated::new(T::Value::from((*value).to_owned())),
            );
        }
        assert_eq!(
            attributes
                .get_value_with_deprecations(key)
                .and_then(Value::as_str),
            expected
        );
    }

    fn assert_both(pairs: &[(&str, &str)], key: &str, expected: Option<&str>) {
        assert_lookup::<Attributes>(pairs, key, expected);
        assert_lookup::<SpanData>(pairs, key, expected);
    }

    #[test]
    fn lookup_prefers_current_then_ordered_aliases() {
        let pairs = [
            ("http.request.method", "current"),
            ("http.method", "first"),
            ("http.request_method", "second"),
            ("method", "third"),
        ];
        for offset in 0..pairs.len() {
            for (key, _) in pairs {
                assert_both(&pairs[offset..], key, Some(pairs[offset].1));
            }
        }
        assert_both(&[], "http.request.method", None);
    }

    #[test]
    fn lookup_handles_dynamic_unknown_and_no_status_names() {
        assert_both(
            &[("url.path.params.id", "123")],
            "url.path.parameter.id",
            Some("123"),
        );
        assert_both(
            &[("custom.attribute", "custom")],
            "custom.attribute",
            Some("custom"),
        );
        assert_both(&[], "custom.attribute", None);
        assert_both(&[("route", "/users")], "http.route", Some("/users"));
    }

    #[test]
    fn lookup_excludes_transforms_and_incompatible_replacements() {
        for (old, current) in [
            ("gen_ai.request.messages", "gen_ai.input.messages"),
            ("hardwareConcurrency", "device.processor_count"),
            ("db.sql.bindings", "db.query.parameter.id"),
        ] {
            assert_both(&[(old, "old")], current, None);
            assert_both(&[(current, "current")], old, None);
        }
    }

    fn check_missing_and_present_values<T: AttributesLike + Default>() {
        let mut attributes = T::default();
        attributes.insert(
            "http.method".to_owned(),
            Annotated::new(T::Value::from("POST".to_owned())),
        );
        attributes.insert("http.request.method".to_owned(), Annotated::empty());
        assert_eq!(
            attributes
                .get_value_with_deprecations("http.request.method")
                .and_then(Value::as_str),
            Some("POST")
        );
        attributes.insert(
            "http.request.method".to_owned(),
            Annotated::new(T::Value::from(String::new())),
        );
        assert_eq!(
            attributes
                .get_value_with_deprecations("http.request.method")
                .and_then(Value::as_str),
            Some("")
        );
        attributes.insert(
            "http.request.method".to_owned(),
            Annotated::new(T::Value::from(42_i64)),
        );
        assert_eq!(
            attributes.get_value_with_deprecations("http.request.method"),
            Some(&Value::I64(42))
        );
    }

    #[test]
    fn lookup_skips_missing_but_not_present_empty_or_invalid_values() {
        check_missing_and_present_values::<Attributes>();
        check_missing_and_present_values::<SpanData>();
        let attributes = Attributes::from([
            (
                "http.request.method".to_owned(),
                Annotated::new(Attribute::from(Annotated::<String>::empty())),
            ),
            (
                "http.method".to_owned(),
                Annotated::new(Attribute::from("POST")),
            ),
        ]);
        assert_eq!(
            AttributesLike::get_value_with_deprecations(&attributes, "http.request.method")
                .and_then(Value::as_str),
            Some("POST")
        );
    }
}
