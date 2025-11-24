/// Adds an attribute to the attributes map if the value is Some.
///
/// # Example
/// ```ignore
/// let mut attributes = Attributes::default();
/// add_optional_attribute!(attributes, "key", Some("value"));
/// ```
#[macro_export]
macro_rules! add_optional_attribute {
    ($attributes:expr, $name:expr, $value:expr) => {{
        if let Some(value) = $value {
            $attributes.insert($name.to_owned(), value);
        }
    }};
}

/// Adds an attribute to the attributes map.
///
/// # Example
/// ```ignore
/// let mut attributes = Attributes::default();
/// add_attribute!(attributes, "key", "value");
/// ```
#[macro_export]
macro_rules! add_attribute {
    ($attributes:expr, $name:expr, $value:expr) => {{
        let val = $value;
        $attributes.insert($name.to_owned(), val);
    }};
}
