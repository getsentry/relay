//! Contains type definitions for the information we want to provide about attributes.
#![expect(dead_code)]

// TODO: Possibly replace this with an existing type
/// Whether an attribute can contain PII.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Pii {
    /// The field will be stripped by default
    True,
    /// The field will only be stripped when addressed with a specific path selector, but generic
    /// selectors such as `$string` do not apply.
    Maybe,
    /// The field cannot be stripped at all
    False,
}

/// The type of an attribute value.
#[derive(Debug, Clone, Copy)]
pub(crate) enum AttributeType {
    String,
    Boolean,
    Integer,
    Double,
    StringArray,
    BooleanArray,
    IntegerArray,
    DoubleArray,
}

/// How this attribute should be normalized.
#[derive(Debug, Clone, Copy)]
pub enum WriteBehavior {
    /// Save the attribute under its current name.
    ///
    /// This is the only choice for attributes that aren't deprecated.
    CurrentName,
    /// Save the attribute under its replacement name instead.
    NewName(&'static str),
    /// Save the attribute under both its current name and
    /// its replacement name.
    BothNames(&'static str),
}

#[derive(Debug, Clone)]
pub(crate) struct AttributeInfo {
    /// A text descrption of the attribute.
    pub(crate) description: &'static str,
    /// How this attribute should be saved.
    pub(crate) write_behavior: WriteBehavior,
    /// Whether this attribute can contain PII.
    pub(crate) pii: Pii,
    /// This attribute's type.
    pub(crate) ty: AttributeType,
}
