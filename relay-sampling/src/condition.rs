//! Types to specify conditions on data.
//!
//! The root type is [`RuleCondition`].

use relay_common::glob3::GlobPatterns;
use relay_protocol::{Getter, Val};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

use crate::utils;

/// Options for [`EqCondition`].
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EqCondOptions {
    /// If `true`, string values are compared in case-insensitive mode.
    ///
    /// This has no effect on numeric or boolean comparisons.
    #[serde(default)]
    pub ignore_case: bool,
}

/// A condition that compares values for equality.
///
/// This operator supports:
///  - boolean
///  - strings, optionally ignoring ASCII-case
///  - UUIDs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EqCondition {
    /// Path of the field that should match the value.
    pub name: String,

    /// The value to check against.
    ///
    /// When comparing with a string field, this value can be an array. The condition matches if any
    /// of the provided values matches the field.
    pub value: Value,

    /// Configuration options for the condition.
    #[serde(default, skip_serializing_if = "utils::is_default")]
    pub options: EqCondOptions,
}

impl EqCondition {
    /// Creates a new condition that checks for equality.
    ///
    /// By default, this condition will perform a case-sensitive check. To ignore ASCII case, use
    /// [`EqCondition::ignore_case`].
    ///
    /// The main way to create this conditions is [`RuleCondition::eq`].
    pub fn new(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self {
            name: field.into(),
            value: value.into(),
            options: EqCondOptions { ignore_case: false },
        }
    }

    /// Enables case-insensitive comparisions for this rule.
    ///
    /// To create such a condition directly, use [`RuleCondition::eq_ignore_case`].
    pub fn ignore_case(mut self) -> Self {
        self.options.ignore_case = true;
        self
    }

    fn cmp(&self, left: &str, right: &str) -> bool {
        if self.options.ignore_case {
            unicase::eq(left, right)
        } else {
            left == right
        }
    }

    fn matches<T>(&self, instance: &T) -> bool
    where
        T: Getter + ?Sized,
    {
        match (instance.get_value(self.name.as_str()), &self.value) {
            (None, Value::Null) => true,
            (Some(Val::String(f)), Value::String(ref val)) => self.cmp(f, val),
            (Some(Val::String(f)), Value::Array(ref arr)) => arr
                .iter()
                .filter_map(|v| v.as_str())
                .any(|v| self.cmp(v, f)),
            (Some(Val::Uuid(f)), Value::String(ref val)) => Some(f) == val.parse().ok(),
            (Some(Val::Bool(f)), Value::Bool(v)) => f == *v,
            _ => false,
        }
    }
}

macro_rules! impl_cmp_condition {
    ($struct_name:ident, $operator:tt, $doc:literal) => {
        #[doc = $doc]
        ///
        /// Strings are explicitly not supported by this.
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        pub struct $struct_name {
            /// Path of the field that should match the value.
            pub name: String,
            /// The numeric value to check against.
            pub value: Number,
        }

        impl $struct_name {
            /// Creates a new condition that comparison condition.
            pub fn new(field: impl Into<String>, value: impl Into<Number>) -> Self {
                Self {
                    name: field.into(),
                    value: value.into(),
                }
            }

            fn matches<T>(&self, instance: &T) -> bool
            where
                T: Getter + ?Sized,
            {
                let Some(value) = instance.get_value(self.name.as_str()) else {
                    return false;
                };

                // Try various conversion functions in order of expensiveness and likelihood
                // - as_i64 is not really fast, but most values in sampling rules can be i64, so we
                //   could return early
                // - f64 is more likely to succeed than u64, but we might lose precision
                if let (Some(a), Some(b)) = (value.as_i64(), self.value.as_i64()) {
                    a $operator b
                } else if let (Some(a), Some(b)) = (value.as_u64(), self.value.as_u64()) {
                    a $operator b
                } else if let (Some(a), Some(b)) = (value.as_f64(), self.value.as_f64()) {
                    a $operator b
                } else {
                    false
                }
            }
        }
    }
}

impl_cmp_condition!(GteCondition, >=, "A condition that applies `>=`.");
impl_cmp_condition!(LteCondition, <=, "A condition that applies `<=`.");
impl_cmp_condition!(GtCondition, >, "A condition that applies `>`.");
impl_cmp_condition!(LtCondition, <, "A condition that applies `<`.");

/// A condition that uses glob matching.
///
/// This is similar to [`EqCondition`], but it allows for wildcards in `value`. This is slightly
/// more expensive to construct and check, so preferrably use [`EqCondition`] when no wildcard
/// matching is needed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GlobCondition {
    /// Path of the field that should match the value.
    pub name: String,
    /// A list of glob patterns to check.
    ///
    /// Note that this cannot be a single value, it must be a list of values.
    pub value: GlobPatterns,
}

impl GlobCondition {
    /// Creates a condition that matches one or more glob patterns.
    pub fn new(field: impl Into<String>, value: impl IntoStrings) -> Self {
        Self {
            name: field.into(),
            value: GlobPatterns::new(value.into_strings()),
        }
    }

    fn matches<T>(&self, instance: &T) -> bool
    where
        T: Getter + ?Sized,
    {
        match instance.get_value(self.name.as_str()) {
            Some(Val::String(s)) => self.value.is_match(s),
            _ => false,
        }
    }
}

/// A type that can be converted to a list of strings.
pub trait IntoStrings {
    /// Creates a list of strings from this type.
    fn into_strings(self) -> Vec<String>;
}

impl IntoStrings for &'_ str {
    fn into_strings(self) -> Vec<String> {
        vec![self.to_owned()]
    }
}

impl IntoStrings for String {
    fn into_strings(self) -> Vec<String> {
        vec![self]
    }
}

impl IntoStrings for std::borrow::Cow<'_, str> {
    fn into_strings(self) -> Vec<String> {
        vec![self.into_owned()]
    }
}

impl IntoStrings for &'_ [&'_ str] {
    fn into_strings(self) -> Vec<String> {
        self.iter().copied().map(str::to_owned).collect()
    }
}

impl IntoStrings for &'_ [String] {
    fn into_strings(self) -> Vec<String> {
        self.to_vec()
    }
}

impl IntoStrings for Vec<&'_ str> {
    fn into_strings(self) -> Vec<String> {
        self.into_iter().map(str::to_owned).collect()
    }
}

impl IntoStrings for Vec<String> {
    fn into_strings(self) -> Vec<String> {
        self
    }
}

/// Combines multiple conditions using logical OR.
///
/// This condition matches if **any** of the inner conditions match. The default value for this
/// condition is `false`, that is, this rule does not match if there are no inner conditions.
///
/// See [`RuleCondition::or`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrCondition {
    /// Inner rules to combine.
    pub inner: Vec<RuleCondition>,
}

impl OrCondition {
    fn supported(&self) -> bool {
        self.inner.iter().all(RuleCondition::supported)
    }

    fn matches<T>(&self, value: &T) -> bool
    where
        T: Getter + ?Sized,
    {
        self.inner.iter().any(|cond| cond.matches(value))
    }
}

/// Combines multiple conditions using logical AND.
///
/// This condition matches if **all** of the inner conditions match. The default value for this
/// condition is `true`, that is, this rule matches if there are no inner conditions.
///
/// See [`RuleCondition::and`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AndCondition {
    /// Inner rules to combine.
    pub inner: Vec<RuleCondition>,
}

impl AndCondition {
    fn supported(&self) -> bool {
        self.inner.iter().all(RuleCondition::supported)
    }
    fn matches<T>(&self, value: &T) -> bool
    where
        T: Getter + ?Sized,
    {
        self.inner.iter().all(|cond| cond.matches(value))
    }
}

/// Applies logical NOT to a condition.
///
/// This condition matches if the inner condition does not match.
///
/// See [`RuleCondition::negate`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotCondition {
    /// An inner rule to negate.
    pub inner: Box<RuleCondition>,
}

impl NotCondition {
    fn supported(&self) -> bool {
        self.inner.supported()
    }

    fn matches<T>(&self, value: &T) -> bool
    where
        T: Getter + ?Sized,
    {
        !self.inner.matches(value)
    }
}

/// A condition that can be evaluated on structured data.
///
/// The basic conditions are [`eq`](Self::eq), [`glob`](Self::glob), and the comparison operators.
/// These conditions compare a data field specified through a path with a value or a set of values.
/// If the field's value [matches](Self::matches) the values declared in the rule, the condition
/// returns `true`.
///
/// Conditions can be combined with the logical operators [`and`](Self::and), [`or`](Self::or), and
/// [`not` (negate)](Self::negate).
///
/// # Data Access
///
/// Rule conditions access data fields through the [`Getter`] trait. Note that getters always have a
/// root namespace which must be part of the field's path. If path's root component does not match
/// the one of the passed getter instance, the rule condition will not be able to retrieve data and
/// likely not match.
///
/// # Serialization
///
/// Conditions are represented as nested JSON objects. The condition type is declared in the `op`
/// field.
///
/// # Example
///
/// ```
/// use relay_sampling::condition::RuleCondition;
///
/// let condition = !RuleCondition::eq("obj.status", "invalid");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "op")]
pub enum RuleCondition {
    /// A condition that compares values for equality.
    ///
    /// This operator supports:
    ///  - boolean
    ///  - strings, optionally ignoring ASCII-case
    ///  - UUIDs
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::eq("obj.status", "invalid");
    /// ```
    Eq(EqCondition),

    /// A condition that applies `>=`.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::gte("obj.length", 10);
    /// ```
    Gte(GteCondition),

    /// A condition that applies `<=`.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::lte("obj.length", 10);
    /// ```
    Lte(LteCondition),

    /// A condition that applies `>`.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::gt("obj.length", 10);
    /// ```
    Gt(GtCondition),

    /// A condition that applies `<`.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::lt("obj.length", 10);
    /// ```
    Lt(LtCondition),

    /// A condition that uses glob matching.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::glob("obj.name", "error: *");
    /// ```
    Glob(GlobCondition),

    /// Combines multiple conditions using logical OR.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::eq("obj.status", "invalid")
    ///     | RuleCondition::eq("obj.status", "unknown");
    /// ```
    Or(OrCondition),

    /// Combines multiple conditions using logical AND.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::eq("obj.status", "invalid")
    ///     & RuleCondition::gte("obj.length", 10);
    /// ```
    And(AndCondition),

    /// Applies logical NOT to a condition.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = !RuleCondition::eq("obj.status", "invalid");
    /// ```
    Not(NotCondition),

    /// An unsupported condition for future compatibility.
    #[serde(other)]
    Unsupported,
}

impl RuleCondition {
    /// Returns a condition that always matches.
    pub fn all() -> Self {
        Self::And(AndCondition { inner: Vec::new() })
    }

    /// Returns a condition that never matches.
    pub fn never() -> Self {
        Self::Or(OrCondition { inner: Vec::new() })
    }

    /// Creates a condition that compares values for equality.
    ///
    /// This operator supports:
    ///  - boolean
    ///  - strings
    ///  - UUIDs
    ///
    /// # Examples
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// // Matches if the value is identical to the given string:
    /// let condition = RuleCondition::eq("obj.status", "invalid");
    ///
    /// // Matches if the value is identical to any of the given strings:
    /// let condition = RuleCondition::eq("obj.status", &["invalid", "unknown"][..]);
    ///
    /// // Matches a boolean flag:
    /// let condition = RuleCondition::eq("obj.valid", false);
    /// ```
    pub fn eq(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Eq(EqCondition::new(field, value))
    }

    /// Creates a condition that compares values for equality ignoring ASCII-case.
    ///
    /// # Examples
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// // Matches if the value is identical to the given string:
    /// let condition = RuleCondition::eq_ignore_case("obj.status", "invalid");
    ///
    /// // Matches if the value is identical to any of the given strings:
    /// let condition = RuleCondition::eq_ignore_case("obj.status", &["invalid", "unknown"][..]);
    /// ```
    pub fn eq_ignore_case(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Self::Eq(EqCondition::new(field, value).ignore_case())
    }

    /// Creates a condition that matches one or more glob patterns.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// // Match a single pattern:
    /// let condition = RuleCondition::glob("obj.name", "error: *");
    ///
    /// // Match any of a list of patterns:
    /// let condition = RuleCondition::glob("obj.name", &["error: *", "*failure*"][..]);
    /// ```
    pub fn glob(field: impl Into<String>, value: impl IntoStrings) -> Self {
        Self::Glob(GlobCondition::new(field, value))
    }

    /// Creates a condition that applies `>`.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::gt("obj.length", 10);
    /// ```
    pub fn gt(field: impl Into<String>, value: impl Into<Number>) -> Self {
        Self::Gt(GtCondition::new(field, value))
    }

    /// Creates a condition that applies `>=`.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::gte("obj.length", 10);
    /// ```
    pub fn gte(field: impl Into<String>, value: impl Into<Number>) -> Self {
        Self::Gte(GteCondition::new(field, value))
    }

    /// Creates a condition that applies `<`.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::lt("obj.length", 10);
    /// ```
    pub fn lt(field: impl Into<String>, value: impl Into<Number>) -> Self {
        Self::Lt(LtCondition::new(field, value))
    }

    /// Creates a condition that applies `<=`.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::lte("obj.length", 10);
    /// ```
    pub fn lte(field: impl Into<String>, value: impl Into<Number>) -> Self {
        Self::Lte(LteCondition::new(field, value))
    }

    /// Combines this condition and another condition with a logical AND operator.
    ///
    /// The short-hand operator for this combinator is `&`.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::eq("obj.status", "invalid")
    ///     & RuleCondition::gte("obj.length", 10);
    /// ```
    pub fn and(mut self, other: RuleCondition) -> Self {
        if let Self::And(ref mut condition) = self {
            condition.inner.push(other);
            self
        } else {
            Self::And(AndCondition {
                inner: vec![self, other],
            })
        }
    }

    /// Combines this condition and another condition with a logical OR operator.
    ///
    /// The short-hand operator for this combinator is `|`.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = RuleCondition::eq("obj.status", "invalid")
    ///     | RuleCondition::eq("obj.status", "unknown");
    /// ```
    pub fn or(mut self, other: RuleCondition) -> Self {
        if let Self::Or(ref mut condition) = self {
            condition.inner.push(other);
            self
        } else {
            Self::Or(OrCondition {
                inner: vec![self, other],
            })
        }
    }

    /// Negates this condition with logical NOT.
    ///
    /// The short-hand operator for this combinator is `!`.
    ///
    /// # Example
    ///
    /// ```
    /// use relay_sampling::condition::RuleCondition;
    ///
    /// let condition = !RuleCondition::eq("obj.status", "invalid");
    /// ```
    pub fn negate(self) -> Self {
        match self {
            Self::Not(condition) => *condition.inner,
            other => Self::Not(NotCondition {
                inner: Box::new(other),
            }),
        }
    }

    /// Checks if Relay supports this condition (in other words if the condition had any unknown configuration
    /// which was serialized as "Unsupported" (because the configuration is either faulty or was created for a
    /// newer relay that supports some other condition types)
    pub fn supported(&self) -> bool {
        match self {
            RuleCondition::Unsupported => false,
            // we have a known condition
            RuleCondition::Gte(_)
            | RuleCondition::Lte(_)
            | RuleCondition::Gt(_)
            | RuleCondition::Lt(_)
            | RuleCondition::Eq(_)
            | RuleCondition::Glob(_) => true,
            // dig down for embedded conditions
            RuleCondition::And(rules) => rules.supported(),
            RuleCondition::Or(rules) => rules.supported(),
            RuleCondition::Not(rule) => rule.supported(),
        }
    }

    /// Returns `true` if the rule matches the given value instance.
    pub fn matches<T>(&self, value: &T) -> bool
    where
        T: Getter + ?Sized,
    {
        match self {
            RuleCondition::Eq(condition) => condition.matches(value),
            RuleCondition::Lte(condition) => condition.matches(value),
            RuleCondition::Gte(condition) => condition.matches(value),
            RuleCondition::Gt(condition) => condition.matches(value),
            RuleCondition::Lt(condition) => condition.matches(value),
            RuleCondition::Glob(condition) => condition.matches(value),
            RuleCondition::And(conditions) => conditions.matches(value),
            RuleCondition::Or(conditions) => conditions.matches(value),
            RuleCondition::Not(condition) => condition.matches(value),
            RuleCondition::Unsupported => false,
        }
    }
}

impl std::ops::BitAnd for RuleCondition {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        self.and(rhs)
    }
}

impl std::ops::BitOr for RuleCondition {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.or(rhs)
    }
}

impl std::ops::Not for RuleCondition {
    type Output = Self;

    fn not(self) -> Self::Output {
        self.negate()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use relay_base_schema::project::ProjectKey;
    use uuid::Uuid;

    use crate::dsc::TraceUserContext;
    use crate::DynamicSamplingContext;

    use super::*;

    fn dsc_dummy() -> DynamicSamplingContext {
        DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        }
    }

    #[test]
    fn deserialize() {
        let serialized_rules = r#"[
            {
                "op":"eq",
                "name": "field_1",
                "value": ["UPPER","lower"],
                "options":{
                    "ignoreCase": true
                }
            },
            {
                "op":"eq",
                "name": "field_2",
                "value": ["UPPER","lower"]
            },
            {
                "op":"glob",
                "name": "field_3",
                "value": ["1.2.*","2.*"]
            },
            {
                "op":"not",
                "inner": {
                    "op":"glob",
                    "name": "field_4",
                    "value": ["1.*"]
                }
            },
            {
                "op":"and",
                "inner": [{
                    "op":"glob",
                    "name": "field_5",
                    "value": ["2.*"]
                }]
            },
            {
                "op":"or",
                "inner": [{
                    "op":"glob",
                    "name": "field_6",
                    "value": ["3.*"]
                }]
            }
        ]"#;

        let rules: Result<Vec<RuleCondition>, _> = serde_json::from_str(serialized_rules);
        assert!(rules.is_ok());
        let rules = rules.unwrap();
        insta::assert_ron_snapshot!(rules, @r#"
            [
              EqCondition(
                op: "eq",
                name: "field_1",
                value: [
                  "UPPER",
                  "lower",
                ],
                options: EqCondOptions(
                  ignoreCase: true,
                ),
              ),
              EqCondition(
                op: "eq",
                name: "field_2",
                value: [
                  "UPPER",
                  "lower",
                ],
              ),
              GlobCondition(
                op: "glob",
                name: "field_3",
                value: [
                  "1.2.*",
                  "2.*",
                ],
              ),
              NotCondition(
                op: "not",
                inner: GlobCondition(
                  op: "glob",
                  name: "field_4",
                  value: [
                    "1.*",
                  ],
                ),
              ),
              AndCondition(
                op: "and",
                inner: [
                  GlobCondition(
                    op: "glob",
                    name: "field_5",
                    value: [
                      "2.*",
                    ],
                  ),
                ],
              ),
              OrCondition(
                op: "or",
                inner: [
                  GlobCondition(
                    op: "glob",
                    name: "field_6",
                    value: [
                      "3.*",
                    ],
                  ),
                ],
              ),
            ]"#);
    }

    #[test]
    fn unsupported_rule_deserialize() {
        let bad_json = r#"{
            "op": "BadOperator",
            "name": "foo",
            "value": "bar"
        }"#;

        let rule: RuleCondition = serde_json::from_str(bad_json).unwrap();
        assert!(matches!(rule, RuleCondition::Unsupported));
    }

    #[test]
    /// test matching for various rules
    fn test_matches() {
        let conditions = [
            (
                "simple",
                RuleCondition::glob("trace.release", "1.1.1")
                    & RuleCondition::eq_ignore_case("trace.environment", "debug")
                    & RuleCondition::eq_ignore_case("trace.user.segment", "vip")
                    & RuleCondition::eq_ignore_case("trace.transaction", "transaction1"),
            ),
            (
                "glob releases",
                RuleCondition::glob("trace.release", "1.*")
                    & RuleCondition::eq_ignore_case("trace.environment", "debug")
                    & RuleCondition::eq_ignore_case("trace.user.segment", "vip"),
            ),
            (
                "glob transaction",
                RuleCondition::glob("trace.transaction", "trans*"),
            ),
            (
                "multiple releases",
                RuleCondition::glob("trace.release", vec!["2.1.1", "1.1.*"])
                    & RuleCondition::eq_ignore_case("trace.environment", "debug")
                    & RuleCondition::eq_ignore_case("trace.user.segment", "vip"),
            ),
            (
                "multiple user segments",
                RuleCondition::glob("trace.release", "1.1.1")
                    & RuleCondition::eq_ignore_case("trace.environment", "debug")
                    & RuleCondition::eq_ignore_case(
                        "trace.user.segment",
                        vec!["paid", "vip", "free"],
                    ),
            ),
            (
                "multiple transactions",
                RuleCondition::glob("trace.transaction", &["t22", "trans*", "t33"][..]),
            ),
            (
                "case insensitive user segments",
                RuleCondition::glob("trace.release", "1.1.1")
                    & RuleCondition::eq_ignore_case("trace.environment", "debug")
                    & RuleCondition::eq_ignore_case("trace.user.segment", &["ViP", "FrEe"][..]),
            ),
            (
                "multiple user environments",
                RuleCondition::glob("trace.release", "1.1.1")
                    & RuleCondition::eq_ignore_case(
                        "trace.environment",
                        &["integration", "debug", "production"][..],
                    )
                    & RuleCondition::eq_ignore_case("trace.user.segment", "vip"),
            ),
            (
                "case insensitive environments",
                RuleCondition::glob("trace.release", "1.1.1")
                    & RuleCondition::eq_ignore_case("trace.environment", &["DeBuG", "PrOd"][..])
                    & RuleCondition::eq_ignore_case("trace.user.segment", "vip"),
            ),
            (
                "all environments",
                RuleCondition::glob("trace.release", "1.1.1")
                    & RuleCondition::eq_ignore_case("trace.user.segment", "vip"),
            ),
            (
                "undefined environments",
                RuleCondition::glob("trace.release", "1.1.1")
                    & RuleCondition::eq_ignore_case("trace.user.segment", "vip"),
            ),
            ("match no conditions", RuleCondition::all()),
        ];

        let dsc = dsc_dummy();

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc), "{failure_name}");
        }
    }

    #[test]
    fn test_or_combinator() {
        let conditions = [
            (
                "both",
                true,
                RuleCondition::eq_ignore_case("trace.environment", "debug")
                    | RuleCondition::eq_ignore_case("trace.user.segment", "vip"),
            ),
            (
                "first",
                true,
                RuleCondition::eq_ignore_case("trace.environment", "debug")
                    | RuleCondition::eq_ignore_case("trace.user.segment", "all"),
            ),
            (
                "second",
                true,
                RuleCondition::eq_ignore_case("trace.environment", "prod")
                    | RuleCondition::eq_ignore_case("trace.user.segment", "vip"),
            ),
            (
                "none",
                false,
                RuleCondition::eq_ignore_case("trace.environment", "prod")
                    | RuleCondition::eq_ignore_case("trace.user.segment", "all"),
            ),
            (
                "empty",
                false,
                RuleCondition::Or(OrCondition { inner: vec![] }),
            ),
            ("never", false, RuleCondition::never()),
        ];

        let dsc = dsc_dummy();

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc) == *expected, "{failure_name}");
        }
    }

    #[test]
    fn test_and_combinator() {
        let conditions = [
            (
                "both",
                true,
                RuleCondition::eq_ignore_case("trace.environment", "debug")
                    & RuleCondition::eq_ignore_case("trace.user.segment", "vip"),
            ),
            (
                "first",
                false,
                RuleCondition::eq_ignore_case("trace.environment", "debug")
                    & RuleCondition::eq_ignore_case("trace.user.segment", "all"),
            ),
            (
                "second",
                false,
                RuleCondition::eq_ignore_case("trace.environment", "prod")
                    & RuleCondition::eq_ignore_case("trace.user.segment", "vip"),
            ),
            (
                "none",
                false,
                RuleCondition::eq_ignore_case("trace.environment", "prod")
                    & RuleCondition::eq_ignore_case("trace.user.segment", "all"),
            ),
            (
                "empty",
                true,
                RuleCondition::And(AndCondition { inner: vec![] }),
            ),
            ("all", true, RuleCondition::all()),
        ];

        let dsc = dsc_dummy();

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc) == *expected, "{failure_name}");
        }
    }

    #[test]
    fn test_not_combinator() {
        let conditions = [
            (
                "not true",
                false,
                !RuleCondition::eq_ignore_case("trace.environment", "debug"),
            ),
            (
                "not false",
                true,
                !RuleCondition::eq_ignore_case("trace.environment", "prod"),
            ),
        ];

        let dsc = dsc_dummy();

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc) == *expected, "{failure_name}");
        }
    }

    #[test]
    /// test various rules that do not match
    fn test_does_not_match() {
        let conditions = [
            (
                "release",
                RuleCondition::glob("trace.release", "1.1.2")
                    & RuleCondition::eq_ignore_case("trace.environment", "debug")
                    & RuleCondition::eq_ignore_case("trace.user", "vip"),
            ),
            (
                "user segment",
                RuleCondition::glob("trace.release", "1.1.1")
                    & RuleCondition::eq_ignore_case("trace.environment", "debug")
                    & RuleCondition::eq_ignore_case("trace.user", "all"),
            ),
            (
                "environment",
                RuleCondition::glob("trace.release", "1.1.1")
                    & RuleCondition::eq_ignore_case("trace.environment", "prod")
                    & RuleCondition::eq_ignore_case("trace.user", "vip"),
            ),
            (
                "transaction",
                RuleCondition::glob("trace.release", "1.1.1")
                    & RuleCondition::glob("trace.transaction", "t22")
                    & RuleCondition::eq_ignore_case("trace.user", "vip"),
            ),
        ];

        let dsc = dsc_dummy();

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(!condition.matches(&dsc), "{failure_name}");
        }
    }
}
