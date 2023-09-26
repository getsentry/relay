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
///  - strings (with `ignore_case` flag)
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

/// Combines multiple conditions using logical OR.
///
/// This condition matches if **any** of the inner conditions matches.
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
/// This condition matches if **all** of the inner conditions matches.
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

/// A condition from a sampling rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "op")]
pub enum RuleCondition {
    /// A condition that compares values for equality.
    Eq(EqCondition),
    /// A condition that applies `>=`.
    Gte(GteCondition),
    /// A condition that applies `<=`.
    Lte(LteCondition),
    /// A condition that applies `>`.
    Gt(GtCondition),
    /// A condition that applies `<`.
    Lt(LtCondition),
    /// A condition that uses glob matching.
    Glob(GlobCondition),
    /// Combines multiple conditions using logical OR.
    Or(OrCondition),
    /// Combines multiple conditions using logical AND.
    And(AndCondition),
    /// Applies logical NOT to a condition.
    Not(NotCondition),
    /// An unsupported condition for future compatibility.
    #[serde(other)]
    Unsupported,
}

impl RuleCondition {
    /// Returns a condition that matches everything.
    pub fn all() -> Self {
        Self::And(AndCondition { inner: Vec::new() })
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use relay_base_schema::project::ProjectKey;
    use uuid::Uuid;

    use crate::dsc::TraceUserContext;
    use crate::tests::{and, eq, glob, not, or};
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
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                    eq("trace.transaction", &["transaction1"], true),
                ]),
            ),
            (
                "glob releases",
                and(vec![
                    glob("trace.release", &["1.*"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "glob transaction",
                and(vec![glob("trace.transaction", &["trans*"])]),
            ),
            (
                "multiple releases",
                and(vec![
                    glob("trace.release", &["2.1.1", "1.1.*"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "multiple user segments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["paid", "vip", "free"], true),
                ]),
            ),
            (
                "multiple transactions",
                and(vec![glob("trace.transaction", &["t22", "trans*", "t33"])]),
            ),
            (
                "case insensitive user segments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["ViP", "FrEe"], true),
                ]),
            ),
            (
                "multiple user environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq(
                        "trace.environment",
                        &["integration", "debug", "production"],
                        true,
                    ),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "case insensitive environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["DeBuG", "PrOd"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "all environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "undefined environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user.segment", &["vip"], true),
                ]),
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
                or(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "first",
                true,
                or(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            (
                "second",
                true,
                or(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "none",
                false,
                or(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            ("empty", false, or(vec![])),
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
                and(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "first",
                false,
                and(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            (
                "second",
                false,
                and(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "none",
                false,
                and(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            ("empty", true, RuleCondition::all()),
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
                not(eq("trace.environment", &["debug"], true)),
            ),
            (
                "not false",
                true,
                not(eq("trace.environment", &["prod"], true)),
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
                and(vec![
                    glob("trace.release", &["1.1.2"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "user segment",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["all"], true),
                ]),
            ),
            (
                "environment",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "transaction",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    glob("trace.transaction", &["t22"]),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
        ];

        let dsc = dsc_dummy();

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(!condition.matches(&dsc), "{failure_name}");
        }
    }
}
