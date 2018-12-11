//! PII stripping processor.
mod builtin;
mod config;
mod processor;
mod redactions;

pub use self::builtin::{BUILTIN_RULES, BUILTIN_SELECTORS};
pub use self::config::{
    AliasRule, AliasSelector, KindSelector, MultipleRule, MultipleSelector, PathSelector, Pattern,
    PatternRule, PiiConfig, RedactPairRule, RuleClassification, RuleSpec, RuleType, SelectorType,
    Vars,
};
pub use self::processor::PiiProcessor;
pub use self::redactions::{
    HashAlgorithm, HashRedaction, MaskRedaction, Redaction, ReplaceRedaction,
};
