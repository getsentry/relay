//! PII stripping processor.
mod builtin;
mod config;
mod processor;
mod redactions;
mod rules;
mod text;

pub use self::builtin::BUILTIN_RULES;
pub use self::config::{PiiConfig, Vars};
pub use self::processor::PiiProcessor;
pub use self::redactions::{
    HashAlgorithm, HashRedaction, MaskRedaction, Redaction, ReplaceRedaction,
};
pub use self::rules::{
    AliasRule, MultipleRule, Pattern, PatternRule, RedactPairRule, Rule, RuleType,
};
