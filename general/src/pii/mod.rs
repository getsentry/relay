//! PII stripping processor.
mod builtin;
mod processor;
mod redactions;
mod rules;
mod text;

pub use self::processor::{PiiConfig, PiiProcessor, Vars};
pub use self::redactions::{
    HashAlgorithm, HashRedaction, MaskRedaction, Redaction, ReplaceRedaction,
};
pub use self::rules::{
    AliasRule, MultipleRule, Pattern, PatternRule, RedactPairRule, Rule, RuleType,
};
pub use self::builtin::BUILTIN_RULES;
