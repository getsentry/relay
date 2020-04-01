//! PII stripping processor.

mod builtin;
mod compiledconfig;
mod config;
mod convert;
mod legacy;
mod processor;
mod redactions;

pub use self::builtin::BUILTIN_RULES;
pub use self::compiledconfig::CompiledPiiConfig;
pub use self::config::{
    AliasRule, MultipleRule, Pattern, PatternRule, PiiConfig, RedactPairRule, RuleSpec, RuleType,
    Vars,
};
pub use self::legacy::DataScrubbingConfig;
pub use self::processor::PiiProcessor;
pub use self::redactions::{
    HashAlgorithm, HashRedaction, MaskRedaction, Redaction, ReplaceRedaction,
};
