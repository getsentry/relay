//! PII stripping processor.

mod builtin;
mod config;
mod convert;
mod legacy;
mod processor;
mod redactions;
mod traits;

pub use self::builtin::{BUILTIN_RULES, BUILTIN_SELECTORS};
pub use self::config::{
    AliasRule, MultipleRule, Pattern, PatternRule, PiiConfig, RedactPairRule, RuleSpec, RuleType,
    Vars,
};
pub use self::legacy::DataScrubbingConfig;
pub use self::processor::PiiProcessor;
pub use self::redactions::{
    HashAlgorithm, HashRedaction, MaskRedaction, Redaction, ReplaceRedaction,
};
pub use self::traits::PiiAttrs;
