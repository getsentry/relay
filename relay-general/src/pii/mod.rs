//! PII stripping processor.

mod attachments;
mod builtin;
mod compiledconfig;
mod config;
mod convert;
mod generate_selectors;
mod legacy;
mod minidumps;
mod processor;
mod redactions;
mod regexes;
mod utils;

pub use self::attachments::{PiiAttachmentsProcessor, ScrubEncodings};
pub use self::compiledconfig::CompiledPiiConfig;
pub use self::config::{
    AliasRule, MultipleRule, Pattern, PatternRule, PiiConfig, RedactPairRule, RuleSpec, RuleType,
    Vars,
};
pub use self::generate_selectors::selector_suggestions_from_value;
pub use self::legacy::DataScrubbingConfig;
pub use self::minidumps::ScrubMinidumpError;
pub use self::processor::PiiProcessor;
pub use self::redactions::{Redaction, ReplaceRedaction};
