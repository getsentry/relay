use regex::Regex;
use regex::bytes::Regex as BytesRegex;
use smallvec::SmallVec;
use std::collections::BTreeSet;

use crate::pii::{CompiledPiiConfig, Redaction};
use crate::pii::compiledconfig::RuleRef;
use crate::pii::utils::in_range;
use crate::pii::regexes::{get_regex_for_rule_type, PatternType, ReplaceBehavior, ANYTHING_REGEX};
use crate::processor::{ProcessingState, ValueType};

lazy_static::lazy_static! {
    // TODO: This could be const
    static ref ATTACHMENT_STATE: ProcessingState<'static> = ProcessingState::root()
        .enter_static("attachments", None, None);
}

fn plain_attachment_state(filename: &str) -> ProcessingState<'_> {
    ATTACHMENT_STATE.enter_borrowed(filename, None, Some(ValueType::Binary))
}

pub struct PiiAttachmentsProcessor<'a> {
    compiled_config: &'a CompiledPiiConfig,
}

impl<'a> PiiAttachmentsProcessor<'a> {
    pub fn new(compiled_config: &'a CompiledPiiConfig) -> PiiAttachmentsProcessor<'a> {
        // this constructor needs to be cheap... a new PiiProcessor is created for each event. Move
        // any init logic into CompiledPiiConfig::new.
        PiiAttachmentsProcessor { compiled_config }
    }

    pub fn scrub_attachment_bytes(&self, filename: &str, data: &mut [u8]) {
        let state = plain_attachment_state(filename);

        for (selector, rules) in &self.compiled_config.applications {
            if state.path().matches_selector(&selector) {
                for rule in rules {
                    // Note:
                    //
                    // - We ignore pattern_type and just treat every regex like a value regex (i.e.
                    //   redactPair becomes pattern rule). Very unlikely anybody would want that
                    //   behavior (e.g.  "Remove passwords on **" would remove a file called
                    //   "passwords.txt", but also "author.txt").  Just use selectors!
                    //
                    // - We impose severe restrictions on how redaction methods work, as we must
                    //   not change the lengths of attachments.
                    for (_pattern_type, regex, replace_behavior) in get_regex_for_rule_type(&rule.ty) {
                        apply_regex_to_bytes(data, rule, regex, &replace_behavior);
                    }
                }
            }
        }
    }
}

fn apply_regex_to_bytes(data: &mut [u8], rule: &RuleRef, regex: &Regex, replace_behavior: &ReplaceBehavior) -> bool {
    let regex = match BytesRegex::new(regex.as_str()) {
        Ok(x) => x,
        Err(e) => {
            // XXX: This is not going to fly long-term
            // Idea: Disable unicode support for regexes entirely, that drastically increases the
            // likelihood this conversion will never fail.
            return false;
        }
    };

    let mut matches = SmallVec::<[(usize, usize); 1]>::new();

    for captures in regex.captures_iter(data) {
        for (idx, group) in captures.iter().enumerate() {
            if let Some(group) = group {
                match replace_behavior {
                    ReplaceBehavior::Groups(ref replace_groups) => {
                        if replace_groups.contains(&(idx as u8)) {
                            matches.push((group.start(), group.end()));
                        }
                    },
                    ReplaceBehavior::Value => {
                        matches.push((0, data.len()));
                        break;
                    }
                }
            }
        }
    }

    if matches.is_empty() {
        return false;
    }

    const DEFAULT_PADDING: u8 = b'x';

    match rule.redaction {
        Redaction::Default | Redaction::Remove => {
            for (start, end) in matches {
                for c in &mut data[start..end] {
                    *c = DEFAULT_PADDING;
                }
            }
        },
        Redaction::Mask(ref mask) => {
            let chars_to_ignore: BTreeSet<char> = mask.chars_to_ignore.chars().collect();
            let mask_char = if mask.mask_char.is_ascii() {
                mask.mask_char as u8
            } else {
                DEFAULT_PADDING
            };

            let match_slice = &mut data[start..end];
            for (start, end) in matches {
                for (idx, c) in match_slice.iter().enumerate() {
                    if in_range(mask.range, idx, match_slice.len()) && !chars_to_ignore.contains(&c) {
                        *c = mask_char;
                    }
                }
            }
        },
    }

    true
}
