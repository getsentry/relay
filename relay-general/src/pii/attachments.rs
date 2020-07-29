use regex::Regex;
use regex::bytes::Regex as BytesRegex;
use smallvec::{smallvec, SmallVec};
use std::collections::BTreeSet;

use crate::pii::{CompiledPiiConfig, Redaction};
use crate::pii::compiledconfig::RuleRef;
use crate::pii::utils::{in_range, hash_value};
use crate::pii::regexes::{get_regex_for_rule_type, ReplaceBehavior};
use crate::processor::{ProcessingState, ValueType};

lazy_static::lazy_static! {
    // TODO: This could be const
    static ref ATTACHMENT_STATE: ProcessingState<'static> = ProcessingState::root()
        .enter_static("attachments", None, None);
}

fn attachment_state(filename: &str, value_type: ValueType) -> ProcessingState<'_> {
    ATTACHMENT_STATE.enter_borrowed(filename, None, Some(value_type))
}

#[derive(Clone, Copy, Debug)]
pub enum AttachmentBytesType {
    PlainAttachment,
    MinidumpHeap,
    MinidumpStack,
}

impl AttachmentBytesType {
    fn get_states_by_filename(self, filename: &str) -> SmallVec<[ProcessingState<'_>; 3]> {
        let binary_state = attachment_state(filename, ValueType::Binary);
        let memory_state = attachment_state(filename, ValueType::Memory);
        let stack_state = attachment_state(filename, ValueType::StackMemory);

        match self {
            AttachmentBytesType::MinidumpStack => smallvec![
                binary_state,memory_state,stack_state
            ],
            AttachmentBytesType::MinidumpHeap => smallvec![
                binary_state,memory_state
            ],
            AttachmentBytesType::PlainAttachment => smallvec![
                binary_state
            ],
        }
    }
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

    pub fn scrub_attachment_bytes(&self, filename: &str, data: &mut [u8], bytes_type: AttachmentBytesType) -> bool {
        let states = bytes_type.get_states_by_filename(filename);

        let mut changed = false;

        for (selector, rules) in &self.compiled_config.applications {
            if states.iter().any(|state| state.path().matches_selector(&selector)) {
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
                        changed |= apply_regex_to_bytes(data, rule, regex, &replace_behavior);
                    }
                }
            }
        }

        changed
    }
}

fn apply_regex_to_bytes(data: &mut [u8], rule: &RuleRef, regex: &Regex, replace_behavior: &ReplaceBehavior) -> bool {
    let regex = match BytesRegex::new(regex.as_str()) {
        Ok(x) => x,
        Err(_) => {
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
            let chars_to_ignore: BTreeSet<u8> = mask.chars_to_ignore.chars().filter_map(|x|
                if x.is_ascii() { Some(x as u8) } else { None }
            ).collect();
            let mask_char = if mask.mask_char.is_ascii() {
                mask.mask_char as u8
            } else {
                DEFAULT_PADDING
            };

            for (start, end) in matches {
                let match_slice = &mut data[start..end];
                let match_slice_len = match_slice.len();
                for (idx, c) in match_slice.iter_mut().enumerate() {
                    if in_range(mask.range, idx, match_slice_len) && !chars_to_ignore.contains(c) {
                        *c = mask_char;
                    }
                }
            }
        },
        Redaction::Hash(ref hash) => {
            for (start, end) in matches {
                let hashed = hash_value(hash.algorithm, &data[start..end], hash.key.as_deref());
                replace_bytes_padded(hashed.as_bytes(), &mut data[start..end], DEFAULT_PADDING);
            }
        },
        Redaction::Replace(ref replace) => {
            for (start, end) in matches {
                replace_bytes_padded(replace.text.as_bytes(), &mut data[start..end], DEFAULT_PADDING);
            }
        },
    }

    true
}

/// Copy `source` into `target`, trunchating/padding with `padding` if necessary.
fn replace_bytes_padded(source: &[u8], target: &mut [u8], padding: u8) {
    for (a, b) in source.iter().zip(target.iter_mut()) {
        *b = *a;
    }

    if source.len() < target.len() {
        for x in &mut target[source.len()..] {
            *x = padding;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pii::PiiConfig;

    use super::*;

    struct AttachmentBytesTestCase<'a> {
        selector: &'a str,
        rule: &'a str,
        filename: &'a str,
        bytes_type: AttachmentBytesType, 
        input: &'a [u8],
        output: &'a [u8],
        changed: bool
    }

    impl<'a> AttachmentBytesTestCase<'a> {
        fn run(&self) {
           let config = PiiConfig::from_json(&format!(
                r##"
                {{
                    "applications": {{
                        "{selector}": ["{rule}"]
                    }}
                }}
            "##,
            selector=self.selector,
            rule=self.rule,
            ))
            .unwrap();

            let compiled = config.compiled();
            let processor = PiiAttachmentsProcessor::new(&compiled);
            let mut data = self.input.to_owned();
            let has_changed = processor.scrub_attachment_bytes(self.filename, &mut data, self.bytes_type);
            assert_eq!(data, self.output);
            assert_eq!(self.changed, has_changed);
        }
    }

    #[test]
    fn test_ip_replace_padding() {
        AttachmentBytesTestCase {
            selector: "$binary",
            rule: "@ip",
            filename: "foo.txt",
            bytes_type: AttachmentBytesType::PlainAttachment,
            input : b"before 127.0.0.1 after",
            output: b"before [ip]xxxxx after",
            changed: true,
        }.run();
    }

    #[test]
    fn test_ip_hash_trunchating() {
        AttachmentBytesTestCase {
            selector: "$binary",
            rule: "@ip:hash",
            filename: "foo.txt",
            bytes_type: AttachmentBytesType::PlainAttachment,
            input : b"before 127.0.0.1 after",
            output: b"before AE12FE3B5 after",
            changed: true,
        }.run();
    }

    #[test]
    fn test_ip_masking() {
        AttachmentBytesTestCase {
            selector: "$binary",
            rule: "@ip:mask",
            filename: "foo.txt",
            bytes_type: AttachmentBytesType::PlainAttachment,
            input : b"before 127.0.0.1 after",
            output: b"before ********* after",
            changed: true,
        }.run();
    }

    #[test]
    fn test_ip_removing() {
        AttachmentBytesTestCase {
            selector: "$binary",
            rule: "@ip:mask",
            filename: "foo.txt",
            bytes_type: AttachmentBytesType::PlainAttachment,
            input : b"before 127.0.0.1 after",
            output: b"before xxxxxxxxx after",
            changed: true,
        }.run();
    }

    #[test]
    fn test_selectors() {
        for wrong_selector in &[
            "$string",
            "$number",
            "$attachments.* && $string",
            "$attachments",
            "** && !$binary"
        ] {
            AttachmentBytesTestCase {
                selector: wrong_selector,
                rule: "@ip:mask",
                filename: "foo.txt",
                bytes_type: AttachmentBytesType::PlainAttachment,
                input : b"before 127.0.0.1 after",
                output: b"before 127.0.0.1 after",
                changed: false,
            }.run();
        }
    }
}
