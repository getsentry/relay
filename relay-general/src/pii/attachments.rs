use std::borrow::Cow;
use std::collections::BTreeSet;

use regex::bytes::RegexBuilder as BytesRegexBuilder;
use regex::Regex;
use smallvec::SmallVec;

use crate::pii::compiledconfig::RuleRef;
use crate::pii::regexes::{get_regex_for_rule_type, ReplaceBehavior};
use crate::pii::utils::{hash_value, in_range};
use crate::pii::{CompiledPiiConfig, Redaction};
use crate::processor::{FieldAttrs, Pii, ProcessingState, ValueType};

/// Copy `source` into `target`, truncating/padding with `padding` if necessary.
fn replace_bytes_padded(source: &[u8], target: &mut [u8], padding: u8) {
    let cutoff = source.len().min(target.len());
    let (left, right) = target.split_at_mut(cutoff);
    left.copy_from_slice(&source[..cutoff]);

    for byte in right {
        *byte = padding;
    }
}

fn apply_regex_to_bytes(
    data: &mut [u8],
    rule: &RuleRef,
    regex: &Regex,
    replace_behavior: &ReplaceBehavior,
) -> bool {
    let regex = match BytesRegexBuilder::new(regex.as_str())
        // https://github.com/rust-lang/regex/issues/697
        .unicode(false)
        .multi_line(false)
        .dot_matches_new_line(true)
        .build()
    {
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
                if group.start() == group.end() {
                    continue;
                }

                match replace_behavior {
                    ReplaceBehavior::Groups(ref replace_groups) => {
                        if replace_groups.contains(&(idx as u8)) {
                            matches.push((group.start(), group.end()));
                        }
                    }
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
        }
        Redaction::Mask(ref mask) => {
            let chars_to_ignore: BTreeSet<u8> = mask
                .chars_to_ignore
                .chars()
                .filter_map(|x| if x.is_ascii() { Some(x as u8) } else { None })
                .collect();
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
        }
        Redaction::Hash(ref hash) => {
            for (start, end) in matches {
                let hashed = hash_value(hash.algorithm, &data[start..end], hash.key.as_deref());
                replace_bytes_padded(hashed.as_bytes(), &mut data[start..end], DEFAULT_PADDING);
            }
        }
        Redaction::Replace(ref replace) => {
            for (start, end) in matches {
                replace_bytes_padded(
                    replace.text.as_bytes(),
                    &mut data[start..end],
                    DEFAULT_PADDING,
                );
            }
        }
    }

    true
}

/// A PII processor for attachment files.
pub struct PiiAttachmentsProcessor<'a> {
    compiled_config: &'a CompiledPiiConfig,
    root_state: ProcessingState<'static>,
}

impl<'a> PiiAttachmentsProcessor<'a> {
    /// Creates a new `PiiAttachmentsProcessor` from the given PII config.
    pub fn new(compiled_config: &'a CompiledPiiConfig) -> Self {
        // this constructor needs to be cheap... a new PiiProcessor is created for each event. Move
        // any init logic into CompiledPiiConfig::new.

        let root_state =
            ProcessingState::root().enter_static("", None, Some(ValueType::Attachments));

        PiiAttachmentsProcessor {
            compiled_config,
            root_state,
        }
    }

    /// Returns the processing state for the file with the given name.
    pub(crate) fn state<'s>(
        &'s self,
        filename: &'s str,
        value_type: ValueType,
    ) -> ProcessingState<'s> {
        self.root_state.enter_borrowed(
            filename,
            Some(Cow::Owned(FieldAttrs::new().pii(Pii::Maybe))),
            Some(value_type),
        )
    }

    /// Applies PII rules to a plain buffer.
    ///
    /// Returns `true`, if the buffer was modified.
    pub(crate) fn scrub_bytes(&self, data: &mut [u8], state: &ProcessingState<'_>) -> bool {
        let mut changed = false;

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
                    for (_pattern_type, regex, replace_behavior) in
                        get_regex_for_rule_type(&rule.ty)
                    {
                        changed |= apply_regex_to_bytes(data, rule, regex, &replace_behavior);
                    }
                }
            }
        }

        changed
    }

    /// Applies PII scrubbing rules to a plain attachment.
    ///
    /// Returns `true`, if the attachment was modified.
    pub fn scrub_attachment(&self, filename: &str, data: &mut [u8]) -> bool {
        let state = self.state(filename, ValueType::Binary);
        self.scrub_bytes(data, &state)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::pii::PiiConfig;

    use super::*;

    enum AttachmentBytesTestCase<'a> {
        Builtin {
            selector: &'a str,
            rule: &'a str,
            filename: &'a str,
            value_type: ValueType,
            input: &'a [u8],
            output: &'a [u8],
            changed: bool,
        },
        Regex {
            selector: &'a str,
            regex: &'a str,
            filename: &'a str,
            value_type: ValueType,
            input: &'a [u8],
            output: &'a [u8],
            changed: bool,
        },
    }

    impl<'a> AttachmentBytesTestCase<'a> {
        fn run(self) {
            let (config, filename, value_type, input, output, changed) = match self {
                AttachmentBytesTestCase::Builtin {
                    selector,
                    rule,
                    filename,
                    value_type,
                    input,
                    output,
                    changed,
                } => {
                    let config = serde_json::from_value::<PiiConfig>(serde_json::json!(
                        {
                            "applications": {
                                selector: [rule]
                            }
                        }
                    ))
                    .unwrap();
                    (config, filename, value_type, input, output, changed)
                }
                AttachmentBytesTestCase::Regex {
                    selector,
                    regex,
                    filename,
                    value_type,
                    input,
                    output,
                    changed,
                } => {
                    let config = serde_json::from_value::<PiiConfig>(serde_json::json!(
                        {
                            "rules": {
                                "custom": {
                                    "type": "pattern",
                                    "pattern": regex,
                                    "redaction": {
                                      "method": "remove"
                                    }
                                }
                            },
                            "applications": {
                                selector: ["custom"]
                            }
                        }
                    ))
                    .unwrap();
                    (config, filename, value_type, input, output, changed)
                }
            };

            let compiled = config.compiled();
            let mut data = input.to_owned();
            let processor = PiiAttachmentsProcessor::new(&compiled);
            let state = processor.state(filename, value_type);
            let has_changed = processor.scrub_bytes(&mut data, &state);

            assert_eq_bytes_str!(data, output);
            assert_eq!(changed, has_changed);
        }
    }

    #[test]
    fn test_ip_replace_padding() {
        AttachmentBytesTestCase::Builtin {
            selector: "$binary",
            rule: "@ip",
            filename: "foo.txt",
            value_type: ValueType::Binary,
            input: b"before 127.0.0.1 after",
            output: b"before [ip]xxxxx after",
            changed: true,
        }
        .run();
    }

    #[test]
    fn test_ip_hash_trunchating() {
        AttachmentBytesTestCase::Builtin {
            selector: "$binary",
            rule: "@ip:hash",
            filename: "foo.txt",
            value_type: ValueType::Binary,
            input: b"before 127.0.0.1 after",
            output: b"before AE12FE3B5 after",
            changed: true,
        }
        .run();
    }

    #[test]
    fn test_ip_masking() {
        AttachmentBytesTestCase::Builtin {
            selector: "$binary",
            rule: "@ip:mask",
            filename: "foo.txt",
            value_type: ValueType::Binary,
            input: b"before 127.0.0.1 after",
            output: b"before ********* after",
            changed: true,
        }
        .run();
    }

    #[test]
    fn test_ip_removing() {
        AttachmentBytesTestCase::Builtin {
            selector: "$binary",
            rule: "@ip:remove",
            filename: "foo.txt",
            value_type: ValueType::Binary,
            input: b"before 127.0.0.1 after",
            output: b"before xxxxxxxxx after",
            changed: true,
        }
        .run();
    }

    #[test]
    fn test_selectors() {
        for wrong_selector in &[
            "$string",
            "$number",
            "$attachments.* && $string",
            "$attachments",
            "** && !$binary",
        ] {
            AttachmentBytesTestCase::Builtin {
                selector: wrong_selector,
                rule: "@ip:mask",
                filename: "foo.txt",
                value_type: ValueType::Binary,
                input: b"before 127.0.0.1 after",
                output: b"before 127.0.0.1 after",
                changed: false,
            }
            .run();
        }
    }

    #[test]
    fn test_all_the_bytes() {
        AttachmentBytesTestCase::Builtin {
            selector: "$binary",
            rule: "@anything:remove",
            filename: "foo.txt",
            value_type: ValueType::Binary,
            input: (0..255 as u8).collect::<Vec<_>>().as_slice(),
            output: &[b'x'; 255],
            changed: true,
        }
        .run();
    }

    #[test]
    fn test_bytes_regexes() {
        // Test that specifically bytes patterns that are not valid UTF-8 can be matched against.
        //
        // From https://www.php.net/manual/en/reference.pcre.pattern.modifiers.php#54805
        let samples: &[&[u8]] = &[
            b"\xc3\x28",                 // Invalid 2 Octet Sequence
            b"\xa0\xa1",                 // Invalid Sequence Identifier
            b"\xe2\x28\xa1",             // Invalid 3 Octet Sequence (in 2nd Octet)
            b"\xe2\x82\x28",             // Invalid 3 Octet Sequence (in 3rd Octet)
            b"\xf0\x28\x8c\xbc",         // Invalid 4 Octet Sequence (in 2nd Octet)
            b"\xf0\x90\x28\xbc",         // Invalid 4 Octet Sequence (in 3rd Octet)
            b"\xf0\x28\x8c\x28",         // Invalid 4 Octet Sequence (in 4th Octet)
            b"\xf8\xa1\xa1\xa1\xa1",     // Valid 5 Octet Sequence (but not Unicode!)
            b"\xfc\xa1\xa1\xa1\xa1\xa1", // Valid 6 Octet Sequence (but not Unicode!)
        ];

        for bytes in samples {
            assert!(String::from_utf8(bytes.to_vec()).is_err());

            AttachmentBytesTestCase::Regex {
                selector: "$binary",
                regex: &bytes.iter().map(|x| format!("\\x{:02x}", x)).join(""),
                filename: "foo.txt",
                value_type: ValueType::Binary,
                input: bytes,
                output: &vec![b'x'; bytes.len()],
                changed: true,
            }
            .run()
        }
    }
}
