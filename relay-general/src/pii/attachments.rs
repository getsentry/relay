use std::borrow::Cow;
use std::collections::BTreeSet;
use std::convert::TryInto;

use encoding::all::UTF_16LE;
use encoding::Encoding;
use regex::bytes::RegexBuilder as BytesRegexBuilder;
use regex::Regex;
use smallvec::SmallVec;

use crate::pii::compiledconfig::RuleRef;
use crate::pii::regexes::{get_regex_for_rule_type, ReplaceBehavior};
use crate::pii::utils::{hash_value, in_range};
use crate::pii::{CompiledPiiConfig, Redaction};
use crate::processor::{FieldAttrs, Pii, ProcessingState, ValueType};

type Range = std::ops::Range<usize>;

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

fn apply_regex_to_utf16_slices(
    data: &mut [u8],
    rule: &RuleRef,
    regex: &Regex,
    replace_behavior: &ReplaceBehavior,
) -> bool {
    let segments = extract_strings(data);
    for segment in segments.iter() {
        let mut matches = Vec::new();
        match replace_behavior {
            ReplaceBehavior::Value => {
                for re_match in regex.find_iter(&segment.decoded) {
                    matches.push(re_match);
                }
            }
            ReplaceBehavior::Groups(ref replace_groups) => {
                for captures in regex.captures_iter(&segment.decoded) {
                    for group_idx in replace_groups.iter() {
                        if let Some(re_match) = captures.get(*group_idx as usize) {
                            matches.push(re_match)
                        }
                    }
                }
            }
        }
        for re_match in matches.iter() {
            let mut char_offset = 0;
            let mut char_len = 0;

            let mut byte_offset = 0;
            for (cur_char_offset, c) in segment.decoded.chars().enumerate() {
                if byte_offset == re_match.start() {
                    char_offset = cur_char_offset;
                }
                if byte_offset == re_match.end() {
                    char_len = cur_char_offset - char_offset;
                    break;
                }
                byte_offset += c.len_utf8();
            }

            let mut utf16_byte_offset = 0;
            let mut utf16_byte_len = 0;

            let mut byte_offset = 0;
            let u16input = &mut data[segment.input_pos.clone()]
                .chunks_exact(2)
                .map(|bb| u16::from_le_bytes(bb.try_into().unwrap()));
            for (cur_char_offset, c) in std::char::decode_utf16(u16input).enumerate() {
                if cur_char_offset == char_offset {
                    utf16_byte_offset = byte_offset;
                }
                if cur_char_offset == char_offset + char_len {
                    utf16_byte_len = byte_offset - utf16_byte_offset;
                }
                byte_offset += c.unwrap().len_utf16();
            }

            let start = segment.input_pos.start + utf16_byte_offset;
            let end = start + utf16_byte_len;
            let utf16_match_slice = &mut data[start..end];

            const DEFAULT_PADDING_UTF16: [u8; 2] = ['x' as u8, '\x00' as u8];
            // ASCII characters can be simply casted to u16
            const DEFAULT_PADDING_U8: u8 = b'x' as u8;

            match rule.redaction {
                Redaction::Default | Redaction::Remove => {
                    for c in utf16_match_slice.chunks_exact_mut(2) {
                        *c.get_mut(0).unwrap() = DEFAULT_PADDING_UTF16[0];
                        *c.get_mut(1).unwrap() = DEFAULT_PADDING_UTF16[1];
                    }
                }
                Redaction::Mask(ref mask) => {
                    // TODO: as long as the masking char fits in one u16 we can mask any
                    // unicode codepoint.  Currently this restricts both to ASCII.
                    let chars_not_masked: BTreeSet<u16> = mask
                        .chars_to_ignore
                        .chars()
                        .filter_map(|x| if x.is_ascii() { Some(x as u16) } else { None })
                        .collect();
                    let mask_char = if mask.mask_char.is_ascii() {
                        mask.mask_char as u8
                    } else {
                        DEFAULT_PADDING_U8
                    };

                    for bb in utf16_match_slice.chunks_exact_mut(2) {
                        let bbb: &[u8] = bb;
                        let c = u16::from_le_bytes(bbb.try_into().unwrap());
                        let b0 = if chars_not_masked.contains(&c) {
                            bb[0]
                        } else {
                            mask_char
                        };
                        *bb.get_mut(0).unwrap() = b0;
                        *bb.get_mut(1).unwrap() = b'\x00';
                    }
                }
                Redaction::Hash(ref hash) => {
                    // Note, we are hashing bytes containing utf16, not utf8.
                    let hashed = hash_value(hash.algorithm, utf16_match_slice, hash.key.as_deref());
                    replace_utf16_bytes_padded(
                        hashed.as_str(),
                        &mut utf16_match_slice[..],
                        DEFAULT_PADDING_U8 as u16,
                    );
                }
                Redaction::Replace(ref replace) => {
                    replace_utf16_bytes_padded(
                        replace.text.as_str(),
                        utf16_match_slice,
                        DEFAULT_PADDING_U8 as u16,
                    );
                }
            }
        }
    }
    true // eh?
}

fn replace_utf16_bytes_padded(source: &str, target: &mut [u8], padding: u16) {
    let target_len = target.len();
    let mut target_offset = 0;
    for code in source.encode_utf16() {
        // Consider iterating over chars instead and use utf16_len() which avoids hardcoding
        // codec knowledge.
        let byte_len = if 0xD800 & code == 0xD800 {
            // let byte_len = if 0xD800 <= code && code <= 0xDBFF {
            4 // high or leading surrogate
        } else {
            2
        };
        if (target_len - target_offset) < byte_len {
            break;
        }
        for byte in code.to_le_bytes().iter() {
            target[target_offset] = *byte;
            target_offset += 1;
        }
    }

    while (target_len - target_offset) > 0 {
        for byte in padding.to_le_bytes().iter() {
            target[target_offset] = *byte;
            target_offset += 1;
        }
    }
}

struct StringSegment {
    decoded: String,
    input_pos: Range,
}

// TODO: Make this an iterator to avoid more allocations?
fn extract_strings(data: &[u8]) -> Vec<StringSegment> {
    let mut ret = Vec::new();
    let mut offset = 0;
    let mut decoder = UTF_16LE.raw_decoder();

    while offset < data.len() {
        let mut decoded = String::new();
        let (unprocessed_offset, err) = decoder.raw_feed(&data[offset..], &mut decoded);

        if decoded.len() > 2 {
            let input_pos = Range {
                start: offset,
                end: offset + unprocessed_offset,
            };
            ret.push(StringSegment { decoded, input_pos });
        }

        if let Some(err) = err {
            if err.upto > 0 {
                offset += err.upto as usize;
            } else {
                // This should never happen, but if it does, re-set the decoder and skip
                // forward to the next 2 bytes.
                offset += std::mem::size_of::<u16>();
                decoder = decoder.from_self();
            }
        } else {
            // We are at the end of input.  There could be some unprocessed bytes left, but
            // we have no more data to feed to the decoder so just stop.
            break;
        }
    }
    ret
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
                        changed |=
                            apply_regex_to_utf16_slices(data, rule, regex, &replace_behavior);
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

    #[test]
    fn test_extract_strings_entire() {
        let data = b"h\x00e\x00l\x00l\x00o\x00";
        let ret = extract_strings(&data[..]);
        assert_eq!(ret.len(), 1);
        assert_eq!(ret[0].decoded, "hello".to_string());
        assert_eq!(ret[0].input_pos, Range { start: 0, end: 10 });
        assert_eq!(&data[ret[0].input_pos.clone()], &data[..]);
    }

    #[test]
    fn test_extract_strings_middle_2_byte_aligned() {
        let data = b"\xd8\xd8\xd8\xd8h\x00e\x00l\x00l\x00o\x00\xd8\xd8";
        let ret = extract_strings(&data[..]);
        assert_eq!(ret.len(), 1);
        assert_eq!(ret[0].decoded, "hello".to_string());
        assert_eq!(ret[0].input_pos, Range { start: 4, end: 14 });
        assert_eq!(
            &data[ret[0].input_pos.clone()],
            b"h\x00e\x00l\x00l\x00o\x00"
        );
    }

    #[test]
    fn test_extract_strings_middle_unaligned() {
        let data = b"\xd8\xd8\xd8h\x00e\x00l\x00l\x00o\x00\xd8\xd8";
        let ret = extract_strings(&data[..]);
        assert_eq!(ret.len(), 1);
        assert_ne!(ret[0].decoded, "hello".to_string());
        assert_eq!(ret[0].input_pos, Range { start: 2, end: 12 });
    }

    #[test]
    fn test_extract_strings_end_aligned() {
        let data = b"\xd8\xd8h\x00e\x00l\x00l\x00o\x00";
        let ret = extract_strings(&data[..]);
        assert_eq!(ret.len(), 1);
        assert_eq!(ret[0].decoded, "hello".to_string());
    }

    #[test]
    fn test_extract_strings_garbage() {
        let data = b"\xd8\xd8";
        let ret = extract_strings(&data[..]);
        assert_eq!(ret.len(), 0);
    }

    #[test]
    fn test_extract_strings_short() {
        let data = b"\xd8\xd8y\x00o\x00\xd8\xd8h\x00e\x00l\x00l\x00o\x00";
        let ret = extract_strings(&data[..]);
        assert_eq!(ret.len(), 1);
        assert_eq!(ret[0].decoded, "hello".to_string());
    }

    // #[test]
    // fn test_extract_strings_minidump() {
    //     let data =
    //         std::fs::read("/Users/flub/code/symbolicator/tests/fixtures/windows.dmp").unwrap();
    //     let ret = extract_strings(&data[..]);
    //     println!("count: {}", ret.len());
    //     for segment in ret {
    //         println!("{}", &segment.decoded);
    //     }
    //     panic!("done");
    // }
}
