use std::borrow::Cow;
use std::iter::FusedIterator;

use regex::bytes::RegexBuilder as BytesRegexBuilder;
use regex::{Match, Regex};
use smallvec::SmallVec;
use utf16string::{LittleEndian, WStr};

use crate::pii::compiledconfig::RuleRef;
use crate::pii::regexes::{get_regex_for_rule_type, ReplaceBehavior};
use crate::pii::utils::hash_value;
use crate::pii::{CompiledPiiConfig, Redaction};
use crate::processor::{FieldAttrs, Pii, ProcessingState, ValueType};

/// The minimum length a string needs to be in a binary blob.
///
/// This module extracts encoded strings from within binary blobs, this specifies the
/// minimum length we require those strings to be before we accept them to match scrubbing
/// selectors on.
const MIN_STRING_LEN: usize = 5;

fn apply_regex_to_utf8_bytes(
    data: &mut [u8],
    rule: &RuleRef,
    regex: &Regex,
    replace_behavior: &ReplaceBehavior,
) -> SmallVec<[(usize, usize); 1]> {
    let mut matches = SmallVec::<[(usize, usize); 1]>::new();

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
            return matches;
        }
    };

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

    for (start, end) in matches.iter() {
        data[*start..*end].apply_redaction(&rule.redaction);
    }
    matches
}

fn apply_regex_to_utf16le_bytes(
    data: &mut [u8],
    rule: &RuleRef,
    regex: &Regex,
    replace_behavior: &ReplaceBehavior,
) -> bool {
    let mut changed = false;
    for segment in WStrSegmentIter::new(data) {
        match replace_behavior {
            ReplaceBehavior::Value => {
                for re_match in regex.find_iter(&segment.decoded) {
                    changed = true;
                    let match_wstr = get_wstr_match(&segment.decoded, re_match, segment.encoded);
                    match_wstr.apply_redaction(&rule.redaction);
                }
            }
            ReplaceBehavior::Groups(ref replace_groups) => {
                for captures in regex.captures_iter(&segment.decoded) {
                    for group_idx in replace_groups.iter() {
                        if let Some(re_match) = captures.get(*group_idx as usize) {
                            changed = true;
                            let match_wstr =
                                get_wstr_match(&segment.decoded, re_match, segment.encoded);
                            match_wstr.apply_redaction(&rule.redaction);
                        }
                    }
                }
            }
        }
    }
    changed
}

/// Extract the matching encoded slice from the encoded string.
fn get_wstr_match<'a>(
    all_text: &str,
    re_match: Match,
    all_encoded: &'a mut WStr<LittleEndian>,
) -> &'a mut WStr<LittleEndian> {
    let mut encoded_start = 0;
    let mut encoded_end = all_encoded.len();

    let offsets_iter = all_text.char_indices().zip(all_encoded.char_indices());
    for ((text_offset, _text_char), (encoded_offset, _encoded_char)) in offsets_iter {
        if text_offset == re_match.start() {
            encoded_start = encoded_offset;
        }
        if text_offset == re_match.end() {
            encoded_end = encoded_offset;
            break;
        }
    }
    &mut all_encoded[encoded_start..encoded_end]
}

/// Traits to modify the strings in ways we need.
trait StringMods: AsRef<[u8]> {
    /// Replace this string's contents by repeating the given character into it.
    ///
    /// # Panics
    ///
    /// The `fill_char` has to encode to the smallest encoding unit, otherwise this will
    /// panic.  Using an ASCII replacement character is usually safe in most encodings.
    fn fill_content(&mut self, fill_char: char);

    /// Replace this string's contents with the given replacement string.
    ///
    /// If the replacement string encodes to a shorter byte-slice than the current string
    /// any remaining space will be filled with the padding character.
    ///
    /// If the replacement string encodes to a longer byte-slice than the current string the
    /// replacement string is truncated.  If this does not align with a character boundary
    /// in the replacement string it is further trucated to the previous character boundary
    /// and the remainder is filled with the padding char.
    ///
    /// # Panics
    ///
    /// The `padding` character has to encode to the smallest encoding unit, otherwise this
    /// will panic.  Using an ASCII padding character is usually safe in most encodings.
    fn swap_content(&mut self, replacement: &str, padding: char);

    /// Apply a PII scrubbing redaction to this string slice.
    fn apply_redaction(&mut self, redaction: &Redaction) {
        const PADDING: char = '*';
        const MASK: char = '*';

        match redaction {
            Redaction::Default | Redaction::Remove => {
                self.fill_content(PADDING);
            }
            Redaction::Mask => {
                self.fill_content(MASK);
            }
            Redaction::Hash => {
                let hashed = hash_value(self.as_ref());
                self.swap_content(&hashed, PADDING);
            }
            Redaction::Replace(ref replace) => {
                self.swap_content(replace.text.as_str(), PADDING);
            }
        }
    }
}

impl StringMods for WStr<LittleEndian> {
    fn fill_content(&mut self, fill_char: char) {
        // If fill_char is too wide, fill_char.encode_utf16() will panic, fulfilling the
        // trait's contract that we must panic if fill_char is too wide.
        let mut buf = [0u16; 1];
        let fill_u16 = fill_char.encode_utf16(&mut buf[..]);
        let fill_buf = fill_u16[0].to_le_bytes();

        unsafe {
            let chunks = self
                .as_bytes_mut()
                .chunks_exact_mut(std::mem::size_of::<u16>());
            for chunk in chunks {
                chunk.copy_from_slice(&fill_buf);
            }
        }
    }

    fn swap_content(&mut self, replacement: &str, padding: char) {
        // If the padding char is too wide, padding.encode_utf16() will panic, fulfilling
        // the trait's contract that we must panic in this case.
        let len = self.len();

        let mut buf = [0u16; 1];
        padding.encode_utf16(&mut buf[..]);
        let fill_buf = buf[0].to_le_bytes();

        let mut offset = 0;
        for code in replacement.encode_utf16() {
            let char_len = if 0xD800 & code == 0xD800 {
                std::mem::size_of::<u16>() * 2 // leading surrogate
            } else {
                std::mem::size_of::<u16>()
            };
            if (len - offset) < char_len {
                break; // Not enough space for this char
            }
            unsafe {
                let target = &mut self.as_bytes_mut()[offset..offset + std::mem::size_of::<u16>()];
                target.copy_from_slice(&code.to_le_bytes());
            }
            offset += std::mem::size_of::<u16>();
        }

        unsafe {
            let remainder_bytes = &mut self.as_bytes_mut()[offset..];
            let chunks = remainder_bytes.chunks_exact_mut(std::mem::size_of::<u16>());
            for chunk in chunks {
                chunk.copy_from_slice(&fill_buf);
            }
        }
    }
}

impl StringMods for [u8] {
    fn fill_content(&mut self, fill_char: char) {
        // If fill_char is too wide, fill_char.encode_utf16() will panic, fulfilling the
        // trait's contract that we must panic if fill_char is too wide.
        let mut buf = [0u8; 1];
        fill_char.encode_utf8(&mut buf[..]);
        for byte in self {
            *byte = buf[0];
        }
    }

    fn swap_content(&mut self, replacement: &str, padding: char) {
        // If the padding char is too wide, padding.encode_utf16() will panic, fulfilling
        // the trait's contract that we must panic in this case.
        let mut buf = [0u8; 1];
        padding.encode_utf8(&mut buf[..]);

        let cutoff = replacement.len().min(self.len());
        let (left, right) = self.split_at_mut(cutoff);
        left.copy_from_slice(&replacement.as_bytes()[..cutoff]);

        for byte in right {
            *byte = buf[0];
        }
    }
}

/// An iterator over segments of text in binary data.
///
/// This iterator will look for blocks of UTF-16 encoded text with little-endian byte order
/// in a block of binary data and yield those slices as segments with both the decoded and
/// encoded text.
struct WStrSegmentIter<'a> {
    data: &'a mut [u8],
    offset: usize,
}

impl<'a> WStrSegmentIter<'a> {
    fn new(data: &'a mut [u8]) -> Self {
        Self { data, offset: 0 }
    }
}

impl<'a> Iterator for WStrSegmentIter<'a> {
    type Item = WStrSegment<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.offset >= self.data.len() {
                return None;
            }

            let slice = match WStr::from_utf16le_mut(&mut self.data[self.offset..]) {
                Ok(wstr) => {
                    self.offset += wstr.len();
                    unsafe { wstr.as_bytes_mut() }
                }
                Err(err) => {
                    let start = self.offset;
                    let end = start + err.valid_up_to();
                    match err.error_len() {
                        Some(len) => self.offset += err.valid_up_to() + len,
                        None => self.offset = self.data.len(),
                    }
                    &mut self.data[start..end]
                }
            };

            // We are handing out multiple mutable slices from the same mutable slice.  This
            // is safe because we know they are not overlapping.  However the compiler
            // doesn't know this so we need to transmute the lifetimes of the slices we
            // return with std::slice::from_raw_parts_mut().
            let ptr = slice.as_mut_ptr();
            let len = slice.len();
            let encoded = unsafe {
                WStr::from_utf16le_unchecked_mut(std::slice::from_raw_parts_mut(ptr, len))
            };

            if encoded.chars().take(MIN_STRING_LEN).count() < MIN_STRING_LEN {
                continue;
            }
            let decoded = encoded.to_utf8();
            return Some(WStrSegment { encoded, decoded });
        }
    }
}

impl<'a> FusedIterator for WStrSegmentIter<'a> {}

/// An encoded string segment in a larger data block.
///
/// The slice of data will contain the entire block which will be valid according to the
/// encoding.  This will be a unique sub-slice of the data in [MutSegmentiter] as the
/// iterator will not yield overlapping slices.
///
/// While the `data` field is mutable, after mutating this the string in `decoded` will no
/// longer match.
struct WStrSegment<'a> {
    /// The raw bytes of this segment.
    encoded: &'a mut WStr<LittleEndian>,
    /// The decoded string of this segment.
    decoded: String,
}

/// A PII processor for attachment files.
pub struct PiiAttachmentsProcessor<'a> {
    compiled_config: &'a CompiledPiiConfig,
    root_state: ProcessingState<'static>,
}

/// Which encodings to scrub for `scrub_bytes`.
pub enum ScrubEncodings {
    Utf8,
    Utf16Le,
    All,
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
            Some(Cow::Owned(FieldAttrs::new().pii(Pii::True))),
            Some(value_type),
        )
    }

    /// Applies PII rules to a plain buffer.
    ///
    /// Returns `true`, if the buffer was modified.
    pub(crate) fn scrub_bytes(
        &self,
        data: &mut [u8],
        state: &ProcessingState<'_>,
        encodings: ScrubEncodings,
    ) -> bool {
        let pii = state.attrs().pii;
        if pii == Pii::False {
            return false;
        }

        let mut changed = false;

        for (selector, rules) in &self.compiled_config.applications {
            if pii == Pii::Maybe && !selector.is_specific() {
                continue;
            }

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
                        match encodings {
                            ScrubEncodings::Utf8 => {
                                let matches =
                                    apply_regex_to_utf8_bytes(data, rule, regex, &replace_behavior);
                                changed |= !(matches.is_empty());
                            }
                            ScrubEncodings::Utf16Le => {
                                changed |= apply_regex_to_utf16le_bytes(
                                    data,
                                    rule,
                                    regex,
                                    &replace_behavior,
                                );
                            }
                            ScrubEncodings::All => {
                                let matches =
                                    apply_regex_to_utf8_bytes(data, rule, regex, &replace_behavior);
                                changed |= !(matches.is_empty());

                                // Only scrub regions with the UTF-16 scrubber if they haven't been
                                // scrubbed yet.
                                let unscrubbed_ranges = matches
                                    .into_iter()
                                    .chain(std::iter::once((data.len(), 0)))
                                    .scan((0usize, 0usize), |previous, current| {
                                        let start = if previous.1 % 2 == 0 {
                                            previous.1
                                        } else {
                                            previous.1 + 1
                                        };
                                        let item = (start, current.0);
                                        *previous = current;
                                        Some(item)
                                    })
                                    .filter(|(start, end)| end > start);
                                for (start, end) in unscrubbed_ranges {
                                    changed |= apply_regex_to_utf16le_bytes(
                                        &mut data[start..end],
                                        rule,
                                        regex,
                                        &replace_behavior,
                                    );
                                }
                            }
                        }
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
        self.scrub_bytes(data, &state, ScrubEncodings::All)
    }

    /// Scrub a filepath, preserving the basename.
    pub fn scrub_utf8_filepath(&self, path: &mut str, state: &ProcessingState<'_>) -> bool {
        if let Some(index) = path.rfind(|c| c == '/' || c == '\\') {
            let data = unsafe { &mut path.as_bytes_mut()[..index] };
            self.scrub_bytes(data, state, ScrubEncodings::Utf8)
        } else {
            false
        }
    }

    /// Scrub a filepath, preserving the basename.
    pub fn scrub_utf16_filepath(
        &self,
        path: &mut WStr<LittleEndian>,
        state: &ProcessingState<'_>,
    ) -> bool {
        let index =
            path.char_indices().rev().find_map(
                |(i, c)| {
                    if c == '/' || c == '\\' {
                        Some(i)
                    } else {
                        None
                    }
                },
            );

        if let Some(index) = index {
            let data = unsafe { &mut path.as_bytes_mut()[..index] };
            self.scrub_bytes(data, state, ScrubEncodings::Utf16Le)
        } else {
            false
        }
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
            let has_changed = processor.scrub_bytes(&mut data, &state, ScrubEncodings::All);

            assert_eq_bytes_str!(data, output);
            assert_eq!(changed, has_changed);
        }
    }

    fn utf16le(s: &str) -> Vec<u8> {
        s.encode_utf16()
            .map(|u| u.to_le_bytes())
            .collect::<Vec<[u8; 2]>>()
            .iter()
            .flatten()
            .copied()
            .collect()
    }

    #[test]
    fn test_ip_replace_padding() {
        AttachmentBytesTestCase::Builtin {
            selector: "$binary",
            rule: "@ip",
            filename: "foo.txt",
            value_type: ValueType::Binary,
            input: b"before 127.0.0.1 after",
            output: b"before [ip]***** after",
            changed: true,
        }
        .run();
    }

    #[test]
    fn test_ip_replace_padding_utf16() {
        AttachmentBytesTestCase::Builtin {
            selector: "$binary",
            rule: "@ip",
            filename: "foo.txt",
            value_type: ValueType::Binary,
            input: utf16le("before 127.0.0.1 after").as_slice(),
            output: utf16le("before [ip]***** after").as_slice(),
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
    fn test_ip_hash_trunchating_utf16() {
        AttachmentBytesTestCase::Builtin {
            selector: "$binary",
            rule: "@ip:hash",
            filename: "foo.txt",
            value_type: ValueType::Binary,
            input: utf16le("before 127.0.0.1 after").as_slice(),
            output: utf16le("before 3FA8F5A46 after").as_slice(),
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
    fn test_ip_masking_utf16() {
        AttachmentBytesTestCase::Builtin {
            selector: "$binary",
            rule: "@ip:mask",
            filename: "foo.txt",
            value_type: ValueType::Binary,
            input: utf16le("before 127.0.0.1 after").as_slice(),
            output: utf16le("before ********* after").as_slice(),
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
            output: b"before ********* after",
            changed: true,
        }
        .run();
    }

    #[test]
    fn test_ip_removing_utf16() {
        AttachmentBytesTestCase::Builtin {
            selector: "$binary",
            rule: "@ip:remove",
            filename: "foo.txt",
            value_type: ValueType::Binary,
            input: utf16le("before 127.0.0.1 after").as_slice(),
            output: utf16le("before ********* after").as_slice(),
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
            input: (0..255u8).collect::<Vec<_>>().as_slice(),
            output: &[b'*'; 255],
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
                output: &vec![b'*'; bytes.len()],
                changed: true,
            }
            .run()
        }
    }

    #[test]
    fn test_segments_all_data() {
        let mut data = Vec::from(&b"h\x00e\x00l\x00l\x00o\x00"[..]);
        let mut iter = WStrSegmentIter::new(&mut data[..]);

        let segment = iter.next().unwrap();
        assert_eq!(segment.decoded, "hello");
        assert_eq!(segment.encoded.as_bytes(), b"h\x00e\x00l\x00l\x00o\x00");

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_segments_middle_2_byte_aligned() {
        let mut data = Vec::from(&b"\xd8\xd8\xd8\xd8h\x00e\x00l\x00l\x00o\x00\xd8\xd8"[..]);
        let mut iter = WStrSegmentIter::new(&mut data[..]);

        let segment = iter.next().unwrap();
        assert_eq!(segment.decoded, "hello");
        assert_eq!(segment.encoded.as_bytes(), b"h\x00e\x00l\x00l\x00o\x00");

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_segments_middle_2_byte_aligned_mutation() {
        let mut data = Vec::from(&b"\xd8\xd8\xd8\xd8h\x00e\x00l\x00l\x00o\x00\xd8\xd8"[..]);
        let mut iter = WStrSegmentIter::new(&mut data[..]);

        let segment = iter.next().unwrap();
        unsafe {
            segment
                .encoded
                .as_bytes_mut()
                .copy_from_slice(&b"w\x00o\x00r\x00l\x00d\x00"[..]);
        }

        assert!(iter.next().is_none());

        assert_eq!(data, b"\xd8\xd8\xd8\xd8w\x00o\x00r\x00l\x00d\x00\xd8\xd8");
    }

    #[test]
    fn test_segments_middle_unaligned() {
        let mut data = Vec::from(&b"\xd8\xd8\xd8h\x00e\x00l\x00l\x00o\x00\xd8\xd8"[..]);
        let mut iter = WStrSegmentIter::new(&mut data);

        // Off-by-one is devastating, nearly everything is valid unicode.
        let segment = iter.next().unwrap();
        assert_eq!(segment.decoded, "棘攀氀氀漀");

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_segments_end_aligned() {
        let mut data = Vec::from(&b"\xd8\xd8h\x00e\x00l\x00l\x00o\x00"[..]);
        let mut iter = WStrSegmentIter::new(&mut data);

        let segment = iter.next().unwrap();
        assert_eq!(segment.decoded, "hello");

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_segments_garbage() {
        let mut data = Vec::from(&b"\xd8\xd8"[..]);
        let mut iter = WStrSegmentIter::new(&mut data);

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_segments_too_short() {
        let mut data = Vec::from(&b"\xd8\xd8y\x00o\x00\xd8\xd8h\x00e\x00l\x00l\x00o\x00"[..]);
        let mut iter = WStrSegmentIter::new(&mut data);

        let segment = iter.next().unwrap();
        assert_eq!(segment.decoded, "hello");

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_segments_multiple() {
        let mut data =
            Vec::from(&b"\xd8\xd8h\x00e\x00l\x00l\x00o\x00\xd8\xd8w\x00o\x00r\x00l\x00d\x00"[..]);

        let mut iter = WStrSegmentIter::new(&mut data);

        let segment = iter.next().unwrap();
        assert_eq!(segment.decoded, "hello");

        let segment = iter.next().unwrap();
        assert_eq!(segment.decoded, "world");

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_fill_content_wstr() {
        let mut b = Vec::from(&b"h\x00e\x00l\x00l\x00o\x00"[..]);
        let s = WStr::from_utf16le_mut(b.as_mut_slice()).unwrap();
        s.fill_content('x');
        assert_eq!(b.as_slice(), b"x\x00x\x00x\x00x\x00x\x00");
    }

    #[test]
    #[should_panic]
    fn test_fill_content_wstr_panic() {
        let mut b = Vec::from(&b"h\x00e\x00y\x00"[..]);
        let s = WStr::from_utf16le_mut(b.as_mut_slice()).unwrap();
        s.fill_content('\u{10000}');
    }

    #[test]
    fn test_swap_content_wstr() {
        // Exact same size
        let mut b = Vec::from(&b"h\x00e\x00l\x00l\x00o\x00"[..]);
        let s = WStr::from_utf16le_mut(b.as_mut_slice()).unwrap();
        s.swap_content("world", 'x');
        assert_eq!(b.as_slice(), b"w\x00o\x00r\x00l\x00d\x00");

        // Shorter, padding fits
        let mut b = Vec::from(&b"h\x00e\x00l\x00l\x00o\x00"[..]);
        let s = WStr::from_utf16le_mut(b.as_mut_slice()).unwrap();
        s.swap_content("hey", 'x');
        assert_eq!(b.as_slice(), b"h\x00e\x00y\x00x\x00x\x00");

        // Longer, truncated fits
        let mut b = Vec::from(&b"h\x00e\x00y\x00"[..]);
        let s = WStr::from_utf16le_mut(b.as_mut_slice()).unwrap();
        s.swap_content("world", 'x');
        assert_eq!(b.as_slice(), b"w\x00o\x00r\x00");

        // Longer, truncated + padding
        let mut b = Vec::from(&b"h\x00e\x00y\x00"[..]);
        let s = WStr::from_utf16le_mut(b.as_mut_slice()).unwrap();
        s.swap_content("yo\u{10000}", 'x');
        assert_eq!(b.as_slice(), b"y\x00o\x00x\x00");
    }

    #[test]
    #[should_panic]
    fn test_swap_content_wstr_panic() {
        let mut b = Vec::from(&b"h\x00e\x00y\x00"[..]);
        let s = WStr::from_utf16le_mut(b.as_mut_slice()).unwrap();
        s.swap_content("yo", '\u{10000}');
    }

    #[test]
    fn test_get_wstr_match() {
        #![allow(clippy::trivial_regex)]

        let s = "hello there";
        let mut b = Vec::from(&b"h\x00e\x00l\x00l\x00o\x00 \x00t\x00h\x00e\x00r\x00e\x00"[..]);
        let w = WStr::from_utf16le_mut(b.as_mut_slice()).unwrap();

        // Partial match
        let re = Regex::new("hello").unwrap();
        let re_match = re.find(s).unwrap();
        let m = get_wstr_match(s, re_match, w);
        assert_eq!(m.as_bytes(), b"h\x00e\x00l\x00l\x00o\x00");

        // Full match
        let re = Regex::new(".*").unwrap();
        let re_match = re.find(s).unwrap();
        let m = get_wstr_match(s, re_match, w);
        assert_eq!(
            m.as_bytes(),
            b"h\x00e\x00l\x00l\x00o\x00 \x00t\x00h\x00e\x00r\x00e\x00"
        );
    }
}
