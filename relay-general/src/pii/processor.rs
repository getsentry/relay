use std::borrow::Cow;
use std::cmp;
use std::collections::BTreeSet;

use hmac::{Hmac, Mac};
use lazy_static::lazy_static;
use regex::Regex;
use sha1::Sha1;
use sha2::{Sha256, Sha512};

use crate::pii::compiledconfig::RuleRef;
use crate::pii::{CompiledPiiConfig, HashAlgorithm, Redaction, RuleType};
use crate::processor::{
    process_chunked_value, process_value, Chunk, Pii, ProcessValue, ProcessingState, Processor,
    SelectorSpec, ValueType,
};
use crate::protocol::{AsPair, NativeImagePath, PairList};
use crate::types::{Meta, ProcessingAction, ProcessingResult, Remark, RemarkType};

lazy_static! {
    static ref NULL_SPLIT_RE: Regex = #[allow(clippy::trivial_regex)]
    Regex::new("\x00").unwrap();
}

#[rustfmt::skip]
macro_rules! ip {
    (v4s) => { "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)" };
    (v4a) => { concat!(ip!(v4s), "\\.", ip!(v4s), "\\.", ip!(v4s), "\\.", ip!(v4s)) };
    (v6s) => { "[0-9a-fA-F]{1,4}" };
}

#[rustfmt::skip]
lazy_static! {
    static ref GROUP_0: BTreeSet<u8> = {
        let mut set = BTreeSet::new();
        set.insert(0);
        set
    };
    static ref GROUP_1: BTreeSet<u8> = {
        let mut set = BTreeSet::new();
        set.insert(1);
        set
    };
    static ref ANYTHING_REGEX: Regex = Regex::new(".*").unwrap();
    static ref IMEI_REGEX: Regex = Regex::new(
        r#"(?x)
            \b
                (\d{2}-?
                 \d{6}-?
                 \d{6}-?
                 \d{1,2})
            \b
        "#
    ).unwrap();
    static ref MAC_REGEX: Regex = Regex::new(
        r#"(?x)
            \b([[:xdigit:]]{2}[:-]){5}[[:xdigit:]]{2}\b
        "#
    ).unwrap();
    static ref UUID_REGEX: Regex = Regex::new(
        r#"(?ix)
            \b
            [a-z0-9]{8}-?
            [a-z0-9]{4}-?
            [a-z0-9]{4}-?
            [a-z0-9]{4}-?
            [a-z0-9]{12}
            \b
        "#
    ).unwrap();
    static ref EMAIL_REGEX: Regex = Regex::new(
        r#"(?x)
            \b
                [a-zA-Z0-9.!\#$%&'*+/=?^_`{|}~-]+
                @
                [a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*
            \b
        "#
    ).unwrap();
    static ref IPV4_REGEX: Regex = Regex::new(concat!("\\b", ip!(v4a), "\\b")).unwrap();
    static ref IPV6_REGEX: Regex = Regex::new(
        concat!(
            "(?i)(?:[\\s]|[[:punct:]]|^)(",
                "(", ip!(v6s), ":){7}", ip!(v6s), "|",
                "(", ip!(v6s), ":){1,7}:|",
                "(", ip!(v6s), ":){1,6}::", ip!(v6s), "|",
                "(", ip!(v6s), ":){1,5}:(:", ip!(v6s), "){1,2}|",
                "(", ip!(v6s), ":){1,4}:(:", ip!(v6s), "){1,3}|",
                "(", ip!(v6s), ":){1,3}:(:", ip!(v6s), "){1,4}|",
                "(", ip!(v6s), ":){1,2}:(:", ip!(v6s), "){1,5}|",
                ip!(v6s), ":((:", ip!(v6s), "){1,6})|",
                ":((:", ip!(v6s), "){1,7}|:)|",
                "fe80:(:", ip!(v6s), "){0,4}%[0-9a-zA-Z]{1,}",
                "::(ffff(:0{1,4}){0,1}:){0,1}", ip!(v4a), "|",
                "(", ip!(v6s), ":){1,4}:", ip!(v4a),
            ")([\\s]|[[:punct:]]|$)",
        )
    ).unwrap();

    // http://www.richardsramblings.com/regex/credit-card-numbers/
    // Re-formatted with comments and dashes support
    //
    // Why so complicated? Because creditcard numbers are variable length and we do not want to
    // strip any number that just happens to have the same length.
    static ref CREDITCARD_REGEX: Regex = Regex::new(
        r#"(?x)
        \b(
            (?:  # vendor specific prefixes
                  3[47]\d      # amex (no 13-digit version) (length: 15)
                | 4\d{3}       # visa (16-digit version only)
                | 5[1-5]\d\d   # mastercard
                | 65\d\d       # discover network (subset)
                | 6011         # discover network (subset)
            )

            # "wildcard" remainder (allowing dashes in every position because of variable length)
            ([-\s]?\d){12}
        )\b
        "#
    ).unwrap();
    static ref PATH_REGEX: Regex = Regex::new(
        r#"(?ix)
            (?:
                (?:
                    \b(?:[a-zA-Z]:[\\/])?
                    (?:users|home|documents and settings|[^/\\]+[/\\]profiles)[\\/]
                ) | (?:
                    /(?:home|users)/
                )
            )
            (
                [^/\\]+
            )
        "#
    ).unwrap();
    static ref PEM_KEY_REGEX: Regex = Regex::new(
        r#"(?sx)
            (?:
                -----
                BEGIN[A-Z\ ]+(?:PRIVATE|PUBLIC)\ KEY
                -----
                [\t\ ]*\r?\n?
            )
            (.+?)
            (?:
                \r?\n?
                -----
                END[A-Z\ ]+(?:PRIVATE|PUBLIC)\ KEY
                -----
            )
        "#
    ).unwrap();
    static ref URL_AUTH_REGEX: Regex = Regex::new(
        r#"(?x)
            \b(?:
                (?:[a-z0-9+-]+:)?//
                ([a-zA-Z0-9%_.-]+(?::[a-zA-Z0-9%_.-]+)?)
            )@
        "#
    ).unwrap();
    static ref US_SSN_REGEX: Regex = Regex::new(
        r#"(?x)
            \b(
                [0-9]{3}-
                [0-9]{2}-
                [0-9]{4}
            )\b
        "#
    ).unwrap();
}

/// A processor that performs PII stripping.
pub struct PiiProcessor<'a> {
    compiled_config: &'a CompiledPiiConfig,
}

impl<'a> PiiProcessor<'a> {
    /// Creates a new processor based on a config.
    pub fn new(compiled_config: &'a CompiledPiiConfig) -> PiiProcessor<'a> {
        // this constructor needs to be cheap... a new PiiProcessor is created for each event. Move
        // any init logic into CompiledPiiConfig::new.
        //
        // Note: We accept both `PiiConfig` and `CompiledPiiConfig` because the latter makes more
        // sense for benchmarks while the former is obviously the cleaner API for relay-server.
        PiiProcessor { compiled_config }
    }

    /// Iterate over all matching rules.
    fn iter_rules<'b>(&self, state: &'b ProcessingState<'b>) -> RuleIterator<'a, 'b> {
        RuleIterator {
            state,
            application_iter: self.compiled_config.applications.iter(),
            pending_refs: None,
        }
    }
}

struct RuleIterator<'a, 'b> {
    state: &'b ProcessingState<'b>,
    application_iter: std::slice::Iter<'a, (SelectorSpec, BTreeSet<RuleRef>)>,
    pending_refs: Option<std::collections::btree_set::Iter<'a, RuleRef>>,
}

impl<'a, 'b> Iterator for RuleIterator<'a, 'b> {
    type Item = &'a RuleRef;

    fn next(&mut self) -> Option<&'a RuleRef> {
        if self.state.attrs().pii == Pii::False {
            return None;
        }

        'outer: loop {
            if let Some(rv) = self.pending_refs.as_mut().and_then(Iterator::next) {
                return Some(rv);
            }

            while let Some((selector, rules)) = self.application_iter.next() {
                if self.state.attrs().pii == Pii::Maybe && !selector.is_specific() {
                    continue;
                }
                if self.state.path().matches_selector(selector) {
                    self.pending_refs = Some(rules.iter());
                    continue 'outer;
                }
            }

            return None;
        }
    }
}

impl<'a> Processor for PiiProcessor<'a> {
    fn before_process<T: ProcessValue>(
        &mut self,
        value: Option<&T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // booleans cannot be PII, and strings are handled in process_string
        if let Some(ValueType::Boolean) | Some(ValueType::String) = state.value_type() {
            return Ok(());
        }

        if value.is_none() {
            return Ok(());
        }

        // apply rules based on key/path
        for rule in self.iter_rules(state) {
            match apply_rule_to_value(meta, rule, state.path().key(), None) {
                Ok(()) => continue,
                other => return other,
            }
        }
        Ok(())
    }

    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if let "" | "true" | "false" | "null" | "undefined" = value.as_str() {
            return Ok(());
        }

        // same as before_process. duplicated here because we can only check for "true",
        // "false" etc in process_string.
        for rule in self.iter_rules(state) {
            match apply_rule_to_value(meta, rule, state.path().key(), Some(value)) {
                Ok(()) => continue,
                other => return other,
            }
        }
        Ok(())
    }

    fn process_native_image_path(
        &mut self,
        NativeImagePath(ref mut value): &mut NativeImagePath,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // In NativeImagePaths we must not strip the file's basename because that would break
        // processing.
        //
        // We pop the basename from the end of the string, call process_string and push the
        // basename again.
        //
        // The ranges in Meta should still be right as long as we only pop/push from the end of the
        // string. If we decide that we need to preserve anything other than suffixes all PII
        // tooltips/annotations are potentially wrong.

        if let Some(index) = value.rfind(|c| c == '/' || c == '\\') {
            let basename = value.split_off(index);
            match self.process_string(value, meta, state) {
                Ok(()) => value.push_str(&basename),
                Err(ProcessingAction::DeleteValueHard) | Err(ProcessingAction::DeleteValueSoft) => {
                    *value = basename[1..].to_owned();
                }
                Err(ProcessingAction::InvalidTransaction(x)) => {
                    return Err(ProcessingAction::InvalidTransaction(x))
                }
            }
        }

        Ok(())
    }

    fn process_pairlist<T: ProcessValue + AsPair>(
        &mut self,
        value: &mut PairList<T>,
        _meta: &mut Meta,
        state: &ProcessingState,
    ) -> ProcessingResult {
        // View pairlists as objects just for the purpose of PII stripping (e.g. `event.tags.mykey`
        // instead of `event.tags.42.0`). For other purposes such as trimming we would run into
        // problems:
        //
        // * tag keys need to be trimmed too and therefore need to have a path

        for (idx, annotated) in value.iter_mut().enumerate() {
            if let Some(ref mut pair) = annotated.value_mut() {
                let (ref mut key, ref mut value) = pair.as_pair_mut();
                if let Some(ref key_name) = key.as_str() {
                    // if the pair has no key name, we skip over it for PII stripping. It is
                    // still processed with index-based path in the invocation of
                    // `process_child_values`.
                    process_value(
                        value,
                        self,
                        &state.enter_borrowed(
                            key_name,
                            state.inner_attrs(),
                            ValueType::for_field(value),
                        ),
                    )?;
                } else {
                    process_value(
                        value,
                        self,
                        &state.enter_index(idx, state.inner_attrs(), ValueType::for_field(value)),
                    )?;
                }
            }
        }

        Ok(())
    }
}

fn apply_rule_to_value(
    meta: &mut Meta,
    rule: &RuleRef,
    key: Option<&str>,
    mut value: Option<&mut String>,
) -> ProcessingResult {
    // The rule might specify to remove or to redact. If redaction is chosen, we need to
    // chunk up the value, otherwise we need to simply mark the value for deletion.
    let should_redact_chunks = match rule.redaction {
        Redaction::Default | Redaction::Remove => false,
        _ => true,
    };

    macro_rules! apply_regex {
        ($regex:expr, $replace_groups:expr) => {
            if let Some(ref mut value) = value {
                process_chunked_value(value, meta, |chunks| {
                    apply_regex_to_chunks(chunks, rule, $regex, $replace_groups)
                });
            }
        };
    }

    match rule.ty {
        RuleType::RedactPair(ref redact_pair) => {
            if redact_pair.key_pattern.is_match(key.unwrap_or("")) {
                if value.is_some() && should_redact_chunks {
                    // If we're given a string value here, redact the value like we would with
                    // @anything.
                    apply_regex!(&ANYTHING_REGEX, Some(&*GROUP_0));
                } else {
                    meta.add_remark(Remark::new(RemarkType::Removed, rule.origin.clone()));
                    return Err(ProcessingAction::DeleteValueHard);
                }
            } else {
                // If we did not redact using the key, we will redact the entire value if the key
                // appears in it.
                //
                // $replace_groups = None: Replace entire value if match is inside
                // $replace_groups = Some(GROUP_0): Replace entire match
                apply_regex!(&redact_pair.key_pattern, None);
            }
        }
        RuleType::Anything => {
            if value.is_some() && should_redact_chunks {
                apply_regex!(&ANYTHING_REGEX, Some(&*GROUP_0));
            } else {
                // The value is a container, @anything on a container can do nothing but delete.
                meta.add_remark(Remark::new(RemarkType::Removed, rule.origin.clone()));
                return Err(ProcessingAction::DeleteValueHard);
            }
        }

        RuleType::Pattern(ref r) => apply_regex!(&r.pattern.0, r.replace_groups.as_ref()),
        RuleType::Imei => apply_regex!(&IMEI_REGEX, Some(&*GROUP_0)),
        RuleType::Mac => apply_regex!(&MAC_REGEX, Some(&*GROUP_0)),
        RuleType::Uuid => apply_regex!(&UUID_REGEX, Some(&*GROUP_0)),
        RuleType::Email => apply_regex!(&EMAIL_REGEX, Some(&*GROUP_0)),
        RuleType::Ip => {
            apply_regex!(&IPV4_REGEX, Some(&*GROUP_0));
            apply_regex!(&IPV6_REGEX, Some(&*GROUP_1));
        }
        RuleType::Creditcard => apply_regex!(&CREDITCARD_REGEX, Some(&*GROUP_0)),
        RuleType::Pemkey => apply_regex!(&PEM_KEY_REGEX, Some(&*GROUP_1)),
        RuleType::UrlAuth => apply_regex!(&URL_AUTH_REGEX, Some(&*GROUP_1)),
        RuleType::UsSsn => apply_regex!(&US_SSN_REGEX, Some(&*GROUP_0)),
        RuleType::Userpath => apply_regex!(&PATH_REGEX, Some(&*GROUP_1)),

        // These have been resolved by `collect_applications` and will never occur here.
        RuleType::Alias(_) | RuleType::Multiple(_) => {}
    }

    Ok(())
}

fn apply_regex_to_chunks<'a>(
    chunks: Vec<Chunk<'a>>,
    rule: &RuleRef,
    regex: &Regex,
    replace_groups: Option<&BTreeSet<u8>>,
) -> Vec<Chunk<'a>> {
    // NB: This function allocates the entire string and all chunks a second time. This means it
    // cannot reuse chunks and reallocates them. Ideally, we would be able to run the regex directly
    // on the chunks, but the `regex` crate does not support that.

    let mut search_string = String::new();
    for chunk in &chunks {
        match chunk {
            Chunk::Text { text } => search_string.push_str(&text.replace("\x00", "")),
            Chunk::Redaction { .. } => search_string.push('\x00'),
        }
    }

    // Early exit if this regex does not match and return the original chunks.
    let mut captures_iter = regex.captures_iter(&search_string).peekable();
    if captures_iter.peek().is_none() {
        return chunks;
    }

    let mut replacement_chunks = vec![];
    for chunk in chunks {
        if let Chunk::Redaction { .. } = chunk {
            replacement_chunks.push(chunk);
        }
    }
    replacement_chunks.reverse();

    fn process_text<'a>(
        text: &str,
        rv: &mut Vec<Chunk<'a>>,
        replacement_chunks: &mut Vec<Chunk<'a>>,
    ) {
        if text.is_empty() {
            return;
        }

        let mut pos = 0;
        for piece in NULL_SPLIT_RE.find_iter(text) {
            rv.push(Chunk::Text {
                text: Cow::Owned(text[pos..piece.start()].to_string()),
            });
            rv.push(replacement_chunks.pop().unwrap());
            pos = piece.end();
        }

        rv.push(Chunk::Text {
            text: Cow::Owned(text[pos..].to_string()),
        });
    }

    let mut pos = 0;
    let mut rv = Vec::with_capacity(replacement_chunks.len());

    for m in captures_iter {
        match replace_groups {
            Some(groups) => {
                for (idx, g) in m.iter().enumerate() {
                    if let Some(g) = g {
                        if groups.contains(&(idx as u8)) {
                            process_text(
                                &search_string[pos..g.start()],
                                &mut rv,
                                &mut replacement_chunks,
                            );
                            insert_replacement_chunks(&rule, g.as_str(), &mut rv);
                            pos = g.end();
                        }
                    }
                }
            }
            None => {
                process_text(&"", &mut rv, &mut replacement_chunks);
                insert_replacement_chunks(&rule, &search_string, &mut rv);
                pos = search_string.len();
                break;
            }
        }
    }

    process_text(&search_string[pos..], &mut rv, &mut replacement_chunks);
    debug_assert!(replacement_chunks.is_empty());

    rv
}

fn in_range(range: (Option<i32>, Option<i32>), pos: usize, len: usize) -> bool {
    fn get_range_index(idx: Option<i32>, len: usize, default: usize) -> usize {
        match idx {
            None => default,
            Some(idx) if idx < 0 => len.saturating_sub(-idx as usize),
            Some(idx) => cmp::min(idx as usize, len),
        }
    }

    let start = get_range_index(range.0, len, 0);
    let end = get_range_index(range.1, len, len);
    pos >= start && pos < end
}

fn insert_replacement_chunks(rule: &RuleRef, text: &str, output: &mut Vec<Chunk<'_>>) {
    match &rule.redaction {
        Redaction::Default | Redaction::Remove => {
            output.push(Chunk::Redaction {
                text: Cow::Borrowed(""),
                rule_id: Cow::Owned(rule.origin.to_string()),
                ty: RemarkType::Removed,
            });
        }
        Redaction::Mask(mask) => {
            let chars_to_ignore: BTreeSet<char> = mask.chars_to_ignore.chars().collect();
            let mut buf = Vec::with_capacity(text.len());

            for (idx, c) in text.chars().enumerate() {
                if in_range(mask.range, idx, text.len()) && !chars_to_ignore.contains(&c) {
                    buf.push(mask.mask_char);
                } else {
                    buf.push(c);
                }
            }
            output.push(Chunk::Redaction {
                ty: RemarkType::Masked,
                rule_id: Cow::Owned(rule.origin.to_string()),
                text: buf.into_iter().collect(),
            })
        }
        Redaction::Hash(hash) => {
            output.push(Chunk::Redaction {
                ty: RemarkType::Pseudonymized,
                rule_id: Cow::Owned(rule.origin.to_string()),
                text: Cow::Owned(hash_value(hash.algorithm, text, hash.key.as_deref())),
            });
        }
        Redaction::Replace(replace) => {
            output.push(Chunk::Redaction {
                ty: RemarkType::Substituted,
                rule_id: Cow::Owned(rule.origin.to_string()),
                text: Cow::Owned(replace.text.clone()),
            });
        }
    }
}

fn hash_value(algorithm: HashAlgorithm, text: &str, key: Option<&str>) -> String {
    let key = key.unwrap_or("");
    macro_rules! hmac {
        ($ty:ident) => {{
            let mut mac = Hmac::<$ty>::new_varkey(key.as_bytes()).unwrap();
            mac.input(text.as_bytes());
            format!("{:X}", mac.result().code())
        }};
    }
    match algorithm {
        HashAlgorithm::HmacSha1 => hmac!(Sha1),
        HashAlgorithm::HmacSha256 => hmac!(Sha256),
        HashAlgorithm::HmacSha512 => hmac!(Sha512),
    }
}

#[cfg(test)]
use {
    crate::pii::PiiConfig,
    crate::protocol::{
        Addr, DebugImage, DebugMeta, Event, ExtraValue, Headers, LogEntry, NativeDebugImage,
        Request,
    },
    crate::types::{Annotated, Object, Value},
};

#[test]
fn test_basic_stripping() {
    use crate::protocol::{TagEntry, Tags};
    let config = PiiConfig::from_json(
        r##"
        {
            "rules": {
                "remove_bad_headers": {
                    "type": "redact_pair",
                    "keyPattern": "(?i)cookie|secret[-_]?key"
                }
            },
            "applications": {
                "$string": ["@ip"],
                "$object.**": ["remove_bad_headers"]
            }
        }
    "##,
    )
    .unwrap();

    let mut event = Annotated::new(Event {
        logentry: Annotated::new(LogEntry {
            formatted: Annotated::new("Hello world!".to_string()),
            ..Default::default()
        }),
        request: Annotated::new(Request {
            env: {
                let mut rv = Object::new();
                rv.insert(
                    "SECRET_KEY".to_string(),
                    Annotated::new(Value::String("134141231231231231231312".into())),
                );
                Annotated::new(rv)
            },
            headers: {
                let mut rv = Vec::new();
                rv.push(Annotated::new((
                    Annotated::new("Cookie".to_string().into()),
                    Annotated::new("super secret".to_string().into()),
                )));
                rv.push(Annotated::new((
                    Annotated::new("X-Forwarded-For".to_string().into()),
                    Annotated::new("127.0.0.1".to_string().into()),
                )));
                Annotated::new(Headers(PairList(rv)))
            },
            ..Default::default()
        }),
        tags: Annotated::new(Tags(
            vec![Annotated::new(TagEntry(
                Annotated::new("forwarded_for".to_string()),
                Annotated::new("127.0.0.1".to_string()),
            ))]
            .into(),
        )),
        ..Default::default()
    });

    let compiled = config.compiled();
    let mut processor = PiiProcessor::new(&*compiled);
    process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
    assert_annotated_snapshot!(event);
}

#[test]
fn test_redact_containers() {
    let config = PiiConfig::from_json(
        r##"
        {
            "applications": {
                "$object": ["@anything"]
            }
        }
    "##,
    )
    .unwrap();

    let mut event = Annotated::new(Event {
        extra: {
            let mut map = Object::new();
            map.insert(
                "foo".to_string(),
                Annotated::new(ExtraValue(Value::String("bar".to_string()))),
            );
            Annotated::new(map)
        },
        ..Default::default()
    });

    let compiled = config.compiled();
    let mut processor = PiiProcessor::new(&*compiled);
    process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
    assert_annotated_snapshot!(event);
}

#[test]
fn test_redact_custom_pattern() {
    let config = PiiConfig::from_json(
        r##"
        {
            "applications": {
                "$string": ["myrule"]
            },
            "rules": {
                "myrule": {
                    "type": "pattern",
                    "pattern": "foo",
                    "redaction": {
                        "method": "replace",
                        "text": "asd"
                    }
                }
            }
        }
    "##,
    )
    .unwrap();

    let mut event = Annotated::new(Event {
        extra: {
            let mut map = Object::new();
            map.insert(
                "myvalue".to_string(),
                Annotated::new(ExtraValue(Value::String("foobar".to_string()))),
            );
            Annotated::new(map)
        },
        ..Default::default()
    });

    let compiled = config.compiled();
    let mut processor = PiiProcessor::new(&*compiled);
    process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
    assert_annotated_snapshot!(event);
}

#[test]
fn test_no_field_upsert() {
    let config = PiiConfig::from_json(
        r##"
        {
            "applications": {
                "**": ["@anything:remove"]
            }
        }
    "##,
    )
    .unwrap();

    let mut event = Annotated::new(Event {
        extra: {
            let mut map = Object::new();
            map.insert(
                "myvalue".to_string(),
                Annotated::new(ExtraValue(Value::String("foobar".to_string()))),
            );
            Annotated::new(map)
        },
        ..Default::default()
    });

    let compiled = config.compiled();
    let mut processor = PiiProcessor::new(&*compiled);
    process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
    assert_annotated_snapshot!(event);
}

#[test]
fn test_anything_hash_on_string() {
    let config = PiiConfig::from_json(
        r##"
        {
            "applications": {
                "$string": ["@anything:hash"]
            }
        }
    "##,
    )
    .unwrap();

    let mut event = Annotated::new(Event {
        extra: {
            let mut map = Object::new();
            map.insert(
                "myvalue".to_string(),
                Annotated::new(ExtraValue(Value::String("foobar".to_string()))),
            );
            Annotated::new(map)
        },
        ..Default::default()
    });

    let compiled = config.compiled();
    let mut processor = PiiProcessor::new(&*compiled);
    process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
    assert_annotated_snapshot!(event);
}

#[test]
fn test_anything_hash_on_container() {
    let config = PiiConfig::from_json(
        r##"
        {
            "applications": {
                "$object": ["@anything:hash"]
            }
        }
    "##,
    )
    .unwrap();

    let mut event = Annotated::new(Event {
        extra: {
            let mut map = Object::new();
            map.insert(
                "myvalue".to_string(),
                Annotated::new(ExtraValue(Value::String("foobar".to_string()))),
            );
            Annotated::new(map)
        },
        ..Default::default()
    });

    let compiled = config.compiled();
    let mut processor = PiiProcessor::new(&*compiled);
    process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
    assert_annotated_snapshot!(event);
}

#[test]
fn test_remove_debugmeta_path() {
    let config = PiiConfig::from_json(
        r##"
        {
            "applications": {
                "debug_meta.images.*.code_file": ["@anything:remove"],
                "debug_meta.images.*.debug_file": ["@anything:remove"]
            }
        }
        "##,
    )
    .unwrap();

    let mut event = Annotated::new(Event {
        debug_meta: Annotated::new(DebugMeta {
            images: Annotated::new(vec![Annotated::new(DebugImage::Symbolic(Box::new(
                NativeDebugImage {
                    code_id: Annotated::new("59b0d8f3183000".parse().unwrap()),
                    code_file: Annotated::new("C:\\Windows\\System32\\ntdll.dll".into()),
                    debug_id: Annotated::new(
                        "971f98e5-ce60-41ff-b2d7-235bbeb34578-1".parse().unwrap(),
                    ),
                    debug_file: Annotated::new("wntdll.pdb".into()),
                    arch: Annotated::new("arm64".to_string()),
                    image_addr: Annotated::new(Addr(0)),
                    image_size: Annotated::new(4096),
                    image_vmaddr: Annotated::new(Addr(32768)),
                    other: {
                        let mut map = Object::new();
                        map.insert(
                            "other".to_string(),
                            Annotated::new(Value::String("value".to_string())),
                        );
                        map
                    },
                },
            )))]),
            ..Default::default()
        }),
        ..Default::default()
    });

    let compiled = config.compiled();
    let mut processor = PiiProcessor::new(&*compiled);
    process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
    assert_annotated_snapshot!(event);
}

#[test]
fn test_replace_debugmeta_path() {
    let config = PiiConfig::from_json(
        r##"
        {
            "applications": {
                "debug_meta.images.*.code_file": ["@anything:replace"],
                "debug_meta.images.*.debug_file": ["@anything:replace"]
            }
        }
        "##,
    )
    .unwrap();

    let mut event = Annotated::new(Event {
        debug_meta: Annotated::new(DebugMeta {
            images: Annotated::new(vec![Annotated::new(DebugImage::Symbolic(Box::new(
                NativeDebugImage {
                    code_id: Annotated::new("59b0d8f3183000".parse().unwrap()),
                    code_file: Annotated::new("C:\\Windows\\System32\\ntdll.dll".into()),
                    debug_id: Annotated::new(
                        "971f98e5-ce60-41ff-b2d7-235bbeb34578-1".parse().unwrap(),
                    ),
                    debug_file: Annotated::new("wntdll.pdb".into()),
                    arch: Annotated::new("arm64".to_string()),
                    image_addr: Annotated::new(Addr(0)),
                    image_size: Annotated::new(4096),
                    image_vmaddr: Annotated::new(Addr(32768)),
                    other: {
                        let mut map = Object::new();
                        map.insert(
                            "other".to_string(),
                            Annotated::new(Value::String("value".to_string())),
                        );
                        map
                    },
                },
            )))]),
            ..Default::default()
        }),
        ..Default::default()
    });

    let compiled = config.compiled();
    let mut processor = PiiProcessor::new(&*compiled);
    process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
    assert_annotated_snapshot!(event);
}

#[test]
fn test_hash_debugmeta_path() {
    let config = PiiConfig::from_json(
        r##"
        {
            "applications": {
                "debug_meta.images.*.code_file": ["@anything:hash"],
                "debug_meta.images.*.debug_file": ["@anything:hash"]
            }
        }
        "##,
    )
    .unwrap();

    let mut event = Annotated::new(Event {
        debug_meta: Annotated::new(DebugMeta {
            images: Annotated::new(vec![Annotated::new(DebugImage::Symbolic(Box::new(
                NativeDebugImage {
                    code_id: Annotated::new("59b0d8f3183000".parse().unwrap()),
                    code_file: Annotated::new("C:\\Windows\\System32\\ntdll.dll".into()),
                    debug_id: Annotated::new(
                        "971f98e5-ce60-41ff-b2d7-235bbeb34578-1".parse().unwrap(),
                    ),
                    debug_file: Annotated::new("wntdll.pdb".into()),
                    arch: Annotated::new("arm64".to_string()),
                    image_addr: Annotated::new(Addr(0)),
                    image_size: Annotated::new(4096),
                    image_vmaddr: Annotated::new(Addr(32768)),
                    other: {
                        let mut map = Object::new();
                        map.insert(
                            "other".to_string(),
                            Annotated::new(Value::String("value".to_string())),
                        );
                        map
                    },
                },
            )))]),
            ..Default::default()
        }),
        ..Default::default()
    });

    let compiled = config.compiled();
    let mut processor = PiiProcessor::new(&*compiled);
    process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
    assert_annotated_snapshot!(event);
}

#[test]
fn test_debugmeta_path_not_addressible_with_wildcard_selector() {
    let config = PiiConfig::from_json(
        r##"
        {
            "applications": {
                "$string": ["@anything:remove"],
                "**": ["@anything:remove"],
                "debug_meta.**": ["@anything:remove"],
                "(debug_meta.images.**.code_file & $string)": ["@anything:remove"]
            }
        }
        "##,
    )
    .unwrap();

    let mut event = Annotated::new(Event {
        debug_meta: Annotated::new(DebugMeta {
            images: Annotated::new(vec![Annotated::new(DebugImage::Symbolic(Box::new(
                NativeDebugImage {
                    code_id: Annotated::new("59b0d8f3183000".parse().unwrap()),
                    code_file: Annotated::new("C:\\Windows\\System32\\ntdll.dll".into()),
                    debug_id: Annotated::new(
                        "971f98e5-ce60-41ff-b2d7-235bbeb34578-1".parse().unwrap(),
                    ),
                    debug_file: Annotated::new("wntdll.pdb".into()),
                    arch: Annotated::new("arm64".to_string()),
                    image_addr: Annotated::new(Addr(0)),
                    image_size: Annotated::new(4096),
                    image_vmaddr: Annotated::new(Addr(32768)),
                    other: {
                        let mut map = Object::new();
                        map.insert(
                            "other".to_string(),
                            Annotated::new(Value::String("value".to_string())),
                        );
                        map
                    },
                },
            )))]),
            ..Default::default()
        }),
        ..Default::default()
    });

    let compiled = config.compiled();
    let mut processor = PiiProcessor::new(&*compiled);
    process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
    assert_annotated_snapshot!(event);
}

#[test]
fn test_quoted_keys() {
    let config = PiiConfig::from_json(
        r##"
        {
            "applications": {
                "extra.'special ,./<>?!@#$%^&*())''gärbage'''": ["@anything:remove"]
            }
        }
        "##,
    )
    .unwrap();

    let mut event = Annotated::new(Event {
        extra: {
            let mut map = Object::new();
            map.insert(
                "do not ,./<>?!@#$%^&*())'ßtrip'".to_string(),
                Annotated::new(ExtraValue(Value::String("foo".to_string()))),
            );
            map.insert(
                "special ,./<>?!@#$%^&*())'gärbage'".to_string(),
                Annotated::new(ExtraValue(Value::String("bar".to_string()))),
            );
            Annotated::new(map)
        },
        ..Default::default()
    });

    let compiled = config.compiled();
    let mut processor = PiiProcessor::new(&*compiled);
    process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
    assert_annotated_snapshot!(event);
}
