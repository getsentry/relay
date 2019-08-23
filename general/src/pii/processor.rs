use std::borrow::Cow;
use std::cmp;
use std::collections::BTreeSet;

use hmac::{Hmac, Mac};
use lazy_static::lazy_static;
use regex::Regex;
use sha1::Sha1;
use sha2::{Sha256, Sha512};
use smallvec::SmallVec;

use crate::pii::config::RuleRef;
use crate::pii::{HashAlgorithm, PiiConfig, Redaction, RuleType};
use crate::processor::{
    process_chunked_value, process_value, Chunk, ProcessValue, ProcessingState, Processor,
    SelectorSpec, ValueType,
};
use crate::protocol::{AsPair, PairList};
use crate::types::{Meta, Remark, RemarkType, ValueAction};

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
    static ref CREDITCARD_REGEX: Regex = Regex::new(
        r#"(?x)(
            \b(?:3[47]\d|(?:4\d|5[1-5]|65)\d{2}|6011)\d{12}\b
        )
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
    config: &'a PiiConfig,
    applications: Vec<(&'a SelectorSpec, BTreeSet<RuleRef<'a>>)>,
}

impl<'a> PiiProcessor<'a> {
    /// Creates a new processor based on a config.
    pub fn new(config: &'a PiiConfig) -> PiiProcessor<'_> {
        let mut applications = Vec::new();
        for (selector, rules) in &config.applications {
            let mut rule_set = BTreeSet::default();
            for rule_id in rules {
                collect_rules(config, &mut rule_set, &rule_id, None);
            }
            applications.push((selector, rule_set));
        }

        PiiProcessor {
            config,
            applications,
        }
    }

    /// Returns a reference to the config.
    pub fn config(&self) -> &PiiConfig {
        self.config
    }

    /// Iterate over all matching rules.
    fn iter_rules<'b>(&'a self, state: &'b ProcessingState<'b>) -> RuleIterator<'a, 'b> {
        RuleIterator {
            state,
            application_iter: self.applications.iter(),
            pending_refs: None,
        }
    }
}

struct RuleIterator<'a, 'b> {
    state: &'b ProcessingState<'b>,
    application_iter: std::slice::Iter<'a, (&'a SelectorSpec, BTreeSet<RuleRef<'a>>)>,
    pending_refs: Option<std::collections::btree_set::Iter<'a, RuleRef<'a>>>,
}

impl<'a, 'b> Iterator for RuleIterator<'a, 'b> {
    type Item = RuleRef<'a>;

    fn next(&mut self) -> Option<RuleRef<'a>> {
        'outer: loop {
            if let Some(&rv) = self.pending_refs.as_mut().and_then(Iterator::next) {
                return Some(rv);
            }

            while let Some((selector, rules)) = self.application_iter.next() {
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
        _value: Option<&T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        // booleans cannot be PII, and strings are handled in process_string
        if let Some(ValueType::Boolean) | Some(ValueType::String) = state.value_type() {
            return ValueAction::Keep;
        }

        // apply rules based on key/path
        for rule in self.iter_rules(state) {
            match apply_rule_to_value(meta, rule, state.path().key()) {
                ValueAction::Keep => continue,
                other => return other,
            }
        }
        ValueAction::Keep
    }

    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        if let "true" | "false" | "null" | "undefined" = value.as_str() {
            return ValueAction::Keep;
        }

        let mut rules = self.iter_rules(state).peekable();
        if rules.peek().is_some() {
            let rules: SmallVec<[RuleRef; 16]> = rules.collect();

            // same as before_process. duplicated here because we can only check for "true",
            // "false" etc in process_string.
            for rule in &rules {
                match apply_rule_to_value(meta, *rule, state.path().key()) {
                    ValueAction::Keep => continue,
                    other => return other,
                }
            }

            // apply rules based on value (basically all the regexes)
            process_chunked_value(value, meta, |mut chunks| {
                for rule in &rules {
                    chunks = apply_rule_to_chunks(chunks, *rule);
                }
                chunks
            });
        }
        ValueAction::Keep
    }

    fn process_pairlist<T: ProcessValue + AsPair>(
        &mut self,
        value: &mut PairList<T>,
        _meta: &mut Meta,
        state: &ProcessingState,
    ) -> ValueAction {
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
                    );
                } else {
                    process_value(
                        value,
                        self,
                        &state.enter_index(idx, state.inner_attrs(), ValueType::for_field(value)),
                    );
                }
            }
        }

        ValueAction::Keep
    }
}

fn collect_rules<'a, 'b>(
    config: &'a PiiConfig,
    rules: &'b mut BTreeSet<RuleRef<'a>>,
    rule_id: &'a str,
    parent: Option<RuleRef<'a>>,
) {
    let rule = match config.rule(rule_id) {
        Some(rule) => rule,
        None => return,
    };

    if rules.contains(&rule) {
        return;
    }

    let rule = match parent {
        Some(parent) => rule.for_parent(parent),
        None => rule,
    };

    match rule.ty {
        RuleType::Multiple(m) => {
            let parent = if m.hide_inner { Some(rule) } else { None };
            for rule_id in &m.rules {
                collect_rules(config, rules, &rule_id, parent);
            }
        }
        RuleType::Alias(a) => {
            let parent = if a.hide_inner { Some(rule) } else { None };
            collect_rules(config, rules, &a.rule, parent);
        }
        _ => {
            rules.insert(rule);
        }
    }
}

fn apply_rule_to_value(meta: &mut Meta, rule: RuleRef<'_>, key: Option<&str>) -> ValueAction {
    match rule.ty {
        RuleType::RedactPair(ref redact_pair) => {
            if redact_pair.key_pattern.is_match(key.unwrap_or("")) {
                meta.add_remark(Remark::new(RemarkType::Removed, rule.origin));
                ValueAction::DeleteHard
            } else {
                ValueAction::Keep
            }
        }
        RuleType::Never => ValueAction::Keep,
        RuleType::Anything => {
            meta.add_remark(Remark::new(RemarkType::Removed, rule.origin));
            ValueAction::DeleteHard
        }

        // These are not handled by the container code but will be independently picked
        // up by the string matching code later.
        RuleType::Pattern(..)
        | RuleType::Imei
        | RuleType::Mac
        | RuleType::Uuid
        | RuleType::Email
        | RuleType::Ip
        | RuleType::Creditcard
        | RuleType::Pemkey
        | RuleType::UrlAuth
        | RuleType::UsSsn
        | RuleType::Userpath => ValueAction::Keep,

        // These have been resolved by `collect_applications` and will never occur here.
        RuleType::Alias(_) | RuleType::Multiple(_) => ValueAction::Keep,
    }
}

fn apply_rule_to_chunks<'a>(mut chunks: Vec<Chunk<'a>>, rule: RuleRef<'_>) -> Vec<Chunk<'a>> {
    macro_rules! apply_regex {
        ($regex:expr, $replace_groups:expr) => {
            chunks = apply_regex_to_chunks(chunks, rule, $regex, $replace_groups);
        };
    }

    match rule.ty {
        RuleType::Never => {}
        RuleType::Anything => apply_regex!(&ANYTHING_REGEX, None),
        RuleType::Pattern(r) => apply_regex!(&r.pattern.0, r.replace_groups.as_ref()),
        RuleType::Imei => apply_regex!(&IMEI_REGEX, None),
        RuleType::Mac => apply_regex!(&MAC_REGEX, None),
        RuleType::Uuid => apply_regex!(&UUID_REGEX, None),
        RuleType::Email => apply_regex!(&EMAIL_REGEX, None),
        RuleType::Ip => {
            apply_regex!(&IPV4_REGEX, None);
            apply_regex!(&IPV6_REGEX, Some(&*GROUP_1));
        }
        RuleType::Creditcard => apply_regex!(&CREDITCARD_REGEX, None),
        RuleType::Pemkey => apply_regex!(&PEM_KEY_REGEX, Some(&*GROUP_1)),
        RuleType::UrlAuth => apply_regex!(&URL_AUTH_REGEX, Some(&*GROUP_1)),
        RuleType::UsSsn => apply_regex!(&US_SSN_REGEX, None),
        RuleType::Userpath => apply_regex!(&PATH_REGEX, Some(&*GROUP_1)),
        RuleType::RedactPair(ref redact_pair) => apply_regex!(&redact_pair.key_pattern, None),
        // does not apply here
        RuleType::Alias(_) => {}
        RuleType::Multiple(_) => {}
    }

    chunks
}

fn apply_regex_to_chunks<'a>(
    chunks: Vec<Chunk<'a>>,
    rule: RuleRef<'_>,
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
        let g0 = m.get(0).unwrap();

        match replace_groups {
            Some(groups) => {
                for (idx, g) in m.iter().enumerate() {
                    if idx == 0 {
                        continue;
                    }

                    if let Some(g) = g {
                        if groups.contains(&(idx as u8)) {
                            process_text(
                                &search_string[pos..g.start()],
                                &mut rv,
                                &mut replacement_chunks,
                            );
                            insert_replacement_chunks(rule, g.as_str(), &mut rv);
                            pos = g.end();
                        }
                    }
                }
            }
            None => {
                process_text(
                    &search_string[pos..g0.start()],
                    &mut rv,
                    &mut replacement_chunks,
                );
                insert_replacement_chunks(rule, g0.as_str(), &mut rv);
                pos = g0.end();
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

fn insert_replacement_chunks(rule: RuleRef<'_>, text: &str, output: &mut Vec<Chunk<'_>>) {
    match rule.redaction {
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
                text: Cow::Owned(hash_value(
                    hash.algorithm,
                    text,
                    hash.key.as_ref().map(String::as_str),
                    rule.config,
                )),
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

fn hash_value(
    algorithm: HashAlgorithm,
    text: &str,
    key: Option<&str>,
    config: &PiiConfig,
) -> String {
    let key = key.unwrap_or_else(|| {
        config
            .vars
            .hash_key
            .as_ref()
            .map(String::as_str)
            .unwrap_or("")
    });
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
    crate::protocol::{Event, ExtraValue, Headers, LogEntry, Request},
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
            formatted: Annotated::new("Hello from 127.0.0.1!".to_string()),
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

    let mut processor = PiiProcessor::new(&config);
    process_value(&mut event, &mut processor, ProcessingState::root());

    assert_eq_str!(
        event.to_json_pretty().unwrap(),
        r#"{
  "logentry": {
    "formatted": "Hello from [ip]!"
  },
  "request": {
    "headers": [
      [
        "Cookie",
        null
      ],
      [
        "X-Forwarded-For",
        "[ip]"
      ]
    ],
    "env": {
      "SECRET_KEY": null
    }
  },
  "tags": [
    [
      "forwarded_for",
      "[ip]"
    ]
  ],
  "_meta": {
    "logentry": {
      "formatted": {
        "": {
          "rem": [
            [
              "@ip",
              "s",
              11,
              15
            ]
          ],
          "len": 21
        }
      }
    },
    "request": {
      "env": {
        "SECRET_KEY": {
          "": {
            "rem": [
              [
                "remove_bad_headers",
                "x"
              ]
            ]
          }
        }
      },
      "headers": {
        "0": {
          "1": {
            "": {
              "rem": [
                [
                  "remove_bad_headers",
                  "x"
                ]
              ]
            }
          }
        },
        "1": {
          "1": {
            "": {
              "rem": [
                [
                  "@ip",
                  "s",
                  0,
                  4
                ]
              ],
              "len": 9
            }
          }
        }
      }
    },
    "tags": {
      "0": {
        "1": {
          "": {
            "rem": [
              [
                "@ip",
                "s",
                0,
                4
              ]
            ],
            "len": 9
          }
        }
      }
    }
  }
}"#
    );
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

    let mut processor = PiiProcessor::new(&config);
    process_value(&mut event, &mut processor, ProcessingState::root());

    assert_eq_str!(
        event.to_json_pretty().unwrap(),
        r#"{
  "extra": null,
  "_meta": {
    "extra": {
      "": {
        "rem": [
          [
            "@anything",
            "x"
          ]
        ]
      }
    }
  }
}"#
    );
}
