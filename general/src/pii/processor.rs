use std::cmp;
use std::collections::{BTreeMap, BTreeSet};

use hmac::{Hmac, Mac};
use lazy_static::lazy_static;
use regex::Regex;
use sha1::Sha1;
use sha2::{Sha256, Sha512};
use smallvec::SmallVec;

use crate::pii::config::{RuleRef, SelectorRef};
use crate::pii::{HashAlgorithm, PiiConfig, Redaction, RuleType, SelectorType};
use crate::processor::{
    process_chunked_value, Chunk, PiiKind, ProcessValue, ProcessingState, Processor,
};
use crate::protocol::{AsPair, PairList};
use crate::types::{Meta, Object, Remark, RemarkType, ValueAction};

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
    static ref CREDITCARD_REGEX: Regex = Regex::new(
        r#"(?x)
            \d{4}[- ]?\d{4,6}[- ]?\d{4,5}(?:[- ]?\d{4})
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
    applications: BTreeMap<SelectorRef<'a>, BTreeSet<RuleRef<'a>>>,
}

impl<'a> PiiProcessor<'a> {
    /// Creates a new processor based on a config.
    pub fn new(config: &'a PiiConfig) -> PiiProcessor<'_> {
        let mut applications = BTreeMap::new();
        for (id, rules) in &config.applications {
            collect_applications(config, &mut applications, id, rules);
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
    fn iter_rules<'b>(
        &'a self,
        kind: PiiKind,
        state: &'b ProcessingState<'b>,
    ) -> RuleIterator<'a, 'b> {
        RuleIterator {
            kind,
            state,
            application_iter: self.applications.iter(),
            pending_refs: None,
        }
    }
}

struct RuleIterator<'a, 'b> {
    kind: PiiKind,
    state: &'b ProcessingState<'b>,
    application_iter: std::collections::btree_map::Iter<'a, SelectorRef<'a>, BTreeSet<RuleRef<'a>>>,
    pending_refs: Option<std::collections::btree_set::Iter<'a, RuleRef<'a>>>,
}

impl<'a, 'b> Iterator for RuleIterator<'a, 'b> {
    type Item = RuleRef<'a>;

    fn next(&mut self) -> Option<RuleRef<'a>> {
        'outer: loop {
            if let Some(&rv) = self.pending_refs.as_mut().and_then(|x| x.next()) {
                return Some(rv);
            }

            while let Some((selector, rules)) = self.application_iter.next() {
                if selector_applies(selector, self.kind, self.state) {
                    self.pending_refs = Some(rules.iter());
                    continue 'outer;
                }
            }

            return None;
        }
    }
}

impl<'a> Processor for PiiProcessor<'a> {
    fn process_string(
        &mut self,
        string: &mut String,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        let mut rules = self.iter_rules(PiiKind::Text, &state).peekable();
        if rules.peek().is_some() {
            process_chunked_value(string, meta, |chunks| {
                rules.fold(chunks, apply_rule_to_chunks)
            });
        }
        ValueAction::Keep
    }

    fn process_object<T: ProcessValue>(
        &mut self,
        object: &mut Object<T>,
        _meta: &mut Meta,
        state: &ProcessingState,
    ) -> ValueAction {
        let mut rules = self.iter_rules(PiiKind::Container, &state).peekable();
        if rules.peek().is_some() {
            let rules: SmallVec<[RuleRef; 16]> = rules.collect();
            for (key, annotated) in object.iter_mut() {
                for rule in &rules {
                    annotated.apply(|value, meta| {
                        apply_rule_to_container(value, meta, *rule, Some(key.as_str()))
                    });
                }
            }
        }

        object.process_child_values(self, state);
        ValueAction::Keep
    }

    fn process_pairlist<T: ProcessValue + AsPair>(
        &mut self,
        list: &mut PairList<T>,
        _meta: &mut Meta,
        state: &ProcessingState,
    ) -> ValueAction {
        let mut rules = self.iter_rules(PiiKind::Container, &state).peekable();
        if rules.peek().is_some() {
            let rules: SmallVec<[RuleRef; 16]> = rules.collect();
            for annotated in list.iter_mut() {
                for rule in &rules {
                    if let Some(ref mut pair) = annotated.value_mut() {
                        let (ref mut key, ref mut value) = pair.as_pair();
                        value.apply(|value, meta| {
                            apply_rule_to_container(value, meta, *rule, key.as_str())
                        });
                    }
                }
            }
        }

        list.process_child_values(self, state);
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

fn collect_applications<'a, 'b>(
    config: &'a PiiConfig,
    applications: &'b mut BTreeMap<SelectorRef<'a>, BTreeSet<RuleRef<'a>>>,
    selector_id: &'a str,
    rules: &'a [String],
) {
    let selector = match config.selector(selector_id) {
        Some(selector) => selector,
        None => return,
    };

    match &selector.ty {
        SelectorType::Alias(alias) => {
            collect_applications(config, applications, &alias.selector, rules);
        }
        SelectorType::Multiple(multiple) => {
            for selector_id in &multiple.selectors {
                collect_applications(config, applications, &selector_id, rules);
            }
        }
        _ => {
            let mut rule_set = applications.entry(selector).or_default();
            for rule_id in rules {
                collect_rules(config, &mut rule_set, &rule_id, None)
            }
        }
    }
}

fn selector_applies(ty: &SelectorType, kind: PiiKind, state: &ProcessingState) -> bool {
    match ty {
        SelectorType::Kind(selector) => selector.kind == kind,
        SelectorType::Path(selector) => state.path().starts_with_pattern(&selector.path),
        // These are resolved beforehand by the PiiProcessor
        SelectorType::Multiple(_) | SelectorType::Alias(_) => false,
    }
}

fn apply_rule_to_container<T: ProcessValue>(
    _value: &mut T,
    meta: &mut Meta,
    rule: RuleRef<'_>,
    key: Option<&str>,
) -> ValueAction {
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

fn apply_rule_to_chunks(mut chunks: Vec<Chunk>, rule: RuleRef<'_>) -> Vec<Chunk> {
    macro_rules! apply_regex {
        ($regex:expr, $replace_groups:expr) => {
            chunks = apply_regex_to_chunks(chunks, rule, $regex, $replace_groups);
        };
    }

    match rule.ty {
        RuleType::Never => {}
        RuleType::Anything => apply_regex!(&ANYTHING_REGEX, None),
        RuleType::Pattern(pattern) => {
            apply_regex!(&pattern.pattern.0, pattern.replace_groups.as_ref())
        }
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
        // does not apply here
        RuleType::RedactPair { .. } => {}
        RuleType::Alias(_) => {}
        RuleType::Multiple(_) => {}
    }

    chunks
}

fn apply_regex_to_chunks(
    chunks: Vec<Chunk>,
    rule: RuleRef<'_>,
    regex: &Regex,
    replace_groups: Option<&BTreeSet<u8>>,
) -> Vec<Chunk> {
    let mut search_string = String::new();
    let mut replacement_chunks = vec![];
    for chunk in chunks {
        match chunk {
            Chunk::Text { ref text } => search_string.push_str(&text.replace("\x00", "")),
            chunk @ Chunk::Redaction { .. } => {
                replacement_chunks.push(chunk);
                search_string.push('\x00');
            }
        }
    }
    replacement_chunks.reverse();
    let mut rv: Vec<Chunk> = vec![];

    fn process_text(text: &str, rv: &mut Vec<Chunk>, replacement_chunks: &mut Vec<Chunk>) {
        if text.is_empty() {
            return;
        }
        let mut pos = 0;
        for piece in NULL_SPLIT_RE.find_iter(text) {
            rv.push(Chunk::Text {
                text: text[pos..piece.start()].to_string(),
            });
            rv.push(replacement_chunks.pop().unwrap());
            pos = piece.end();
        }
        rv.push(Chunk::Text {
            text: text[pos..].to_string(),
        });
    }

    let mut pos = 0;
    for m in regex.captures_iter(&search_string) {
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

        process_text(
            &search_string[pos..g0.end()],
            &mut rv,
            &mut replacement_chunks,
        );
        pos = g0.end();
    }

    process_text(&search_string[pos..], &mut rv, &mut replacement_chunks);

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

fn insert_replacement_chunks(rule: RuleRef<'_>, text: &str, output: &mut Vec<Chunk>) {
    match rule.redaction {
        Redaction::Default | Redaction::Remove => {
            output.push(Chunk::Redaction {
                rule_id: rule.origin.to_string(),
                ty: RemarkType::Removed,
                text: "".to_string(),
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
                rule_id: rule.origin.to_string(),
                text: buf.into_iter().collect(),
            })
        }
        Redaction::Hash(hash) => {
            output.push(Chunk::Redaction {
                ty: RemarkType::Pseudonymized,
                rule_id: rule.origin.to_string(),
                text: hash_value(
                    hash.algorithm,
                    text,
                    hash.key.as_ref().map(|x| x.as_str()),
                    rule.config,
                ),
            });
        }
        Redaction::Replace(replace) => {
            output.push(Chunk::Redaction {
                ty: RemarkType::Substituted,
                rule_id: rule.origin.to_string(),
                text: replace.text.clone(),
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
            .map(|x| x.as_str())
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
    crate::processor::process_value,
    crate::protocol::{Event, Headers, LogEntry, Request},
    crate::types::{Annotated, Value},
};

#[test]
fn test_basic_stripping() {
    use crate::protocol::{TagEntry, Tags};
    let config = PiiConfig::from_json(
        r#"
        {
            "rules": {
                "remove_bad_headers": {
                    "type": "redact_pair",
                    "keyPattern": "(?i)cookie|secret[-_]?key"
                }
            },
            "applications": {
                "text": ["@ip"],
                "container": ["remove_bad_headers"]
            }
        }
    "#,
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
                    Annotated::new("Cookie".to_string()),
                    Annotated::new("super secret".into()),
                )));
                rv.push(Annotated::new((
                    Annotated::new("X-Forwarded-For".to_string()),
                    Annotated::new("127.0.0.1".into()),
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
        r#"
        {
            "applications": {
                "container": ["@anything"]
            }
        }
    "#,
    )
    .unwrap();

    let mut event = Annotated::new(Event {
        extra: {
            let mut map = Object::new();
            map.insert(
                "foo".to_string(),
                Annotated::new(Value::String("bar".to_string())),
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
  "extra": {
    "foo": null
  },
  "_meta": {
    "extra": {
      "foo": {
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
  }
}"#
    );
}
