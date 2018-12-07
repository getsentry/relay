use std::cmp;
use std::collections::BTreeSet;

use hmac::{Hmac, Mac};
use lazy_static::lazy_static;
use regex::Regex;
use sha1::Sha1;
use sha2::{Sha256, Sha512};

use crate::pii::config::PiiConfig;
use crate::pii::redactions::{HashAlgorithm, Redaction};
use crate::pii::rules::{Rule, RuleType};
use crate::processor::Chunk;
use crate::types::RemarkType;

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
}

pub fn apply_rule_to_chunks(rule: &Rule<'_>, chunks: Vec<Chunk>) -> Vec<Chunk> {
    apply_rule_to_chunks_impl(rule, chunks, None, None)
}

fn apply_rule_to_chunks_impl(
    rule: &Rule<'_>,
    chunks: Vec<Chunk>,
    report_rule: Option<&Rule<'_>>,
    redaction_override: Option<&Redaction>,
) -> Vec<Chunk> {
    let report_rule = report_rule.unwrap_or(rule);
    let redaction = redaction_override.unwrap_or(&rule.spec.redaction);

    let mut rv = chunks;
    macro_rules! apply_regex {
        ($regex:expr, $replace_groups:expr) => {{
            rv = apply_regex_to_chunks(
                redaction,
                rv,
                $regex,
                $replace_groups,
                report_rule,
                rule.cfg,
            );
        }};
    }

    match rule.spec.ty {
        RuleType::Anything => apply_regex!(&ANYTHING_REGEX, None),
        RuleType::Pattern(ref pattern) => {
            apply_regex!(&pattern.pattern.0, pattern.replace_groups.as_ref())
        }
        RuleType::Imei => apply_regex!(&IMEI_REGEX, None),
        RuleType::Mac => apply_regex!(&MAC_REGEX, None),
        RuleType::Email => apply_regex!(&EMAIL_REGEX, None),
        RuleType::Ip => {
            apply_regex!(&IPV4_REGEX, None);
            apply_regex!(&IPV6_REGEX, Some(&*GROUP_1));
        }
        RuleType::Creditcard => apply_regex!(&CREDITCARD_REGEX, None),
        RuleType::Userpath => apply_regex!(&PATH_REGEX, Some(&*GROUP_1)),
        RuleType::Alias(ref alias) => {
            if let Some((rule, report_rule, redaction_override)) =
                rule.lookup_referenced_rule(&alias.rule, alias.hide_rule)
            {
                rv = apply_rule_to_chunks_impl(&rule, rv, report_rule, redaction_override);
            }
        }
        RuleType::Multiple(ref multiple) => {
            for rule_id in multiple.rules.iter() {
                if let Some((rule, report_rule, redaction_override)) =
                    rule.lookup_referenced_rule(rule_id, multiple.hide_rule)
                {
                    rv = apply_rule_to_chunks_impl(&rule, rv, report_rule, redaction_override);
                }
            }
        }
        // does not apply here
        RuleType::RedactPair { .. } => {}
    }

    rv
}

fn apply_regex_to_chunks(
    redaction: &Redaction,
    chunks: Vec<Chunk>,
    regex: &Regex,
    replace_groups: Option<&BTreeSet<u8>>,
    rule: &Rule<'_>,
    config: &PiiConfig,
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
                            insert_replacement_chunks(redaction, rule, config, g.as_str(), &mut rv);
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
                insert_replacement_chunks(redaction, rule, config, g0.as_str(), &mut rv);
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

fn insert_replacement_chunks(
    redaction: &Redaction,
    rule: &Rule<'_>,
    config: &PiiConfig,
    text: &str,
    output: &mut Vec<Chunk>,
) {
    match *redaction {
        Redaction::Default | Redaction::Remove => {
            output.push(Chunk::Redaction {
                rule_id: rule.id.to_string(),
                ty: RemarkType::Removed,
                text: "".to_string(),
            });
        }
        Redaction::Mask(ref mask) => {
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
                rule_id: rule.id.to_string(),
                text: buf.into_iter().collect(),
            })
        }
        Redaction::Hash(ref hash) => {
            output.push(Chunk::Redaction {
                ty: RemarkType::Pseudonymized,
                rule_id: rule.id.to_string(),
                text: hash_value(
                    hash.algorithm,
                    text,
                    hash.key.as_ref().map(|x| x.as_str()),
                    config,
                ),
            });
        }
        Redaction::Replace(ref replace) => {
            output.push(Chunk::Redaction {
                ty: RemarkType::Substituted,
                rule_id: rule.id.to_string(),
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
