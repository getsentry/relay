use once_cell::sync::Lazy;
use regex::Regex;
use smallvec::{smallvec, SmallVec};

use crate::pii::RuleType;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum PatternType {
    /// Pattern-match on key and value
    KeyValue,
    /// Pattern-match on value only
    Value,
}

/// What to do with regex matches once found.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplaceBehavior {
    /// Replace the entire string value (or just more than the match, depending on context).
    Value,

    /// Replace the following specific regex groups.
    Groups(SmallVec<[u8; 1]>),
}

impl ReplaceBehavior {
    /// Replace the entire string value (or just more than the match, depending on context).
    pub fn replace_value() -> Self {
        ReplaceBehavior::Value
    }

    /// Replace the entire match, equivalent to `ReplaceBehavior::Groups([0])`.
    pub fn replace_match() -> Self {
        ReplaceBehavior::replace_group(0)
    }

    /// Replace the following singular regex group.
    pub fn replace_group(g: u8) -> Self {
        ReplaceBehavior::Groups(smallvec![g])
    }

    /// Replace the following specific regex groups.
    pub fn replace_groups(gs: SmallVec<[u8; 1]>) -> Self {
        ReplaceBehavior::Groups(gs)
    }
}

/// Return a list of regexes to apply for the given rule type.
pub fn get_regex_for_rule_type(
    ty: &RuleType,
) -> SmallVec<[(PatternType, &Regex, ReplaceBehavior); 2]> {
    let v = PatternType::Value;
    let kv = PatternType::KeyValue;

    match ty {
        RuleType::RedactPair(ref redact_pair) => smallvec![(
            kv,
            &redact_pair.key_pattern.0,
            ReplaceBehavior::replace_value()
        )],
        RuleType::Password => {
            smallvec![(kv, &*PASSWORD_KEY_REGEX, ReplaceBehavior::replace_value())]
        }
        RuleType::Anything => smallvec![(v, &*ANYTHING_REGEX, ReplaceBehavior::replace_match())],
        RuleType::Pattern(ref r) => {
            let replace_behavior = match r.replace_groups {
                Some(ref groups) => {
                    ReplaceBehavior::replace_groups(groups.iter().copied().collect())
                }
                None => ReplaceBehavior::replace_match(),
            };

            smallvec![(v, &r.pattern.0, replace_behavior)]
        }

        RuleType::Imei => smallvec![(v, &*IMEI_REGEX, ReplaceBehavior::replace_match())],
        RuleType::Mac => smallvec![(v, &*MAC_REGEX, ReplaceBehavior::replace_match())],
        RuleType::Uuid => smallvec![(v, &*UUID_REGEX, ReplaceBehavior::replace_match())],
        RuleType::Email => smallvec![(v, &*EMAIL_REGEX, ReplaceBehavior::replace_match())],
        RuleType::Ip => smallvec![
            (v, &*IPV4_REGEX, ReplaceBehavior::replace_match()),
            (v, &*IPV6_REGEX, ReplaceBehavior::replace_group(1)),
        ],
        RuleType::Creditcard => {
            smallvec![(v, &*CREDITCARD_REGEX, ReplaceBehavior::replace_match())]
        }
        RuleType::Pemkey => smallvec![(v, &*PEM_KEY_REGEX, ReplaceBehavior::replace_group(1))],
        RuleType::UrlAuth => smallvec![(v, &*URL_AUTH_REGEX, ReplaceBehavior::replace_group(1))],
        RuleType::UsSsn => smallvec![(v, &*US_SSN_REGEX, ReplaceBehavior::replace_match())],
        RuleType::Userpath => smallvec![(v, &*PATH_REGEX, ReplaceBehavior::replace_group(1))],

        // These ought to have been resolved in CompiledConfig
        RuleType::Alias(_) | RuleType::Multiple(_) => smallvec![],
    }
}

#[rustfmt::skip]
macro_rules! ip {
    (v4s) => { "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)" };
    (v4a) => { concat!(ip!(v4s), "\\.", ip!(v4s), "\\.", ip!(v4s), "\\.", ip!(v4s)) };
    (v6s) => { "[0-9a-fA-F]{1,4}" };
}

pub static ANYTHING_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(".*").unwrap());

static IMEI_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?x)
            \b
                (\d{2}-?
                 \d{6}-?
                 \d{6}-?
                 \d{1,2})
            \b
        "#,
    )
    .unwrap()
});

static MAC_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?x)
            \b([[:xdigit:]]{2}[:-]){5}[[:xdigit:]]{2}\b
        "#,
    )
    .unwrap()
});

static UUID_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?ix)
            \b
            [a-z0-9]{8}-?
            [a-z0-9]{4}-?
            [a-z0-9]{4}-?
            [a-z0-9]{4}-?
            [a-z0-9]{12}
            \b
        "#,
    )
    .unwrap()
});

static EMAIL_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?x)
            \b
                [a-zA-Z0-9.!\#$%&'*+/=?^_`{|}~-]+
                @
                [a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*
            \b
        "#,
    )
    .unwrap()
});

static IPV4_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(concat!("\\b", ip!(v4a), "\\b")).unwrap());

#[rustfmt::skip]
static  IPV6_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
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
    ).unwrap()
});

// http://www.richardsramblings.com/regex/credit-card-numbers/
// Re-formatted with comments and dashes support
//
// Why so complicated? Because creditcard numbers are variable length and we do not want to
// strip any number that just happens to have the same length.
static CREDITCARD_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
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
        "#,
    )
    .unwrap()
});

static PATH_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
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
        "#,
    )
    .unwrap()
});

static PEM_KEY_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
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
        "#,
    )
    .unwrap()
});

static URL_AUTH_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?x)
            \b(?:
                (?:[a-z0-9+-]+:)?//
                ([a-zA-Z0-9%_.-]+(?::[a-zA-Z0-9%_.-]+)?)
            )@
        "#,
    )
    .unwrap()
});

static US_SSN_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?x)
            \b(
                [0-9]{3}-
                [0-9]{2}-
                [0-9]{4}
            )\b
        "#,
    )
    .unwrap()
});

static PASSWORD_KEY_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)(password|secret|passwd|api_key|apikey|auth|credentials|mysql_pwd|privatekey|private_key|set-cookie|token)"
    ).unwrap()
});
