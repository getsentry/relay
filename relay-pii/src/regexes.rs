use std::sync::LazyLock;

use regex::Regex;
use smallvec::{SmallVec, smallvec};

use crate::config::RuleType;

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
        RuleType::RedactPair(redact_pair) => {
            if let Ok(pattern) = redact_pair.key_pattern.compiled() {
                smallvec![(kv, pattern, ReplaceBehavior::replace_value())]
            } else {
                smallvec![]
            }
        }
        RuleType::Bearer => {
            smallvec![(v, &*BEARER_TOKEN_REGEX, ReplaceBehavior::replace_match())]
        }
        RuleType::Password => {
            smallvec![
                // Bearer token was moved to its own regest and type out of the passwords, but we
                // still keep it here for backwards compatibility.
                (v, &*BEARER_TOKEN_REGEX, ReplaceBehavior::replace_match()),
                (kv, &*PASSWORD_KEY_REGEX, ReplaceBehavior::replace_value()),
            ]
        }
        RuleType::Anything => smallvec![(v, &*ANYTHING_REGEX, ReplaceBehavior::replace_match())],
        RuleType::Pattern(r) => {
            let replace_behavior = match r.replace_groups {
                Some(ref groups) => {
                    ReplaceBehavior::replace_groups(groups.iter().copied().collect())
                }
                None => ReplaceBehavior::replace_match(),
            };
            if let Ok(pattern) = r.pattern.compiled() {
                smallvec![(v, pattern, replace_behavior)]
            } else {
                smallvec![]
            }
        }

        RuleType::Imei => smallvec![(v, &*IMEI_REGEX, ReplaceBehavior::replace_match())],
        RuleType::Mac => smallvec![(v, &*MAC_REGEX, ReplaceBehavior::replace_match())],
        RuleType::Uuid => smallvec![(v, &*UUID_REGEX, ReplaceBehavior::replace_match())],
        RuleType::Email => smallvec![(v, &*EMAIL_REGEX, ReplaceBehavior::replace_match())],
        RuleType::Iban => smallvec![(v, &*IBAN_REGEX, ReplaceBehavior::replace_match())],
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
        RuleType::Alias(_) | RuleType::Multiple(_) | RuleType::Unknown(_) => smallvec![],
    }
}

#[rustfmt::skip]
macro_rules! ip {
    (v4s) => { "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)" };
    (v4a) => { concat!(ip!(v4s), "\\.", ip!(v4s), "\\.", ip!(v4s), "\\.", ip!(v4s)) };
    (v6s) => { "[0-9a-fA-F]{1,4}" };
}

macro_rules! regex {
    ($name:ident, $rule:expr) => {
        #[allow(non_snake_case)]
        mod $name {
            use super::*;
            pub static $name: LazyLock<Regex> = LazyLock::new(|| Regex::new($rule).unwrap());

            #[test]
            fn supports_byte_mode() {
                assert!(
                    regex::bytes::RegexBuilder::new($name.as_str())
                        .unicode(false)
                        .multi_line(false)
                        .dot_matches_new_line(true)
                        .build()
                        .is_ok()
                );
            }
        }
        use $name::$name;
    };
}

pub static ANYTHING_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(".*").unwrap());

regex!(
    IMEI_REGEX,
    r"(?x)
        \b
            (\d{2}-?
                \d{6}-?
                \d{6}-?
                \d{1,2})
        \b
    "
);

regex!(
    MAC_REGEX,
    r"(?x)
        \b([[:xdigit:]]{2}[:-]){5}[[:xdigit:]]{2}\b
    "
);

regex!(
    UUID_REGEX,
    r"(?ix)
        \b
        [a-z0-9]{8}-?
        [a-z0-9]{4}-?
        [a-z0-9]{4}-?
        [a-z0-9]{4}-?
        [a-z0-9]{12}
        \b
    "
);

regex!(
    EMAIL_REGEX,
    r"(?x)
        \b
            [a-zA-Z0-9.!\#$%&'*+/=?^_`{|}~-]+
            @
            [a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*
        \b
    "
);

regex!(
    IBAN_REGEX,
    r"(?x)
        \b
        (AT|AD|AE|AL|AZ|BA|BE|BG|BH|BR|BY|CH|CR|CY|CZ|DE|DK|DO|EE|EG|ES|FI|FO|FR|GB|GE|GI|GL|GR|GT|HR|HU|IE|IL|IQ|IS|IT|JO|KW|KZ|LB|LC|LI|LT|LU|LV|LY|MC|MD|ME|MK|MR|MT|MU|NL|NO|PK|PL|PS|PT|QA|RO|RU|RS|SA|SC|SE|SI|SK|SM|ST|SV|TL|TN|TR|UA|VA|VG|XK|DZ|AO|BJ|BF|BI|CV|CM|CF|TD|KM|CG|CI|DJ|GQ|GA|GW|HN|IR|MG|ML|MA|MZ|NI|NE|SN|TG)\d{2}[a-zA-Z0-9]{11,29}
        \b
    "
);

regex!(IPV4_REGEX, concat!("\\b", ip!(v4a), "\\b"));

regex!(
    IPV6_REGEX,
    concat!(
        "(?i)(?:[\\s]|[[:punct:]]|^)(",
        "(",
        ip!(v6s),
        ":){7}",
        ip!(v6s),
        "|",
        "(",
        ip!(v6s),
        ":){1,7}:|",
        "(",
        ip!(v6s),
        ":){1,6}::",
        ip!(v6s),
        "|",
        "(",
        ip!(v6s),
        ":){1,5}:(:",
        ip!(v6s),
        "){1,2}|",
        "(",
        ip!(v6s),
        ":){1,4}:(:",
        ip!(v6s),
        "){1,3}|",
        "(",
        ip!(v6s),
        ":){1,3}:(:",
        ip!(v6s),
        "){1,4}|",
        "(",
        ip!(v6s),
        ":){1,2}:(:",
        ip!(v6s),
        "){1,5}|",
        ip!(v6s),
        ":((:",
        ip!(v6s),
        "){1,6})|",
        ":((:",
        ip!(v6s),
        "){1,7}|:)|",
        "fe80:(:",
        ip!(v6s),
        "){0,4}%[0-9a-zA-Z]{1,}",
        "::(ffff(:0{1,4}){0,1}:){0,1}",
        ip!(v4a),
        "|",
        "(",
        ip!(v6s),
        ":){1,4}:",
        ip!(v4a),
        ")([\\s]|[[:punct:]]|$)",
    )
);

// http://www.richardsramblings.com/regex/credit-card-numbers/
// Re-formatted with comments and dashes support
//
// Why so complicated? Because creditcard numbers are variable length and we do not want to
// strip any number that just happens to have the same length.
regex!(
    CREDITCARD_REGEX,
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
);

regex!(
    PATH_REGEX,
    r"(?ix)
        (?:
            (?:
                \b(?:[a-zA-Z]:[\\/])?
                (?:users|home|documents and settings|[^/\\]+[/\\]profiles)[\\/]
            ) | (?:
                /(?:home|users)/
            )
        )
        (
            [^/\\\r\n]+
        )
    "
);

regex!(
    PEM_KEY_REGEX,
    r"(?sx)
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
    "
);

regex!(
    URL_AUTH_REGEX,
    r"(?x)
        \b(?:
            (?:[a-z0-9+-]+:)?//
            ([a-zA-Z0-9%_.-]+(?::[a-zA-Z0-9%_.-]+)?)
        )@
    "
);

regex!(
    US_SSN_REGEX,
    r"(?x)
        \b(
            [0-9]{3}-
            [0-9]{2}-
            [0-9]{4}
        )\b
    "
);

regex!(BEARER_TOKEN_REGEX, r"(?i)\b(Bearer\s+)([^\s]+)");

regex!(
    PASSWORD_KEY_REGEX,
    r"(?i)(password|secret|passwd|api_key|apikey|auth|credentials|mysql_pwd|privatekey|private_key|token|otp|two[-_]factor)"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_userpath_utf8_bytes() {
        // This mimicks `apply_regex_to_utf8_bytes`, which is used in minidump scrubbing.
        // Ideally we would not compile a regex on the fly for every minidump
        // (either add another lazy static or remove the distinction entirely).
        let regex = regex::bytes::RegexBuilder::new(PATH_REGEX.as_str())
            .unicode(false)
            .multi_line(false)
            .dot_matches_new_line(true)
            .build()
            .unwrap();
        assert!(regex.is_match(br"C:\\Users\jane\somefile"));
    }
}
