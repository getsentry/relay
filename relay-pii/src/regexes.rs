use std::sync::LazyLock;

use regex::Regex;
use smallvec::{SmallVec, smallvec};

use crate::config::RuleType;
use crate::regex::{ScrubbingRegex, ScrubbingRegexRef};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum PatternType {
    /// Pattern-match on key and value
    KeyValue,
    /// Pattern-match on key only, apply replacement on the value.
    Key,
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
) -> SmallVec<[(PatternType, ScrubbingRegexRef<'_>, ReplaceBehavior); 2]> {
    let v = PatternType::Value;
    let k = PatternType::Key;
    let kv = PatternType::KeyValue;
    let replace_match = ReplaceBehavior::replace_match;
    let replace_value = ReplaceBehavior::replace_value;
    let replace_group = ReplaceBehavior::replace_group;

    match ty {
        RuleType::RedactPair(redact_pair) => {
            if let Ok(pattern) = redact_pair.key_pattern.compiled() {
                smallvec![(kv, ScrubbingRegex::new(pattern), replace_value())]
            } else {
                smallvec![]
            }
        }
        RuleType::Bearer => smallvec![(v, BEARER_TOKEN_REGEX.as_ref(), replace_match())],
        RuleType::Password => {
            smallvec![
                // Bearer token was moved to its own regest and type out of the passwords, but we
                // still keep it here for backwards compatibility.
                (v, BEARER_TOKEN_REGEX.as_ref(), replace_match()),
                (k, TOKEN_KEY_REGEX.as_ref(), replace_value()),
                (kv, PASSWORD_KEY_REGEX.as_ref(), replace_value()),
            ]
        }
        RuleType::Anything => smallvec![(v, ANYTHING_REGEX.as_ref(), replace_match())],
        RuleType::Pattern(r) => {
            let replace_behavior = match r.replace_groups {
                Some(ref groups) => {
                    ReplaceBehavior::replace_groups(groups.iter().copied().collect())
                }
                None => replace_match(),
            };
            if let Ok(pattern) = r.pattern.compiled() {
                smallvec![(v, ScrubbingRegex::new(pattern), replace_behavior)]
            } else {
                smallvec![]
            }
        }

        RuleType::Imei => smallvec![(v, IMEI_REGEX.as_ref(), replace_match())],
        RuleType::Mac => smallvec![(v, MAC_REGEX.as_ref(), replace_match())],
        RuleType::Uuid => smallvec![(v, UUID_REGEX.as_ref(), replace_match())],
        RuleType::Email => smallvec![(v, EMAIL_REGEX.as_ref(), replace_match())],
        RuleType::Iban => smallvec![(v, IBAN_REGEX.as_ref(), replace_match())],
        RuleType::Ip => smallvec![
            (v, IPV4_REGEX.as_ref(), replace_match()),
            (v, IPV6_REGEX.as_ref(), replace_group(1)),
        ],
        RuleType::Creditcard => smallvec![(v, CREDITCARD_REGEX.as_ref(), replace_match())],
        RuleType::Pemkey => smallvec![(v, PEM_KEY_REGEX.as_ref(), replace_group(1))],
        RuleType::UrlAuth => smallvec![(v, URL_AUTH_REGEX.as_ref(), replace_group(1))],
        RuleType::UsSsn => smallvec![(v, US_SSN_REGEX.as_ref(), replace_match())],
        RuleType::Userpath => smallvec![(v, PATH_REGEX.as_ref(), replace_group(1))],
        RuleType::Cookies => smallvec![(kv, COOKIE_REGEX.as_ref(), replace_group(1))],

        // These ought to have been resolved in CompiledConfig
        RuleType::Alias(_) | RuleType::Multiple(_) | RuleType::Unknown(_) => smallvec![],
    }
}

/// Validates the Luhn checksum of all ASCII digits in `s`, ignoring separators.
fn is_luhn_valid(s: &[u8]) -> bool {
    let digits = s.iter().rev().filter(|b| b.is_ascii_digit());
    let sum: u32 = digits
        .enumerate()
        .map(|(i, b)| {
            let d = u32::from(b - b'0');
            match i % 2 {
                0 => d,
                _ if d > 4 => d * 2 - 9,
                _ => d * 2,
            }
        })
        .sum();
    sum.is_multiple_of(10)
}

#[rustfmt::skip]
macro_rules! ip {
    (v4s) => { "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)" };
    (v4a) => { concat!(ip!(v4s), "\\.", ip!(v4s), "\\.", ip!(v4s), "\\.", ip!(v4s)) };
    (v6s) => { "[0-9a-fA-F]{1,4}" };
}

macro_rules! regex {
    ($name:ident, $rule:expr $(, $validator:expr)?) => {
        #[allow(non_snake_case)]
        mod $name {
            use super::*;
            pub static $name: LazyLock<ScrubbingRegex> = LazyLock::new(|| {
                ScrubbingRegex::new(Regex::new($rule).unwrap())$(.with_validator($validator))?
            });

            #[test]
            fn supports_byte_mode() {
                assert!($name.as_ref().to_bytes().is_ok());
            }
        }
        use $name::$name;
    };
}

pub(crate) static ANYTHING_REGEX: LazyLock<ScrubbingRegex> =
    LazyLock::new(|| ScrubbingRegex::new(Regex::new(".*").unwrap()));

pub static SENSITIVE_COOKIE_NAMES: &[&str] = &[
    // Common session cookie names for popular web frameworks
    "sentrysid", // Sentry default session cookie name
    "sudo",      // Sentry default sudo cookie name
    "su",        // Sentry superuser cookie name
    "session",
    "__session",
    "sessionid",
    "user_session",
    "symfony",
    "phpsessid",
    "fasthttpsessionid",
    "mysession",
    "irissessionid",
    "_vercel_jwt",
    "connect.sid",
    "JSESSIONID",
    "ASP.NET_SessionId",
    "laravel_session",
    // Common CSRF/XSRF cookie names for popular web frameworks
    "csrf",
    "xsrf",
    "_xsrf",
    "_csrf",
    "csrf-token",
    "csrf_token",
    "xsrf-token",
    "xsrf_token",
    "fastcsrf",
    "_iris_csrf",
];

static COOKIE_REGEX: LazyLock<ScrubbingRegex> = LazyLock::new(|| {
    let variants = SENSITIVE_COOKIE_NAMES.join("|").replace('.', "\\.");
    let r = format!("(?i)\\b(?:(?:{variants})(?:__\\d+)?)=([^;]+)");
    ScrubbingRegex::new(Regex::new(&r).unwrap())
});

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
            [a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*\.[a-zA-Z]{2,}
        \b
    "
);

regex!(
    IBAN_REGEX,
    r"(?x)
        \b
        (AT|AD|AE|AL|AZ|BA|BE|BG|BH|BR|BY|CH|CR|CY|CZ|DE|DK|DO|EE|EG|ES|FI|FO|FR|GB|GE|GI|GL|GR|GT|HR|HU|IE|IL|IQ|IS|IT|JO|KW|KZ|LB|LC|LI|LT|LU|LV|LY|MC|MD|ME|MK|MR|MT|MU|NL|NO|PK|PL|PS|PT|QA|RO|RU|RS|SA|SC|SE|SI|SK|SM|ST|SV|TL|TN|TR|UA|VA|VG|XK|DZ|AO|BJ|BF|BI|CV|CM|CF|TD|KM|CG|CI|DJ|GQ|GA|GW|HN|IR|MG|ML|MA|MZ|NI|NE|SN|TG)\d{2}
        (?:
            # Electronic format: DE89370400440532013000
            [a-zA-Z0-9]{11,29}
            # Print format, groups of four separated by single spaces: DE89 3704 0044 0532 0130 00
            | (?:\x20[A-Z0-9]{4}){3,7}(?:\x20[A-Z0-9]{1,4})?
            # Shortest print format (Norway): NO93 8601 1117 947
            | (?:\x20[A-Z0-9]{4}){2}\x20[A-Z0-9]{3}
        )
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
    "#,
    is_luhn_valid
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
            [^/\\\r\n\x00]+
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

regex!(TOKEN_KEY_REGEX, r"(?i)(token)");

regex!(
    PASSWORD_KEY_REGEX,
    r"(?i)(password|secret|passwd|api[-_]key|apikey|auth|credentials|mysql_pwd|privatekey|private[-_]key|token[^\s]*[:=]|^otp$|^two[-_]factor$)"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_userpath_utf8_bytes() {
        let regex = PATH_REGEX.as_ref().to_bytes().unwrap();
        let mut matches = regex.captures_iter(br"C:\\Users\jane\somefile");
        assert!(matches.next().is_some());
    }

    #[test]
    fn test_luhn() {
        assert!(is_luhn_valid(b"4111111111111111"));
        assert!(is_luhn_valid(b"4111-1111 1111-1111"));
        assert!(is_luhn_valid(b"378282246310005"));
        assert!(!is_luhn_valid(b"4111111111111112"));
        assert!(!is_luhn_valid(b"4510944073809921"));
    }
}
