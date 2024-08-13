use once_cell::sync::Lazy;
use regex::Regex;

/// Contains multiple capture groups which will be used as a replace placeholder.
///
/// This regex is inspired by one used for grouping:
/// <https://github.com/getsentry/sentry/blob/6ba59023a78bfe033e48ea4e035b64710a905c6b/src/sentry/grouping/strategies/message.py#L16-L97>
pub static TRANSACTION_NAME_NORMALIZER_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?x)
    (?P<uuid>[^/\\]*
        (?-u:\b)[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?-u:\b)
    [^/\\]*) |
    (?P<sha1>[^/\\]*
        (?-u:\b)[0-9a-fA-F]{40}(?-u:\b)
    [^/\\]*) |
    (?P<md5>[^/\\]*
        (?-u:\b)[0-9a-fA-F]{32}(?-u:\b)
    [^/\\]*) |
    (?P<date>[^/\\]*
        (?:
            (?:[0-9]{4}-[01][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]\.[0-9]+([+-][0-2][0-9]:[0-5][0-9]|Z))|
            (?:[0-9]{4}-[01][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]:[0-5][0-9]([+-][0-2][0-9]:[0-5][0-9]|Z))|
            (?:[0-9]{4}-[01][0-9]-[0-3][0-9]T[0-2][0-9]:[0-5][0-9]([+-][0-2][0-9]:[0-5][0-9]|Z))
        ) |
        (?:
            (?-u:\b)(?:(Sun|Mon|Tue|Wed|Thu|Fri|Sat)(?-u:\s)+)?
            (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?-u:\s)+
            (?:[0-9]{1,2})(?-u:\s)+
            (?:[0-9]{2}:[0-9]{2}:[0-9]{2})(?-u:\s)+
            [0-9]{4}
        ) |
        (?:
            (?-u:\b)(?:(Sun|Mon|Tue|Wed|Thu|Fri|Sat),(?-u:\s)+)?
            (?:0[1-9]|[1-2]?[0-9]|3[01])(?-u:\s)+
            (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(?-u:\s)+
            (?:19[0-9]{2}|[2-9][0-9]{3})(?-u:\s)+
            (?:2[0-3]|[0-1][0-9]):([0-5][0-9])
            (?::(60|[0-5][0-9]))?(?-u:\s)+
            (?:[-\+][0-9]{2}[0-5][0-9]|(?:UT|GMT|(?:E|C|M|P)(?:ST|DT)|[A-IK-Z]))
        )
    [^/\\]*) |
    (?P<hex>[^/\\]*
        (?-u:\b)0[xX][0-9a-fA-F]+(?-u:\b)
    [^/\\]*) |
    (?:^|[/\\])
    (?P<int>
        (:?[^%/\\]|%[0-9a-fA-F]{2})*[0-9]{2,}
    [^/\\]*)",
    )
    .unwrap()
});

/// Captures initial all-caps words as redis command, the rest as arguments.
pub static REDIS_COMMAND_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?-u:\s)*(?P<command>[A-Z]+((?-u:\s)+[A-Z]+)*(?-u:\b))(?P<args>.+)?").unwrap()
});

/// Regex with multiple capture groups for resource tokens we should scrub.
///
/// Resource tokens are the tokens that exist in resource spans that generate
/// high cardinality or are noise for the product. For example, the hash of the
/// file next to its name.
///
/// Slightly modified Regex from
/// <https://github.com/getsentry/sentry/blob/de5949a9a313d7ef0bf0685f84fe6e981ac38558/src/sentry/utils/performance_issues/base.py#L292-L306>
pub static RESOURCE_NORMALIZER_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?x)
        # UUIDs.
        (?P<uuid>[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}) |
        # Version strings.
        (?P<version>(v[0-9]+(?:\.[0-9]+)*)) |
        # Hexadecimal strings with more than 5 digits.
        (?P<hex>[a-fA-F0-9]{5}[a-fA-F0-9]+) |
        # Integer IDs with more than one digit.
        (?P<int>[0-9][0-9]+)
        ",
    )
    .unwrap()
});

pub static DB_SQL_TRANSACTION_CORE_DATA_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?P<int>[0-9]+)").unwrap());

pub static DB_SUPABASE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?x)
        # UUIDs.
        (?P<uuid>[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}) |
        # Hexadecimal strings with more than 5 digits.
        (?P<hex>[a-fA-F0-9]{5}[a-fA-F0-9]+) |
        # Integer IDs with more than one digit.
        (?P<int>[0-9][0-9]+)
        ",
    )
    .unwrap()
});

pub static FUNCTION_NORMALIZER_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?x)
        # UUIDs.
        (?P<uuid>[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}) |
        # Hexadecimal strings with more than 5 digits.
        (?P<hex>[a-fA-F0-9]{5}[a-fA-F0-9]+)
        ",
    )
    .unwrap()
});
