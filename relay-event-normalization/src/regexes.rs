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
        \b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b
    [^/\\]*) |
    (?P<sha1>[^/\\]*
        \b[0-9a-fA-F]{40}\b
    [^/\\]*) |
    (?P<md5>[^/\\]*
        \b[0-9a-fA-F]{32}\b
    [^/\\]*) |
    (?P<date>[^/\\]*
        (?:
            (?:\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z))|
            (?:\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d([+-][0-2]\d:[0-5]\d|Z))|
            (?:\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d([+-][0-2]\d:[0-5]\d|Z))
        ) |
        (?:
            \b(?:(Sun|Mon|Tue|Wed|Thu|Fri|Sat)\s+)?
            (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+
            (?:[\d]{1,2})\s+
            (?:[\d]{2}:[\d]{2}:[\d]{2})\s+
            [\d]{4}
        ) |
        (?:
            \b(?:(Sun|Mon|Tue|Wed|Thu|Fri|Sat),\s+)?
            (?:0[1-9]|[1-2]?[\d]|3[01])\s+
            (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+
            (?:19[\d]{2}|[2-9][\d]{3})\s+
            (?:2[0-3]|[0-1][\d]):([0-5][\d])
            (?::(60|[0-5][\d]))?\s+
            (?:[-\+][\d]{2}[0-5][\d]|(?:UT|GMT|(?:E|C|M|P)(?:ST|DT)|[A-IK-Z]))
        )
    [^/\\]*) |
    (?P<hex>[^/\\]*
        \b0[xX][0-9a-fA-F]+\b
    [^/\\]*) |
    (?:^|[/\\])
    (?P<int>
        (:?[^%/\\]|%[0-9a-fA-F]{2})*\d{2,}
    [^/\\]*)",
    )
    .unwrap()
});

/// Captures initial all-caps words as redis command, the rest as arguments.
pub static REDIS_COMMAND_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\s*(?P<command>[A-Z]+(\s+[A-Z]+)*\b)(?P<args>.+)?").unwrap());

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
        r"(?xi)
        (?P<query>\?.*) |
        # UUIDs.
        (?P<uuid>[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}) |
        (?P<version>(v[0-9]+(?:\.[0-9]+)*)) |
        # Hexadecimal strings with more than 5 digits
        (?P<hex>[a-f0-9]{5}[a-f0-9]+) |
        # Integer IDs with more than one digit.
        (?P<int>\d\d+)
        ",
    )
    .unwrap()
});
