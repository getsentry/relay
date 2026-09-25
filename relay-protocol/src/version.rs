//! Version constraints for the `version` rule condition.

use std::cmp::Ordering;

use semver::{BuildMetadata, Prerelease, Version};
use sentry_release_parser::{Release, Version as ReleaseVersion};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A list of version constraints, of which a release must satisfy at least one.
///
/// Serialized as a list of strings. Each entry is a comma-separated list of comparators that must
/// all hold, such as `">=1.2.0, <2.0.0"`. A comparator is an operator followed by a version. The
/// operators are `=`, `!=`, `>`, `>=`, `<`, `<=`, and `~>`. A version without an operator means
/// `=`.
///
/// Versions have one to three numeric components, an optional pre-release tag, and an optional
/// build code. Missing components are zero, so `>=1.2` means `>=1.2.0`. The `~>` operator allows
/// the rightmost given component to grow: `~>1.2.3` means `>=1.2.3, <1.3.0` and `~>1.2` means
/// `>=1.2.0, <2.0.0`.
///
/// Releases are parsed as Sentry releases, so `myapp@1.2.3+build` compares as `1.2.3`. A
/// pre-release orders below its final release, build codes are ignored, and a release without a
/// version, such as a commit hash, satisfies no constraint. Entries that do not parse are skipped
/// while deserializing, like invalid glob patterns.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct VersionConstraints(Vec<VersionConstraint>);

impl VersionConstraints {
    /// Returns `true` if the version of the release satisfies any of the constraints.
    pub fn matches(&self, release: &str) -> bool {
        let Ok(release) = Release::parse(release) else {
            return false;
        };

        // The parser only extracts a version behind a `package@` prefix, but releases without a
        // package are common. Parse the version part directly, but still reject a bare hash.
        let version_raw = release.version_raw();
        if release.build_hash() == Some(version_raw) {
            return false;
        }
        let Ok(version) = ReleaseVersion::parse(version_raw) else {
            return false;
        };

        let version = version.as_semver1();
        self.0.iter().any(|constraint| constraint.matches(&version))
    }
}

impl<S: AsRef<str>> FromIterator<S> for VersionConstraints {
    fn from_iter<I: IntoIterator<Item = S>>(iter: I) -> Self {
        Self(
            iter.into_iter()
                .filter_map(|entry| VersionConstraint::parse(entry.as_ref()))
                .collect(),
        )
    }
}

impl Serialize for VersionConstraints {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(|constraint| &constraint.raw))
    }
}

impl<'de> Deserialize<'de> for VersionConstraints {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Vec::<String>::deserialize(deserializer)?
            .into_iter()
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct VersionConstraint {
    raw: String,
    comparators: Vec<Comparator>,
}

impl VersionConstraint {
    fn parse(raw: &str) -> Option<Self> {
        let mut comparators = Vec::new();
        for part in raw.split(',') {
            let part = part.trim();

            if let Some(version) = part.strip_prefix("~>") {
                let (version, components) = parse_version(version.trim())?;
                let upper = if components == 3 {
                    Version::new(version.major, version.minor.checked_add(1)?, 0)
                } else {
                    Version::new(version.major.checked_add(1)?, 0, 0)
                };
                comparators.push(Comparator::new(Op::Gte, version));
                comparators.push(Comparator::new(Op::Lt, upper));
                continue;
            }

            let (op, version) = Op::strip(part);
            let (version, _) = parse_version(version.trim())?;
            comparators.push(Comparator::new(op, version));
        }

        Some(Self {
            raw: raw.to_owned(),
            comparators,
        })
    }

    fn matches(&self, version: &Version) -> bool {
        self.comparators
            .iter()
            .all(|comparator| comparator.matches(version))
    }
}

/// Parses a version of one to three components and returns it with the number of components.
fn parse_version(input: &str) -> Option<(Version, u8)> {
    let (input, _build) = input.split_once('+').unwrap_or((input, ""));
    let (numbers, pre) = match input.split_once('-') {
        Some((numbers, pre)) => (numbers, Prerelease::new(pre).ok()?),
        None => (input, Prerelease::EMPTY),
    };

    let mut parts = numbers.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().map(str::parse).transpose().ok()?;
    let patch = parts.next().map(str::parse).transpose().ok()?;
    if parts.next().is_some() {
        return None;
    }

    let components = 1 + u8::from(minor.is_some()) + u8::from(patch.is_some());
    let version = Version {
        major,
        minor: minor.unwrap_or(0),
        patch: patch.unwrap_or(0),
        pre,
        build: BuildMetadata::EMPTY,
    };

    Some((version, components))
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Op {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl Op {
    const PREFIXES: [(&str, Op); 6] = [
        (">=", Op::Gte),
        ("<=", Op::Lte),
        ("!=", Op::Ne),
        (">", Op::Gt),
        ("<", Op::Lt),
        ("=", Op::Eq),
    ];

    /// Splits the operator off the start of a comparator. Without an operator, it is `=`.
    fn strip(comparator: &str) -> (Op, &str) {
        Self::PREFIXES
            .iter()
            .find_map(|(prefix, op)| comparator.strip_prefix(prefix).map(|rest| (*op, rest)))
            .unwrap_or((Op::Eq, comparator))
    }

    fn holds(self, ordering: Ordering) -> bool {
        match self {
            Op::Eq => ordering.is_eq(),
            Op::Ne => ordering.is_ne(),
            Op::Gt => ordering.is_gt(),
            Op::Gte => ordering.is_ge(),
            Op::Lt => ordering.is_lt(),
            Op::Lte => ordering.is_le(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Comparator {
    op: Op,
    version: Version,
}

impl Comparator {
    fn new(op: Op, version: Version) -> Self {
        Self { op, version }
    }

    fn matches(&self, version: &Version) -> bool {
        self.op.holds(version.cmp_precedence(&self.version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(entries: &[&str]) -> VersionConstraints {
        entries.iter().collect()
    }

    #[test]
    fn test_operators() {
        let cases = [
            ("1.2.3", &["1.2.2", "1.2.4"][..], &["1.2.3"][..]),
            ("=1.2.3", &["1.2.2", "1.2.4"], &["1.2.3"]),
            ("!=1.2.3", &["1.2.3"], &["1.2.2", "1.2.4"]),
            (">1.2.3", &["1.2.3"], &["1.2.4", "1.10.0", "2.0.0"]),
            (">=1.2.3", &["1.2.2"], &["1.2.3", "1.2.4"]),
            ("<1.2.3", &["1.2.3", "1.10.0"], &["1.2.2", "0.9.9"]),
            ("<=1.2.3", &["1.2.4"], &["1.2.3", "1.2.2"]),
            ("~>1.2.3", &["1.2.2", "1.3.0"], &["1.2.3", "1.2.10"]),
            ("~>1.2", &["1.1.9", "2.0.0"], &["1.2.0", "1.9.0"]),
            ("~>1", &["0.9.0", "2.0.0"], &["1.0.0", "1.9.9"]),
            (">=1.2.0, <2.0.0", &["1.1.0", "2.0.0"], &["1.2.0", "1.99.0"]),
            (" >= 1.2 , < 2 ", &["1.1.0", "2.0.0"], &["1.2.0", "1.99.0"]),
        ];

        for (constraint, rejected, accepted) in cases {
            let constraints = parse(&[constraint]);
            for release in rejected {
                assert!(!constraints.matches(release), "{constraint} on {release}");
            }
            for release in accepted {
                assert!(constraints.matches(release), "{constraint} on {release}");
            }
        }
    }

    #[test]
    fn test_prerelease_orders_below_release() {
        let constraints = parse(&["<2.0.0"]);
        assert!(constraints.matches("2.0.0-rc1"));
        assert!(constraints.matches("1.9.0-beta.2"));
        assert!(!constraints.matches("2.0.0"));

        let constraints = parse(&[">=1.0.0-beta.2"]);
        assert!(constraints.matches("1.0.0-beta.10"));
        assert!(constraints.matches("1.0.0"));
        assert!(!constraints.matches("1.0.0-beta.1"));
        assert!(!constraints.matches("1.0.0-alpha"));
    }

    #[test]
    fn test_sentry_release_formats() {
        let constraints = parse(&[">=1.2.3"]);
        assert!(constraints.matches("myapp@1.2.3"));
        assert!(constraints.matches("myapp@1.2.3+20240101"));
        assert!(constraints.matches("com.example.app@1.2.3.4"));
        assert!(constraints.matches("1.3"));
        assert!(!constraints.matches("1.2"));
        assert!(!constraints.matches("myapp@1.2.2"));
        assert!(!constraints.matches("a4b7e0f9c2d1"));
        assert!(!constraints.matches("myapp@a4b7e0f9c2d1"));
        assert!(!constraints.matches("not a version"));
        assert!(!constraints.matches(""));
    }

    #[test]
    fn test_any_entry_matches() {
        let constraints = parse(&["<1.0.0", ">=2.0.0"]);
        assert!(constraints.matches("0.9.0"));
        assert!(constraints.matches("2.0.0"));
        assert!(!constraints.matches("1.5.0"));
        assert!(!parse(&[]).matches("1.5.0"));
    }

    #[test]
    fn test_invalid_entries_are_skipped() {
        let constraints = parse(&["", "garbage", ">=", "1.2.3.4", ">=1.2.3, ", ">=2.0.0"]);
        assert_eq!(
            serde_json::to_value(&constraints).unwrap(),
            serde_json::json!([">=2.0.0"])
        );
        assert!(constraints.matches("2.0.0"));
        assert!(!constraints.matches("1.2.3"));
    }

    #[test]
    fn test_roundtrip() {
        let json = serde_json::json!([">=1.2.0, <2.0.0", "~>3.1"]);
        let constraints: VersionConstraints = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(serde_json::to_value(&constraints).unwrap(), json);
    }
}
