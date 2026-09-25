//! Version constraints for the `version` rule condition.

use std::cmp::Ordering;

use sentry_release_parser::{Release, Version};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A list of version constraints, of which a release must satisfy at least one.
///
/// Serialized as a list of strings. Each entry is a comma-separated list of comparators that must
/// all hold, such as `">=1.2.0, <2.0.0"`. A comparator is one of the operators `>`, `>=`, `<`, or
/// `<=` followed by a version.
///
/// Versions have one to four numeric components and an optional pre-release tag. Missing
/// components are zero, so `>=1.2` means `>=1.2.0`. A pre-release orders below its final release,
/// and pre-release tags compare as text, like Sentry orders releases.
///
/// Releases are parsed as Sentry releases, so `myapp@1.2.3+build` compares as `1.2.3`. Build codes
/// are ignored, and a release without a version, such as a commit hash, satisfies no constraint.
/// Entries that do not parse are skipped while deserializing, like invalid glob patterns.
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
        let Ok(version) = Version::parse(version_raw) else {
            return false;
        };

        let key = VersionKey::from(&version);
        self.0.iter().any(|constraint| constraint.matches(&key))
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
            let (op, version) = Op::strip(part.trim())?;
            let version = Version::parse(version.trim()).ok()?;
            comparators.push(Comparator {
                op,
                version: VersionKey::from(&version),
            });
        }

        Some(Self {
            raw: raw.to_owned(),
            comparators,
        })
    }

    fn matches(&self, version: &VersionKey) -> bool {
        self.comparators
            .iter()
            .all(|comparator| comparator.matches(version))
    }
}

/// The parts of a version that take part in ordering.
///
/// Orders by the numeric components first. On a tie, a final release orders above a pre-release,
/// and two pre-release tags compare as text.
#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionKey {
    quad: (u64, u64, u64, u64),
    pre: Option<String>,
}

impl From<&Version<'_>> for VersionKey {
    fn from(version: &Version<'_>) -> Self {
        Self {
            quad: version.quad(),
            pre: version.pre().map(str::to_owned),
        }
    }
}

impl Ord for VersionKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.quad
            .cmp(&other.quad)
            .then_with(|| self.pre.is_none().cmp(&other.pre.is_none()))
            .then_with(|| self.pre.cmp(&other.pre))
    }
}

impl PartialOrd for VersionKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Op {
    Gt,
    Gte,
    Lt,
    Lte,
}

impl Op {
    const PREFIXES: [(&str, Op); 4] = [
        (">=", Op::Gte),
        ("<=", Op::Lte),
        (">", Op::Gt),
        ("<", Op::Lt),
    ];

    /// Splits the operator off the start of a comparator.
    fn strip(comparator: &str) -> Option<(Op, &str)> {
        Self::PREFIXES
            .iter()
            .find_map(|(prefix, op)| comparator.strip_prefix(prefix).map(|rest| (*op, rest)))
    }

    fn holds(self, ordering: Ordering) -> bool {
        match self {
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
    version: VersionKey,
}

impl Comparator {
    fn matches(&self, version: &VersionKey) -> bool {
        self.op.holds(version.cmp(&self.version))
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
            (">1.2.3", &["1.2.3"][..], &["1.2.4", "1.10.0", "2.0.0"][..]),
            (">=1.2.3", &["1.2.2"], &["1.2.3", "1.2.4"]),
            ("<1.2.3", &["1.2.3", "1.10.0"], &["1.2.2", "0.9.9"]),
            ("<=1.2.3", &["1.2.4"], &["1.2.3", "1.2.2"]),
            (">=1.2.0, <2.0.0", &["1.1.0", "2.0.0"], &["1.2.0", "1.99.0"]),
            (">=1.2.3, <=1.2.3", &["1.2.2", "1.2.4"], &["1.2.3"]),
            (" >= 1.2 , < 2 ", &["1.1.0", "2.0.0"], &["1.2.0", "1.99.0"]),
            (">1.2.3.4", &["1.2.3.4", "1.2.3"], &["1.2.3.5", "1.2.4"]),
            // Missing components are zero, like in Sentry's release search.
            (
                ">1.2",
                &["1.2", "1.2.0", "1.1.9"],
                &["1.2.1", "1.2.3", "1.3.0"],
            ),
            ("<=1.2", &["1.2.1"], &["1.2", "1.2.0", "1.1.9"]),
            (">=2", &["1.99.99"], &["2", "2.0.0", "2.0.1"]),
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
        assert!(constraints.matches("1.0.0-beta.2"));
        assert!(constraints.matches("1.0.0-beta.3"));
        assert!(constraints.matches("1.0.0-rc1"));
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
    fn test_build_code_is_ignored() {
        let constraints = parse(&["<=1.2.3"]);
        assert!(constraints.matches("1.2.3+build.7"));
        assert!(constraints.matches("myapp@1.2.3+a4b7e0f9c2d1"));
        assert!(!constraints.matches("1.2.4+build.1"));
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
        let constraints = parse(&[
            "",
            "garbage",
            "1.2.3",
            "=1.2.3",
            "!=1.2.3",
            "~>1.2",
            ">=",
            ">=1.2.3.4.5",
            ">=1.2.3, ",
            ">=2.0.0",
        ]);
        assert_eq!(
            serde_json::to_value(&constraints).unwrap(),
            serde_json::json!([">=2.0.0"])
        );
        assert!(constraints.matches("2.0.0"));
        assert!(!constraints.matches("1.2.3"));
    }

    #[test]
    fn test_roundtrip() {
        let json = serde_json::json!([">=1.2.0, <2.0.0", "<1.0.0"]);
        let constraints: VersionConstraints = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(serde_json::to_value(&constraints).unwrap(), json);
    }
}
