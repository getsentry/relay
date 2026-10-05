//! Releases for the `semver` rule condition.

use std::cmp::Ordering;
use std::fmt;

use semver::Prerelease;
use sentry_release_parser::{Release, Version};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A release to compare the versions of other releases against.
///
/// A release is a version such as `1.2.3-rc.1+build`, optionally behind a package, such as
/// `myapp@1.2.3`. The version has one to four numeric components. Missing components are zero.
/// The build code takes no part in comparisons, so it is dropped.
///
/// Serializes as the normalized release string, such as `myapp@1.2.3-rc.1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Semver {
    package: Option<String>,
    quad: Quad,
    pre: Prerelease,
}

/// The numeric components of a version. Missing components are zero.
type Quad = (u64, u64, u64, u64);

impl Semver {
    /// Parses a release.
    ///
    /// Returns `None` if the release has no version, such as a commit hash.
    pub fn parse(release: &str) -> Option<Self> {
        let (package, quad, pre) = parse_parts(release)?;
        Some(Self {
            package: package.map(str::to_owned),
            quad,
            pre,
        })
    }

    /// Returns how the version of `release` orders relative to this version.
    ///
    /// Returns `None` if `release` has no version, or if this release names a package and
    /// `release` has a different one. Without a package, this compares with releases of every
    /// package.
    ///
    /// Versions order by their numeric components, then by semver precedence of the pre-release.
    /// A pre-release orders below its final release. Build codes are ignored.
    pub fn compare(&self, release: &str) -> Option<Ordering> {
        let (package, quad, pre) = parse_parts(release)?;

        if self.package.is_some() && self.package.as_deref() != package {
            return None;
        }

        Some(quad.cmp(&self.quad).then_with(|| pre.cmp(&self.pre)))
    }
}

impl fmt::Display for Semver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(package) = &self.package {
            write!(f, "{package}@")?;
        }

        let (major, minor, patch, revision) = self.quad;
        write!(f, "{major}.{minor}.{patch}")?;
        if revision != 0 {
            write!(f, ".{revision}")?;
        }
        if !self.pre.is_empty() {
            write!(f, "-{}", self.pre)?;
        }

        Ok(())
    }
}

impl Serialize for Semver {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Semver {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let release = String::deserialize(deserializer)?;
        Self::parse(&release).ok_or_else(|| D::Error::custom("release has no version"))
    }
}

/// Splits a release into the parts that take part in comparisons.
fn parse_parts(release: &str) -> Option<(Option<&str>, Quad, Prerelease)> {
    let release = Release::parse(release).ok()?;

    // The parser only extracts a version behind a package, so parse the version part
    // directly. Only the parser knows that a version made of digits is a commit hash.
    let version = release.version_raw();
    if release.build_hash() == Some(version) {
        return None;
    }
    let version = Version::parse(version).ok()?;

    Some((release.package(), version.quad(), version.as_semver1().pre))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_releases_without_version() {
        for release in ["a4b7e0f9c2d1", "myapp@a4b7e0f9c2d1", "123456789012", ""] {
            assert_eq!(Semver::parse(release), None, "{release}");
            assert_eq!(
                Semver::parse("1.0.0").unwrap().compare(release),
                None,
                "{release}"
            );
        }
    }

    #[test]
    fn test_compare_versions() {
        let cases = [
            ("1.9.0", "1.10.0", Ordering::Less),
            ("1.2.3.4", "1.2.3.5", Ordering::Less),
            ("1.2", "1.2.0", Ordering::Equal),
            ("1.0.0-rc.1", "1.0.0", Ordering::Less),
            ("1.0.0-beta.2", "1.0.0-beta.11", Ordering::Less),
            ("1.0.0+build.2", "1.0.0+build.1", Ordering::Equal),
        ];

        for (release, other, expected) in cases {
            let ordering = Semver::parse(other).unwrap().compare(release);
            assert_eq!(ordering, Some(expected), "{release} {other}");
        }
    }

    #[test]
    fn test_compare_packages() {
        let cases = [
            ("myapp@1.2.3", "1.2.3", Some(Ordering::Equal)),
            ("1.2.3", "1.2.3", Some(Ordering::Equal)),
            ("myapp@1.2.3", "myapp@1.2.3", Some(Ordering::Equal)),
            ("myapp@1.2.3", "other@1.2.3", None),
            ("1.2.3", "myapp@1.2.3", None),
        ];

        for (release, other, expected) in cases {
            let ordering = Semver::parse(other).unwrap().compare(release);
            assert_eq!(ordering, expected, "{release} {other}");
        }
    }

    #[test]
    fn test_serde_normalizes_the_release() {
        let cases = [
            ("1.2.0", "1.2.0"),
            ("1.2", "1.2.0"),
            ("1.2.3.4", "1.2.3.4"),
            ("myapp@1.2.0+build", "myapp@1.2.0"),
            ("myapp@1.2.0-rc.1+build", "myapp@1.2.0-rc.1"),
        ];

        for (input, normalized) in cases {
            let semver: Semver = serde_json::from_str(&format!("{input:?}")).unwrap();
            assert_eq!(
                serde_json::to_string(&semver).unwrap(),
                format!("{normalized:?}")
            );
            assert_eq!(Semver::parse(normalized), Some(semver), "{input}");
        }
    }

    #[test]
    fn test_deserialize_rejects_releases_without_version() {
        assert!(serde_json::from_str::<Semver>(r#""a4b7e0f9c2d1""#).is_err());
    }
}
