//! Releases for the `semver` rule condition.

use std::cmp::Ordering;

use semver::Prerelease;
use sentry_release_parser::{Release, Version};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A release to compare the versions of other releases against.
///
/// A release is a version such as `1.2.3-rc.1+build`, optionally behind a package, such as
/// `myapp@1.2.3`. The version has one to four numeric components. Missing components are zero.
///
/// Serialized as the release string, which is parsed once when deserializing. A string without a
/// version, such as a commit hash, still deserializes. It is not [valid](Self::is_valid) and
/// compares with no release.
#[derive(Debug, Clone, PartialEq)]
pub struct Semver {
    raw: String,
    parsed: Option<Parsed<String>>,
}

impl Semver {
    /// Parses a release.
    pub fn new(release: impl Into<String>) -> Self {
        let raw = release.into();
        let parsed = Parsed::parse(&raw).map(|parsed| Parsed {
            package: parsed.package.map(str::to_owned),
            quad: parsed.quad,
            pre: parsed.pre,
        });

        Self { raw, parsed }
    }

    /// Returns `true` if the release carries a version.
    pub fn is_valid(&self) -> bool {
        self.parsed.is_some()
    }

    /// Returns how the version of `release` orders relative to this version.
    ///
    /// Returns `None` if either release has no version, or if this release names a package and
    /// `release` has a different one. Without a package, this compares with releases of every
    /// package.
    ///
    /// Versions order by their numeric components, then by semver precedence of the pre-release.
    /// A pre-release orders below its final release. Build codes are ignored.
    pub fn compare(&self, release: &str) -> Option<Ordering> {
        let expected = self.parsed.as_ref()?;
        let release = Parsed::parse(release)?;

        if expected.package.is_some() && expected.package.as_deref() != release.package {
            return None;
        }

        let ordering = release
            .quad
            .cmp(&expected.quad)
            .then_with(|| release.pre.cmp(&expected.pre));

        Some(ordering)
    }
}

impl Serialize for Semver {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.raw.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Semver {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self::new)
    }
}

/// The parts of a release that take part in comparisons.
#[derive(Debug, Clone, PartialEq)]
struct Parsed<P> {
    package: Option<P>,
    quad: (u64, u64, u64, u64),
    pre: Prerelease,
}

impl<'a> Parsed<&'a str> {
    fn parse(release: &'a str) -> Option<Self> {
        let release = Release::parse(release).ok()?;

        // The parser only extracts a version behind a package, so parse the version part
        // directly. Only the parser knows that a version made of digits is a commit hash.
        let version = release.version_raw();
        if release.build_hash() == Some(version) {
            return None;
        }
        let version = Version::parse(version).ok()?;

        Some(Self {
            package: release.package(),
            quad: version.quad(),
            pre: version.as_semver1().pre,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_releases_without_version() {
        for release in ["a4b7e0f9c2d1", "myapp@a4b7e0f9c2d1", "123456789012", ""] {
            assert!(!Semver::new(release).is_valid(), "{release}");
            assert_eq!(Semver::new("1.0.0").compare(release), None, "{release}");
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
            let ordering = Semver::new(other).compare(release);
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
            let ordering = Semver::new(other).compare(release);
            assert_eq!(ordering, expected, "{release} {other}");
        }
    }

    #[test]
    fn test_serde_keeps_the_release_string() {
        for json in [r#""myapp@1.2.0+build""#, r#""a4b7e0f9c2d1""#] {
            let semver: Semver = serde_json::from_str(json).unwrap();
            assert_eq!(serde_json::to_string(&semver).unwrap(), json);
        }
    }
}
