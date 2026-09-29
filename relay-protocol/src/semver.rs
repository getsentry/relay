//! Versions of releases for the `semver` rule condition.

use std::cmp::Ordering;

use sentry_release_parser::{Release, Version};

/// The package and the version of a release, as Sentry parses releases.
///
/// A release is a version such as `1.2.3-rc.1+build`, optionally behind a package, such as
/// `myapp@1.2.3`. The version has one to four numeric components. Missing components are zero.
pub struct Semver<'a> {
    package: Option<&'a str>,
    version: Version<'a>,
}

impl<'a> Semver<'a> {
    /// Parses a release.
    ///
    /// Returns `None` if the release does not carry a version, such as a commit hash.
    pub fn parse(release: &'a str) -> Option<Self> {
        let release = Release::parse(release).ok()?;

        // The parser only extracts a version behind a package, so parse the version part
        // directly. Only the parser knows that a version made of digits is a commit hash.
        let version = release.version_raw();
        if release.build_hash() == Some(version) {
            return None;
        }

        Some(Self {
            package: release.package(),
            version: Version::parse(version).ok()?,
        })
    }

    /// Compares the version of this release with the version of `other`.
    ///
    /// Returns `None` if `other` names a package and this release has a different one. If `other`
    /// names no package, it compares with releases of every package.
    ///
    /// Versions order by their numeric components, then by semver precedence of the pre-release.
    /// A pre-release orders below its final release. Build codes are ignored.
    pub fn compare(&self, other: &Self) -> Option<Ordering> {
        if other.package.is_some() && other.package != self.package {
            return None;
        }

        let (version, other) = (&self.version, &other.version);
        let ordering = version
            .quad()
            .cmp(&other.quad())
            .then_with(|| version.as_semver1().pre.cmp(&other.as_semver1().pre));

        Some(ordering)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compare(release: &str, other: &str) -> Option<Ordering> {
        Semver::parse(release)?.compare(&Semver::parse(other)?)
    }

    #[test]
    fn test_parse_rejects_releases_without_version() {
        for release in ["a4b7e0f9c2d1", "myapp@a4b7e0f9c2d1", "123456789012", ""] {
            assert!(Semver::parse(release).is_none(), "{release}");
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
            assert_eq!(compare(release, other), Some(expected), "{release} {other}");
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
            assert_eq!(compare(release, other), expected, "{release} {other}");
        }
    }
}
