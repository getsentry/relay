//! Semantic versions for the `semver` rule condition.

use std::cmp::Ordering;

use semver::Version;

/// The semantic version of a release.
///
/// A release is either a version such as `1.2.3-rc.1+build` or a Sentry release with a package,
/// such as `myapp@1.2.3`. The version must be a full semantic version with three components.
pub(crate) struct Semver(Version);

impl Semver {
    /// Parses the version of a release.
    ///
    /// Returns `None` if the release does not carry a full semantic version, such as `1.2` or a
    /// commit hash.
    pub(crate) fn parse(release: &str) -> Option<Self> {
        let version = release
            .rsplit_once('@')
            .map_or(release, |(_package, version)| version);
        Version::parse(version.trim()).ok().map(Self)
    }

    /// Orders two versions by semver precedence.
    ///
    /// A pre-release orders below its final release, and build metadata is ignored.
    pub(crate) fn cmp_precedence(&self, other: &Self) -> Ordering {
        self.0.cmp_precedence(&other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmp(a: &str, b: &str) -> Ordering {
        let parse = |release| Semver::parse(release).unwrap();
        parse(a).cmp_precedence(&parse(b))
    }

    #[test]
    fn test_parse_release_formats() {
        assert_eq!(cmp("myapp@1.2.3", "1.2.3"), Ordering::Equal);
        assert_eq!(cmp("@scope/pkg@1.2.3", "1.2.3"), Ordering::Equal);
        assert_eq!(cmp(" 1.2.3 ", "1.2.3"), Ordering::Equal);
    }

    #[test]
    fn test_parse_rejects_non_semver() {
        for release in ["1.2", "1.2.3.4", "myapp@a4b7e0f9c2d1", ""] {
            assert!(Semver::parse(release).is_none(), "{release}");
        }
    }

    #[test]
    fn test_cmp_precedence() {
        assert_eq!(cmp("1.9.0", "1.10.0"), Ordering::Less);
        assert_eq!(cmp("1.0.0-rc.1", "1.0.0"), Ordering::Less);
        assert_eq!(cmp("1.0.0-beta.2", "1.0.0-beta.11"), Ordering::Less);
        assert_eq!(cmp("1.0.0+build.2", "1.0.0+build.1"), Ordering::Equal);
    }
}
