//! Semantic versions for generic filter conditions.

use std::cmp::Ordering;

/// A semantic version parsed from a release, ordered by precedence.
///
/// A release is either a version such as `1.2.3-rc.1+build` or a Sentry release with a package,
/// such as `myapp@1.2.3`. The version must be a full semantic version with three components.
/// Ordering follows semver precedence: a pre-release orders below its final release, and build
/// metadata is ignored.
#[derive(Debug, Clone, Eq)]
pub(crate) struct Semver(::semver::Version);

impl Semver {
    /// Parses the version of a release.
    ///
    /// Returns `None` if the release does not carry a full semantic version, such as `1.2` or a
    /// commit hash.
    pub(crate) fn parse(release: &str) -> Option<Self> {
        let version = release
            .rsplit_once('@')
            .map_or(release, |(_package, version)| version);
        ::semver::Version::parse(version.trim()).ok().map(Self)
    }
}

impl Ord for Semver {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp_precedence(&other.0)
    }
}

impl PartialOrd for Semver {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Semver {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn semver(release: &str) -> Semver {
        Semver::parse(release).unwrap()
    }

    #[test]
    fn test_ordering() {
        let ascending = [
            "0.9.9",
            "1.0.0-alpha",
            "1.0.0-alpha.1",
            "1.0.0-beta.2",
            "1.0.0-beta.11",
            "1.0.0-rc.1",
            "1.0.0",
            "1.2.2",
            "1.2.3-rc.1",
            "1.2.3",
            "1.2.4",
            "1.10.0",
            "2.0.0-rc.1",
            "2.0.0",
        ];

        for pair in ascending.windows(2) {
            assert!(
                semver(pair[0]) < semver(pair[1]),
                "{} < {}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn test_build_metadata_is_ignored() {
        assert_eq!(semver("1.2.3+build.7"), semver("1.2.3"));
        assert_eq!(semver("1.2.3+a4b7e0f9c2d1"), semver("1.2.3+20240101"));
        assert!(semver("1.2.3+build.7") < semver("1.2.4"));
    }

    #[test]
    fn test_sentry_release_formats() {
        assert_eq!(semver("myapp@1.2.3"), semver("1.2.3"));
        assert_eq!(semver("@scope/pkg@1.2.3"), semver("1.2.3"));
        assert_eq!(semver("myapp@1.2.3-rc.1+build"), semver("1.2.3-rc.1"));
        assert_eq!(semver(" 1.2.3 "), semver("1.2.3"));
    }

    #[test]
    fn test_releases_without_semver() {
        assert_eq!(Semver::parse("1.2"), None);
        assert_eq!(Semver::parse("2"), None);
        assert_eq!(Semver::parse("1.2.3.4"), None);
        assert_eq!(Semver::parse("a4b7e0f9c2d1"), None);
        assert_eq!(Semver::parse("myapp@a4b7e0f9c2d1"), None);
        assert_eq!(Semver::parse("not a version"), None);
        assert_eq!(Semver::parse(""), None);
    }
}
