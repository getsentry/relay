//! Ordering of releases by version for generic filter conditions.

use std::cmp::Ordering;

/// A release, ordered by its version like Sentry orders releases.
///
/// Parsed from a release such as `myapp@1.2.3+build`. Only the version part takes part in
/// ordering: one to four numeric components and an optional pre-release tag. Missing components
/// are zero, so `1.2` orders like `1.2.0`. Build codes are ignored. A final release orders above
/// its pre-releases, and pre-release tags compare as text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Release {
    quad: (u64, u64, u64, u64),
    pre: Option<String>,
}

impl Release {
    /// Parses a release. Returns `None` for a release without a version, such as a commit hash.
    pub(crate) fn parse(release: &str) -> Option<Self> {
        let release = sentry_release_parser::Release::parse(release).ok()?;

        // The parser only extracts a version behind a `package@` prefix, but releases without a
        // package are common. Parse the version part directly, but still reject a bare hash.
        let version_raw = release.version_raw();
        if release.build_hash() == Some(version_raw) {
            return None;
        }

        let version = sentry_release_parser::Version::parse(version_raw).ok()?;
        Some(Self {
            quad: version.quad(),
            pre: version.pre().map(str::to_owned),
        })
    }
}

impl Ord for Release {
    fn cmp(&self, other: &Self) -> Ordering {
        self.quad
            .cmp(&other.quad)
            .then_with(|| self.pre.is_none().cmp(&other.pre.is_none()))
            .then_with(|| self.pre.cmp(&other.pre))
    }
}

impl PartialOrd for Release {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn release(release: &str) -> Release {
        Release::parse(release).unwrap()
    }

    #[test]
    fn test_ordering() {
        let ascending = [
            "0.9.9",
            "1.0.0-alpha",
            "1.0.0-beta.1",
            "1.0.0-beta.2",
            "1.0.0-rc1",
            "1.0.0",
            "1.2.2",
            "1.2.3-rc1",
            "1.2.3",
            "1.2.3.5",
            "1.2.4",
            "1.10.0",
            "2.0.0-rc1",
            "2.0.0",
        ];

        for pair in ascending.windows(2) {
            assert!(
                release(pair[0]) < release(pair[1]),
                "{} < {}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn test_missing_components_are_zero() {
        assert_eq!(release("1.2"), release("1.2.0"));
        assert_eq!(release("2"), release("2.0.0.0"));
        assert!(release("1.2") < release("1.2.1"));
    }

    #[test]
    fn test_sentry_release_formats() {
        assert_eq!(release("myapp@1.2.3"), release("1.2.3"));
        assert_eq!(release("myapp@1.2.3+20240101"), release("1.2.3"));
        assert_eq!(release("myapp@1.2.3+a4b7e0f9c2d1"), release("1.2.3"));
        assert_eq!(release("com.example.app@1.2.3.4"), release("1.2.3.4"));
        assert_eq!(release(" 1.2.3 "), release("1.2.3"));
    }

    #[test]
    fn test_releases_without_version() {
        assert_eq!(Release::parse("a4b7e0f9c2d1"), None);
        assert_eq!(Release::parse("myapp@a4b7e0f9c2d1"), None);
        assert_eq!(Release::parse("not a version"), None);
        assert_eq!(Release::parse(""), None);
        assert_eq!(Release::parse("1.2.3.4.5"), None);
    }
}
