//! Version ordering for releases in comparison conditions.

use std::cmp::Ordering;

use sentry_release_parser::{Release, Version};

/// Parses the version of a Sentry release for ordering.
///
/// A release looks like `myapp@1.2.3+build`. Only the version part takes part in ordering: one to
/// four numeric components and an optional pre-release tag. Missing components are zero, so `1.2`
/// orders like `1.2.0`. Build codes are ignored. Returns `None` for a release without a version,
/// such as a commit hash.
pub(crate) fn parse_version(release: &str) -> Option<VersionKey> {
    let release = Release::parse(release).ok()?;

    // The parser only extracts a version behind a `package@` prefix, but releases without a
    // package are common. Parse the version part directly, but still reject a bare hash.
    let version_raw = release.version_raw();
    if release.build_hash() == Some(version_raw) {
        return None;
    }

    Version::parse(version_raw).ok().map(|version| VersionKey {
        quad: version.quad(),
        pre: version.pre().map(str::to_owned),
    })
}

/// The parts of a version that take part in ordering.
///
/// Orders by the numeric components first. On a tie, a final release orders above a pre-release,
/// and two pre-release tags compare as text, like Sentry orders releases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionKey {
    quad: (u64, u64, u64, u64),
    pre: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn version(release: &str) -> VersionKey {
        parse_version(release).unwrap()
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
                version(pair[0]) < version(pair[1]),
                "{} < {}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn test_missing_components_are_zero() {
        assert_eq!(version("1.2"), version("1.2.0"));
        assert_eq!(version("2"), version("2.0.0.0"));
        assert!(version("1.2") < version("1.2.1"));
    }

    #[test]
    fn test_sentry_release_formats() {
        assert_eq!(version("myapp@1.2.3"), version("1.2.3"));
        assert_eq!(version("myapp@1.2.3+20240101"), version("1.2.3"));
        assert_eq!(version("myapp@1.2.3+a4b7e0f9c2d1"), version("1.2.3"));
        assert_eq!(version("com.example.app@1.2.3.4"), version("1.2.3.4"));
        assert_eq!(version(" 1.2.3 "), version("1.2.3"));
    }

    #[test]
    fn test_releases_without_version() {
        assert_eq!(parse_version("a4b7e0f9c2d1"), None);
        assert_eq!(parse_version("myapp@a4b7e0f9c2d1"), None);
        assert_eq!(parse_version("not a version"), None);
        assert_eq!(parse_version(""), None);
        assert_eq!(parse_version("1.2.3.4.5"), None);
    }
}
