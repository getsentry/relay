use std::str::FromStr;

const DEFAULT_VERSION_IF_EMPTY: usize = 0;

#[derive(PartialEq, Eq, Debug, thiserror::Error)]
pub enum SdkVersionParseError {
    #[error("Version contains invalid non numeric parts")]
    VersionNotNumeric,

    #[error("Release type was neither 'alpha' nor 'beta'")]
    ReleaseTypeInvalid,
}

/// Represents an SDK Version using the semvar versioning, meaning MAJOR.MINOR.PATCH.
/// An optional release type can be specified, then it becomes
/// MAJOR.MINOR.PATCH-(alpha|beta).RELEASE_VERSION
#[derive(Debug, PartialOrd, Eq, PartialEq, Ord)]
pub struct SdkVersion {
    major: usize,
    minor: usize,
    patch: usize,
    release_type: ReleaseType,
}

/// Represents the release type which might be present in a version string,
/// for example: 9.0.0-alpha.0.
/// Release types are also comparable to each other, using the rules:
/// alpha < beta < release.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
enum ReleaseType {
    Alpha(usize),
    Beta(usize),
    Release,
}

impl SdkVersion {
    pub const fn new(major: usize, minor: usize, patch: usize) -> Self {
        Self {
            major,
            minor,
            patch,
            release_type: ReleaseType::Release,
        }
    }
}

impl FromStr for SdkVersion {
    type Err = SdkVersionParseError;

    /// Attempts to parse a version string and returning a [`SdkVersionParseError`] if it contains any
    /// non-numerical characters apart from dots.
    /// If a version part is not provided, 0 will be assumed.
    /// For example:
    /// 1.2 -> 1.2.0
    /// 1   -> 1.0.0
    ///
    /// Also supports the release types `alpha` and `beta` in the form `1.2.3-beta.1`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split = s.split(".");
        let major = split
            .next()
            .map(str::parse)
            .transpose()
            .map_err(|_| SdkVersionParseError::VersionNotNumeric)?
            .unwrap_or(DEFAULT_VERSION_IF_EMPTY);
        let minor = split
            .next()
            .map(str::parse)
            .transpose()
            .map_err(|_| SdkVersionParseError::VersionNotNumeric)?
            .unwrap_or(DEFAULT_VERSION_IF_EMPTY);
        let patch_segment = split.next();
        let release_version = split
            .next()
            .map(str::parse)
            .transpose()
            .map_err(|_| SdkVersionParseError::VersionNotNumeric)?
            .unwrap_or(DEFAULT_VERSION_IF_EMPTY);
        let (patch, release_type) = if let Some(next) = patch_segment {
            match next.split_once("-") {
                Some((patch, release_type)) => (
                    patch
                        .parse()
                        .map_err(|_| SdkVersionParseError::VersionNotNumeric)?,
                    match release_type {
                        "alpha" => ReleaseType::Alpha(release_version),
                        "beta" => ReleaseType::Beta(release_version),
                        _ => return Err(SdkVersionParseError::ReleaseTypeInvalid),
                    },
                ),
                None => (
                    next.parse()
                        .map_err(|_| SdkVersionParseError::VersionNotNumeric)?,
                    ReleaseType::Release,
                ),
            }
        } else {
            (DEFAULT_VERSION_IF_EMPTY, ReleaseType::Release)
        };
        Ok(Self {
            major,
            minor,
            patch,
            release_type,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::sdk_version::{SdkVersion, SdkVersionParseError};

    #[test]
    fn test_version_compare() {
        let main_version = SdkVersion::new(1, 2, 3);
        let less = SdkVersion::new(1, 2, 1);
        let greater = SdkVersion::new(2, 1, 1);
        let equal = SdkVersion::new(1, 2, 3);
        assert!(main_version > less);
        assert!(main_version < greater);
        assert_eq!(main_version, equal);
    }

    #[test]
    fn test_version_string_compare() {
        let main_version: SdkVersion = "1.2.3".parse().unwrap();
        let less = "1.2.1".parse().unwrap();
        let greater = "2.1.1".parse().unwrap();
        let equal = "1.2.3".parse().unwrap();
        assert!(main_version > less);
        assert!(main_version < greater);
        assert_eq!(main_version, equal);
    }

    #[test]
    fn test_release_type() {
        let alpha: SdkVersion = "9.0.0-alpha.2".parse().unwrap();
        let beta = "9.0.0-beta.2".parse().unwrap();
        let release = "9.0.0".parse().unwrap();

        assert!(alpha < beta);
        assert!(beta < release);
        assert!(alpha < release)
    }

    #[test]
    fn test_invalid_release_type() {
        assert_eq!(
            "9.0.0-foobar.3".parse::<SdkVersion>(),
            Err(SdkVersionParseError::ReleaseTypeInvalid)
        );
    }

    #[test]
    fn test_release_type_versions() {
        let first_alpha: SdkVersion = "9.0.0-alpha.1".parse().unwrap();
        let second_alpha = "9.0.0-alpha.2".parse().unwrap();

        assert!(first_alpha < second_alpha);
    }

    #[test]
    fn test_alpha_always_before_beta() {
        let large_alpha: SdkVersion = "9.0.0-alpha.150".parse().unwrap();
        let small_beta = "9.0.0-beta.0".parse().unwrap();

        assert!(large_alpha < small_beta);
    }

    #[test]
    fn test_version_defaults_to_zero() {
        let only_major: SdkVersion = "9".parse().unwrap();
        assert_eq!(only_major, SdkVersion::new(9, 0, 0))
    }

    #[test]
    fn test_version_string_parse_failed() {
        assert_eq!(
            "amd64".parse::<SdkVersion>(),
            Err(SdkVersionParseError::VersionNotNumeric)
        );
    }
}
