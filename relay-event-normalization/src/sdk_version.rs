use std::num::ParseIntError;

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
/// **NOTE**: The discriminants are explicitly set to keep the comparison rules
///           even if the order is changed.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
enum ReleaseType {
    Alpha = 1,
    Beta = 2,
    Release = 3,
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

    /// Attempts to parse a version string and returning a [`ParseIntError`] if it contains any
    /// non-numerical characters apart from dots.
    /// If a version part is not provided, 0 will be assumed.
    /// For example:
    /// 1.2 -> 1.2.0
    /// 1   -> 1.0.0
    pub fn try_parse(input: &str) -> Result<Self, ParseIntError> {
        let mut split = input.split(".");
        let major = split.next().map(str::parse).transpose()?.unwrap_or(0);
        let minor = split.next().map(str::parse).transpose()?.unwrap_or(0);
        let (patch, release_type) = if let Some(next) = split.next() {
            match next.split_once("-") {
                Some((patch, release_type)) => (
                    patch.parse()?,
                    if release_type.starts_with("alpha") {
                        ReleaseType::Alpha
                    } else {
                        ReleaseType::Beta
                    },
                ),
                None => (next.parse()?, ReleaseType::Release),
            }
        } else {
            (0, ReleaseType::Release)
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
    use crate::sdk_version::SdkVersion;

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
        let main_version = SdkVersion::try_parse("1.2.3").unwrap();
        let less = SdkVersion::try_parse("1.2.1").unwrap();
        let greater = SdkVersion::try_parse("2.1.1").unwrap();
        let equal = SdkVersion::try_parse("1.2.3").unwrap();
        assert!(main_version > less);
        assert!(main_version < greater);
        assert_eq!(main_version, equal);
    }

    #[test]
    fn test_release_type() {
        let alpha = SdkVersion::try_parse("9.0.0-alpha.2").unwrap();
        let beta = SdkVersion::try_parse("9.0.0-beta.2").unwrap();
        let release = SdkVersion::try_parse("9.0.0").unwrap();

        assert!(alpha < beta);
        assert!(beta < release);
        assert!(alpha < release)
    }

    #[test]
    fn test_version_string_parse_failed() {
        assert!(SdkVersion::try_parse("amd64").is_err());
    }
}
