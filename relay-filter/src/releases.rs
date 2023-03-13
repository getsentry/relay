//! Implements event filtering based on application release version.
//!
//! A user may configure the server to ignore certain application releases
//! (known old bad releases) and Sentry will ignore events originating from
//! clients with the specified release.

use relay_general::protocol::Event;

use crate::{FilterStatKey, ReleasesFilterConfig};

/// Filters events generated by known problematic SDK clients.
pub fn should_filter(event: &Event, config: &ReleasesFilterConfig) -> Result<(), FilterStatKey> {
    if let Some(release) = event.release.as_str() {
        if config.releases.is_match(release) {
            return Err(FilterStatKey::ReleaseVersion);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use relay_general::protocol::{Event, LenientString};
    use relay_general::types::Annotated;

    use super::*;
    use crate::GlobPatterns;

    fn get_event_for_release(release: &str) -> Event {
        Event {
            release: Annotated::from(LenientString::from(release.to_string())),
            ..Event::default()
        }
    }

    #[test]
    fn test_release_filtering() {
        let examples = &[
            //simple matches
            ("1.2.3", &["1.3.0", "1.2.3", "1.3.1"][..], true),
            ("1.2.3", &["1.3.0", "1.3.1", "1.2.3"], true),
            ("1.2.3", &["1.2.3", "1.3.0", "1.3.1"], true),
            //pattern matches
            ("1.2.3", &["1.3.0", "1.2.*", "1.3.1"], true),
            ("1.2.3", &["1.3.0", "1.3.*", "1.*"], true),
            ("1.2.3", &["*", "1.3.0", "1.3.*"], true),
            //simple non match
            ("1.2.3", &["1.3.0", "1.2.4", "1.3.1"], false),
            //pattern non match
            ("1.2.3", &["1.4.0", "1.3.*", "3.*"], false),
            //sentry compatibility tests
            ("1.2.3", &[], false),
            ("1.2.3", &["1.1.1", "1.1.2", "1.3.1"], false),
            ("1.2.3", &["1.2.3"], true),
            ("1.2.3", &["1.2.*", "1.3.0", "1.3.1"], true),
            ("1.2.3", &["1.3.0", "1.*", "1.3.1"], true),
        ];

        for &(release, blocked_releases, expected) in examples {
            let evt = get_event_for_release(release);
            let config = ReleasesFilterConfig {
                releases: GlobPatterns::new(
                    blocked_releases.iter().map(|&r| r.to_string()).collect(),
                ),
            };

            let actual = should_filter(&evt, &config) != Ok(());
            assert_eq!(
                actual,
                expected,
                "Release {} should have {} been filtered by {:?}",
                release,
                if expected { "" } else { "not" },
                blocked_releases
            )
        }
    }
}
