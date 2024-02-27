//! Implements generic filtering based on the [`RuleCondition`] DSL.
//!
//! Multiple generic filters can be defined and they are going to be checked in FIFO order. The
//! first one that matches, will result in the event being discarded with a [`FilterStatKey`]
//! identifying the matching filter.

use std::collections::HashSet;
use std::iter::FusedIterator;

use crate::{FilterStatKey, GenericFilterConfig, GenericFiltersConfig};
use relay_event_schema::protocol::Event;
use relay_protocol::RuleCondition;

/// Maximum supported version of the generic filters schema.
///
/// If the version in the project config is higher, no generic filters are applied.
const VERSION: u16 = 1;

/// Returns whether the given generic config versions are supported.
pub fn are_generic_filters_supported(
    global_filters_version: Option<u16>,
    project_filters_version: u16,
) -> bool {
    global_filters_version.map_or(false, |v| v <= VERSION) && project_filters_version <= VERSION
}

/// Checks events by patterns in their error messages.
fn matches(event: &Event, condition: Option<&RuleCondition>) -> bool {
    // TODO: the condition DSL needs to be extended to support more complex semantics, such as
    //  collections operations.
    condition.map_or(false, |condition| condition.matches(event))
}

/// Filters events by patterns in their error messages.
pub(crate) fn should_filter(
    event: &Event,
    project_filters: &GenericFiltersConfig,
    global_filters: Option<&GenericFiltersConfig>,
) -> Result<(), FilterStatKey> {
    let generic_filters_config =
        DynamicGenericFiltersConfig::new(project_filters, global_filters, VERSION);
    let filters = generic_filters_config.into_iter();

    for filter_config in filters {
        if matches(event, filter_config.condition.as_ref()) {
            return Err(FilterStatKey::GenericFilter(filter_config.id.clone()));
        }
    }

    Ok(())
}

/// Configuration of generic filters combined from project and global config.
///
/// See [`DynamicFiltersConfigIter`] for details on how to iterate easily
/// through the applicable filters.
#[derive(Copy, Clone, Debug)]
struct DynamicGenericFiltersConfig<'a> {
    /// Configuration of generic filters from project configs.
    project_filters: &'a GenericFiltersConfig,
    /// Configuration of generic filters from global config.
    global_filters: Option<&'a GenericFiltersConfig>,
    /// Maximum supported version for generic filters.
    ///
    /// It applies to all filters, from both project and global configs.
    supported_version: u16,
}

impl<'a> DynamicGenericFiltersConfig<'a> {
    /// Creates a [`DynamicFiltersConfig`] from the project and global configs.
    pub fn new(
        project_filters: &'a GenericFiltersConfig,
        global_filters: Option<&'a GenericFiltersConfig>,
        supported_version: u16,
    ) -> Self {
        DynamicGenericFiltersConfig {
            project_filters,
            global_filters,
            supported_version,
        }
    }
}

impl<'a> IntoIterator for DynamicGenericFiltersConfig<'a> {
    type Item = &'a GenericFilterConfig;
    type IntoIter = DynamicGenericFiltersConfigIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        DynamicGenericFiltersConfigIter::new(self)
    }
}

/// Iterator over [`GenericFilterConfig`] of a project and global config.
///
/// Iterates in order through the generic filters in project configs and global
/// configs yielding the filters according to the principles below:
///
/// - Filters from project configs are evaluated before filters from global
/// configs.
/// - No duplicates: once a filter is evaluated (yielded or skipped), no filter
/// with the same id is evaluated again.
/// - Filters in project configs override filters from global configs, but the
/// opposite is never the case.
/// - A filter in the project config can be a flag, where only `is_enabled` is
/// defined and `condition` is not. In that case:
///   - If `is_enabled` is true, the filter with a matching ID from global
///   configs is yielded without evaluating its `is_enabled`. Unless the filter
///   in the global config also has an empty condition, in which case the filter
///   is not yielded.
///   - If `is_enabled` is false, no filters with the same IDs are returned,
///   including matching filters from global configs.
#[derive(Debug)]
struct DynamicGenericFiltersConfigIter<'a> {
    /// Configuration of project and global filters.
    config: DynamicGenericFiltersConfig<'a>,
    /// Index of the next filter in project config to evaluate.
    project_index: usize,
    /// Index of the next filter in global config to evaluate.
    global_index: usize,
    /// Filters that have been evaluated, either yielded or ignored.
    evaluated: HashSet<&'a str>,
}

impl<'a> DynamicGenericFiltersConfigIter<'a> {
    /// Creates an iterator over the filters in [`DynamicFiltersConfig`].
    pub fn new(config: DynamicGenericFiltersConfig<'a>) -> Self {
        DynamicGenericFiltersConfigIter {
            config,
            project_index: 0,
            global_index: 0,
            evaluated: HashSet::new(),
        }
    }
}

impl<'a> DynamicGenericFiltersConfigIter<'a> {
    /// Returns whether the inbound filters support the maximum version.
    ///
    /// Filters are supported if the versions of filters of both project and
    /// global configs are not greater than the given maximum version.
    ///
    /// Filters from the project and global configs are complementary, and in
    /// isolation, they don't provide enough valuable information to perform the
    /// filtering. Additionally, new versions may include features not supported
    /// in the current Relay.
    fn is_version_supported(&self) -> bool {
        self.config.project_filters.version <= self.config.supported_version
            && self
                .config
                .global_filters
                .map_or(true, |gf| gf.version <= self.config.supported_version)
    }

    fn next_project_filter(&mut self) -> Option<&'a GenericFilterConfig> {
        let Some((id, filter)) = self
            .config
            .project_filters
            .filters
            .get_index(self.project_index)
        else {
            return None;
        };
        self.project_index += 1;
        self.evaluated.insert(id);

        if !filter.is_enabled {
            return None;
        }

        if filter.condition.is_some() {
            Some(filter)
        } else {
            self.config
                .global_filters
                .and_then(|config| config.filters.get(id))
                .filter(|filter| filter.condition.is_some())
        }
    }

    fn next_global_filter(&mut self) -> Option<&'a GenericFilterConfig> {
        let Some(global_filters) = self.config.global_filters else {
            return None;
        };
        let Some((id, filter)) = global_filters.filters.get_index(self.global_index) else {
            return None;
        };
        self.global_index += 1;

        if filter.is_empty() {
            return None;
        }

        self.evaluated.insert(id).then_some(filter)
    }
}

impl<'a> Iterator for DynamicGenericFiltersConfigIter<'a> {
    type Item = &'a GenericFilterConfig;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_version_supported() {
            return None;
        }

        // This loop starts by iterating through all project filters: increments
        // `project_index` on every iteration until it grows too much (i.e. all
        // project filters are evaluated), yielding the evaluated filter if
        // necessary.
        // Then, the same approach is applied to global filters.
        // `continue` restarts the loop, so global filters aren't evaluated
        // until all project filters are.
        loop {
            if self.project_index < self.config.project_filters.filters.len() {
                let filter = self.next_project_filter();
                if filter.is_some() {
                    return filter;
                }
                continue;
            }

            if let Some(global_filters) = self.config.global_filters {
                if self.global_index < global_filters.filters.len() {
                    let filter = self.next_global_filter();
                    if filter.is_some() {
                        return filter;
                    }
                    continue;
                }
            }

            return None;
        }
    }
}

impl<'a> FusedIterator for DynamicGenericFiltersConfigIter<'a> {}

#[cfg(test)]
mod tests {
    use std::iter;

    use super::*;

    use crate::generic::{should_filter, VERSION};
    use crate::{FilterStatKey, GenericFilterConfig, GenericFiltersConfig};
    use indexmap::IndexMap;
    use relay_event_schema::protocol::{Event, LenientString};
    use relay_protocol::Annotated;
    use relay_protocol::RuleCondition;

    fn mock_filters() -> IndexMap<String, GenericFilterConfig> {
        IndexMap::from([
            (
                "firstReleases".to_owned(),
                GenericFilterConfig {
                    id: "firstReleases".to_string(),
                    is_enabled: true,
                    condition: Some(RuleCondition::eq("event.release", "1.0")),
                },
            ),
            (
                "helloTransactions".to_owned(),
                GenericFilterConfig {
                    id: "helloTransactions".to_string(),
                    is_enabled: true,
                    condition: Some(RuleCondition::eq("event.transaction", "/hello")),
                },
            ),
        ])
    }

    #[test]
    fn test_should_filter_match_rules() {
        let config = GenericFiltersConfig {
            version: 1,
            filters: mock_filters(),
        };

        // Matching first rule.
        let event = Event {
            release: Annotated::new(LenientString("1.0".to_string())),
            ..Default::default()
        };
        assert_eq!(
            should_filter(&event, &config, None),
            Err(FilterStatKey::GenericFilter("firstReleases".to_string()))
        );

        // Matching second rule.
        let event = Event {
            transaction: Annotated::new("/hello".to_string()),
            ..Default::default()
        };
        assert_eq!(
            should_filter(&event, &config, None),
            Err(FilterStatKey::GenericFilter(
                "helloTransactions".to_string()
            ))
        );
    }

    #[test]
    fn test_should_filter_fifo_match_rules() {
        let config = GenericFiltersConfig {
            version: 1,
            filters: mock_filters(),
        };

        // Matching both rules (first is taken).
        let event = Event {
            release: Annotated::new(LenientString("1.0".to_string())),
            transaction: Annotated::new("/hello".to_string()),
            ..Default::default()
        };
        assert_eq!(
            should_filter(&event, &config, None),
            Err(FilterStatKey::GenericFilter("firstReleases".to_string()))
        );
    }

    #[test]
    fn test_should_filter_match_no_rules() {
        let config = GenericFiltersConfig {
            version: 1,
            filters: mock_filters(),
        };

        // Matching no rule.
        let event = Event {
            transaction: Annotated::new("/world".to_string()),
            ..Default::default()
        };
        assert_eq!(should_filter(&event, &config, None), Ok(()));
    }

    #[test]
    fn test_should_filter_with_higher_config_version() {
        let config = GenericFiltersConfig {
            // We simulate receiving a higher configuration version, which we don't support.
            version: VERSION + 1,
            filters: mock_filters(),
        };

        let event = Event {
            release: Annotated::new(LenientString("1.0".to_string())),
            transaction: Annotated::new("/hello".to_string()),
            ..Default::default()
        };
        assert_eq!(should_filter(&event, &config, None), Ok(()));
    }

    #[test]
    fn test_should_filter_from_global_filters() {
        let project = GenericFiltersConfig {
            version: 1,
            filters: IndexMap::from([(
                "firstReleases".to_owned(),
                GenericFilterConfig {
                    id: "firstReleases".to_string(),
                    is_enabled: true,
                    condition: Some(RuleCondition::eq("event.release", "1.0")),
                },
            )]),
        };

        let global = GenericFiltersConfig {
            version: 1,
            filters: IndexMap::from([(
                "helloTransactions".to_owned(),
                GenericFilterConfig {
                    id: "helloTransactions".to_string(),
                    is_enabled: true,
                    condition: Some(RuleCondition::eq("event.transaction", "/hello")),
                },
            )]),
        };

        let event = Event {
            transaction: Annotated::new("/hello".to_string()),
            ..Default::default()
        };

        assert_eq!(
            should_filter(&event, &project, Some(&global)),
            Err(FilterStatKey::GenericFilter(
                "helloTransactions".to_string()
            ))
        );
    }

    fn empty_filter() -> GenericFiltersConfig {
        GenericFiltersConfig {
            version: 1,
            filters: IndexMap::new(),
        }
    }

    /// Returns a complete and enabled [`GenericFiltersConfig`].
    fn enabled_filter(id: &str) -> GenericFiltersConfig {
        GenericFiltersConfig {
            version: 1,
            filters: IndexMap::from([(
                id.to_owned(),
                GenericFilterConfig {
                    id: id.to_owned(),
                    is_enabled: true,
                    condition: Some(RuleCondition::eq("event.exceptions", "myError")),
                },
            )]),
        }
    }

    /// Returns an enabled flag of a [`GenericFiltersConfig`].
    fn enabled_flag_filter(id: &str) -> GenericFiltersConfig {
        GenericFiltersConfig {
            version: 1,
            filters: IndexMap::from([(
                id.to_owned(),
                GenericFilterConfig {
                    id: id.to_owned(),
                    is_enabled: true,
                    condition: None,
                },
            )]),
        }
    }

    /// Returns a complete but disabled [`GenericFiltersConfig`].
    fn disabled_filter(id: &str) -> GenericFiltersConfig {
        GenericFiltersConfig {
            version: 1,
            filters: IndexMap::from([(
                id.to_owned(),
                GenericFilterConfig {
                    id: id.to_owned(),
                    is_enabled: false,
                    condition: Some(RuleCondition::eq("event.exceptions", "myError")),
                },
            )]),
        }
    }

    /// Returns a disabled flag of a [`GenericFiltersConfig`].
    fn disabled_flag_filter(id: &str) -> GenericFiltersConfig {
        GenericFiltersConfig {
            version: 1,
            filters: IndexMap::from([(
                id.to_owned(),
                GenericFilterConfig {
                    id: id.to_owned(),
                    is_enabled: false,
                    condition: None,
                },
            )]),
        }
    }

    macro_rules! assert_filters {
        ($combined_filters:expr, $expected:expr) => {
            let actual_ids = $combined_filters.into_iter();
            assert!(actual_ids.eq($expected));
        };
    }

    #[test]
    fn test_no_combined_filters_on_unsupported_project_version() {
        let mut project = enabled_filter("unsupported-project");
        project.version = 2;
        let global = enabled_filter("supported-global");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_filters_on_unsupported_project_version_no_global() {
        let mut project = enabled_filter("unsupported-project");
        project.version = 2;
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, None, 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_filters_on_unsupported_global_version() {
        let project = enabled_filter("supported-project");
        let mut global = enabled_filter("unsupported-global");
        global.version = 2;
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_filters_on_unsupported_project_and_global_version() {
        let mut project = enabled_filter("unsupported-project");
        project.version = 2;
        let mut global = enabled_filter("unsupported-global");
        global.version = 2;
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_combined_filters_on_supported_project_and_global_version() {
        let project = enabled_filter("supported-project");
        let global = enabled_filter("supported-global");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            [
                project.filters.first().unwrap().1,
                global.filters.first().unwrap().1
            ]
            .into_iter()
        );
    }

    #[test]
    fn test_no_combined_duplicates_when_both_enabled() {
        let project = enabled_filter("filter");
        let global = enabled_filter("filter");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            [project.filters.first().unwrap().1,].into_iter()
        );
    }

    #[test]
    fn test_no_combined_filters_when_no_filters() {
        let project = empty_filter();
        let global = empty_filter();
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_global_filters() {
        let project = empty_filter();
        let global = disabled_filter("disabled-global");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_global_combined_when_enabled_global_filters() {
        let project = empty_filter();
        let global = enabled_filter("enabled-global");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            [global.filters.first().unwrap().1].into_iter()
        );
    }

    #[test]
    fn test_no_combined_when_global_is_flag() {
        let project = empty_filter();
        let global = enabled_flag_filter("skip");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_project_filters_empty_global() {
        let project = disabled_filter("disabled-project");
        let global = empty_filter();
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }
    #[test]
    fn test_no_combined_when_disabled_project_filters_missing_global() {
        let project = disabled_filter("disabled-project");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, None, 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_in_project_and_global() {
        let project = disabled_filter("disabled-project");
        let global = disabled_filter("disabled-global");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_duplicates_when_both_disabled() {
        let project = disabled_filter("filter");
        let global = disabled_filter("filter");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_project_enabled_global() {
        let project = disabled_filter("filter");
        let global = enabled_filter("filter");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_enabled_flag_project_empty_global() {
        let project = enabled_flag_filter("filter");
        let global = empty_filter();
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_enabled_flag_project_missing_global() {
        let project = enabled_flag_filter("filter");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, None, 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_flag_project_empty_global() {
        let project = disabled_flag_filter("filter");
        let global = empty_filter();
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_flag_project_missing_global() {
        let project = disabled_flag_filter("filter");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, None, 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_project_combined_when_only_project_enabled_empty_global() {
        let project = enabled_filter("enabled-project");
        let global = empty_filter();
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            [project.filters.first().unwrap().1].into_iter()
        );
    }

    #[test]
    fn test_project_combined_when_only_project_enabled_missing_global() {
        let project = enabled_filter("enabled-project");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, None, 1),
            [project.filters.first().unwrap().1].into_iter()
        );
    }

    #[test]
    fn test_project_combined_when_enabled_flag_project_disabled_global() {
        let project = enabled_filter("filter");
        let global = disabled_filter("filter");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            [project.filters.first().unwrap().1].into_iter()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_flag_project_disabled_global() {
        let project = disabled_flag_filter("filter");
        let global = disabled_filter("filter");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_project_combined_when_enabled_project_disabled_global() {
        let project = enabled_filter("filter");
        let global = disabled_filter("filter");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            [project.filters.first().unwrap().1].into_iter()
        );
    }

    #[test]
    fn test_common_combined_when_enabled_flag_project_enabled_global() {
        let project = enabled_flag_filter("filter");
        let global = enabled_filter("filter");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            [global.filters.first().unwrap().1].into_iter()
        );
    }

    #[test]
    fn test_no_combined_when_disabled_flag_project_enabled_global() {
        let project = disabled_flag_filter("filter");
        let global = enabled_filter("filter");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_common_combined_when_enabled_project_enabled_global() {
        let project = enabled_filter("filter");
        let global = enabled_flag_filter("filter");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            [project.filters.first().unwrap().1].into_iter()
        );
    }

    #[test]
    fn test_no_combined_when_enabled_flags_project_and_global() {
        let project = enabled_flag_filter("filter");
        let global = enabled_flag_filter("filter");
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            iter::empty::<&GenericFilterConfig>()
        );
    }

    #[test]
    fn test_multiple_combined_filters() {
        let project = GenericFiltersConfig {
            version: 1,
            filters: IndexMap::from([
                (
                    "0".to_owned(),
                    GenericFilterConfig {
                        id: "0".to_owned(),
                        is_enabled: true,
                        condition: Some(RuleCondition::eq("event.exceptions", "myError")),
                    },
                ),
                (
                    "1".to_owned(),
                    GenericFilterConfig {
                        id: "1".to_owned(),
                        is_enabled: true,
                        condition: None,
                    },
                ),
                (
                    "2".to_owned(),
                    GenericFilterConfig {
                        id: "2".to_owned(),
                        is_enabled: true,
                        condition: Some(RuleCondition::eq("event.exceptions", "myError")),
                    },
                ),
                (
                    "not-picked".to_owned(),
                    GenericFilterConfig {
                        id: "not-picked".to_owned(),
                        is_enabled: true,
                        condition: None,
                    },
                ),
            ]),
        };
        let global = GenericFiltersConfig {
            version: 1,
            filters: IndexMap::from([
                (
                    "1".to_owned(),
                    GenericFilterConfig {
                        id: "1".to_owned(),
                        is_enabled: true,
                        condition: Some(RuleCondition::eq("event.exceptions", "myOtherError")),
                    },
                ),
                (
                    "3".to_owned(),
                    GenericFilterConfig {
                        id: "3".to_owned(),
                        is_enabled: true,
                        condition: Some(RuleCondition::eq("event.exceptions", "myLastError")),
                    },
                ),
            ]),
        };
        assert_filters!(
            DynamicGenericFiltersConfig::new(&project, Some(&global), 1),
            [
                &project.filters[0], // id=0, picked from project
                &global.filters[0],  // id=1, picked from global through the project's flag
                &project.filters[2], // id=2, picked from project
                &global.filters[1]   // id=3, picked from global
            ]
            .into_iter()
        );
    }
}
