//! Implements generic filtering based on the [`RuleCondition`] DSL.
//!
//! Multiple generic filters can be defined and they are going to be checked in FIFO order. The
//! first one that matches, will result in the event being discarded with a [`FilterStatKey`]
//! identifying the matching filter.

use std::iter::FusedIterator;

use crate::{FilterStatKey, GenericFilterConfig, GenericFiltersConfig};
use indexmap::IndexMap;
use relay_event_schema::protocol::Event;
use relay_protocol::RuleCondition;

/// Maximum supported version of the generic filters schema.
///
/// If the version in the project config is higher, no generic filters are applied.
const VERSION: u16 = 1;

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
    let filters = merge_generic_filters(
        project_filters,
        global_filters,
        #[cfg(test)]
        VERSION,
    );

    for filter_config in filters {
        if filter_config.is_enabled && matches(event, filter_config.condition) {
            return Err(FilterStatKey::GenericFilter(filter_config.id.to_owned()));
        }
    }

    Ok(())
}

/// Returns an iterator that yields merged generic configs.
///
/// Since filters of project and global configs are complementary and don't
/// provide much value in isolation, both versions must match [`VERSION`] to be
/// compatible.  If filters aren't compatible, an empty iterator is returned.
///
/// If filters are compatible, an iterator over all filters is returned. This
/// iterator yields filters according to the principles below:
/// - Filters from project configs are evaluated before filters from global
/// configs.
/// - No duplicates: once a filter is evaluated (yielded or skipped), no filter
/// with the same id is evaluated again.
/// - If a filter with the same id exists in projects and global configs, both
/// are merged and the filter is yielded. Values from the filter in the project
/// config are prioritized.
fn merge_generic_filters<'a>(
    project: &'a GenericFiltersConfig,
    global: Option<&'a GenericFiltersConfig>,
    #[cfg(test)] version: u16,
) -> impl Iterator<Item = MergedFilter<'a>> {
    #[cfg(not(test))]
    let version = VERSION;

    let is_supported =
        project.version <= version && global.map_or(true, |gf| gf.version <= version);

    is_supported
        .then(|| {
            DynamicGenericFiltersConfigIter::new(&project.filters, global.map(|gc| &gc.filters))
        })
        .into_iter()
        .flatten()
}

/// Iterator over the generic filters of the project and global configs.
struct DynamicGenericFiltersConfigIter<'a> {
    /// Generic project filters.
    project: &'a IndexMap<String, GenericFilterConfig>,
    /// Index of the next filter in project configs to evaluate.
    project_index: usize,
    /// Generic global filters.
    global: Option<&'a IndexMap<String, GenericFilterConfig>>,
    /// Index of the next filter in global configs to evaluate.
    global_index: usize,
}

impl<'a> DynamicGenericFiltersConfigIter<'a> {
    pub fn new(
        project: &'a IndexMap<String, GenericFilterConfig>,
        global: Option<&'a IndexMap<String, GenericFilterConfig>>,
    ) -> Self {
        DynamicGenericFiltersConfigIter {
            project,
            project_index: 0,
            global,
            global_index: 0,
        }
    }
}

impl<'a> Iterator for DynamicGenericFiltersConfigIter<'a> {
    type Item = MergedFilter<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((id, filter)) = self.project.get_index(self.project_index) {
            self.project_index += 1;
            let merged = merge_filters(filter, self.global.and_then(|gf| gf.get(id)));
            return Some(merged);
        }

        loop {
            let Some((id, filter)) = self
                .global
                .and_then(|filters| filters.get_index(self.global_index))
            else {
                return None;
            };
            self.global_index += 1;
            if !self.project.contains_key(id) {
                return Some(filter.into());
            }
        }
    }
}

impl<'a> FusedIterator for DynamicGenericFiltersConfigIter<'a> {}

/// Merges the two filters with the same id, prioritizing values from the primary.
///
/// It's assumed both filters share the same id. The returned filter will have
/// the primary filter's ID.
fn merge_filters<'a>(
    primary: &'a GenericFilterConfig,
    secondary: Option<&'a GenericFilterConfig>,
) -> MergedFilter<'a> {
    MergedFilter {
        id: primary.id.as_str(),
        is_enabled: primary.is_enabled,
        condition: primary
            .condition
            .as_ref()
            .or(secondary.and_then(|filter| filter.condition.as_ref())),
    }
}

#[derive(Debug, Default, PartialEq)]
struct MergedFilter<'a> {
    id: &'a str,
    is_enabled: bool,
    condition: Option<&'a RuleCondition>,
}

impl<'a> From<&'a GenericFilterConfig> for MergedFilter<'a> {
    fn from(value: &'a GenericFilterConfig) -> Self {
        MergedFilter {
            id: value.id.as_str(),
            is_enabled: value.is_enabled,
            condition: value.condition.as_ref(),
        }
    }
}

#[cfg(test)]
mod tests {

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

    #[test]
    fn test_no_combined_when_unsupported_project_version() {
        let mut project = enabled_filter("unsupported-project");
        project.version = 2;
        let global = enabled_filter("supported-global");
        assert!(merge_generic_filters(&project, Some(&global), 1).eq(None.into_iter()));
    }

    #[test]
    fn test_no_combined_when_unsupported_project_version_no_global() {
        let mut project = enabled_filter("unsupported-project");
        project.version = 2;
        assert!(merge_generic_filters(&project, None, 1).eq(None.into_iter()));
    }

    #[test]
    fn test_no_combined_when_unsupported_global_version() {
        let project = enabled_filter("supported-project");
        let mut global = enabled_filter("unsupported-global");
        global.version = 2;
        assert!(merge_generic_filters(&project, Some(&global), 1).eq(None.into_iter()));
    }

    #[test]
    fn test_no_combined_when_unsupported_project_and_global_version() {
        let mut project = enabled_filter("unsupported-project");
        project.version = 2;
        let mut global = enabled_filter("unsupported-global");
        global.version = 2;
        assert!(merge_generic_filters(&project, Some(&global), 1).eq(None.into_iter()));
    }

    #[test]
    fn test_both_combined_when_supported_project_and_global_version() {
        let project = enabled_filter("supported-project");
        let global = enabled_filter("supported-global");
        assert!(merge_generic_filters(&project, Some(&global), 1).eq([
            project.filters.first().unwrap().1.into(),
            global.filters.first().unwrap().1.into()
        ]
        .into_iter()));
    }

    #[test]
    fn test_project_combined_when_duplicated_filter_project_and_global() {
        let project = enabled_filter("filter");
        let global = enabled_filter("filter");
        assert!(
            merge_generic_filters(&project, Some(&global), 1).eq([project
                .filters
                .first()
                .unwrap()
                .1
                .into()]
            .into_iter())
        );
    }

    #[test]
    fn test_no_combined_when_empty_project_and_global() {
        let project = empty_filter();
        let global = empty_filter();
        assert!(merge_generic_filters(&project, Some(&global), 1).eq(None.into_iter()));
    }

    #[test]
    fn test_global_combined_when_empty_project_disabled_global_filter() {
        let project = empty_filter();
        let global = disabled_filter("disabled-global");
        assert!(merge_generic_filters(&project, Some(&global), 1).eq([global
            .filters
            .first()
            .unwrap()
            .1
            .into()]
        .into_iter()));
    }

    #[test]
    fn test_global_combined_when_empty_project_enabled_global_filters() {
        let project = empty_filter();
        let global = enabled_filter("enabled-global");
        assert!(merge_generic_filters(&project, Some(&global), 1).eq([global
            .filters
            .first()
            .unwrap()
            .1
            .into()]
        .into_iter()));
    }

    #[test]
    fn test_global_combined_when_empty_project_enabled_flag_global() {
        let project = empty_filter();
        let global = enabled_flag_filter("skip");
        assert!(merge_generic_filters(&project, Some(&global), 1).eq([global
            .filters
            .first()
            .unwrap()
            .1
            .into()]));
    }

    #[test]
    fn test_project_combined_when_disabled_project_empty_global() {
        let project = disabled_filter("disabled-project");
        let global = empty_filter();
        assert!(
            merge_generic_filters(&project, Some(&global), 1).eq([project
                .filters
                .first()
                .unwrap()
                .1
                .into()]
            .into_iter())
        );
    }

    #[test]
    fn test_project_combined_when_disabled_project_missing_global() {
        let project = disabled_filter("disabled-project");
        assert!(merge_generic_filters(&project, None, 1).eq([project
            .filters
            .first()
            .unwrap()
            .1
            .into(),]
        .into_iter()));
    }

    #[test]
    fn test_both_combined_when_different_disabled_project_and_global() {
        let project = disabled_filter("disabled-project");
        let global = disabled_filter("disabled-global");
        assert!(merge_generic_filters(&project, Some(&global), 1).eq([
            project.filters.first().unwrap().1.into(),
            global.filters.first().unwrap().1.into()
        ]));
    }

    #[test]
    fn test_project_combined_duplicated_disabled_project_and_global() {
        let project = disabled_filter("filter");
        let global = disabled_filter("filter");
        assert!(
            merge_generic_filters(&project, Some(&global), 1).eq([project
                .filters
                .first()
                .unwrap()
                .1
                .into()])
        );
    }

    #[test]
    fn test_merged_combined_when_disabled_project_enabled_global() {
        let project = disabled_filter("filter");
        let global = enabled_filter("filter");
        let expected = &GenericFilterConfig {
            id: "filter".to_owned(),
            is_enabled: false,
            condition: global.filters.first().unwrap().1.condition.clone(),
        };
        assert!(merge_generic_filters(&project, Some(&global), 1).eq([expected.into()].into_iter()));
    }

    #[test]
    fn test_no_combined_when_enabled_flag_project_empty_global() {
        let project = enabled_flag_filter("filter");
        let global = empty_filter();
        assert!(
            merge_generic_filters(&project, Some(&global), 1).eq([project
                .filters
                .first()
                .unwrap()
                .1
                .into()]
            .into_iter())
        );
    }

    #[test]
    fn test_project_combined_when_enabled_flag_project_missing_global() {
        let project = enabled_flag_filter("filter");
        assert!(merge_generic_filters(&project, None, 1).eq([project
            .filters
            .first()
            .unwrap()
            .1
            .into()]
        .into_iter()));
    }

    #[test]
    fn test_project_combined_when_disabled_flag_project_empty_global() {
        let project = disabled_flag_filter("filter");
        let global = empty_filter();
        assert!(
            merge_generic_filters(&project, Some(&global), 1).eq([project
                .filters
                .first()
                .unwrap()
                .1
                .into()])
        );
    }

    #[test]
    fn test_project_combined_when_disabled_flag_project_missing_global() {
        let project = disabled_flag_filter("filter");
        assert!(merge_generic_filters(&project, None, 1).eq([project
            .filters
            .first()
            .unwrap()
            .1
            .into()]));
    }

    #[test]
    fn test_project_combined_when_enabled_project_empty_global() {
        let project = enabled_filter("enabled-project");
        let global = empty_filter();
        assert!(
            merge_generic_filters(&project, Some(&global), 1).eq([project
                .filters
                .first()
                .unwrap()
                .1
                .into()]
            .into_iter())
        );
    }

    #[test]
    fn test_project_combined_when_enabled_project_missing_global() {
        let project = enabled_filter("enabled-project");
        assert!(merge_generic_filters(&project, None, 1).eq([project
            .filters
            .first()
            .unwrap()
            .1
            .into()]
        .into_iter()));
    }

    #[test]
    fn test_merged_combined_when_enabled_flag_project_disabled_global() {
        let project = enabled_flag_filter("filter");
        let global = disabled_filter("filter");
        let expected = &GenericFilterConfig {
            id: "filter".to_owned(),
            is_enabled: true,
            condition: global.filters.first().unwrap().1.condition.clone(),
        };
        assert!(merge_generic_filters(&project, Some(&global), 1).eq([expected.into()].into_iter()));
    }

    #[test]
    fn test_global_combined_when_disabled_flag_project_disabled_global() {
        let project = disabled_flag_filter("filter");
        let global = disabled_filter("filter");
        assert!(merge_generic_filters(&project, Some(&global), 1).eq([global
            .filters
            .first()
            .unwrap()
            .1
            .into()]));
    }

    #[test]
    fn test_project_combined_when_enabled_project_disabled_global() {
        let project = enabled_filter("filter");
        let global = disabled_filter("filter");
        assert!(
            merge_generic_filters(&project, Some(&global), 1).eq([project
                .filters
                .first()
                .unwrap()
                .1
                .into()]
            .into_iter())
        );
    }

    #[test]
    fn test_global_combined_when_enabled_flag_project_enabled_global() {
        let project = enabled_flag_filter("filter");
        let global = enabled_filter("filter");
        assert!(merge_generic_filters(&project, Some(&global), 1).eq([global
            .filters
            .first()
            .unwrap()
            .1
            .into()]
        .into_iter()));
    }

    #[test]
    fn test_merged_combined_when_disabled_flag_project_enabled_global() {
        let project = disabled_flag_filter("filter");
        let global = enabled_filter("filter");
        let expected = &GenericFilterConfig {
            id: "filter".to_owned(),
            is_enabled: false,
            condition: global.filters.first().unwrap().1.condition.clone(),
        };
        assert!(merge_generic_filters(&project, Some(&global), 1).eq([expected.into()].into_iter()));
    }

    #[test]
    fn test_project_combined_when_enabled_project_enabled_flag_global() {
        let project = enabled_filter("filter");
        let global = enabled_flag_filter("filter");
        assert!(
            merge_generic_filters(&project, Some(&global), 1).eq([project
                .filters
                .first()
                .unwrap()
                .1
                .into()]
            .into_iter())
        );
    }

    #[test]
    fn test_project_combined_when_enabled_flags_project_and_global() {
        let project = enabled_flag_filter("filter");
        let global = enabled_flag_filter("filter");
        assert!(
            merge_generic_filters(&project, Some(&global), 1).eq([project
                .filters
                .first()
                .unwrap()
                .1
                .into()]
            .into_iter())
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
            ]),
        };
        let global = GenericFiltersConfig {
            version: 1,
            filters: IndexMap::from([
                (
                    "1".to_owned(),
                    GenericFilterConfig {
                        id: "1".to_owned(),
                        is_enabled: false,
                        condition: Some(RuleCondition::eq("event.exceptions", "myOtherError")),
                    },
                ),
                (
                    "3".to_owned(),
                    GenericFilterConfig {
                        id: "3".to_owned(),
                        is_enabled: false,
                        condition: Some(RuleCondition::eq("event.exceptions", "myLastError")),
                    },
                ),
            ]),
        };

        let expected0 = &project.filters[0];
        let expected1 = &GenericFilterConfig {
            id: "1".to_owned(),
            is_enabled: true,
            condition: Some(RuleCondition::eq("event.exceptions", "myOtherError")),
        };
        let expected2 = &project.filters[2];
        let expected3 = &global.filters[1];

        assert!(merge_generic_filters(&project, Some(&global), 1).eq([
            expected0.into(),
            expected1.into(),
            expected2.into(),
            expected3.into()
        ]
        .into_iter()));
    }
}
