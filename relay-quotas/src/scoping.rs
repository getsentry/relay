use relay_common::{DataCategory, ProjectId, ProjectKey};
use relay_project_config::quota::{DataCategories, Quota, QuotaScope};

/// Data scoping information.
///
/// This structure holds information of all scopes required for attributing an item to quotas.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Scoping {
    /// The organization id.
    pub organization_id: u64,

    /// The project id.
    pub project_id: ProjectId,

    /// The DSN public key.
    pub project_key: ProjectKey,

    /// The public key's internal id.
    pub key_id: Option<u64>,
}

impl Scoping {
    /// Returns an `ItemScoping` for this scope.
    ///
    /// The item scoping will contain a reference to this scope and the information passed to this
    /// function. This is a cheap operation to allow rate limiting for an individual item.
    pub fn item(&self, category: DataCategory) -> ItemScoping<'_> {
        ItemScoping {
            category,
            scoping: self,
        }
    }
}

/// Data categorization and scoping information.
///
/// `ItemScoping` is always attached to a `Scope` and references it internally. It is a cheap,
/// copyable type intended for the use with `RateLimits` and `RateLimiter`. It implements
/// `Deref<Target = Scoping>` and `AsRef<Scoping>` for ease of use.
#[derive(Clone, Copy, Debug)]
pub struct ItemScoping<'a> {
    /// The data category of the item.
    pub category: DataCategory,

    /// Scoping of the data.
    pub scoping: &'a Scoping,
}

impl AsRef<Scoping> for ItemScoping<'_> {
    fn as_ref(&self) -> &Scoping {
        self.scoping
    }
}

impl std::ops::Deref for ItemScoping<'_> {
    type Target = Scoping;

    fn deref(&self) -> &Self::Target {
        self.scoping
    }
}

impl ItemScoping<'_> {
    /// Returns the identifier of the given scope.
    pub fn scope_id(&self, scope: QuotaScope) -> Option<u64> {
        match scope {
            QuotaScope::Organization => Some(self.organization_id),
            QuotaScope::Project => Some(self.project_id.value()),
            QuotaScope::Key => self.key_id,
            QuotaScope::Unknown => None,
        }
    }

    /// Checks whether the category matches any of the quota's categories.
    pub fn matches_categories(&self, categories: &DataCategories) -> bool {
        // An empty list of categories means that this quota matches all categories. Note that we
        // skip `Unknown` categories silently. If the list of categories only contains `Unknown`s,
        // we do **not** match, since apparently the quota is meant for some data this Relay does
        // not support yet.
        categories.is_empty() || categories.iter().any(|cat| *cat == self.category)
    }

    /// Checks whether this item scoping matches the given quota's scope.
    ///
    /// This quota matches, if:
    ///  - there is no `scope_id` constraint
    ///  - the `scope_id` constraint is not numeric
    ///  - the scope identifier matches the one from ascoping and the scope is known
    fn matches_quota_scope(&self, quota: &Quota) -> bool {
        // Check for a scope identifier constraint. If there is no constraint, this means that the
        // quota matches any scope. In case the scope is unknown, it will be coerced to the most
        // specific scope later.
        let scope_id = match quota.scope_id {
            Some(ref scope_id) => scope_id,
            None => return true,
        };

        // Check if the scope identifier in the quota is parseable. If not, this means we cannot
        // fulfill the constraint, so the quota does not match.
        let parsed = match scope_id.parse::<u64>() {
            Ok(parsed) => parsed,
            Err(_) => return false,
        };

        // At this stage, require that the scope is known since we have to fulfill the constraint.
        self.scope_id(quota.scope) == Some(parsed)
    }

    /// Checks whether the quota's constraints match the current item.
    pub fn matches(&self, quota: &Quota) -> bool {
        self.matches_quota_scope(&quota) && self.matches_categories(&quota.categories)
    }
}

#[cfg(test)]
mod tests {
    use smallvec::smallvec;

    use super::*;

    #[test]
    fn test_quota_matches_no_categories() {
        let quota = Quota {
            id: None,
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: None,
            window: None,
            reason_code: None,
        };

        let item_scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
        };
        assert!(item_scoping.matches(&quota));
    }

    #[test]
    fn test_quota_matches_unknown_category() {
        let quota = Quota {
            id: None,
            categories: smallvec![DataCategory::Unknown],
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: None,
            window: None,
            reason_code: None,
        };

        let item_scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
        };
        assert!(!item_scoping.matches(&quota));
    }

    #[test]
    fn test_quota_matches_multiple_categores() {
        let quota = Quota {
            id: None,
            categories: smallvec![DataCategory::Unknown, DataCategory::Error],
            scope: QuotaScope::Organization,
            scope_id: None,
            limit: None,
            window: None,
            reason_code: None,
        };

        let item_scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
        };
        assert!(item_scoping.matches(&quota));

        let item_scoping = ItemScoping {
            category: DataCategory::Transaction,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
        };
        assert!(!item_scoping.matches(&quota));
    }

    #[test]
    fn test_quota_matches_no_invalid_scope() {
        let quota = Quota {
            id: None,
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: Some("not_a_number".to_owned()),
            limit: None,
            window: None,
            reason_code: None,
        };

        let item_scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
        };
        assert!(!item_scoping.matches(&quota));
    }

    #[test]
    fn test_quota_matches_organization_scope() {
        let quota = Quota {
            id: None,
            categories: DataCategories::new(),
            scope: QuotaScope::Organization,
            scope_id: Some("42".to_owned()),
            limit: None,
            window: None,
            reason_code: None,
        };

        let item_scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
        };
        assert!(item_scoping.matches(&quota));

        let item_scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 0,
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
        };
        assert!(!item_scoping.matches(&quota));
    }

    #[test]
    fn test_quota_matches_project_scope() {
        let quota = Quota {
            id: None,
            categories: DataCategories::new(),
            scope: QuotaScope::Project,
            scope_id: Some("21".to_owned()),
            limit: None,
            window: None,
            reason_code: None,
        };

        let item_scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
        };
        assert!(item_scoping.matches(&quota));

        let item_scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(0),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
        };
        assert!(!item_scoping.matches(&quota));
    }

    #[test]
    fn test_quota_matches_key_scope() {
        let quota = Quota {
            id: None,
            categories: DataCategories::new(),
            scope: QuotaScope::Key,
            scope_id: Some("17".to_owned()),
            limit: None,
            window: None,
            reason_code: None,
        };

        let item_scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(17),
            },
        };
        assert!(item_scoping.matches(&quota));

        let item_scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: Some(0),
            },
        };
        assert!(!item_scoping.matches(&quota));

        let item_scoping = ItemScoping {
            category: DataCategory::Error,
            scoping: &Scoping {
                organization_id: 42,
                project_id: ProjectId::new(21),
                project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                key_id: None,
            },
        };
        assert!(!item_scoping.matches(&quota));
    }
}
