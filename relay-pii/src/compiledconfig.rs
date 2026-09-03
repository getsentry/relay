use std::cmp::Ordering;
use std::collections::BTreeSet;

use crate::builtin::BUILTIN_RULES_MAP;
use crate::{PiiConfig, PiiConfigError, Redaction, RuleSpec, RuleType, SelectorSpec};

/// A representation of `PiiConfig` that is more (CPU-)efficient for use in `PiiProcessor`.
///
/// It is lossy in the sense that it cannot be consumed by downstream Relays, so both versions have
/// to be kept around.
#[derive(Debug, Clone)]
pub struct CompiledPiiConfig {
    pub(super) applications: Vec<(SelectorSpec, BTreeSet<RuleRef>)>,
}

impl CompiledPiiConfig {
    /// Computes the compiled PII config.
    pub fn new(config: &PiiConfig) -> Self {
        let mut applications = Vec::new();
        for (selector, rules) in &config.applications {
            #[allow(clippy::mutable_key_type)]
            let mut rule_set = BTreeSet::default();
            for rule_id in rules {
                collect_rules(config, &mut rule_set, rule_id, None);
            }
            applications.push((selector.clone(), rule_set));
        }

        CompiledPiiConfig { applications }
    }

    /// Force compilation of all regex patterns in this config.
    ///
    /// Used to verify that all patterns are valid regex.
    pub fn force_compile(&self) -> Result<(), PiiConfigError> {
        for rule in self.applications.iter().flat_map(|(_, rules)| rules.iter()) {
            match &rule.ty {
                RuleType::Pattern(rule) => {
                    rule.pattern.compiled().map_err(|e| e.clone())?;
                }
                RuleType::RedactPair(rule) => {
                    rule.key_pattern.compiled().map_err(|e| e.clone())?;
                }
                RuleType::Anything
                | RuleType::Imei
                | RuleType::Mac
                | RuleType::Uuid
                | RuleType::Email
                | RuleType::Ip
                | RuleType::Creditcard
                | RuleType::Iban
                | RuleType::Userpath
                | RuleType::Pemkey
                | RuleType::UrlAuth
                | RuleType::UsSsn
                | RuleType::Bearer
                | RuleType::Password
                | RuleType::Multiple(_)
                | RuleType::Alias(_)
                | RuleType::Unknown(_) => {}
            }
        }
        Ok(())
    }
}

fn get_rule(config: &PiiConfig, id: &str) -> Option<RuleRef> {
    if let Some(spec) = config.rules.get(id) {
        Some(RuleRef::new(id.to_owned(), spec))
    } else {
        BUILTIN_RULES_MAP
            .get(id)
            .map(|spec| RuleRef::new(id.to_owned(), spec))
    }
}

#[allow(clippy::mutable_key_type)]
fn collect_rules(
    config: &PiiConfig,
    rules: &mut BTreeSet<RuleRef>,
    rule_id: &str,
    parent: Option<RuleRef>,
) {
    let rule = match get_rule(config, rule_id) {
        Some(rule) => rule,
        None => return,
    };

    if rules.contains(&rule) {
        return;
    }

    let rule = match parent {
        Some(parent) => rule.for_parent(parent),
        None => rule,
    };

    match rule.ty {
        RuleType::Multiple(ref m) => {
            let parent = if m.hide_inner {
                Some(rule.clone())
            } else {
                None
            };
            rules.insert(rule.clone()); // insert to break cycles
            for rule_id in &m.rules {
                collect_rules(config, rules, rule_id, parent.clone());
            }
            rules.remove(&rule); // don't persist intermediates
        }
        RuleType::Alias(ref a) => {
            let parent = if a.hide_inner {
                Some(rule.clone())
            } else {
                None
            };
            rules.insert(rule.clone()); // insert to break cycles
            collect_rules(config, rules, &a.rule, parent);
            rules.remove(&rule); // don't persist intermediates
        }
        RuleType::Unknown(_) => {}
        _ => {
            rules.insert(rule);
        }
    }
}

/// Reference to a PII rule.
#[derive(Debug, Clone)]
pub(super) struct RuleRef {
    pub id: String,
    pub origin: String,
    pub ty: RuleType,
    pub redaction: Redaction,
}

impl RuleRef {
    fn new(id: String, spec: &RuleSpec) -> Self {
        RuleRef {
            origin: id.clone(),
            id,
            ty: spec.ty.clone(),
            redaction: spec.redaction.clone(),
        }
    }

    pub fn for_parent(self, parent: Self) -> Self {
        RuleRef {
            id: self.id,
            origin: parent.origin,
            ty: self.ty,
            redaction: match parent.redaction {
                Redaction::Default => self.redaction,
                _ => parent.redaction,
            },
        }
    }
}

impl PartialEq for RuleRef {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for RuleRef {}

impl PartialOrd for RuleRef {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RuleRef {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::AliasRule;

    use super::*;

    #[test]
    fn cycle_singleton() {
        // a -> a
        let config = PiiConfig {
            rules: BTreeMap::from([(
                "a".to_owned(),
                RuleSpec {
                    ty: RuleType::Alias(AliasRule {
                        rule: "a".to_owned(),
                        hide_inner: false,
                    }),
                    redaction: Redaction::Default,
                },
            )]),
            ..Default::default()
        };
        #[allow(clippy::mutable_key_type)]
        let mut collected_rules = Default::default();
        collect_rules(&config, &mut collected_rules, "a", None);

        // The cycle has been removed:
        assert!(collected_rules.is_empty());
    }

    #[test]
    fn cycle_pair() {
        // a -> b -> a
        let config = PiiConfig {
            rules: BTreeMap::from([
                (
                    "a".to_owned(),
                    RuleSpec {
                        ty: RuleType::Alias(AliasRule {
                            rule: "b".to_owned(),
                            hide_inner: false,
                        }),
                        redaction: Redaction::Default,
                    },
                ),
                (
                    "b".to_owned(),
                    RuleSpec {
                        ty: RuleType::Alias(AliasRule {
                            rule: "a".to_owned(),
                            hide_inner: false,
                        }),
                        redaction: Redaction::Default,
                    },
                ),
            ]),
            ..Default::default()
        };
        #[allow(clippy::mutable_key_type)]
        let mut collected_rules = Default::default();
        collect_rules(&config, &mut collected_rules, "a", None);

        // The cycle has been removed:
        assert!(collected_rules.is_empty());
    }

    #[test]
    fn only_one_shared_rule_survives() {
        // When multiple aliases point to the same rule, only one of their names survives.
        // a -> c
        // b -> c
        let config = PiiConfig {
            rules: BTreeMap::from([
                (
                    "a".to_owned(),
                    RuleSpec {
                        ty: RuleType::Alias(AliasRule {
                            rule: "c".to_owned(),
                            hide_inner: true,
                        }),
                        redaction: Redaction::Default,
                    },
                ),
                (
                    "b".to_owned(),
                    RuleSpec {
                        ty: RuleType::Alias(AliasRule {
                            rule: "c".to_owned(),
                            hide_inner: true,
                        }),
                        redaction: Redaction::Default,
                    },
                ),
                (
                    "c".to_owned(),
                    RuleSpec {
                        ty: RuleType::Anything,
                        redaction: Redaction::Default,
                    },
                ),
            ]),
            ..Default::default()
        };
        #[allow(clippy::mutable_key_type)]
        let mut collected_rules = Default::default();
        collect_rules(&config, &mut collected_rules, "a", None);
        collect_rules(&config, &mut collected_rules, "b", None);

        let collected_rules: Vec<_> = collected_rules
            .into_iter()
            .map(|rr| (rr.origin, rr.id))
            .collect();

        insta::assert_debug_snapshot!(collected_rules, @r#"
        [
            (
                "a",
                "c",
            ),
        ]
        "#);
    }
}
