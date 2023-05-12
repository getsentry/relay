#![allow(clippy::needless_update)]
use std::collections::BTreeMap;

use once_cell::sync::Lazy;

use crate::pii::{
    AliasRule, MultipleRule, PatternRule, Redaction, ReplaceRedaction, RuleSpec, RuleType,
};

macro_rules! declare_builtin_rules {
    ($($rule_id:expr => $spec:expr;)*) => {
        pub(crate) static BUILTIN_RULES_MAP: Lazy<BTreeMap<&str, RuleSpec>> = Lazy::new(|| {
            let mut map = BTreeMap::new();
            $(
                map.insert($rule_id, $spec);
            )*
            map
        });
    }
}

macro_rules! rule_alias {
    ($target:expr) => {
        RuleSpec {
            ty: RuleType::Alias(AliasRule {
                rule: ($target).into(),
                hide_inner: true,
            }),
            redaction: Redaction::Default,
        }
    };
}

declare_builtin_rules! {
    // collections
    "@common" => RuleSpec {
        ty: RuleType::Multiple(MultipleRule {
            rules: vec![
                "@iban".into(),
                "@ip".into(),
                "@email".into(),
                "@creditcard".into(),
                "@pemkey".into(),
                "@urlauth".into(),
                "@userpath".into(),
                "@password".into(),
                "@usssn".into(),
            ],
            hide_inner: false,
        }),
        redaction: Redaction::Default,
    };
    // legacy data scrubbing equivalent. Note
    "@common:filter" => RuleSpec {
        ty: RuleType::Multiple(MultipleRule {
            rules: vec![
                "@creditcard:filter".into(),
                "@pemkey:filter".into(),
                "@urlauth:legacy".into(),
                "@userpath:filter".into(),
                "@password:filter".into(),
                "@usssn:filter".into(),
            ],
            hide_inner: false,
        }),
        redaction: Redaction::Default,
    };

    // anything
    "@anything" => rule_alias!("@anything:replace");
    "@anything:remove" => RuleSpec {
        ty: RuleType::Anything,
        redaction: Redaction::Remove,
    };
    "@anything:replace" => RuleSpec {
        ty: RuleType::Anything,
        redaction: Redaction::Replace(ReplaceRedaction::default()),
    };
    "@anything:hash" => RuleSpec {
        ty: RuleType::Anything,
        redaction: Redaction::Hash,
    };
    "@anything:mask" => RuleSpec {
        ty: RuleType::Anything,
        redaction: Redaction::Mask,
    };
    "@anything:filter" => RuleSpec {
        ty: RuleType::Anything,
        redaction: Redaction::Replace(ReplaceRedaction::default()),
    };

    // ip rules
    "@ip" => rule_alias!("@ip:replace");
    "@ip:replace" => RuleSpec {
        ty: RuleType::Ip,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[ip]".into(),
        }),
    };
    "@ip:hash" => RuleSpec {
        ty: RuleType::Ip,
        redaction: Redaction::Hash,
    };
    "@ip:mask" => RuleSpec {
        ty: RuleType::Ip,
        redaction: Redaction::Mask,
    };
    "@ip:remove" => RuleSpec {
        ty: RuleType::Ip,
        redaction: Redaction::Remove,
    };

    // imei rules
    "@imei" => rule_alias!("@imei:replace");
    "@imei:replace" => RuleSpec {
        ty: RuleType::Imei,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[imei]".into(),
        }),
    };
    "@imei:hash" => RuleSpec {
        ty: RuleType::Imei,
        redaction: Redaction::Hash,
    };
    "@imei:mask" => RuleSpec {
        ty: RuleType::Imei,
        redaction: Redaction::Hash,
    };
    "@imei:remove" => RuleSpec {
        ty: RuleType::Imei,
        redaction: Redaction::Remove,
    };

    // mac rules
    "@mac" => rule_alias!("@mac:mask");
    "@mac:replace" => RuleSpec {
        ty: RuleType::Mac,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[mac]".into(),
        }),
    };
    "@mac:hash" => RuleSpec {
        ty: RuleType::Mac,
        redaction: Redaction::Hash,
    };
    "@mac:mask" => RuleSpec {
        ty: RuleType::Mac,
        redaction: Redaction::Mask,
    };
    "@mac:remove" => RuleSpec {
        ty: RuleType::Mac,
        redaction: Redaction::Remove
    };

    // uuid rules
    "@uuid" => rule_alias!("@uuid:mask");
    "@uuid:replace" => RuleSpec {
        ty: RuleType::Uuid,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[uuid]".into(),
        }),
    };
    "@uuid:hash" => RuleSpec {
        ty: RuleType::Uuid,
        redaction: Redaction::Hash,
    };
    "@uuid:mask" => RuleSpec {
        ty: RuleType::Uuid,
        redaction: Redaction::Mask,
    };
    "@uuid:remove" => RuleSpec {
        ty: RuleType::Uuid,
        redaction: Redaction::Remove,
    };

    // email rules
    "@email" => rule_alias!("@email:replace");
    "@email:replace" => RuleSpec {
        ty: RuleType::Email,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[email]".into(),
        }),
    };
    "@email:hash" => RuleSpec {
        ty: RuleType::Email,
        redaction: Redaction::Hash,
    };
    "@email:mask" => RuleSpec {
        ty: RuleType::Email,
        redaction: Redaction::Mask,
    };
    "@email:remove" => RuleSpec {
        ty: RuleType::Email,
        redaction: Redaction::Remove,
    };

    // iban rules
    "@iban" => rule_alias!("@iban:replace");
    "@iban:hash" => RuleSpec {
        ty: RuleType::Iban,
        redaction: Redaction::Hash,
    };
    "@iban:replace" => RuleSpec {
        ty: RuleType::Iban,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[iban]".into(),
        }),
    };
    "@iban:mask" => RuleSpec {
        ty: RuleType::Iban,
        redaction: Redaction::Mask,
    };
    "@iban:filter" => RuleSpec {
        ty: RuleType::Iban,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[Filtered]".into(),
        }),
    };
    "@iban:remove" => RuleSpec {
        ty: RuleType::Iban,
        redaction: Redaction::Remove
    };

    // creditcard rules
    "@creditcard" => rule_alias!("@creditcard:replace");
    "@creditcard:hash" => RuleSpec {
        ty: RuleType::Creditcard,
        redaction: Redaction::Hash,
    };
    "@creditcard:replace" => RuleSpec {
        ty: RuleType::Creditcard,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[creditcard]".into(),
        }),
    };
    "@creditcard:mask" => RuleSpec {
        ty: RuleType::Creditcard,
        redaction: Redaction::Mask,
    };
    "@creditcard:filter" => RuleSpec {
        ty: RuleType::Creditcard,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[Filtered]".into(),
        }),
    };
    "@creditcard:remove" => RuleSpec {
        ty: RuleType::Creditcard,
        redaction: Redaction::Remove
    };

    // pem rules
    "@pemkey" => rule_alias!("@pemkey:replace");
    "@pemkey:replace" => RuleSpec {
        ty: RuleType::Pemkey,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[pemkey]".into(),
        }),
    };
    "@pemkey:filter" => RuleSpec {
        ty: RuleType::Pemkey,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[Filtered]".into(),
        }),
    };
    "@pemkey:hash" => RuleSpec {
        ty: RuleType::Pemkey,
        redaction: Redaction::Hash,
    };
    "@pemkey:mask" => RuleSpec {
        ty: RuleType::Pemkey,
        redaction: Redaction::Mask,
    };
    "@pemkey:remove" => RuleSpec {
        ty: RuleType::Pemkey,
        redaction: Redaction::Remove
    };

    // url secrets
    "@urlauth" => rule_alias!("@urlauth:replace");
    "@urlauth:replace" => RuleSpec {
        ty: RuleType::UrlAuth,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[auth]".into(),
        }),
    };
    "@urlauth:hash" => RuleSpec {
        ty: RuleType::UrlAuth,
        redaction: Redaction::Hash,
    };
    "@urlauth:mask" => RuleSpec {
        ty: RuleType::UrlAuth,
        redaction: Redaction::Mask,
    };
    "@urlauth:remove" => RuleSpec {
        ty: RuleType::UrlAuth,
        redaction: Redaction::Remove,
    };
    "@urlauth:legacy" => RuleSpec {
        ty: RuleType::Pattern(PatternRule {
            // Regex copied from legacy Sentry `URL_PASSWORD_RE`
            pattern: r"\b((?:[a-z0-9]+:)?//[a-zA-Z0-9%_.-]+:)([a-zA-Z0-9%_.-]+)@".into(),
            replace_groups: Some([2].iter().copied().collect()),
        }),
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[Filtered]".into(),
        }),
    };

    // US SSN
    "@usssn" => rule_alias!("@usssn:mask");
    "@usssn:replace" => RuleSpec {
        ty: RuleType::UsSsn,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[us-ssn]".into(),
        }),
    };
    "@usssn:filter" => RuleSpec {
        ty: RuleType::UsSsn,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[Filtered]".into(),
        }),
    };
    "@usssn:mask" => RuleSpec {
        ty: RuleType::UsSsn,
        redaction: Redaction::Mask,
    };
    "@usssn:hash" => RuleSpec {
        ty: RuleType::UsSsn,
        redaction: Redaction::Hash,
    };
    "@usssn:remove" => RuleSpec {
        ty: RuleType::UsSsn,
        redaction: Redaction::Remove,
    };

    // user path rules
    "@userpath" => rule_alias!("@userpath:replace");
    "@userpath:replace" => RuleSpec {
        ty: RuleType::Userpath,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[user]".into(),
        }),
    };
    "@userpath:mask" => RuleSpec {
        ty: RuleType::Userpath,
        redaction: Redaction::Mask,
    };
    "@userpath:hash" => RuleSpec {
        ty: RuleType::Userpath,
        redaction: Redaction::Hash,
    };
    "@userpath:remove" => RuleSpec {
        ty: RuleType::Userpath,
        redaction: Redaction::Remove,
    };

    // password field removal
    "@password" => rule_alias!("@password:remove");
    "@password:filter" => RuleSpec {
        ty: RuleType::Password,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[Filtered]".into(),
        }),
    };
    "@password:hash" => RuleSpec {
        ty: RuleType::Password,
        redaction: Redaction::Hash,
    };
    "@password:replace" => RuleSpec {
        ty: RuleType::Password,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[password]".into(),
        }),
    };
    "@password:mask" => RuleSpec {
        ty: RuleType::Password,
        redaction: Redaction::Mask,
    };
    "@password:remove" => RuleSpec {
        ty: RuleType::Password,
        redaction: Redaction::Remove,
    };
}

// TODO: Move these tests to /tests
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use similar_asserts::assert_eq;

    use super::*;
    use crate::pii::config::PiiConfig;
    use crate::pii::processor::PiiProcessor;
    use crate::processor::{process_value, ProcessingState, ValueType};
    use crate::types::{Annotated, Remark, RemarkType};

    #[derive(Clone, Debug, PartialEq, Empty, FromValue, ProcessValue, IntoValue)]
    struct FreeformRoot {
        #[metastructure(pii = "true")]
        value: Annotated<String>,
    }

    macro_rules! assert_rule_not_applied {
        (
            rule = $rule:expr; input = $input:expr;
        ) => {{
            let config = PiiConfig {
                applications: {
                    let mut map = BTreeMap::new();
                    map.insert(ValueType::String.into(), vec![$rule.to_string()]);
                    map
                },
                ..PiiConfig::default()
            };
            let input = $input.to_string();
            let compiled = config.compiled();
            let mut processor = PiiProcessor::new(&compiled);
            let mut root = Annotated::new(FreeformRoot {
                value: Annotated::new(input),
            });
            process_value(&mut root, &mut processor, ProcessingState::root()).unwrap();
            let root = root.0.unwrap();
            assert_eq!(root.value.value().unwrap(), $input);
        }};
    }

    macro_rules! assert_text_rule {
        (
            rule = $rule:expr; input = $input:expr; output = $output:expr; remarks = $remarks:expr;
        ) => {{
            let config = PiiConfig {
                applications: {
                    let mut map = BTreeMap::new();
                    map.insert(ValueType::String.into(), vec![$rule.to_string()]);
                    map
                },
                ..PiiConfig::default()
            };
            let input = $input.to_string();
            let compiled = config.compiled();
            let mut processor = PiiProcessor::new(&compiled);
            let mut root = Annotated::new(FreeformRoot {
                value: Annotated::new(input),
            });
            process_value(&mut root, &mut processor, ProcessingState::root()).unwrap();
            let root = root.0.unwrap();
            assert_eq!(root.value.value().unwrap(), $output);
            let remarks: Vec<Remark> = $remarks;
            assert_eq!(
                root.value.meta().iter_remarks().collect::<Vec<_>>(),
                remarks.iter().collect::<Vec<_>>()
            );
        }};
    }

    macro_rules! assert_custom_rulespec {
        (
            rule = $rule:expr; input = $input:expr; output = $output:expr; remarks = $remarks:expr;
        ) => {{
            let mut chunks = $rule[1..].split(':');
            let a = chunks.next().unwrap();
            let b = chunks.next().unwrap();

            let config2 = PiiConfig {
                rules: {
                    let mut map = BTreeMap::new();
                    map.insert(
                        "0".to_owned(),
                        RuleSpec {
                            ty: match a {
                                "ip" => RuleType::Ip,
                                "email" => RuleType::Email,
                                "creditcard" => RuleType::Creditcard,
                                "mac" => RuleType::Mac,
                                "uuid" => RuleType::Uuid,
                                "iban" => RuleType::Iban,
                                "imei" => RuleType::Imei,
                                _ => panic!("Unknown RuleType"),
                            },
                            redaction: match b {
                                "remove" => Redaction::Remove,
                                "replace" => Redaction::Replace(ReplaceRedaction::default()),
                                "mask" => Redaction::Mask,
                                "hash" => Redaction::Hash,
                                _ => panic!("Unknown redaction method"),
                            },
                        },
                    );
                    map
                },
                applications: {
                    let mut map = BTreeMap::new();
                    map.insert(ValueType::String.into(), vec!["0".to_owned()]);
                    map
                },
                ..Default::default()
            };

            let input = $input.to_string();
            let compiled = config2.compiled();
            let mut processor = PiiProcessor::new(&compiled);
            let mut root = Annotated::new(FreeformRoot {
                value: Annotated::new(input),
            });
            process_value(&mut root, &mut processor, ProcessingState::root()).unwrap();
            let root = root.0.unwrap();
            assert_eq!(root.value.value().unwrap(), $output);
            let remarks: Vec<Remark> = $remarks;
            assert_eq!(
                root.value.meta().iter_remarks().collect::<Vec<_>>(),
                remarks.iter().collect::<Vec<_>>()
            );
        }};
    }

    #[test]
    fn test_ipv4() {
        assert_text_rule!(
            rule = "@ip";
            input = "before 127.0.0.1 after";
            output = "before [ip] after";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@ip", (7, 11)),
            ];
        );
        assert_text_rule!(
            rule = "@ip:replace";
            input = "before 127.0.0.1 after";
            output = "before [ip] after";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@ip:replace", (7, 11)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@ip:replace";
            input = "before 127.0.0.1 after";
            output = "before [Filtered] after";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "0", (7, 17)),
            ];
        );
        assert_text_rule!(
            rule = "@ip:hash";
            input = "before 127.0.0.1 after";
            output = "before AE12FE3B5F129B5CC4CDD2B136B7B7947C4D2741 after";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "@ip:hash", (7, 47)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@ip:hash";
            input = "before 127.0.0.1 after";
            output = "before AE12FE3B5F129B5CC4CDD2B136B7B7947C4D2741 after";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "0", (7, 47)),
            ];
        );
    }

    #[test]
    fn test_ipv6() {
        assert_text_rule!(
            rule = "@ip";
            input = "before ::1 after";
            output = "before [ip] after";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@ip", (7, 11)),
            ];
        );
        assert_text_rule!(
            rule = "@ip:replace";
            input = "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]";
            output = "[[ip]]";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@ip:replace", (1, 5)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@ip:replace";
            input = "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]";
            output = "[[Filtered]]";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "0", (1, 11)),
            ];
        );
        assert_text_rule!(
            rule = "@ip:hash";
            input = "before 2001:0db8:85a3:0000:0000:8a2e:0370:7334 after";
            output = "before 8C3DC9BEED9ADE493670547E24E4E45EDE69FF03 after";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "@ip:hash", (7, 47)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@ip:hash";
            input = "before 2001:0db8:85a3:0000:0000:8a2e:0370:7334 after";
            output = "before 8C3DC9BEED9ADE493670547E24E4E45EDE69FF03 after";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "0", (7, 47)),
            ];
        );
        assert_text_rule!(
            rule = "@ip";
            input = "foo::1";
            output = "foo::1";
            remarks = vec![];
        );
    }

    #[test]
    fn test_imei() {
        assert_text_rule!(
            rule = "@imei";
            input = "before 356938035643809 after";
            output = "before [imei] after";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@imei", (7, 13)),
            ];
        );
        assert_text_rule!(
            rule = "@imei:replace";
            input = "before 356938035643809 after";
            output = "before [imei] after";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@imei:replace", (7, 13)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@imei:replace";
            input = "before 356938035643809 after";
            output = "before [Filtered] after";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "0", (7, 17)),
            ];
        );
        assert_text_rule!(
            rule = "@imei:hash";
            input = "before 356938035643809 after";
            output = "before 3888108AA99417402969D0B47A2CA4ECD2A1AAD3 after";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "@imei:hash", (7, 47)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@imei:hash";
            input = "before 356938035643809 after";
            output = "before 3888108AA99417402969D0B47A2CA4ECD2A1AAD3 after";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "0", (7, 47)),
            ];
        );
    }

    #[test]
    fn test_mac() {
        assert_text_rule!(
            rule = "@mac";
            input = "ether 4a:00:04:10:9b:50";
            output = "ether *****************";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "@mac", (6, 23)),
            ];
        );
        assert_text_rule!(
            rule = "@mac:mask";
            input = "ether 4a:00:04:10:9b:50";
            output = "ether *****************";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "@mac:mask", (6, 23)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@mac:mask";
            input = "ether 4a:00:04:10:9b:50";
            output = "ether *****************";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "0", (6, 23)),
            ];
        );
        assert_text_rule!(
            rule = "@mac:replace";
            input = "ether 4a:00:04:10:9b:50";
            output = "ether [mac]";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@mac:replace", (6, 11)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@mac:replace";
            input = "ether 4a:00:04:10:9b:50";
            output = "ether [Filtered]";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "0", (6, 16)),
            ];
        );
        assert_text_rule!(
            rule = "@mac:hash";
            input = "ether 4a:00:04:10:9b:50";
            output = "ether 6220F3EE59BF56B32C98323D7DE43286AAF1F8F1";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "@mac:hash", (6, 46)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@mac:hash";
            input = "ether 4a:00:04:10:9b:50";
            output = "ether 6220F3EE59BF56B32C98323D7DE43286AAF1F8F1";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "0", (6, 46)),
            ];
        );
    }

    #[test]
    fn test_uuid() {
        assert_text_rule!(
            rule = "@uuid";
            input = "user id ceee0822-ed8f-4622-b2a3-789e73e75cd1";
            output = "user id ************************************";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "@uuid", (8, 44)),
            ];
        );
        assert_text_rule!(
            rule = "@uuid:mask";
            input = "user id ceee0822-ed8f-4622-b2a3-789e73e75cd1";
            output = "user id ************************************";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "@uuid:mask", (8, 44)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@uuid:mask";
            input = "user id ceee0822-ed8f-4622-b2a3-789e73e75cd1";
            output = "user id ************************************";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "0", (8, 44)),
            ];
        );
        assert_text_rule!(
            rule = "@uuid:hash";
            input = "user id ceee0822-ed8f-4622-b2a3-789e73e75cd1";
            output = "user id EBFA4DBDAA4A619F10B6DE2ABB630DE0122F5CDB";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "@uuid:hash", (8, 48)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@uuid:hash";
            input = "user id ceee0822-ed8f-4622-b2a3-789e73e75cd1";
            output = "user id EBFA4DBDAA4A619F10B6DE2ABB630DE0122F5CDB";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "0", (8, 48)),
            ];
        );
        assert_text_rule!(
            rule = "@uuid:replace";
            input = "user id ceee0822-ed8f-4622-b2a3-789e73e75cd1";
            output = "user id [uuid]";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@uuid:replace", (8, 14)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@uuid:replace";
            input = "user id ceee0822-ed8f-4622-b2a3-789e73e75cd1";
            output = "user id [Filtered]";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "0", (8, 18)),
            ];
        );
    }

    #[test]
    fn test_email() {
        assert_text_rule!(
            rule = "@email";
            input = "John Appleseed <john@appleseed.com>";
            output = "John Appleseed <[email]>";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@email", (16, 23)),
            ];
        );
        assert_text_rule!(
            rule = "@email:replace";
            input = "John Appleseed <john@appleseed.com>";
            output = "John Appleseed <[email]>";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@email:replace", (16, 23)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@email:replace";
            input = "John Appleseed <john@appleseed.com>";
            output = "John Appleseed <[Filtered]>";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "0", (16, 26)),
            ];
        );
        assert_text_rule!(
            rule = "@email:mask";
            input = "John Appleseed <john@appleseed.com>";
            output = "John Appleseed <******************>";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "@email:mask", (16, 34)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@email:mask";
            input = "John Appleseed <john@appleseed.com>";
            output = "John Appleseed <******************>";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "0", (16, 34)),
            ];
        );
        assert_text_rule!(
            rule = "@email:hash";
            input = "John Appleseed <john@appleseed.com>";
            output = "John Appleseed <33835528AC0FFF1B46D167C35FEAAA6F08FD3F46>";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "@email:hash", (16, 56)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@email:hash";
            input = "John Appleseed <john@appleseed.com>";
            output = "John Appleseed <33835528AC0FFF1B46D167C35FEAAA6F08FD3F46>";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "0", (16, 56)),
            ];
        );
    }

    #[test]
    fn test_iban_different_rules() {
        assert_text_rule!(
            rule = "@iban";
            input = "some iban: DE89370400440532013000!";
            output = "some iban: [iban]!";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@iban", (11, 17)),
            ];
        );
        assert_text_rule!(
            rule = "@iban";
            input = "some iban: DE89370400440532013000!";
            output = "some iban: [iban]!";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@iban", (11, 17)),
            ];
        );
        assert_text_rule!(
            rule = "@iban:mask";
            input = "some iban: DE89370400440532013000!";
            output = "some iban: **********************!";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "@iban:mask", (11, 33)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@iban:mask";
            input = "some iban: DE89370400440532013000!";
            output = "some iban: **********************!";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "0", (11, 33)),
            ];
        );
        assert_text_rule!(
            rule = "@iban:replace";
            input = "some iban: DE89370400440532013000!";
            output = "some iban: [iban]!";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@iban:replace", (11, 17)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@iban:replace";
            input = "some iban: DE89370400440532013000!";
            output = "some iban: [Filtered]!";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "0", (11, 21)),
            ];
        );
        assert_text_rule!(
            rule = "@iban:hash";
            input = "some iban: DE89370400440532013000!";
            output = "some iban: 8A1248B6F40D38FBC59ADE6AD0DF69C7BB9C936A!";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "@iban:hash", (11, 51)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@iban:hash";
            input = "some iban: DE89370400440532013000!";
            output = "some iban: 8A1248B6F40D38FBC59ADE6AD0DF69C7BB9C936A!";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "0", (11, 51)),
            ];
        );
    }

    #[test]
    fn test_invalid_iban_codes() {
        assert_text_rule!(
            rule = "@iban";
            input = "some iban: NO9386011117945!";
            output = "some iban: [iban]!";
            remarks = vec![Remark::with_range(RemarkType::Substituted, "@iban", (11, 17))];
        );

        // Norway has the shortest iban, here I removed the final number, which means it should not
        // be scrubbed.
        assert_rule_not_applied!(
            rule = "@iban";
            input = "some iban: NO938601111794!";
        );

        assert_text_rule!(
            rule = "@iban";
            input = "some iban: RU0204452560040702810412345678901!";
            output = "some iban: [iban]!";
            remarks = vec![Remark::with_range(RemarkType::Substituted, "@iban", (11, 17))];
        );

        // Russia has the longest iban. Here I add a final number, which means the rule won't be
        // applied as it exceeds the maximum iban length.
        assert_rule_not_applied!(
            rule = "@iban";
            input = "some iban: RU02044525600407028104123456789018!";
        );

        assert_text_rule!(
            rule = "@iban";
            input = "some iban: NO9386011117945!";
            output = "some iban: [iban]!";
            remarks = vec![Remark::with_range(RemarkType::Substituted, "@iban", (11, 17))];
        );

        // Don't apply if it doesnt start with two uppercase letters.
        assert_rule_not_applied!(
            rule = "@iban";
            input = "some iban: N19386011117945!";
        );
    }

    #[test]
    fn test_valid_iban_codes() {
        // list taken from: `https://github.com/jschaedl/iban-validation/blob/master/tests/ValidatorTest.php`
        // Todo: verify that it's okay to copy it.
        let valid_iban_codes = vec![
            "AD1200012030200359100100",
            "AE070331234567890123456",
            "AL47212110090000000235698741",
            "AT611904300234573201",
            "AZ21NABZ00000000137010001944",
            "BA391290079401028494",
            "BE68539007547034",
            "BG80BNBG96611020345678",
            "BH67BMAG00001299123456",
            "BR1800360305000010009795493C1",
            "BY13NBRB3600900000002Z00AB00",
            "CH9300762011623852957",
            "CR05015202001026284066",
            "CY17002001280000001200527600",
            "CZ6508000000192000145399",
            "DE89370400440532013000",
            "DK5000400440116243",
            "DO28BAGR00000001212453611324",
            "EE382200221020145685",
            "EG380019000500000000263180002",
            "ES9121000418450200051332",
            "FI2112345600000785",
            "FO6264600001631634",
            "FR1420041010050500013M02606",
            "GB29NWBK60161331926819",
            "GE29NB0000000101904917",
            "GI75NWBK000000007099453",
            "GL8964710001000206",
            "GR1601101250000000012300695",
            "GT82TRAJ01020000001210029690",
            "HR1210010051863000160",
            "HU42117730161111101800000000",
            "IE29AIBK93115212345678",
            "IL620108000000099999999",
            "IQ98NBIQ850123456789012",
            "IS140159260076545510730339",
            "IT60X0542811101000000123456",
            "JO94CBJO0010000000000131000302",
            "KW81CBKU0000000000001234560101",
            "KZ86125KZT5004100100",
            "LB62099900000001001901229114",
            "LC55HEMM000100010012001200023015",
            "LI21088100002324013AA",
            "LT121000011101001000",
            "LU280019400644750000",
            "LV80BANK0000435195001",
            "LY83002048000020100120361",
            "MC5811222000010123456789030",
            "MD24AG000225100013104168",
            "ME25505000012345678951",
            "MK07250120000058984",
            "MR1300020001010000123456753",
            "MT84MALT011000012345MTLCAST001S",
            "MU17BOMM0101101030300200000MUR",
            "NL91ABNA0417164300",
            "NO9386011117947",
            "PK36SCBL0000001123456702",
            "PL61109010140000071219812874",
            "PS92PALS000000000400123456702",
            "PT50000201231234567890154",
            "QA58DOHB00001234567890ABCDEFG",
            "RO49AAAA1B31007593840000",
            "RS35260005601001611379",
            "SA0380000000608010167519",
            "SC18SSCB11010000000000001497USD",
            "SE4550000000058398257466",
            "SI56263300012039086",
            "SK3112000000198742637541",
            "SM86U0322509800000000270100",
            "SV62CENR00000000000000700025",
            "TL380080012345678910157",
            "TN5910006035183598478831",
            "TR330006100519786457841326",
            "UA213223130000026007233566001",
            "VA59001123000012345678",
            "VG96VPVG0000012345678901",
            "XK051212012345678906",
        ];

        for iban_code in valid_iban_codes {
            assert_text_rule!(
                rule = "@iban";
                input = &format!("some iban: {}!", iban_code);
                output = "some iban: [iban]!";
                remarks = vec![Remark::with_range(RemarkType::Substituted, "@iban", (11, 17))];
            );
        }
    }

    #[test]
    fn test_creditcard() {
        assert_text_rule!(
            rule = "@creditcard";
            input = "John Appleseed 4571234567890111!";
            output = "John Appleseed [creditcard]!";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@creditcard", (15, 27)),
            ];
        );
        assert_text_rule!(
            rule = "@creditcard";
            input = "John Appleseed 4571 2345 6789 0111!";
            output = "John Appleseed [creditcard]!";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@creditcard", (15, 27)),
            ];
        );
        assert_text_rule!(
            rule = "@creditcard:mask";
            input = "John Appleseed 4571234567890111!";
            output = "John Appleseed ****************!";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "@creditcard:mask", (15, 31)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@creditcard:mask";
            input = "John Appleseed 4571234567890111!";
            output = "John Appleseed ****************!";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "0", (15, 31)),
            ];
        );
        assert_text_rule!(
            rule = "@creditcard:replace";
            input = "John Appleseed 4571234567890111!";
            output = "John Appleseed [creditcard]!";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@creditcard:replace", (15, 27)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@creditcard:replace";
            input = "John Appleseed 4571234567890111!";
            output = "John Appleseed [Filtered]!";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "0", (15, 25)),
            ];
        );
        assert_text_rule!(
            rule = "@creditcard:hash";
            input = "John Appleseed 4571234567890111!";
            output = "John Appleseed 68C796A9ED3FB51BF850A11140FCADD8E2D88466!";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "@creditcard:hash", (15, 55)),
            ];
        );
        assert_custom_rulespec!(
            rule = "@creditcard:hash";
            input = "John Appleseed 4571234567890111!";
            output = "John Appleseed 68C796A9ED3FB51BF850A11140FCADD8E2D88466!";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "0", (15, 55)),
            ];
        );
    }

    #[test]
    fn test_pemkey() {
        assert_text_rule!(
            rule = "@pemkey";
            input = "This is a comment I left on my key

            -----BEGIN EC PRIVATE KEY-----
MIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL
fojr3jkJIxFS07AoLQpsawOFZipxSQHieaAHBgUrgQQAI6GBiQOBhgAEAdaBHsBo
KBuPVQL2F6z57Xb034TRlpaarne/XGWaCsohtl3YYcFll3A7rV+wHudcKFSvrgjp
soH5cUhpnhOL9ujuAM7Ldk1G+11Mf7EZ5sjrLe81fSB8S16D2vjtxkf/+mmwwhlM
HdmUCGvfKiF2CodxyLon1XkK8pX+Ap86MbJhluqK
-----END EC PRIVATE KEY-----";
            output = "This is a comment I left on my key

            -----BEGIN EC PRIVATE KEY-----
[pemkey]
-----END EC PRIVATE KEY-----";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@pemkey", (79, 87)),
            ];
        );
        assert_text_rule!(
            rule = "@pemkey:hash";
            input = "This is a comment I left on my key

            -----BEGIN EC PRIVATE KEY-----
MIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL
fojr3jkJIxFS07AoLQpsawOFZipxSQHieaAHBgUrgQQAI6GBiQOBhgAEAdaBHsBo
KBuPVQL2F6z57Xb034TRlpaarne/XGWaCsohtl3YYcFll3A7rV+wHudcKFSvrgjp
soH5cUhpnhOL9ujuAM7Ldk1G+11Mf7EZ5sjrLe81fSB8S16D2vjtxkf/+mmwwhlM
HdmUCGvfKiF2CodxyLon1XkK8pX+Ap86MbJhluqK
-----END EC PRIVATE KEY-----";
            output = "This is a comment I left on my key

            -----BEGIN EC PRIVATE KEY-----
291134FEC77C62B11E2DC6D89910BB43157294BC
-----END EC PRIVATE KEY-----";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "@pemkey:hash", (79, 119)),
            ];
        );
    }

    #[test]
    fn test_urlauth() {
        assert_text_rule!(
            rule = "@urlauth";
            input = "https://username:password@example.com/";
            output = "https://[auth]@example.com/";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@urlauth", (8, 14)),
            ];
        );
        assert_text_rule!(
            rule = "@urlauth:replace";
            input = "https://username:password@example.com/";
            output = "https://[auth]@example.com/";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@urlauth:replace", (8, 14)),
            ];
        );
        assert_text_rule!(
            rule = "@urlauth:hash";
            input = "https://username:password@example.com/";
            output = "https://2F82AB8A5E3FAD655B5F81E3BEA30D2A14FEF2AC@example.com/";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "@urlauth:hash", (8, 48)),
            ];
        );
    }

    #[test]
    fn test_usssn() {
        assert_text_rule!(
            rule = "@usssn";
            input = "Hi I'm Hilda and my SSN is 078-05-1120";
            output = "Hi I'm Hilda and my SSN is ***********";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "@usssn", (27, 38)),
            ];
        );
        assert_text_rule!(
            rule = "@usssn:mask";
            input = "Hi I'm Hilda and my SSN is 078-05-1120";
            output = "Hi I'm Hilda and my SSN is ***********";
            remarks = vec![
                Remark::with_range(RemarkType::Masked, "@usssn:mask", (27, 38)),
            ];
        );
        assert_text_rule!(
            rule = "@usssn:replace";
            input = "Hi I'm Hilda and my SSN is 078-05-1120";
            output = "Hi I'm Hilda and my SSN is [us-ssn]";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@usssn:replace", (27, 35)),
            ];
        );
        assert_text_rule!(
            rule = "@usssn:hash";
            input = "Hi I'm Hilda and my SSN is 078-05-1120";
            output = "Hi I'm Hilda and my SSN is 01328BB9C55B35F354FBC7B60CADAC789DC2035C";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "@usssn:hash", (27, 67)),
            ];
        );
    }

    #[test]
    fn test_userpath() {
        assert_text_rule!(
            rule = "@userpath";
            input = "C:\\Users\\mitsuhiko\\Desktop";
            output = "C:\\Users\\[user]\\Desktop";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@userpath", (9, 15)),
            ];
        );
        assert_text_rule!(
            rule = "@userpath:replace";
            input = "File in /Users/mitsuhiko/Development/sentry-stripping";
            output = "File in /Users/[user]/Development/sentry-stripping";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@userpath:replace", (15, 21)),
            ];
        );
        assert_text_rule!(
            rule = "@userpath:replace";
            input = "C:\\Windows\\Profiles\\Armin\\Temp";
            output = "C:\\Windows\\Profiles\\[user]\\Temp";
            remarks = vec![
                Remark::with_range(RemarkType::Substituted, "@userpath:replace", (20, 26)),
            ];
        );
        assert_text_rule!(
            rule = "@userpath:hash";
            input = "File in /Users/mitsuhiko/Development/sentry-stripping";
            output = "File in /Users/A8791A1A8D11583E0200CC1B9AB971B4D78B8A69/Development/sentry-stripping";
            remarks = vec![
                Remark::with_range(RemarkType::Pseudonymized, "@userpath:hash", (15, 55)),
            ];
        );
    }

    #[test]
    fn test_builtin_rules_completeness() {
        // Test that all combinations of ruletype and redactionmethod work, because that's what the
        // UI assumes.
        //
        // Note: Keep these lists in sync with what is defined and exposed in the UI, not what we
        // define internally.
        for rule_type in &[
            "creditcard",
            "password",
            "ip",
            "imei",
            "email",
            "uuid",
            "pemkey",
            "urlauth",
            "usssn",
            "userpath",
            "mac",
            "anything",
        ] {
            for redaction_method in &["mask", "remove", "hash", "replace"] {
                let key = format!("@{rule_type}:{redaction_method}");
                println!("looking up {key}");
                assert!(BUILTIN_RULES_MAP.contains_key(key.as_str()));
            }
        }
    }
}
