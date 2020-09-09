#![allow(clippy::needless_update)]
use std::collections::BTreeMap;

use lazy_static::lazy_static;

use crate::pii::{
    AliasRule, MultipleRule, PatternRule, Redaction, ReplaceRedaction, RuleSpec, RuleType,
};

macro_rules! declare_builtin_rules {
    ($($rule_id:expr => $spec:expr;)*) => {
        lazy_static! {
            pub(crate) static ref BUILTIN_RULES_MAP: BTreeMap<&'static str, &'static RuleSpec> = {
                let mut map = BTreeMap::new();
                $(
                    map.insert($rule_id, Box::leak(Box::new($spec)) as &'static _);
                )*
                map
            };
        }

        /// Names of all builtin rules
        pub static BUILTIN_RULES: &[&'static str] = &[
            $($rule_id,)*
        ];
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

    use crate::pii::config::{PiiConfig, RuleSpec, RuleType};
    use crate::pii::processor::PiiProcessor;
    use crate::pii::{Redaction, ReplaceRedaction};
    use crate::processor::{process_value, ProcessingState, ValueType};
    use crate::types::{Annotated, Remark, RemarkType};

    use super::{BUILTIN_RULES, BUILTIN_RULES_MAP};

    #[derive(Clone, Debug, PartialEq, Empty, FromValue, ProcessValue, ToValue)]
    struct FreeformRoot {
        #[metastructure(pii = "true")]
        value: Annotated<String>,
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
            assert_eq_str!(root.value.value().unwrap(), $output);
            let remarks: Vec<Remark> = $remarks;
            assert_eq_dbg!(
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
            assert_eq_str!(root.value.value().unwrap(), $output);
            let remarks: Vec<Remark> = $remarks;
            assert_eq_dbg!(
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
                let key = format!("@{}:{}", rule_type, redaction_method);
                println!("looking up {}", key);
                assert!(BUILTIN_RULES.contains(&key.as_str()));
                assert!(BUILTIN_RULES_MAP.contains_key(key.as_str()));
            }
        }
    }
}
