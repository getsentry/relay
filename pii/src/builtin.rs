#![allow(clippy::needless_update)]

use std::collections::BTreeMap;

use lazy_static::lazy_static;

use crate::{
    AliasRule, HashAlgorithm, HashRedaction, MaskRedaction, MultipleRule, PatternRule,
    RedactPairRule, Redaction, ReplaceRedaction, RuleSpec, RuleType,
};

/// Names of builtin selectors.
pub static BUILTIN_SELECTORS: &[&str] = &["text", "container"];

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
            ..Default::default()
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
        ..Default::default()
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
        ..Default::default()
    };

    // anything
    "@anything" => rule_alias!("@anything:replace");
    "@anything:remove" => RuleSpec {
        ty: RuleType::Anything,
        redaction: Redaction::Remove,
        ..Default::default()
    };
    "@anything:replace" => RuleSpec {
        ty: RuleType::Anything,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[redacted]".into(),
        }),
        ..Default::default()
    };
    "@anything:hash" => RuleSpec {
        ty: RuleType::Anything,
        redaction: Redaction::Hash(HashRedaction {
            algorithm: HashAlgorithm::HmacSha1,
            key: None,
        }),
        ..Default::default()
    };

    // ip rules
    "@ip" => rule_alias!("@ip:replace");
    "@ip:replace" => RuleSpec {
        ty: RuleType::Ip,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[ip]".into(),
        }),
        ..Default::default()
    };
    "@ip:hash" => RuleSpec {
        ty: RuleType::Ip,
        redaction: Redaction::Hash(HashRedaction {
            algorithm: HashAlgorithm::HmacSha1,
            key: None,
        }),
        ..Default::default()
    };

    // imei rules
    "@imei" => rule_alias!("@imei:replace");
    "@imei:replace" => RuleSpec {
        ty: RuleType::Imei,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[imei]".into(),
        }),
        ..Default::default()
    };
    "@imei:hash" => RuleSpec {
        ty: RuleType::Imei,
        redaction: Redaction::Hash(HashRedaction {
            algorithm: HashAlgorithm::HmacSha1,
            key: None,
        }),
        ..Default::default()
    };

    // mac rules
    "@mac" => rule_alias!("@mac:mask");
    "@mac:replace" => RuleSpec {
        ty: RuleType::Mac,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[mac]".into(),
        }),
        ..Default::default()
    };
    "@mac:mask" => RuleSpec {
        ty: RuleType::Mac,
        redaction: Redaction::Mask(MaskRedaction {
            mask_char: '*',
            chars_to_ignore: "-:".into(),
            range: (Some(9), None),
        }),
        ..Default::default()
    };
    "@mac:hash" => RuleSpec {
        ty: RuleType::Mac,
        redaction: Redaction::Hash(HashRedaction {
            algorithm: HashAlgorithm::HmacSha1,
            key: None,
        }),
        ..Default::default()
    };

    // uuid rules
    "@uuid" => rule_alias!("@uuid:mask");
    "@uuid:replace" => RuleSpec {
        ty: RuleType::Uuid,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[uuid]".into(),
        }),
        ..Default::default()
    };
    "@uuid:mask" => RuleSpec {
        ty: RuleType::Uuid,
        redaction: Redaction::Mask(MaskRedaction {
            mask_char: '*',
            chars_to_ignore: "-".into(),
            range: (None, None),
        }),
        ..Default::default()
    };
    "@uuid:hash" => RuleSpec {
        ty: RuleType::Uuid,
        redaction: Redaction::Hash(HashRedaction {
            algorithm: HashAlgorithm::HmacSha1,
            key: None,
        }),
        ..Default::default()
    };

    // email rules
    "@email" => rule_alias!("@email:replace");
    "@email:mask" => RuleSpec {
        ty: RuleType::Email,
        redaction: Redaction::Mask(MaskRedaction {
            mask_char: '*',
            chars_to_ignore: ".@".into(),
            range: (None, None),
        }),
        ..Default::default()
    };
    "@email:replace" => RuleSpec {
        ty: RuleType::Email,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[email]".into(),
        }),
        ..Default::default()
    };
    "@email:hash" => RuleSpec {
        ty: RuleType::Email,
        redaction: Redaction::Hash(HashRedaction {
            algorithm: HashAlgorithm::HmacSha1,
            key: None,
        }),
        ..Default::default()
    };

    // creditcard rules
    "@creditcard" => rule_alias!("@creditcard:replace");
    "@creditcard:mask" => RuleSpec {
        ty: RuleType::Creditcard,
        redaction: Redaction::Mask(MaskRedaction {
            mask_char: '*',
            chars_to_ignore: " -".into(),
            range: (None, Some(-4)),
        }),
        ..Default::default()
    };
    "@creditcard:replace" => RuleSpec {
        ty: RuleType::Creditcard,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[creditcard]".into(),
        }),
        ..Default::default()
    };
    "@creditcard:filter" => RuleSpec {
        ty: RuleType::Creditcard,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[Filtered]".into(),
        }),
        ..Default::default()
    };
    "@creditcard:hash" => RuleSpec {
        ty: RuleType::Creditcard,
        redaction: Redaction::Hash(HashRedaction {
            algorithm: HashAlgorithm::HmacSha1,
            key: None,
        }),
        ..Default::default()
    };

    // pem rules
    "@pemkey" => rule_alias!("@pemkey:replace");
    "@pemkey:replace" => RuleSpec {
        ty: RuleType::Pemkey,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[pemkey]".into(),
        }),
        ..Default::default()
    };
    "@pemkey:filter" => RuleSpec {
        ty: RuleType::Pemkey,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[Filtered]".into(),
        }),
        ..Default::default()
    };
    "@pemkey:hash" => RuleSpec {
        ty: RuleType::Pemkey,
        redaction: Redaction::Hash(HashRedaction {
            algorithm: HashAlgorithm::HmacSha1,
            key: None,
        }),
        ..Default::default()
    };

    // url secrets
    "@urlauth" => rule_alias!("@urlauth:replace");
    "@urlauth:replace" => RuleSpec {
        ty: RuleType::UrlAuth,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[auth]".into(),
        }),
        ..Default::default()
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
        ..Default::default()
    };
    "@urlauth:hash" => RuleSpec {
        ty: RuleType::UrlAuth,
        redaction: Redaction::Hash(HashRedaction {
            algorithm: HashAlgorithm::HmacSha1,
            key: None,
        }),
        ..Default::default()
    };

    // US SSN
    "@usssn" => rule_alias!("@usssn:mask");
    "@usssn:replace" => RuleSpec {
        ty: RuleType::UsSsn,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[us-ssn]".into(),
        }),
        ..Default::default()
    };
    "@usssn:filter" => RuleSpec {
        ty: RuleType::UsSsn,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[Filtered]".into(),
        }),
        ..Default::default()
    };
    "@usssn:mask" => RuleSpec {
        ty: RuleType::UsSsn,
        redaction: Redaction::Mask(MaskRedaction {
            mask_char: '*',
            chars_to_ignore: "-".into(),
            range: (None, None),
        }),
        ..Default::default()
    };
    "@usssn:hash" => RuleSpec {
        ty: RuleType::UsSsn,
        redaction: Redaction::Hash(HashRedaction {
            algorithm: HashAlgorithm::HmacSha1,
            key: None,
        }),
        ..Default::default()
    };

    // user path rules
    "@userpath" => rule_alias!("@userpath:replace");
    "@userpath:replace" => RuleSpec {
        ty: RuleType::Userpath,
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[user]".into(),
        }),
        ..Default::default()
    };
    "@userpath:hash" => RuleSpec {
        ty: RuleType::Userpath,
        redaction: Redaction::Hash(HashRedaction {
            algorithm: HashAlgorithm::HmacSha1,
            key: None,
        }),
        ..Default::default()
    };

    // password field removal
    "@password" => rule_alias!("@password:remove");
    "@password:filter" => RuleSpec {
        ty: RuleType::RedactPair(RedactPairRule {
            key_pattern: r"(?i)(password|secret|passwd|api_key|apikey|access_token|auth|credentials|mysql_pwd|stripetoken)".into(),
        }),
        redaction: Redaction::Replace(ReplaceRedaction {
            text: "[Filtered]".into(),
        }),
        ..Default::default()
    };
    "@password:remove" => RuleSpec {
        ty: RuleType::RedactPair(RedactPairRule {
            key_pattern: r"(?i)(password|secret|passwd|api_key|apikey|access_token|auth|credentials|mysql_pwd|stripetoken)".into(),
        }),
        redaction: Redaction::Remove,
        ..Default::default()
    };
}
