use crate::pii::rules::{Rule, RuleType};
use crate::processor::ProcessValue;
use crate::types::{Meta, Remark, RemarkType, ValueAction};

pub fn apply_rule_to_databag_value<T: ProcessValue>(
    rule: &Rule,
    key: Option<&str>,
    value: &mut T,
    meta: &mut Meta,
) -> ValueAction {
    apply_rule_to_databag_value_impl(rule, key, value, meta, None)
}

fn apply_rule_to_databag_value_impl<T: ProcessValue>(
    rule: &Rule,
    key: Option<&str>,
    value: &mut T,
    meta: &mut Meta,
    report_rule: Option<&Rule>,
) -> ValueAction {
    match rule.spec.ty {
        // this makes a value just disappear
        RuleType::Anything => ValueAction::DeleteHard,
        // these are not handled by the databag code but will be independently picked
        // up by the string matching code later.
        RuleType::Pattern(..)
        | RuleType::Imei
        | RuleType::Mac
        | RuleType::Email
        | RuleType::Ip
        | RuleType::Creditcard
        | RuleType::Userpath => ValueAction::Keep,
        //
        RuleType::Alias(ref alias) => {
            if let Some((rule, report_rule, _)) =
                rule.lookup_referenced_rule(&alias.rule, alias.hide_rule)
            {
                apply_rule_to_databag_value_impl(&rule, key, value, meta, report_rule)
            } else {
                ValueAction::Keep
            }
        }
        RuleType::Multiple(ref multiple) => {
            for rule_id in multiple.rules.iter() {
                if let Some((rule, report_rule, _)) =
                    rule.lookup_referenced_rule(rule_id, multiple.hide_rule)
                {
                    match apply_rule_to_databag_value_impl(&rule, key, value, meta, report_rule) {
                        ValueAction::Keep => continue,
                        other => return other,
                    };
                }
            }
            ValueAction::Keep
        }
        // does not apply here
        RuleType::RedactPair(ref redact_pair) => {
            let key = key.unwrap_or("");
            if redact_pair.key_pattern.0.is_match(key) {
                meta.add_remark(Remark::new(
                    RemarkType::Removed,
                    report_rule.unwrap_or(rule).id.to_string(),
                ));
                ValueAction::DeleteHard
            } else {
                ValueAction::Keep
            }
        }
    }
}
