use std::hash::{Hash, Hasher};

use relay_common::EventType;
use relay_general::protocol::Event;
use relay_general::types::{Annotated, ProcessingAction, RemarkType};

use crate::statsd::{RelayCounters, RelaySets};

/// Log statsd metrics about transaction name modifications.
///
/// We have to look at event & meta before and after the modification is made,
/// so we delegate to `f` in the middle of the function.
pub fn log_transaction_name_metrics<F, R>(
    event: &Annotated<Event>,
    f: F,
) -> Result<R, ProcessingAction>
where
    F: Fn() -> Result<R, ProcessingAction>,
{
    let Some(event) = event.value_mut() else {
        return f();
    };

    if event.ty.value() != Some(&EventType::Transaction) {
        return f();
    }

    let old_source = event.get_transaction_source();
    let old_name = event.transaction.as_str().unwrap_or_default();
    let old_remarks = event.transaction.meta().iter_remarks().count();

    let res = f();

    let mut pattern_based_changes = false;
    let mut rule_based_changes = false;
    let remarks = event.transaction.meta().iter_remarks().skip(old_remarks);
    for remark in remarks {
        if remark.ty() == RemarkType::Substituted {
            if remark.range().is_some() {
                pattern_based_changes = true;
            } else {
                rule_based_changes = true;
            }
        }
    }

    let changes = match (pattern_based_changes, rule_based_changes) {
        (true, true) => "both",
        (true, false) => "pattern",
        (false, true) => "rule",
        (false, false) => "none",
    };

    let new_source = event.get_transaction_source();

    relay_statsd::metric!(
        counter(RelayCounters::TransactionNameChanges) += 1,
        source_in = old_source.as_str(),
        changes = changes,
        source_out = new_source.as_str(),
    );

    relay_statsd::metric!(
        set(RelaySets::TransactionNameChanges) = {
            let mut hasher = fnv::FnvHasher::default();
            std::hash::Hash::hash(old_name, &mut hasher);
            hasher.finish() as i64 // 2-complement
        },
        source_in = old_source.as_str(),
        changes = changes,
        source_out = new_source.as_str(),
    );

    res
}
