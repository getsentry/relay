use relay_common::EventType;
use relay_general::protocol::Event;
use relay_general::types::{Annotated, RemarkType};

use crate::statsd::RelayCounters;

/// Log statsd metrics about transaction name modifications.
///
/// We have to look at event & meta before and after the modification is made,
/// so we delegate to `f` in the middle of the function.
pub fn log_transaction_name_metrics<F, R>(event: &mut Annotated<Event>, mut f: F) -> R
where
    F: FnMut(&mut Annotated<Event>) -> R,
{
    let Some(inner) = event.value() else {
            return f(event);
        };

    if inner.ty.value() != Some(&EventType::Transaction) {
        return f(event);
    }

    let old_source = inner.get_transaction_source().to_string();
    let old_remarks = inner.transaction.meta().iter_remarks().count();

    let res = f(event);

    // Need to reborrow event so the reference's lifetime does not overlap with `f`:
    let Some(inner) = event.value() else {
        return res;
    };

    let mut pattern_based_changes = false;
    let mut rule_based_changes = false;
    let remarks = inner.transaction.meta().iter_remarks().skip(old_remarks);
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

    let new_source = inner.get_transaction_source();

    relay_statsd::metric!(
        counter(RelayCounters::TransactionNameChanges) += 1,
        source_in = old_source.as_str(),
        changes = changes,
        source_out = new_source.as_str(),
    );

    res
}
