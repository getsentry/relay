use relay_common::EventType;
use relay_general::protocol::{Event, TransactionSource};
use relay_general::types::{Annotated, RemarkType};

use crate::metrics_extraction::transactions::extract_http_status_code;
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
    let Some(event) = event.value() else {
        return res;
    };

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
    let is_404 = extract_http_status_code(event).map_or(false, |s| s == "404");

    // Temporarily log a sentry error for every 100th transaction that goes out as URL.
    // This block can be deleted once the investigation is done.
    if let Some(event_id) = event.id.value() {
        if new_source == &TransactionSource::Url && (event_id.0.as_u128() % 100) == 0 {
            relay_log::error!(
                tags.project_id = event.project.0.unwrap_or_default(),
                event_id = event_id.0.to_string(),
                "Transaction marked as URL"
            );
        }
    }

    relay_statsd::metric!(
        counter(RelayCounters::TransactionNameChanges) += 1,
        source_in = old_source.as_str(),
        changes = changes,
        source_out = new_source.as_str(),
        is_404 = if is_404 { "true" } else { "false" },
    );

    res
}
