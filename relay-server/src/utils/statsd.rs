use relay_base_schema::events::EventType;
use relay_event_normalization::utils::extract_http_status_code;
use relay_event_schema::protocol::{Event, TransactionSource};
use relay_protocol::{Annotated, RemarkType};

use crate::statsd::RelayCounters;

/// Maps the event's transaction source to a low-cardinality statsd tag.
pub fn transaction_source_tag(event: &Event) -> &str {
    let source = event
        .transaction_info
        .value()
        .and_then(|i| i.source.value());
    match source {
        None => "none",
        Some(TransactionSource::Other(_)) => "other",
        Some(source) => source.as_str(),
    }
}

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

    let old_source = transaction_source_tag(inner).to_owned();
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

    let new_source = transaction_source_tag(inner);
    let is_404 = extract_http_status_code(inner).is_some_and(|s| s == "404");

    relay_statsd::metric!(
        counter(RelayCounters::TransactionNameChanges) += 1,
        source_in = old_source.as_str(),
        changes = changes,
        source_out = new_source,
        is_404 = if is_404 { "true" } else { "false" },
    );

    res
}

/// Low-cardinality platform that can be used as a statsd tag.
pub enum PlatformTag {
    Cocoa,
    Csharp,
    Edge,
    Go,
    Java,
    Javascript,
    Julia,
    Native,
    Node,
    Objc,
    Other,
    Perl,
    Php,
    Python,
    Ruby,
    Swift,
}

impl PlatformTag {
    pub fn name(&self) -> &str {
        match self {
            Self::Cocoa => "cocoa",
            Self::Csharp => "csharp",
            Self::Edge => "edge",
            Self::Go => "go",
            Self::Java => "java",
            Self::Javascript => "javascript",
            Self::Julia => "julia",
            Self::Native => "native",
            Self::Node => "node",
            Self::Objc => "objc",
            Self::Other => "other",
            Self::Perl => "perl",
            Self::Php => "php",
            Self::Python => "python",
            Self::Ruby => "ruby",
            Self::Swift => "swift",
        }
    }
}

impl From<&str> for PlatformTag {
    fn from(value: &str) -> Self {
        match value {
            "cocoa" => Self::Cocoa,
            "csharp" => Self::Csharp,
            "edge" => Self::Edge,
            "go" => Self::Go,
            "java" => Self::Java,
            "javascript" => Self::Javascript,
            "julia" => Self::Julia,
            "native" => Self::Native,
            "node" => Self::Node,
            "objc" => Self::Objc,
            "perl" => Self::Perl,
            "php" => Self::Php,
            "python" => Self::Python,
            "ruby" => Self::Ruby,
            "swift" => Self::Swift,
            _ => Self::Other,
        }
    }
}

impl From<Option<&str>> for PlatformTag {
    fn from(value: Option<&str>) -> Self {
        value.map(Self::from).unwrap_or(PlatformTag::Other)
    }
}
