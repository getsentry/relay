use relay_base_schema::events::EventType;
use relay_event_normalization::utils::extract_http_status_code;
use relay_event_schema::protocol::{Event, TransactionSource};
use relay_protocol::{Annotated, RemarkType};

use crate::{envelope::ClientName, statsd::RelayCounters};

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

/// Maps the event's platform to a low-cardinality statsd tag.
pub fn platform_tag(event: &Event) -> &'static str {
    let platform = event.platform.as_str();

    match platform {
        Some("cocoa") => "cocoa",
        Some("csharp") => "csharp",
        Some("edge") => "edge",
        Some("go") => "go",
        Some("java") => "java",
        Some("javascript") => "javascript",
        Some("julia") => "julia",
        Some("native") => "native",
        Some("node") => "node",
        Some("objc") => "objc",
        Some("perl") => "perl",
        Some("php") => "php",
        Some("python") => "python",
        Some("ruby") => "ruby",
        Some("swift") => "swift",
        Some(_) => "other",
        None => "missing",
    }
}

/// Maps a client name to a low-cardinality statsd tag.
pub fn client_name_tag(client_name: ClientName<'_>) -> &str {
    match client_name {
        ClientName::Other(_) => "other",
        well_known => well_known.as_str(),
    }
}

/// Maps a client name to a low-cardinality AI origin tag for cost calculation metrics.
///
/// Returns "other" for unknown/unrecognized SDKs, or the well-known SDK name.
/// "manual" should be used for manually instrumented spans (not SDK-generated).
pub fn ai_origin_tag(client_name: ClientName<'_>) -> &'static str {
    match client_name {
        ClientName::Relay => "sentry.relay",
        ClientName::Ruby => "sentry-ruby",
        ClientName::CocoaFlutter => "sentry.cocoa.flutter",
        ClientName::CocoaReactNative => "sentry.cocoa.react-native",
        ClientName::Cocoa => "sentry.cocoa",
        ClientName::Dotnet => "sentry.dotnet",
        ClientName::AndroidReactNative => "sentry.java.android.react-native",
        ClientName::AndroidJava => "sentry.java.android",
        ClientName::SpringBoot => "sentry.java.spring-boot.jakarta",
        ClientName::JavascriptBrowser => "sentry.javascript.browser",
        ClientName::Electron => "sentry.javascript.electron",
        ClientName::NestJs => "sentry.javascript.nestjs",
        ClientName::NextJs => "sentry.javascript.nextjs",
        ClientName::Node => "sentry.javascript.node",
        ClientName::React => "sentry.javascript.react",
        ClientName::Vue => "sentry.javascript.vue",
        ClientName::Native => "sentry.native",
        ClientName::Laravel => "sentry.php.laravel",
        ClientName::Symfony => "sentry.php.symfony",
        ClientName::Php => "sentry.php",
        ClientName::Python => "sentry.python",
        ClientName::Other(_) => "other",
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
