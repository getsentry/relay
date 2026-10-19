const CRASH_REPORTER_PROCESS_ENV_VAR: &str = "_RELAY_CRASH_REPORTER_PROCESS";

/// Returns `true` if the current process is the crash reporting process.
pub fn is_crash_reporter_process() -> bool {
    cfg!(feature = "crash-handler") && std::env::var_os(CRASH_REPORTER_PROCESS_ENV_VAR).is_some()
}

/// Builds the minidump integration for reporting fatal crashes.
///
/// The integration spawns a secondary process from the same binary during
/// `sentry::init`. The two processes talk over IPC. If the primary process
/// crashes, the secondary process captures a minidump and reports it to
/// Sentry. The crashed process waits for the upload before it exits.
///
/// Returns `None` if no crash directory is configured.
#[cfg(feature = "crash-handler")]
pub fn integration(
    config: &crate::SentryConfig,
) -> Option<sentry::integrations::minidump::MinidumpIntegration> {
    use sentry::integrations::minidump::MinidumpIntegration;

    let db = config._crash_db.clone()?;

    if !is_crash_reporter_process() {
        crate::info!("Initializing crash handler in {}", db.display());
    }

    let integration = MinidumpIntegration::new()
        .crashes_dir(db)
        .server_env_var(CRASH_REPORTER_PROCESS_ENV_VAR)
        .process_name("relay-crash")
        .flush_timeout(std::time::Duration::from_secs(15))
        .before_capture(|_scope, path| {
            crate::info!("Minidump captured ({})", path.display());
        });

    // Make sure the implementations agree on the detection.
    debug_assert_eq!(
        is_crash_reporter_process(),
        integration.is_crash_reporter_process()
    );

    Some(integration)
}
