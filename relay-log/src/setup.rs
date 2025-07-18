use std::borrow::Cow;
use std::collections::BTreeMap;
use std::env;
use std::fmt::{self, Display};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use relay_common::impl_str_serde;
use sentry::integrations::tracing::EventFilter;
use sentry::types::Dsn;
use sentry::{TracesSampler, TransactionContext};
use serde::{Deserialize, Serialize};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{EnvFilter, Layer, prelude::*};

/// The full release name including the Relay version and SHA.
const RELEASE: &str = std::env!("RELAY_RELEASE");

// Import CRATE_NAMES, which lists all crates in the workspace.
include!(concat!(env!("OUT_DIR"), "/constants.gen.rs"));

/// Controls the log format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Auto detect the best format.
    ///
    /// This chooses [`LogFormat::Pretty`] for TTY, otherwise [`LogFormat::Simplified`].
    Auto,

    /// Pretty printing with colors.
    ///
    /// ```text
    ///  INFO  relay::setup > relay mode: managed
    /// ```
    Pretty,

    /// Simplified plain text output.
    ///
    /// ```text
    /// 2020-12-04T12:10:32Z [relay::setup] INFO: relay mode: managed
    /// ```
    Simplified,

    /// Dump out JSON lines.
    ///
    /// ```text
    /// {"timestamp":"2020-12-04T12:11:08.729716Z","level":"INFO","logger":"relay::setup","message":"  relay mode: managed","module_path":"relay::setup","filename":"relay/src/setup.rs","lineno":31}
    /// ```
    Json,
}

/// The logging format parse error.
#[derive(Clone, Debug)]
pub struct FormatParseError(String);

impl Display for FormatParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"error parsing "{}" as format: expected one of "auto", "pretty", "simplified", "json""#,
            self.0
        )
    }
}

impl FromStr for LogFormat {
    type Err = FormatParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "" => LogFormat::Auto,
            s if s.eq_ignore_ascii_case("auto") => LogFormat::Auto,
            s if s.eq_ignore_ascii_case("pretty") => LogFormat::Pretty,
            s if s.eq_ignore_ascii_case("simplified") => LogFormat::Simplified,
            s if s.eq_ignore_ascii_case("json") => LogFormat::Json,
            s => return Err(FormatParseError(s.into())),
        };

        Ok(result)
    }
}

impl std::error::Error for FormatParseError {}

/// The logging level parse error.
#[derive(Clone, Debug)]
pub struct LevelParseError(String);

impl Display for LevelParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"error parsing "{}" as level: expected one of "error", "warn", "info", "debug", "trace", "off""#,
            self.0
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Level {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
    Off,
}

impl_str_serde!(Level, "The logging level.");

impl Level {
    /// Returns the tracing [`LevelFilter`].
    pub const fn level_filter(&self) -> LevelFilter {
        match self {
            Level::Error => LevelFilter::ERROR,
            Level::Warn => LevelFilter::WARN,
            Level::Info => LevelFilter::INFO,
            Level::Debug => LevelFilter::DEBUG,
            Level::Trace => LevelFilter::TRACE,
            Level::Off => LevelFilter::OFF,
        }
    }
}

impl Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{self:?}").to_lowercase())
    }
}

impl FromStr for Level {
    type Err = LevelParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "" => Level::Error,
            s if s.eq_ignore_ascii_case("error") => Level::Error,
            s if s.eq_ignore_ascii_case("warn") => Level::Warn,
            s if s.eq_ignore_ascii_case("info") => Level::Info,
            s if s.eq_ignore_ascii_case("debug") => Level::Debug,
            s if s.eq_ignore_ascii_case("trace") => Level::Trace,
            s if s.eq_ignore_ascii_case("off") => Level::Off,
            s => return Err(LevelParseError(s.into())),
        };

        Ok(result)
    }
}

impl std::error::Error for LevelParseError {}

/// Controls the logging system.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct LogConfig {
    /// The log level for Relay.
    pub level: Level,

    /// Controls the log output format.
    ///
    /// Defaults to [`LogFormat::Auto`], which detects the best format based on the TTY.
    pub format: LogFormat,

    /// When set to `true`, backtraces are forced on.
    ///
    /// Otherwise, backtraces can be enabled by setting the `RUST_BACKTRACE` variable to `full`.
    pub enable_backtraces: bool,

    /// Sets the trace sample rate for performance monitoring.
    ///
    /// Defaults to `0.0` for release builds and `1.0` for local development builds.
    pub traces_sample_rate: f32,
}

impl LogConfig {
    /// Returns the tracing [`LevelFilter`].
    pub const fn level_filter(&self) -> LevelFilter {
        self.level.level_filter()
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: Level::Info,
            format: LogFormat::Auto,
            enable_backtraces: false,
            #[cfg(debug_assertions)]
            traces_sample_rate: 1.0,
            #[cfg(not(debug_assertions))]
            traces_sample_rate: 0.0,
        }
    }
}

/// Controls internal reporting to Sentry.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct SentryConfig {
    /// The [`DSN`](sentry::types::Dsn) specifying the Project to report to.
    pub dsn: Option<Dsn>,

    /// Enables reporting to Sentry.
    pub enabled: bool,

    /// Sets the environment for this service.
    pub environment: Option<Cow<'static, str>>,

    /// Sets the server name for this service.
    ///
    /// This is overridden by the `RELAY_SERVER_NAME`
    /// environment variable.
    pub server_name: Option<Cow<'static, str>>,

    /// Add defaults tags to the events emitted by Relay
    pub default_tags: Option<BTreeMap<String, String>>,

    /// Internal. Enables crash handling and sets the absolute path to where minidumps should be
    /// cached on disk. The path is created if it doesn't exist. Path must be UTF-8.
    pub _crash_db: Option<PathBuf>,
}

impl SentryConfig {
    /// Returns a reference to the [`DSN`](sentry::types::Dsn) if Sentry is enabled.
    pub fn enabled_dsn(&self) -> Option<&Dsn> {
        self.dsn.as_ref().filter(|_| self.enabled)
    }
}

impl Default for SentryConfig {
    fn default() -> Self {
        Self {
            dsn: "https://0cc4a37e5aab4da58366266a87a95740@sentry.io/1269704"
                .parse()
                .ok(),
            enabled: false,
            environment: None,
            server_name: None,
            default_tags: None,
            _crash_db: None,
        }
    }
}

/// Captures an envelope from the native crash reporter using the main Sentry SDK.
#[cfg(feature = "crash-handler")]
fn capture_native_envelope(data: &[u8]) {
    if let Some(client) = sentry::Hub::main().client() {
        match sentry::Envelope::from_bytes_raw(data.to_owned()) {
            Ok(envelope) => client.send_envelope(envelope),
            Err(error) => {
                let error = &error as &dyn std::error::Error;
                crate::error!(error, "failed to capture crash")
            }
        }
    } else {
        crate::error!("failed to capture crash: no sentry client registered");
    }
}

/// Configures the given log level for all of Relay's crates.
fn get_default_filters() -> EnvFilter {
    // Configure INFO as default, except for crates that are very spammy on INFO level.
    let mut env_filter = EnvFilter::new(
        "INFO,\
        sqlx=WARN,\
        tower_http=TRACE,\
        trust_dns_proto=WARN,\
        minidump=ERROR,\
        ",
    );

    // Add all internal modules with maximum log-level.
    for name in CRATE_NAMES {
        env_filter = env_filter.add_directive(format!("{name}=TRACE").parse().unwrap());
    }

    env_filter
}

/// Initialize the logging system and reporting to Sentry.
///
/// # Safety
///
/// The function is not safe to be called from a multi-threaded program,
/// due to modifications of environment variables.
///
/// # Example
///
/// ```
/// let log_config = relay_log::LogConfig {
///     enable_backtraces: true,
///     ..Default::default()
/// };
///
/// let sentry_config = relay_log::SentryConfig::default();
///
/// unsafe { relay_log::init(&log_config, &sentry_config) };
/// ```
pub unsafe fn init(config: &LogConfig, sentry: &SentryConfig) {
    if config.enable_backtraces {
        unsafe {
            env::set_var("RUST_BACKTRACE", "full");
        }
    }

    let subscriber = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true);

    let format = match (config.format, console::user_attended()) {
        (LogFormat::Auto, true) | (LogFormat::Pretty, _) => {
            subscriber.compact().without_time().boxed()
        }
        (LogFormat::Auto, false) | (LogFormat::Simplified, _) => {
            subscriber.with_ansi(false).boxed()
        }
        (LogFormat::Json, _) => subscriber
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(true)
            .with_file(true)
            .with_line_number(true)
            .boxed(),
    };

    tracing_subscriber::registry()
        .with(format.with_filter(config.level_filter()))
        .with(
            // Same as the default filter, except it converts warnings into events
            // and also sends everything at or above INFO as logs instead of breadcrumbs.
            sentry::integrations::tracing::layer().event_filter(|md| match *md.level() {
                tracing::Level::ERROR | tracing::Level::WARN => {
                    EventFilter::Event | EventFilter::Log
                }
                tracing::Level::INFO => EventFilter::Log,
                tracing::Level::DEBUG | tracing::Level::TRACE => EventFilter::Ignore,
            }),
        )
        .with(match env::var(EnvFilter::DEFAULT_ENV) {
            Ok(value) => EnvFilter::new(value),
            Err(_) => get_default_filters(),
        })
        .init();

    if let Some(dsn) = sentry.enabled_dsn() {
        let traces_sample_rate = config.traces_sample_rate;
        // We're explicitly setting a `traces_sampler` here to circumvent trace
        // propagation. A trace sampler that always just returns the constant
        // `traces_sample_rate` is equivalent to using the `traces_sample_rate`
        // directly, except it doesn't take into account whether the context
        // was previously sampled. We don't want to take that into account because
        // SDKs send headers with their envelopes that erroneously cause us to
        // sample transactions.
        let traces_sampler =
            Some(Arc::new(move |_: &TransactionContext| traces_sample_rate) as Arc<TracesSampler>);
        let mut options = sentry::ClientOptions {
            dsn: Some(dsn).cloned(),
            in_app_include: vec!["relay"],
            release: Some(RELEASE.into()),
            attach_stacktrace: config.enable_backtraces,
            environment: sentry.environment.clone(),
            server_name: sentry.server_name.clone(),
            traces_sampler,
            enable_logs: true,
            ..Default::default()
        };

        // If `default_tags` is set in Sentry configuration install the `before_send` hook
        // in order to inject said tags into each event
        if let Some(default_tags) = sentry.default_tags.clone() {
            // Install hook
            options.before_send = Some(Arc::new(move |mut event| {
                // Extend `event.tags` with `default_tags` without replacing tags already present
                let previous_event_tags = std::mem::replace(&mut event.tags, default_tags.clone());
                event.tags.extend(previous_event_tags);
                Some(event)
            }));
        }

        crate::info!(
            release = RELEASE,
            server_name = sentry.server_name.as_deref(),
            environment = sentry.environment.as_deref(),
            traces_sample_rate,
            "Initialized Sentry client options"
        );

        let guard = sentry::init(options);

        // Keep the client initialized. The client is flushed manually in `main`.
        std::mem::forget(guard);
    }

    // Initialize native crash reporting after the Rust SDK, so that `capture_native_envelope` has
    // access to an initialized Hub to capture crashes from the previous run.
    #[cfg(feature = "crash-handler")]
    {
        if let Some(dsn) = sentry.enabled_dsn().map(|d| d.to_string()) {
            if let Some(db) = sentry._crash_db.as_deref() {
                crate::info!("initializing crash handler in {}", db.display());
                relay_crash::CrashHandler::new(dsn.as_str(), db)
                    .transport(capture_native_envelope)
                    .release(Some(RELEASE))
                    .environment(sentry.environment.as_deref())
                    .install();
            }
        }
    }
}
