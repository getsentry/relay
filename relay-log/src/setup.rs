use std::borrow::Cow;
use std::env;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{DateTime, Utc};
use log::{Level, LevelFilter};
use sentry::integrations::log::SentryLogger;
use sentry::types::Dsn;
use serde::{Deserialize, Serialize};

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

/// Controls the logging system.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct LogConfig {
    /// The log level for Relay.
    pub level: log::LevelFilter,

    /// Controls the log output format.
    ///
    /// Defaults to [`LogFormat::Auto`], which detects the best format based on the TTY.
    pub format: LogFormat,

    /// When set to `true`, backtraces are forced on.
    ///
    /// Otherwise, backtraces can be enabled by setting the `RUST_BACKTRACE` variable to `full`.
    pub enable_backtraces: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: log::LevelFilter::Info,
            format: LogFormat::Auto,
            enable_backtraces: false,
        }
    }
}

/// Controls interal reporting to Sentry.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct SentryConfig {
    /// The [`DSN`](sentry::types::Dsn) specifying the Project to report to.
    pub dsn: Option<Dsn>,

    /// Enables reporting to Sentry.
    pub enabled: bool,

    /// Sets the environment for this service.
    pub environment: Option<Cow<'static, str>>,

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
            _crash_db: None,
        }
    }
}

/// Captures an envelope from the native crash reporter using the main Sentry SDK.
#[cfg(feature = "relay-crash")]
fn capture_native_envelope(data: &[u8]) {
    if let Some(client) = sentry::Hub::main().client() {
        match sentry::Envelope::from_slice(data) {
            Ok(envelope) => client.send_envelope(envelope),
            Err(error) => crate::error!("failed to capture crash: {}", crate::LogError(&error)),
        }
    } else {
        crate::error!("failed to capture crash: no sentry client registered");
    }
}

/// Configures the given log level for all of Relay's crates.
fn set_default_filters(builder: &mut env_logger::Builder) {
    builder
        // Configure INFO as default for all third-party crates.
        .filter_level(LevelFilter::Info)
        // Trust DNS is very spammy on INFO, so configure a higher warn level.
        .filter_module("trust_dns_proto", LevelFilter::Warn)
        // Actix-web has useful information on the debug stream, so allow this.
        .filter_module("actix_web::pipeline", LevelFilter::Debug);

    // Add all internal modules with maximum log-level.
    for name in CRATE_NAMES {
        builder.filter_module(name, LevelFilter::Trace);
    }
}

/// Initialize the logging system and reporting to Sentry.
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
/// relay_log::init(&log_config, &sentry_config);
/// ```
pub fn init(config: &LogConfig, sentry: &SentryConfig) {
    if config.enable_backtraces {
        env::set_var("RUST_BACKTRACE", "full");
    }

    let mut log_builder = env_logger::Builder::from_env(env_logger::DEFAULT_FILTER_ENV);
    if env::var(env_logger::DEFAULT_FILTER_ENV).is_err() {
        set_default_filters(&mut log_builder);
    }

    match (config.format, console::user_attended()) {
        (LogFormat::Auto, true) | (LogFormat::Pretty, _) => log_builder.format(format_pretty),
        (LogFormat::Auto, false) | (LogFormat::Simplified, _) => log_builder.format(format_plain),
        (LogFormat::Json, _) => log_builder.format(format_json),
    };

    log::set_max_level(config.level);
    log::set_boxed_logger(Box::new(SentryLogger::with_dest(log_builder.build()))).ok();

    if let Some(dsn) = sentry.enabled_dsn() {
        let guard = sentry::init(sentry::ClientOptions {
            dsn: Some(dsn).cloned(),
            in_app_include: vec!["relay"],
            release: Some(RELEASE.into()),
            attach_stacktrace: config.enable_backtraces,
            environment: sentry.environment.clone(),
            ..Default::default()
        });

        // Keep the client initialized. The client is flushed manually in `main`.
        std::mem::forget(guard);
    }

    // Initialize native crash reporting after the Rust SDK, so that `capture_native_envelope` has
    // access to an initialized Hub to capture crashes from the previous run.
    #[cfg(feature = "relay-crash")]
    {
        if let Some(dsn) = sentry.enabled_dsn().map(|d| d.to_string()) {
            if let Some(db) = sentry._crash_db.as_deref() {
                relay_crash::CrashHandler::new(dsn.as_str(), db)
                    .transport(capture_native_envelope)
                    .release(Some(RELEASE))
                    .install();
            }
        }
    }
}

static MAX_MODULE_WIDTH: AtomicUsize = AtomicUsize::new(0);

fn max_target_width(target: &str) -> usize {
    let len = target.len();
    MAX_MODULE_WIDTH.fetch_max(len, Ordering::Relaxed).max(len)
}

fn format_pretty(f: &mut env_logger::fmt::Formatter, record: &log::Record) -> io::Result<()> {
    let color = match record.level() {
        Level::Trace => env_logger::fmt::Color::Magenta,
        Level::Debug => env_logger::fmt::Color::Blue,
        Level::Info => env_logger::fmt::Color::Green,
        Level::Warn => env_logger::fmt::Color::Yellow,
        Level::Error => env_logger::fmt::Color::Red,
    };

    let mut style = f.style();
    let styled_level = style.set_color(color).value(record.level());

    let mut style = f.style();
    let target = record.target();
    let styled_target = style.set_bold(true).value(target);

    writeln!(
        f,
        " {styled_level:5} {styled_target:width$} > {}",
        record.args(),
        width = max_target_width(target),
    )
}

fn format_plain(f: &mut env_logger::fmt::Formatter, record: &log::Record) -> io::Result<()> {
    let ts = f.timestamp();

    writeln!(
        f,
        "{} [{}] {}: {}",
        ts,
        record.module_path().unwrap_or("<unknown>"),
        record.level(),
        record.args()
    )
}

#[derive(Serialize, Deserialize, Debug)]
struct LogRecord<'a> {
    timestamp: DateTime<Utc>,
    level: Level,
    logger: &'a str,
    message: String,
    module_path: Option<&'a str>,
    filename: Option<&'a str>,
    lineno: Option<u32>,
}

fn format_json(mut f: &mut env_logger::fmt::Formatter, record: &log::Record) -> io::Result<()> {
    let record = LogRecord {
        timestamp: Utc::now(),
        level: record.level(),
        logger: record.target(),
        message: record.args().to_string(),
        module_path: record.module_path(),
        filename: record.file(),
        lineno: record.line(),
    };

    serde_json::to_writer(&mut f, &record)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

    f.write_all(b"\n")
}
