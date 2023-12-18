use std::borrow::Cow;
use std::env;
use std::path::PathBuf;

use sentry::types::Dsn;
use serde::{Deserialize, Serialize};
use tracing::{level_filters::LevelFilter, Level};
use tracing_subscriber::{prelude::*, EnvFilter, Layer};

#[cfg(feature = "dashboard")]
use crate::dashboard;

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

mod level_serde {
    use std::fmt;

    use serde::de::{Error, Unexpected, Visitor};
    use serde::{Deserializer, Serializer};
    use tracing::Level;

    pub fn serialize<S>(filter: &Level, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(filter)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Level;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a log level")
            }

            fn visit_str<E>(self, value: &str) -> Result<Level, E>
            where
                E: Error,
            {
                value
                    .parse()
                    .map_err(|_| Error::invalid_value(Unexpected::Str(value), &self))
            }
        }

        deserializer.deserialize_str(V)
    }
}

/// Controls the logging system.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct LogConfig {
    /// The log level for Relay.
    #[serde(with = "level_serde")]
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

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: Level::INFO,
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

    let logs_subscriber = tracing_subscriber::registry()
        .with(format.with_filter(LevelFilter::from(config.level)))
        .with(sentry::integrations::tracing::layer())
        .with(match env::var(EnvFilter::DEFAULT_ENV) {
            Ok(value) => EnvFilter::new(value),
            Err(_) => get_default_filters(),
        });

    // Also add dashboard subscriber if the feature is enabled.
    #[cfg(feature = "dashboard")]
    let logs_subscriber = logs_subscriber.with(dashboard::dashboard_subscriber());

    logs_subscriber.init();

    if let Some(dsn) = sentry.enabled_dsn() {
        let guard = sentry::init(sentry::ClientOptions {
            dsn: Some(dsn).cloned(),
            in_app_include: vec!["relay"],
            release: Some(RELEASE.into()),
            attach_stacktrace: config.enable_backtraces,
            environment: sentry.environment.clone(),
            traces_sample_rate: config.traces_sample_rate,
            ..Default::default()
        });

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
