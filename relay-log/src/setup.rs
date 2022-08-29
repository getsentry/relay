use std::env;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use log::{Level, LevelFilter};
use sentry::types::Dsn;
use serde::{Deserialize, Serialize};

use crate::sentry_failure::FailureIntegration;

/// The full release name including the Relay version and SHA.
const RELEASE: &str = std::env!("RELAY_RELEASE");

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
            _crash_db: None,
        }
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

    if env::var("RUST_LOG").is_err() {
        let log = match config.level {
            LevelFilter::Off => "",
            LevelFilter::Error => "ERROR",
            LevelFilter::Warn => "WARN",
            LevelFilter::Info => {
                "INFO,\
                 trust_dns_proto=WARN"
            }
            LevelFilter::Debug => {
                "INFO,\
                 trust_dns_proto=WARN,\
                 actix_web::pipeline=DEBUG,\
                 relay_auth=DEBUG,\
                 relay_common=DEBUG,\
                 relay_config=DEBUG,\
                 relay_filter=DEBUG,\
                 relay_general=DEBUG,\
                 relay_quotas=DEBUG,\
                 relay_redis=DEBUG,\
                 relay_server=DEBUG,\
                 relay=DEBUG"
            }
            LevelFilter::Trace => {
                "INFO,\
                 trust_dns_proto=WARN,\
                 actix_web::pipeline=DEBUG,\
                 relay_auth=TRACE,\
                 relay_common=TRACE,\
                 relay_config=TRACE,\
                 relay_filter=TRACE,\
                 relay_general=TRACE,\
                 relay_quotas=TRACE,\
                 relay_redis=TRACE,\
                 relay_server=TRACE,\
                 relay=TRACE"
            }
        }
        .to_string();

        env::set_var("RUST_LOG", log);
    }

    let mut log_builder = {
        match (config.format, console::user_attended()) {
            (LogFormat::Auto, true) | (LogFormat::Pretty, _) => {
                pretty_env_logger::formatted_builder()
            }
            (LogFormat::Auto, false) | (LogFormat::Simplified, _) => {
                let mut builder = env_logger::Builder::new();
                builder.format(|buf, record| {
                    let ts = buf.timestamp();
                    writeln!(
                        buf,
                        "{} [{}] {}: {}",
                        ts,
                        record.module_path().unwrap_or("<unknown>"),
                        record.level(),
                        record.args()
                    )
                });
                builder
            }
            (LogFormat::Json, _) => {
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

                let mut builder = env_logger::Builder::new();
                builder.format(|mut buf, record| -> io::Result<()> {
                    serde_json::to_writer(
                        &mut buf,
                        &LogRecord {
                            timestamp: Utc::now(),
                            level: record.level(),
                            logger: record.target(),
                            message: record.args().to_string(),
                            module_path: record.module_path(),
                            filename: record.file(),
                            lineno: record.line(),
                        },
                    )
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
                    buf.write_all(b"\n")?;
                    Ok(())
                });
                builder
            }
        }
    };

    match env::var("RUST_LOG") {
        Ok(rust_log) => log_builder.parse_filters(&rust_log),
        Err(_) => log_builder.filter_level(config.level),
    };

    let dest_log = log_builder.build();
    log::set_max_level(dest_log.filter());

    let log = sentry::integrations::log::SentryLogger::with_dest(dest_log);
    log::set_boxed_logger(Box::new(log)).ok();

    #[cfg(feature = "relay-crash")]
    {
        if let Some(dsn) = sentry.enabled_dsn().map(|d| d.to_string()) {
            if let Some(db) = sentry._crash_db.as_deref() {
                relay_crash::CrashHandler::new(dsn.as_ref(), db)
                    .release(Some(RELEASE))
                    .install();
            }
        }
    }

    if let Some(dsn) = sentry.enabled_dsn() {
        let guard = sentry::init(sentry::ClientOptions {
            dsn: Some(dsn).cloned(),
            in_app_include: vec![
                "relay_auth::",
                "relay_common::",
                "relay_config::",
                "relay_filter::",
                "relay_general::",
                "relay_quotas::",
                "relay_redis::",
                "relay_server::",
                "relay::",
            ],
            integrations: vec![Arc::new(FailureIntegration::new())],
            release: Some(RELEASE.into()),
            attach_stacktrace: config.enable_backtraces,
            ..Default::default()
        });

        // Keep the client initialized. The client is flushed manually in `main`.
        std::mem::forget(guard);
    }
}
