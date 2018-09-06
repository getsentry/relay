use std::env;
use std::io;
use std::io::Write;

use chrono::{DateTime, Utc};
use console;
use env_logger;
use failure::Error;
use log::{Level, LevelFilter};
use pretty_env_logger;
use sentry;
use serde_json;

use semaphore_common::{metrics, Config, LogFormat};

/// Print spawn infos to the log.
pub fn dump_spawn_infos(config: &Config) {
    info!(
        "launching relay from config folder {}",
        config.path().display()
    );
    info!("  relay id: {}", config.relay_id());
    info!("  public key: {}", config.public_key());
    info!("  log level: {}", config.log_level_filter());
}

/// Dumps out credential info.
pub fn dump_credentials(config: &Config) {
    println!("  relay id: {}", config.relay_id());
    println!("  public key: {}", config.public_key());
}

/// Initialize the logging system.
pub fn init_logging(config: &Config) {
    sentry::init(sentry::ClientOptions {
        dsn: config
            .sentry_dsn()
            .map(|dsn| dsn.to_string().parse().unwrap()),
        in_app_include: vec!["semaphore_common::", "semaphore_server::", "semaphore::"],
        release: sentry_crate_release!(),
        ..Default::default()
    });

    if config.enable_backtraces() {
        env::set_var("RUST_BACKTRACE", "1");
    }

    if env::var("RUST_LOG").is_err() {
        let log = match config.log_level_filter() {
            LevelFilter::Off => "",
            LevelFilter::Error => "ERROR",
            LevelFilter::Warn => "WARN",
            LevelFilter::Info => "INFO",
            LevelFilter::Debug => {
                "INFO,\
                 actix_web::pipeline=DEBUG,\
                 semaphore_common=DEBUG,\
                 semaphore_server=DEBUG,\
                 semaphore=DEBUG"
            }
            LevelFilter::Trace => {
                "INFO,\
                 actix_web::pipeline=DEBUG,\
                 semaphore_common=TRACE,\
                 semaphore_server=TRACE,\
                 semaphore=TRACE"
            }
        }.to_string();

        env::set_var("RUST_LOG", log);
    }

    let mut log_builder = {
        match (config.log_format(), console::user_attended()) {
            (LogFormat::Auto, true) | (LogFormat::Pretty, _) => {
                pretty_env_logger::formatted_builder().unwrap()
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
                    ).map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
                    buf.write_all(b"\n")?;
                    Ok(())
                });
                builder
            }
        }
    };

    match env::var("RUST_LOG") {
        Ok(rust_log) => log_builder.parse(&rust_log),
        Err(_) => log_builder.filter_level(config.log_level_filter()),
    };

    let log = Box::new(log_builder.build());
    let global_filter = log.filter();

    sentry::integrations::log::init(
        Some(log),
        sentry::integrations::log::LoggerOptions {
            global_filter: Some(global_filter),
            ..Default::default()
        },
    );
    sentry::integrations::panic::register_panic_handler();
}

/// Initialize the metric system.
pub fn init_metrics(config: &Config) -> Result<(), Error> {
    let addrs = config.statsd_addrs()?;
    if !addrs.is_empty() {
        metrics::configure_statsd(config.metrics_prefix(), &addrs[..]);
    }
    Ok(())
}
