use std::env;
use std::io;
use std::io::Write;
use std::mem;

use chrono::{DateTime, Utc};
use failure::{err_msg, Error};
use log::{Level, LevelFilter};
use serde::{Deserialize, Serialize};

use relay_common::metrics;
use relay_config::{Config, LogFormat, RelayMode};

pub fn check_config(config: &Config) -> Result<(), Error> {
    if config.relay_mode() == RelayMode::Managed && config.credentials().is_none() {
        return Err(err_msg(
            "relay has no credentials, which are required in managed mode. \
             Generate some with \"relay credentials generate\" first.",
        ));
    }

    Ok(())
}

/// Print spawn infos to the log.
pub fn dump_spawn_infos(config: &Config) {
    log::info!(
        "launching relay from config folder {}",
        config.path().display()
    );
    log::info!("  relay mode: {}", config.relay_mode());

    match config.relay_id() {
        Some(id) => log::info!("  relay id: {}", id),
        None => log::info!("  relay id: -"),
    };
    match config.public_key() {
        Some(key) => log::info!("  public key: {}", key),
        None => log::info!("  public key: -"),
    };
    log::info!("  log level: {}", config.log_level_filter());
}

/// Dumps out credential info.
pub fn dump_credentials(config: &Config) {
    match config.relay_id() {
        Some(id) => println!("  relay id: {}", id),
        None => println!("  relay id: -"),
    };
    match config.public_key() {
        Some(key) => println!("  public key: {}", key),
        None => println!("  public key: -"),
    };
}

/// Initialize the logging system.
pub fn init_logging(config: &Config) {
    let guard = sentry::init(sentry::ClientOptions {
        dsn: config
            .sentry_dsn()
            .map(|dsn| dsn.to_string().parse().unwrap()),
        in_app_include: vec![
            "relay_common::",
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
        release: sentry::release_name!(),
        ..Default::default()
    });

    mem::forget(guard);

    if config.enable_backtraces() {
        env::set_var("RUST_BACKTRACE", "1");
    }

    if env::var("RUST_LOG").is_err() {
        let log = match config.log_level_filter() {
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
        match (config.log_format(), console::user_attended()) {
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
        Err(_) => log_builder.filter_level(config.log_level_filter()),
    };

    let log = Box::new(log_builder.build());
    let global_filter = log.filter();

    sentry::integrations::log::init(
        Some(log),
        sentry::integrations::log::LoggerOptions {
            global_filter: Some(global_filter),
            attach_stacktraces: config.enable_backtraces(),
            ..Default::default()
        },
    );

    sentry::integrations::panic::register_panic_handler();
}

/// Initialize the metric system.
pub fn init_metrics(config: &Config) -> Result<(), Error> {
    let addrs = config.statsd_addrs()?;
    if addrs.is_empty() {
        return Ok(());
    }

    let mut default_tags = config.metrics_default_tags().clone();
    if let Some(hostname_tag) = config.metrics_hostname_tag() {
        if let Some(hostname) = hostname::get().ok().and_then(|s| s.into_string().ok()) {
            default_tags.insert(hostname_tag.to_owned(), hostname);
        }
    }
    metrics::configure_statsd(config.metrics_prefix(), &addrs[..], default_tags);

    Ok(())
}
