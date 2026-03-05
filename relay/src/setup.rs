#[cfg(feature = "processing")]
use anyhow::Context;
use anyhow::Result;
use relay_config::{Config, RelayMode};
use relay_server::MemoryStat;
use relay_statsd::MetricsConfig;

/// Validates that the `batch_size_bytes` of the configuration is correct and doesn't lead to
/// deadlocks in the buffer.
fn assert_batch_size_bytes(config: &Config) -> Result<()> {
    // We create a temporary memory reading used just for the config check.
    let memory = MemoryStat::current_memory();

    // We expect the batch size for the spooler to be 10% of the memory threshold over which the
    // buffer stops unspooling.
    //
    // The 10% threshold was arbitrarily chosen to give the system leeway when spooling.
    let configured_batch_size_bytes = config.spool_envelopes_batch_size_bytes() as f32;
    let maximum_batch_size_bytes =
        memory.total as f32 * config.spool_max_backpressure_memory_percent() * 0.1;

    if configured_batch_size_bytes > maximum_batch_size_bytes {
        anyhow::bail!(
            "the configured `spool.envelopes.batch_size_bytes` is {} bytes but it must be <= than {} bytes",
            configured_batch_size_bytes,
            maximum_batch_size_bytes
        )
    }

    Ok(())
}

pub fn check_config(config: &Config) -> Result<()> {
    if config.relay_mode() == RelayMode::Managed && config.credentials().is_none() {
        anyhow::bail!(
            "relay has no credentials, which are required in managed mode. \
             Generate some with \"relay credentials generate\" first.",
        );
    }

    if config.relay_mode() != RelayMode::Managed && config.processing_enabled() {
        anyhow::bail!("Processing can only be enabled in managed mode.");
    }

    #[cfg(feature = "processing")]
    if config.processing_enabled() {
        for name in config.unused_topic_assignments().names() {
            relay_log::with_scope(
                |scope| scope.set_extra("topic", name.as_str().into()),
                || relay_log::error!("unused topic assignment '{name}'"),
            );
        }

        for topic in relay_kafka::KafkaTopic::iter() {
            let _ = config
                .kafka_configs(*topic)
                .with_context(|| format!("invalid kafka configuration for topic '{topic:?}'"))?;
        }
    }

    assert_batch_size_bytes(config)?;

    Ok(())
}

/// Print spawn infos to the log.
pub fn dump_spawn_infos(config: &Config) {
    if config.path().as_os_str().is_empty() {
        relay_log::info!("launching relay without config folder");
    } else {
        relay_log::info!(
            "launching relay from config folder {}",
            config.path().display()
        );
    }
    relay_log::info!("  relay mode: {}", config.relay_mode());

    match config.relay_id() {
        Some(id) => relay_log::info!("  relay id: {id}"),
        None => relay_log::info!("  relay id: -"),
    };
    match config.public_key() {
        Some(key) => relay_log::info!("  public key: {key}"),
        None => relay_log::info!("  public key: -"),
    };
    relay_log::info!("  log level: {}", config.logging().level);
}

/// Dumps out credential info.
pub fn dump_credentials(config: &Config) {
    match config.relay_id() {
        Some(id) => println!("  relay id: {id}"),
        None => println!("  relay id: -"),
    };
    match config.public_key() {
        Some(key) => println!("  public key: {key}"),
        None => println!("  public key: -"),
    };
}

/// Initialize the metric system.
pub fn init_metrics(config: &Config) -> Result<()> {
    let Some(host) = config.statsd_addr() else {
        return Ok(());
    };

    let mut default_tags = config.metrics_default_tags().clone();
    if let Some(hostname_tag) = config.metrics_hostname_tag()
        && let Some(hostname) = hostname::get().ok().and_then(|s| s.into_string().ok())
    {
        default_tags.insert(hostname_tag.to_owned(), hostname);
    }
    relay_statsd::init(MetricsConfig {
        prefix: config.metrics_prefix().to_owned(),
        host: host.to_owned(),
        buffer_size: config.statsd_buffer_size(),
        default_tags,
    })?;

    Ok(())
}
