use crate::processing::logs::{
    Error, ExpandedLogs, Result, SerializedLogs, get_calculated_byte_size,
};
use crate::processing::{Context, Managed};
use crate::statsd::{RelayCounters, RelayDistributions};

/// Validates that there is only a single log container processed at a time.
///
/// Currently it is only allowed to send a single log container in an envelope.
/// Since all logs of the same envelope must belong to the same trace (due to the dynamic sampling
/// context on the envelope), they also should be collapsed by SDKs into a single container.
///
/// Integrations are only created within the same Relay and must not create multiple items at the moment.
///
/// Mixed log items are not supported/allowed.
pub fn invalid(logs: &Managed<SerializedLogs>) -> Result<()> {
    if !logs.invalid.is_empty() {
        return Err(Error::DuplicateItem);
    }

    Ok(())
}

/// Validate that the envelope has no trace context header.
///
/// For now, this only emits a metric so we can verify that logs envelopes do not
/// contain a trace context.
pub fn dsc(logs: &Managed<SerializedLogs>) {
    relay_statsd::metric!(
        counter(RelayCounters::EnvelopeWithLogs) += 1,
        dsc = match logs.headers.dsc() {
            Some(_) => "yes",
            None => "no",
        },
        sdk = crate::utils::client_name_tag(logs.headers.meta().client_name())
    )
}

/// Validates contained logs do not exceed the maximum size limit.
///
/// Currently this only considers the maximum log size configured in the configuration.
///
/// In the future we may want to increase the limit or start trimming excessive
/// attributes/payloads. For now we drop logs which exceed our size limit.
///
/// This matches the logic defined in [`check_envelope_size_limits`](crate::utils::check_envelope_size_limits),
/// when validating envelope sizes, the actual size of individual logs is not known and therefore
/// must be enforced consistently after parsing again.
pub fn size(logs: &mut Managed<ExpandedLogs>, ctx: Context<'_>) {
    let max_size_bytes = ctx.config.max_log_size();

    logs.retain(
        |logs| &mut logs.logs,
        |log, _| {
            let size = get_calculated_byte_size(log);
            let is_too_large = size > max_size_bytes;

            relay_statsd::metric!(
                distribution(RelayDistributions::TraceItemCanonicalSize) = size as u64,
                item = "log",
                too_large = if is_too_large { "true" } else { "false" },
            );

            match is_too_large {
                true => Err(Error::TooLarge),
                false => Ok(()),
            }
        },
    );
}
