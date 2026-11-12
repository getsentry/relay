//! Heroku Logplex log expansion.

use crate::envelope::IntegrationItem;

use relay_cogs::AppFeature;
use relay_event_schema::protocol::ourlog::container::{InferIp, InferUserAgent, IngestSettings};
use relay_ourlogs::{HEROKU_LOGPLEX_HEADER_DRAIN_TOKEN, HEROKU_LOGPLEX_HEADER_MSG_COUNT};
use relay_quotas::DataCategory;

use crate::envelope::WithHeader;
use crate::integrations::{Integration, LogsIntegration};
use crate::managed::{Managed, ManagedEnvelope, RecordKeeper, Rejected};
use crate::processing::logs::integrations::{
    Error, SerializedLogsIntegration, create_intermediates,
};
use crate::processing::{self, Intermediates, Nothing, Output};
use crate::services::outcome::{DiscardItemType, DiscardReason};

/// The maximum amount of messages accepted in a single Logplex payload.
///
/// Logplex sends batches of around 100KB with at most 1024 buffered messages, payloads with
/// more messages are not sent by Logplex and are rejected as a whole.
const MAX_MESSAGES: u32 = 10_000;

/// Maximum size/length of the drain token which is added to all logs.
const MAX_DRAIN_TOKEN: usize = 256;

/// A processor for Heroku log integrations.
///
/// Converts Logplex integration items into intermediate log envelopes for further processing.
#[derive(Debug)]
pub struct HerokuLogsIntegrationProcessor;

impl processing::Processor for HerokuLogsIntegrationProcessor {
    type Input = SerializedLogsIntegration;
    type Output = Nothing;
    type Error = Error;

    fn cogs() -> relay_cogs::FeatureWeights {
        AppFeature::Logs.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let item = envelope
            .envelope_mut()
            .take_item_if::<IntegrationItem>(|item| {
                matches!(
                    item.integration(),
                    Some(Integration::Logs(LogsIntegration::HerokuLogDrain))
                )
            })?;
        let headers = envelope.envelope().headers().clone();

        let work = SerializedLogsIntegration { headers, item };
        Some(Managed::with_meta_from_managed_envelope(envelope, work))
    }

    async fn process(
        &self,
        work: Managed<Self::Input>,
        _ctx: processing::Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let intermediates = work.try_map(expand)?;
        Ok(Output::intermediates(intermediates))
    }
}

/// Expands a Heroku log integration into an envelope of logs that can be
/// returned to the processing pipeline.
pub fn expand(
    SerializedLogsIntegration { headers, item }: SerializedLogsIntegration,
    records: &mut RecordKeeper,
) -> Result<Intermediates, Error> {
    records.lenient(DataCategory::LogByte);

    let payload = item.payload();

    let msg_count = headers
        .get_header(HEROKU_LOGPLEX_HEADER_MSG_COUNT)
        .and_then(|h| h.as_str())
        .map(|h| h.parse::<u32>())
        .transpose()
        .map_err(|_| Error::Invalid(DiscardReason::InvalidLog))?;
    let drain_token = headers
        .get_header(HEROKU_LOGPLEX_HEADER_DRAIN_TOKEN)
        .and_then(|h| h.as_str())
        .filter(|h| h.len() < MAX_DRAIN_TOKEN);

    let mut count: u32 = 0;
    let mut logs = Vec::new();

    for frame in relay_ourlogs::parse_logplex_frames(&payload) {
        count += 1;

        if count > MAX_MESSAGES {
            return Err(Error::Invalid(DiscardReason::ItemTooLarge(
                DiscardItemType::Log,
            )));
        }

        let frame = frame
            .inspect_err(|err| {
                relay_log::debug!(
                    error = err as &dyn std::error::Error,
                    "failed to parse Logplex frame"
                )
            })
            .map_err(|_| Error::Invalid(DiscardReason::InvalidLog))?;

        let frame = String::from_utf8_lossy(frame);
        let message = relay_ourlogs::parse_logplex_message(&frame)
            .inspect_err(|err| {
                relay_log::debug!(
                    error = err as &dyn std::error::Error,
                    "Failed to parse Logplex message"
                )
            })
            .map_err(|_| Error::Invalid(DiscardReason::InvalidLog))?;

        let log = relay_ourlogs::logplex_message_to_sentry_log(message, drain_token);
        let byte_size = relay_ourlogs::calculate_size(&log);

        records.modify_by(DataCategory::LogItem, 1);
        records.modify_by(DataCategory::LogByte, byte_size as isize);

        logs.push(WithHeader::just(log.into()));
    }

    if msg_count.is_some_and(|expected| expected != count) {
        relay_log::debug!("Parsed message count differs from Logplex-Msg-Count");
        return Err(Error::Invalid(DiscardReason::InvalidLog));
    }

    // Undo all the base item quantities, as they will be completely taken over by the parsed
    // contents, which contains an arbitrary amount of items (even 0).
    records.remove(&item);

    let settings = IngestSettings {
        infer_user_agent: Some(InferUserAgent::Never),
        infer_ip: Some(InferIp::Never),
    };

    create_intermediates(headers, logs, settings)
}
