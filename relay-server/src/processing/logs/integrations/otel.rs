use prost::Message;
use relay_cogs::AppFeature;
use relay_conventions::attributes::SENTRY__RELAY__INGRESS;
use relay_event_normalization::eap::Ingress;
use relay_event_schema::protocol::OurLogHeader;
use relay_event_schema::protocol::ourlog::container::{
    ContainerMetadata, InferIp, InferUserAgent, IngestSettings,
};
use relay_ourlogs::otel_logs::LogsData;
use relay_quotas::DataCategory;

use crate::Envelope;
use crate::envelope::{Item, ItemContainer, ItemType, WithHeader};
use crate::integrations::{Integration, LogsIntegration, OtelFormat};
use crate::managed::{Counted, Managed, ManagedEnvelope, RecordKeeper, Rejected};
use crate::processing::logs::integrations::{Error, SerializedLogsIntegration};
use crate::processing::{self, Forward, Output};
use crate::services::outcome::DiscardReason;

/// The "output" of a [`OtelLogsIntegrationProcessor`].
///
/// This type is uninhabited to enforce that the processor
/// can only produce intermediates.
#[derive(Debug, Clone, Copy)]
pub enum OtelLogsIntegrationOutput {}

impl Forward for OtelLogsIntegrationOutput {
    fn serialize_envelope(
        self,
        _ctx: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<crate::Envelope>>, Rejected<()>> {
        match self {}
    }

    fn forward_store(
        self,
        _s: processing::StoreHandle<'_>,
        _ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        match self {}
    }
}

#[derive(Debug)]
pub struct SerializedOtelLogsIntegration(SerializedLogsIntegration, OtelFormat);

impl Counted for SerializedOtelLogsIntegration {
    fn quantities(&self) -> crate::managed::Quantities {
        self.0.quantities()
    }
}

/// A processor for OTEL log integrations.
///
/// This processor only produces intermediates (items
/// that are put back into the processing pipeline) by
/// converting log integration items into actual logs. It
/// never passes anything on to the store or another Relay.
#[derive(Debug)]
pub struct OtelLogsIntegrationProcessor {}

impl OtelLogsIntegrationProcessor {
    /// Creates a new [`Self`].
    pub fn new() -> Self {
        Self {}
    }
}

impl processing::Processor for OtelLogsIntegrationProcessor {
    type Input = SerializedOtelLogsIntegration;
    type Output = OtelLogsIntegrationOutput;
    type Error = Error;

    fn cogs() -> relay_cogs::FeatureWeights {
        AppFeature::Logs.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let headers = envelope.envelope().headers().clone();

        let (item, format) =
            envelope
                .envelope_mut()
                .take_item_by_map(|item| match item.integration() {
                    Some(Integration::Logs(LogsIntegration::OtelV1 { format })) => Some(format),
                    _ => None,
                })?;

        let work = SerializedLogsIntegration { headers, item };
        Some(Managed::with_meta_from_managed_envelope(
            envelope,
            SerializedOtelLogsIntegration(work, format),
        ))
    }

    async fn process(
        &self,
        work: Managed<Self::Input>,
        _ctx: processing::Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let intermediates = work.try_map(expand)?;

        Ok(Output {
            main: None,
            metrics: None,
            intermediates: Some(ManagedEnvelope::from(intermediates)),
        })
    }
}

/// Expands a OTEL log integration into an envelope of logs that can be
/// returned to the processing pipeline.
pub fn expand(
    work: SerializedOtelLogsIntegration,
    records: &mut RecordKeeper,
) -> Result<Box<Envelope>, Error> {
    let SerializedOtelLogsIntegration(SerializedLogsIntegration { mut headers, item }, format) =
        work;

    // Log byte counts will change here as we go from an estimated count based on the body, to
    // accurately counted bytes.
    records.lenient(DataCategory::LogByte);

    let payload = item.payload();
    let mut logs = Vec::new();
    let parsed = parse_logs_data(format, &payload)?;

    for resource_logs in parsed.resource_logs {
        let resource = resource_logs.resource.as_ref();
        for scope_logs in resource_logs.scope_logs {
            let scope = scope_logs.scope.as_ref();
            for log_record in scope_logs.log_records {
                let mut log = relay_ourlogs::otel_to_sentry_log(log_record, resource, scope);
                let byte_size = relay_ourlogs::calculate_size(&log);

                records.modify_by(DataCategory::LogItem, 1);
                records.modify_by(DataCategory::LogByte, byte_size as isize);

                // Set the `ingress` attribute. The logs processor will not overwrite it.
                log.attributes
                    .value_mut()
                    .get_or_insert_default()
                    .insert(SENTRY__RELAY__INGRESS, Ingress::Integration.to_string());

                logs.push(WithHeader {
                    header: Some(OurLogHeader {
                        byte_size: Some(byte_size),
                        other: Default::default(),
                    }),
                    value: log.into(),
                });
            }
        }
    }

    // Undo all the base item quantities, as they will be completely taken over by the parsed
    // contents, which contains an arbitrary amount of items (even 0).
    for (category, quantity) in item.quantities() {
        records.modify_by(category, -(quantity as isize));
    }

    let settings = IngestSettings {
        infer_user_agent: Some(InferUserAgent::Never),
        infer_ip: Some(InferIp::Never),
    };

    let metadata = ContainerMetadata {
        version: Some(2),
        ingest_settings: Some(settings),
    };

    let mut log_container = Item::new(ItemType::Log);
    ItemContainer::from_parts(metadata, logs)
        .write_to(&mut log_container)
        .map_err(|_| Error::Invalid(DiscardReason::Internal))?;

    // The envelope comes from a trusted Relay (this one).
    headers
        .meta_mut()
        .set_request_trust(crate::extractors::RequestTrust::Trusted);

    Ok(Envelope::from_parts(
        headers,
        smallvec::smallvec![log_container],
    ))
}

fn parse_logs_data(format: OtelFormat, payload: &[u8]) -> Result<LogsData, Error> {
    match format {
        OtelFormat::Json => serde_json::from_slice(payload).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse logs data as JSON"
            );
            Error::Invalid(DiscardReason::InvalidJson)
        }),
        OtelFormat::Protobuf => LogsData::decode(payload).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse logs data as protobuf"
            );
            Error::Invalid(DiscardReason::InvalidProtobuf)
        }),
    }
}
