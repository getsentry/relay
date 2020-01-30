use crate::processor::ProcessValue;
use crate::types::Annotated;

/// Metrics captured during event ingestion and processing.
///
/// These values are collected in Relay and Sentry and finally persisted into the event payload. A
/// value of `0` is equivalent to N/A and should not be considered in aggregations and analysis.
#[derive(Clone, Debug, Default, Empty, PartialEq, FromValue, ToValue)]
pub struct Metrics {
    /// The size of the original event payload ingested into Sentry.
    ///
    /// The size is measured after applying all content- and transfer-encodings  and thus
    /// independent of compression over the wire.
    ///
    /// For security reports, this is the size of the JSON request sent by the browser and not of
    /// the event payload we extract from it. For ingested envelopes, this is only the size of the
    /// "event" item.
    ///
    /// This metric is measured in Relay during ingestion.
    #[metastructure(field = "bytes.ingested.event")]
    pub bytes_ingested_event: Annotated<u64>,

    /// The size of the minidump ingested into Sentry.
    ///
    /// The size is measured after applying all content- and transfer-encodings  and thus
    /// independent of compression over the wire.
    ///
    /// This metric is measured in Relay during ingestion.
    #[metastructure(field = "bytes.ingested.event.minidump")]
    pub bytes_ingested_event_minidump: Annotated<u64>,

    /// The size of an apple crash report ingested into Sentry.
    ///
    /// The size is measured after applying all content- and transfer-encodings  and thus
    /// independent of compression over the wire.
    ///
    /// This metric is measured in Relay during ingestion.
    #[metastructure(field = "bytes.ingested.event.applecrashreport")]
    pub bytes_ingested_event_applecrashreport: Annotated<u64>,

    /// The cumulative size of all additional attachments ingested into Sentry.
    ///
    /// This is a sum of all all generic attachment payloads (excluding headers). These attachments
    /// are never processed and only potentially stored. Minidumps and apple crash reports are
    /// counted separately in their respective metrics.
    ///
    /// This metric is measured in Relay during ingestion.
    #[metastructure(field = "bytes.ingested.event.attachment")]
    pub bytes_ingested_event_attachment: Annotated<u64>,

    /// The size of the event payload as it is saved in node store.
    ///
    /// For security reports, this is the size of the event derived from the original JSON. For
    /// processed events, such as native or javascript, this includes information derived during
    /// symbolication and internal meta data.
    ///
    /// This metric is measured in Sentry after all processing has completed and when it is posted
    /// into node store.
    #[metastructure(field = "bytes.stored.event")]
    pub bytes_stored_event: Annotated<u64>,

    /// The size of the minidump as it is saved in blob store.
    ///
    /// Minidumps are not saved in two conditions. In this case, this number will be zero (missing):
    ///  1. The attachment feature is not enabled for an organization.
    ///  2. The option "Store Native Crash Reports" is disabled (default).
    ///
    /// This metric is measured in Sentry after all processing has completed and when it is posted
    /// into node store.
    #[metastructure(field = "bytes.stored.event.minidump")]
    pub bytes_stored_event_minidump: Annotated<u64>,

    /// The size of the apple crash report as it is saved in blob store.
    ///
    /// Apple crash reports are not saved in two conditions. In this case, this number will be zero
    /// (missing):
    ///  1. The attachment feature is not enabled for an organization.
    ///  2. The option "Store Native Crash Reports" is disabled (default).
    ///
    /// This metric is measured in Sentry after all processing has completed and when it is posted
    /// into node store.
    #[metastructure(field = "bytes.stored.event.applecrashreport")]
    pub bytes_stored_event_applecrashreport: Annotated<u64>,

    /// The cumulative size of all additional attachments as saved in blob store.
    ///
    /// Attachments are not saved if the attachment feature is not enabled for the organization. In
    /// this case, this number will be zero (missing).
    ///
    /// This metric is measured in Sentry after all processing has completed and when it is posted
    /// into node store.
    #[metastructure(field = "bytes.stored.event.attachment")]
    pub bytes_stored_event_attachment: Annotated<u64>,

    /// The number of milliseconds Symbolicator spent processing the native event.
    ///
    /// This metric is measured in Symbolicator and reported in the native processing task. There
    /// are additional overheads polling symbolicator for the response, but the time reported from
    /// symbolicator reflects used resources most accurately.
    #[metastructure(field = "ms.processing.symbolicator")]
    pub ms_processing_symbolicator: Annotated<u64>,

    /// The number of milliseconds Sentry spent resolving proguard mappings for a java event.
    ///
    /// This metric is measured in Sentry and reported in the java processing task.
    #[metastructure(field = "ms.processing.proguard")]
    pub ms_processing_proguard: Annotated<u64>,

    /// The number of milliseconds sentry spent resolving minified stack traces for a javascript event.
    ///
    /// This metric is measured in Sentry and reported in the javascript processing task.
    #[metastructure(field = "ms.processing.sourcemaps")]
    pub ms_processing_sourcemaps: Annotated<u64>,
}

// Do not process Metrics
impl ProcessValue for Metrics {}
