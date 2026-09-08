#[cfg(feature = "processing")]
use relay_dynamic_config::GlobalConfig;
use relay_dynamic_config::RetentionConfig;
#[cfg(feature = "processing")]
use relay_system::{Addr, FromMessage};

use crate::Envelope;
#[cfg(feature = "processing")]
use crate::managed::ManagedEnvelope;
use crate::managed::{Managed, Rejected};
use crate::processing::Context;
#[cfg(feature = "processing")]
use crate::services::objectstore::Objectstore;
#[cfg(feature = "processing")]
use crate::services::processor::{EnvelopeProcessor, ProcessEnvelope};
#[cfg(feature = "processing")]
use crate::services::store::{Store, StoreEvent};

/// A transparent handle that dispatches between store-like services.
#[cfg(feature = "processing")]
#[derive(Debug, Clone, Copy)]
pub struct StoreHandle<'a> {
    store: &'a Addr<Store>,
    objectstore: Option<&'a Addr<Objectstore>>,
    global_config: &'a GlobalConfig,
}

#[cfg(feature = "processing")]
impl<'a> StoreHandle<'a> {
    pub fn new(
        store: &'a Addr<Store>,
        objectstore: Option<&'a Addr<Objectstore>>,
        global_config: &'a GlobalConfig,
    ) -> Self {
        Self {
            store,
            objectstore,
            global_config,
        }
    }

    /// Dispatches an event message to either the [`Objectstore`] or [`Store`] service.
    pub fn send_event(&self, message: Managed<Box<StoreEvent>>) {
        if message.attachments.is_empty() {
            self.store.send(message);
            return;
        }

        let Some(objectstore) = self.objectstore else {
            self.store.send(message);
            return;
        };

        let use_objectstore = crate::utils::sample(
            self.global_config
                .options
                .objectstore_attachments_sample_rate,
        )
        .is_keep();

        match use_objectstore {
            true => objectstore.send(message),
            false => self.store.send(message),
        }
    }

    /// Sends a message to the [`Store`] service.
    pub fn send_to_store<M>(&self, message: M)
    where
        Store: FromMessage<M>,
    {
        self.store.send(message);
    }

    /// Sends a message to the [`Objectstore`] service.
    pub fn send_to_objectstore<M>(&self, message: M)
    where
        Objectstore: FromMessage<M>,
    {
        if let Some(objectstore) = self.objectstore {
            objectstore.send(message);
        } else {
            relay_log::error!("Objectstore service not configured. Dropping message.");
        }
    }
}

/// A handle to an envelope processor, which can be used to
/// re-enqueue additional items during processing.
#[cfg(feature = "processing")]
#[derive(Debug, Clone, Copy)]
pub struct EnvelopeProcessorHandle<'a>(&'a Addr<EnvelopeProcessor>);

#[cfg(feature = "processing")]
impl<'a> EnvelopeProcessorHandle<'a> {
    pub fn new(addr: &'a Addr<EnvelopeProcessor>) -> Self {
        Self(addr)
    }

    pub fn send_envelope(&self, envelope: ManagedEnvelope, ctx: Context<'_>) {
        self.0.send(ProcessEnvelope {
            envelope,
            project_info: ctx.project_info.clone(),
            rate_limits: ctx.rate_limits.clone(),
            sampling_project_info: ctx.sampling_project_info.cloned(),
        })
    }
}

/// A processor output which can be forwarded to a different destination.
pub trait Forward {
    /// Serializes the output into an [`Envelope`].
    ///
    /// All output must be serializable as an envelope.
    fn serialize_envelope(self, ctx: Context<'_>) -> Result<Managed<Box<Envelope>>, Rejected<()>>;

    /// Serializes the output into a [`crate::services::store::StoreService`] compatible format.
    ///
    /// Additional items which need to go through the processing pipeline in an additional pass
    /// can be wrapped in an envelope and passed to the [`EnvelopeProcessorHandle`].
    ///
    /// This function must only be called when Relay is configured to be in processing mode.
    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: StoreHandle<'_>,
        e: EnvelopeProcessorHandle,
        ctx: Context<'_>,
    ) -> Result<(), Rejected<()>>;
}

/// The [`Nothing`] output.
///
/// Some processors may only produce by-products and not have any output of their own.
#[derive(Debug, Copy, Clone)]
pub struct Nothing(std::convert::Infallible);

impl Forward for Nothing {
    fn serialize_envelope(self, _: Context<'_>) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        match self {}
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        _: StoreHandle<'_>,
        _: EnvelopeProcessorHandle,
        _: Context<'_>,
    ) -> Result<(), Rejected<()>> {
        match self {}
    }
}

impl From<Nothing> for crate::processing::Outputs {
    fn from(value: Nothing) -> Self {
        match value {}
    }
}

/// Full retention settings to apply to specific payloads.
#[derive(Debug, Copy, Clone)]
pub struct Retention {
    /// Standard / full fidelity retention policy in days.
    pub standard: u16,
    /// Downsampled retention policy in days.
    #[cfg_attr(not(feature = "processing"), expect(unused))]
    pub downsampled: u16,
}

impl From<RetentionConfig> for Retention {
    fn from(value: RetentionConfig) -> Self {
        Self {
            standard: value.standard,
            downsampled: value.downsampled.unwrap_or(value.standard),
        }
    }
}
