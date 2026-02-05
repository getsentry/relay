use relay_config::Config;
use relay_dynamic_config::GlobalConfig;
use relay_dynamic_config::{RetentionConfig, RetentionsConfig};
#[cfg(feature = "processing")]
use relay_system::{Addr, FromMessage};

use crate::Envelope;
use crate::managed::{Managed, Rejected};
use crate::services::projects::project::ProjectInfo;
#[cfg(feature = "processing")]
use crate::services::store::Store;
#[cfg(feature = "processing")]
use crate::services::upload::Upload;

/// A transparent handle that dispatches between store-like services.
#[cfg(feature = "processing")]
#[derive(Debug, Clone, Copy)]
pub struct StoreHandle<'a> {
    store: &'a Addr<Store>,
    upload: Option<&'a Addr<Upload>>,
}

#[cfg(feature = "processing")]
impl<'a> StoreHandle<'a> {
    pub fn new(store: &'a Addr<Store>, upload: Option<&'a Addr<Upload>>) -> Self {
        Self { store, upload }
    }

    /// Sends a message to the [`Store`] service.
    pub fn store<M>(&self, message: M)
    where
        Store: FromMessage<M>,
    {
        self.store.send(message);
    }

    /// Sends a message to the [`Upload`] service.
    pub fn upload<M>(&self, message: M)
    where
        Upload: FromMessage<M>,
    {
        if let Some(upload) = self.upload {
            upload.send(message);
        } else {
            relay_log::error!("Upload service not configured. Dropping message.");
        }
    }
}

/// A processor output which can be forwarded to a different destination.
pub trait Forward {
    /// Serializes the output into an [`Envelope`].
    ///
    /// All output must be serializable as an envelope.
    fn serialize_envelope(
        self,
        ctx: ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>>;

    /// Serializes the output into a [`crate::services::store::StoreService`] compatible format.
    ///
    /// This function must only be called when Relay is configured to be in processing mode.
    #[cfg(feature = "processing")]
    fn forward_store(self, s: StoreHandle<'_>, ctx: ForwardContext<'_>)
    -> Result<(), Rejected<()>>;
}

/// Context passed to [`Forward`].
///
/// A minified version of [`Context`](super::Context), which does not contain processing specific information.
#[derive(Copy, Clone, Debug)]
pub struct ForwardContext<'a> {
    /// The Relay configuration.
    #[expect(unused, reason = "not yet used")]
    pub config: &'a Config,
    /// A view of the currently active global configuration.
    #[expect(unused, reason = "not yet used")]
    pub global_config: &'a GlobalConfig,
    /// Project configuration associated with the unit of work.
    pub project_info: &'a ProjectInfo,
}

impl ForwardContext<'_> {
    /// Returns the [`Retention`] for a specific type/product.
    pub fn retention<F>(&self, f: F) -> Retention
    where
        F: FnOnce(&RetentionsConfig) -> Option<&RetentionConfig>,
    {
        if let Some(retention) = f(&self.project_info.config.retentions) {
            return Retention::from(*retention);
        }

        self.event_retention()
    }

    /// Returns the event [`Retention`].
    ///
    /// This retention is also often used for older products and can be considered a default
    /// retention for products which do not define their own retention.
    pub fn event_retention(&self) -> Retention {
        Retention::from(RetentionConfig {
            standard: self
                .project_info
                .config
                .event_retention
                .unwrap_or(crate::constants::DEFAULT_EVENT_RETENTION),
            downsampled: self.project_info.config.downsampled_event_retention,
        })
    }
}

/// The [`Nothing`] output.
///
/// Some processors may only produce by-products and not have any output of their own.
pub struct Nothing(std::convert::Infallible);

impl Forward for Nothing {
    fn serialize_envelope(
        self,
        _: ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        match self {}
    }

    #[cfg(feature = "processing")]
    fn forward_store(self, _: StoreHandle<'_>, _: ForwardContext<'_>) -> Result<(), Rejected<()>> {
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
