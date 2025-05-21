//! A new experimental way to structure Relay's processing pipeline.
//!
//! The idea is to slowly move processing steps from the original
//! [`processor`](crate::services::processor) module into this module.
//! This is where all the product processing logic should be located or called from.
//!
//! The processor service, will then do its actual work using the processing logic defined here.

use crate::services::processor::ProcessingError;
use crate::utils::ManagedEnvelope;

pub mod logs;

/// A processor, for an arbitrary unit of work extracted from an envelope.
///
/// The processor takes items from an envelope, to process it.
/// To fully process an envelope, multiple processor may need to take items from the same envelope.
///
/// Event based envelopes should only be handled by a single processor, as the protocol
/// defines all items in an event based envelope to relate to the envelope.
pub trait Processor {
    type UnitOfWork;

    /// The result after processing a [`Self::UnitOfWork`].
    type Result;
    /// The error returned by [`Self::process`].
    ///
    /// Implementations should use specific errors, instead of returning
    /// [`ProcessingError`] directly.
    type Error: Into<ProcessingError>;

    /// Extracts a [`Self::UnitOfWork`] from a [`ManagedEnvelope`].
    ///
    /// This is infallible, if a processor wants to report an error,
    /// it should return a [`Self::UnitOfWork`] which later, can produce an error when being
    ///
    // TODO: better name
    // TODO: specify that the unit of work needs to be tracked, maybe encode that as a Track<UnitOfWork>,
    // for outcomes etc.
    // Maybe unit of work needs to implement a trait which can describe contents for outcomes etc,
    // emitted by `Track`.
    fn prepare(&self, envelope: &mut ManagedEnvelope) -> Option<UnitOfWork>;

    // TODO: move processing error when it makes sense
    // TODO: we may need multiple different entry points, one for each mode:
    //  - processing
    //  - managed internal (pop)
    //  - managed external
    //  - proxy
    //  - static
    // TODO: Context, project config etc.
    fn process(&self, work: UnitOfWork) -> Result<Self::Result, Self::Error>;
}
