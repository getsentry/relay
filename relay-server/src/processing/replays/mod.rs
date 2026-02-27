use std::sync::Arc;

use bytes::Bytes;

use relay_event_normalization::GeoIpLookup;
use relay_event_schema::processor::ProcessingAction;
use relay_event_schema::protocol::Replay;
use relay_filter::FilterStatKey;
use relay_pii::PiiConfigError;
use relay_protocol::Annotated;
use relay_quotas::{DataCategory, RateLimits};
use serde::{Deserialize, Serialize};

use crate::envelope::{EnvelopeHeaders, ItemType, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Rejected};
use crate::processing::{self, Context, CountRateLimited, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};

pub type Result<T, E = Error> = std::result::Result<T, E>;

mod filter;
mod forward;
mod process;
#[cfg(feature = "processing")]
mod store;
mod validate;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Replays filtered because of a missing feature flag.
    #[error("replay feature flag missing")]
    FilterFeatureFlag,

    /// There is an invalid number of `replay_event`, `replay_recording` and `replay_video` items in
    /// the envelope.
    ///
    /// Valid quantity combinations: (0, 0, 0), (1, 1, 0), (0, 1, 0) or (0, 0, 1).
    /// Standalone recordings are supported for SDK compatibility.
    #[error("invalid item count")]
    InvalidItemCount,

    /// Replay event without a recording.
    #[error("replay event without recording")]
    EventWithoutRecording,

    /// The Replay event could not be parsed from JSON.
    #[error("invalid json: {0}")]
    CouldNotParseEvent(#[from] serde_json::Error),

    /// The Replay event was parsed but did not match the schema.
    #[error("no data found")]
    NoEventContent,

    /// The Replay contains invalid data or is missing a required field.
    #[error("replay validation failed: {0}")]
    InvalidPayload(#[from] relay_event_normalization::replay::ReplayError),

    /// The Replay video could not be parsed.
    #[error("invalid replay video")]
    InvalidReplayVideoEvent,

    /// The processing of the Replay Recording failed.
    #[error("invalid replay")]
    InvalidReplayRecordingEvent,

    /// The PII config could not be loaded.
    #[error("invalid pii config")]
    PiiConfig(PiiConfigError),

    /// An error occurred during PII scrubbing of the Replay.
    #[error("failed to scrub PII: {0}")]
    CouldNotScrub(#[from] ProcessingAction),

    /// The Replay was rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),

    /// Replay filtered due to a filtering rule.
    #[error("replay filtered with reason: {0}")]
    Filtered(FilterStatKey),

    /// Failed to re-serialize the replay.
    #[cfg(feature = "processing")]
    #[error("failed to serialize replay")]
    FailedToSerializeReplay,

    /// Replay recording too large for the consumer.
    #[cfg(feature = "processing")]
    #[error("replay recording too large")]
    TooLarge,

    /// The envelope did not contain an event ID.
    #[cfg(feature = "processing")]
    #[error("missing replay ID")]
    NoEventId,
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::FilterFeatureFlag => None,
            Self::InvalidItemCount => Some(Outcome::Invalid(DiscardReason::DuplicateItem)),
            Self::EventWithoutRecording => Some(Outcome::Invalid(
                DiscardReason::InvalidReplayMissingRecording,
            )),
            Self::CouldNotParseEvent(_) => {
                Some(Outcome::Invalid(DiscardReason::InvalidReplayEvent))
            }
            Self::NoEventContent => {
                Some(Outcome::Invalid(DiscardReason::InvalidReplayEventNoPayload))
            }
            Self::InvalidPayload(_) => Some(Outcome::Invalid(DiscardReason::InvalidReplayEvent)),
            Self::InvalidReplayVideoEvent => {
                Some(Outcome::Invalid(DiscardReason::InvalidReplayVideoEvent))
            }
            Self::InvalidReplayRecordingEvent => {
                Some(Outcome::Invalid(DiscardReason::InvalidReplayRecordingEvent))
            }
            Self::PiiConfig(_) => Some(Outcome::Invalid(DiscardReason::ProjectStatePii)),
            Self::CouldNotScrub(_) => Some(Outcome::Invalid(DiscardReason::InvalidReplayEventPii)),

            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            Self::Filtered(key) => Some(Outcome::Filtered(key.clone())),
            #[cfg(feature = "processing")]
            Self::FailedToSerializeReplay => Some(Outcome::Invalid(DiscardReason::Internal)),
            #[cfg(feature = "processing")]
            Self::TooLarge => Some(Outcome::Invalid(DiscardReason::TooLarge(
                crate::services::outcome::DiscardItemType::ReplayRecording,
            ))),
            #[cfg(feature = "processing")]
            Self::NoEventId => Some(Outcome::Invalid(DiscardReason::Internal)),
        };
        (outcome, self)
    }
}

impl From<RateLimits> for Error {
    fn from(value: RateLimits) -> Self {
        Self::RateLimited(value)
    }
}

/// A processor for Replays.
pub struct ReplaysProcessor {
    limiter: Arc<QuotaRateLimiter>,
    geoip_lookup: GeoIpLookup,
}

impl ReplaysProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>, geoip_lookup: GeoIpLookup) -> Self {
        Self {
            limiter,
            geoip_lookup,
        }
    }
}

impl processing::Processor for ReplaysProcessor {
    type UnitOfWork = SerializedReplays;
    type Output = ReplaysOutput;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let headers = envelope.envelope().headers().clone();
        let events = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.ty(), ItemType::ReplayEvent));

        let recordings = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.ty(), ItemType::ReplayRecording));

        let videos = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.ty(), ItemType::ReplayVideo));

        // If there are no events, recordings or videos there is nothing for us to do.
        if events.is_empty() && recordings.is_empty() && videos.is_empty() {
            return None;
        }

        let work = SerializedReplays {
            headers,
            events,
            recordings,
            videos,
        };

        Some(Managed::with_meta_from(envelope, work))
    }

    async fn process(
        &self,
        replays: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let replays = filter::feature_flag(replays, ctx)?;
        let mut replay = process::expand(replays)?;

        validate::validate(&replay).reject(&replay)?;
        process::normalize(&mut replay, &self.geoip_lookup);
        filter::filter(&replay, ctx).reject(&replay)?;

        let mut replay = self.limiter.enforce_quotas(replay, ctx).await?;

        process::scrub(&mut replay, ctx)?;

        Ok(Output::just(ReplaysOutput(replay)))
    }
}

/// Serialized replays extracted from an envelope.
#[derive(Debug)]
pub struct SerializedReplays {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// A list of replay events.
    events: Items,
    /// A list of replay recordings.
    recordings: Items,
    /// A list of replay videos.
    videos: Items,
}

impl Counted for SerializedReplays {
    fn quantities(&self) -> crate::managed::Quantities {
        smallvec::smallvec![(
            DataCategory::Replay,
            // There is a discrepancy between how 'web replays' and 'native replays' are counted.
            // For 'web replays' we count the replay_event and the replay_recording separately.
            // For 'native replays' we count the replay_video as one even though it contains
            // a replay_event and replay_recording.
            self.events.len() + self.recordings.len() + self.videos.len()
        )]
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReplayVideoEvent {
    /// Replay event (json).
    pub replay_event: Bytes,
    /// Replay recording data (rrweb).
    pub replay_recording: Bytes,
    /// Replay video (raw bytes).
    pub replay_video: Bytes,
}

impl Counted for ReplayVideoEvent {
    fn quantities(&self) -> crate::managed::Quantities {
        smallvec::smallvec![(DataCategory::Replay, 1)]
    }
}

/// A Replay which has been parsed and expanded from its serialized state.
#[derive(Debug)]
pub struct ExpandedReplay {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// Replay payload.
    payload: ReplayPayload,
}

impl Counted for ExpandedReplay {
    fn quantities(&self) -> crate::managed::Quantities {
        self.payload.quantities()
    }
}

impl CountRateLimited for Managed<ExpandedReplay> {
    type Error = Error;
}

/// The payload of an [`ExpandedReplay`].
#[derive(Debug)]
enum ReplayPayload {
    /// A web replay (event + rrweb recording)
    WebReplay {
        event: Annotated<Replay>,
        recording: Bytes,
    },
    /// A native replay (event + rrweb recording + video)
    NativeReplay {
        event: Annotated<Replay>,
        recording: Bytes,
        video: Bytes,
    },
    /// A standalone replay recording.
    ///
    /// Although a `replay_event` and `replay_recording` should always be sent together in an
    /// envelope, this is not the case for some SDKs. As such, support this to not break them.
    StandaloneRecording { recording: Bytes },
}

impl Counted for ReplayPayload {
    fn quantities(&self) -> crate::managed::Quantities {
        match self {
            ReplayPayload::WebReplay { .. } => {
                smallvec::smallvec![(DataCategory::Replay, 2)]
            }
            ReplayPayload::NativeReplay { .. } | ReplayPayload::StandaloneRecording { .. } => {
                smallvec::smallvec![(DataCategory::Replay, 1)]
            }
        }
    }
}

impl ReplayPayload {
    fn event(&self) -> Option<&Annotated<Replay>> {
        match self {
            ReplayPayload::WebReplay { event, .. } => Some(event),
            ReplayPayload::NativeReplay { event, .. } => Some(event),
            ReplayPayload::StandaloneRecording { .. } => None,
        }
    }

    fn event_mut(&mut self) -> Option<&mut Annotated<Replay>> {
        match self {
            ReplayPayload::WebReplay { event, .. } => Some(event),
            ReplayPayload::NativeReplay { event, .. } => Some(event),
            ReplayPayload::StandaloneRecording { .. } => None,
        }
    }

    fn recording_mut(&mut self) -> &mut Bytes {
        match self {
            ReplayPayload::WebReplay { recording, .. } => recording,
            ReplayPayload::NativeReplay { recording, .. } => recording,
            ReplayPayload::StandaloneRecording { recording } => recording,
        }
    }
}

/// Output produced by the [`ReplaysProcessor`].
#[derive(Debug)]
pub struct ReplaysOutput(Managed<ExpandedReplay>);
