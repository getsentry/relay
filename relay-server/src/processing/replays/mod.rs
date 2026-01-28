use std::sync::Arc;

use bytes::Bytes;

use relay_event_normalization::GeoIpLookup;
use relay_event_schema::protocol::Replay;
use relay_filter::FilterStatKey;
use relay_pii::PiiConfigError;
use relay_protocol::Annotated;
use relay_quotas::{DataCategory, RateLimits};
use serde::{Deserialize, Serialize};

use crate::envelope::{EnvelopeHeaders, Item, ItemHeaders, ItemType};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Rejected};
use crate::processing::{self, Context, CountRateLimited, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};

pub type Result<T, E = Error> = std::result::Result<T, E>;

mod filter;
mod forward;
mod process;
mod validate;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Replays filtered because of a missing feature flag.
    #[error("replay feature flag missing")]
    FilterFeatureFlag,

    // TODO:: Q: Add a more detailed version here, for the (0,n) case?
    /// There is an invalid amount of `replay_event` and `replay_recording` items in the envelope.
    ///
    /// There should be either 0 of both or 1 of both.
    #[error("invalid item count")]
    InvalidItemCount,

    /// The Replay event could not be parsed from JSON.
    #[error("invalid json: {0}")]
    CouldNotParseEvent(String),

    /// The Replay event was parsed but did not match the schema.
    #[error("no data found")]
    NoEventContent,

    /// The Replay contains invalid data or is missing a required field.
    #[error("invalid payload {0}")]
    InvalidPayload(String),

    /// The Replay video could not be parsed.
    #[error("invalid replay video")]
    InvalidReplayVideoEvent,

    /// The processing of the Replay Recording failed.
    #[error("invalid replay")]
    InvalidReplayRecordingEvent,

    // FIXME: Think about merging these since having both seems a bit overkill.
    /// The PII config for scrubbing the recording could not be loaded.
    #[error("invalid pii config")]
    PiiConfigError(PiiConfigError),

    /// An error occurred during PII scrubbing of the Replay.
    ///
    /// This error is usually returned when the PII configuration fails to parse.
    #[error("failed to scrub PII: {0}")]
    CouldNotScrub(String),

    /// The Replays are rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),

    /// Replays filtered due to a filtering rule
    #[error("replay filtered with reason: {0:?}")]
    Filtered(FilterStatKey),

    /// Failed to re-serialize the replay event.
    #[error("failed to serialize replay")]
    FailedToSerializeReplayEvent,
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::FilterFeatureFlag => None,
            Self::InvalidItemCount => Some(Outcome::Invalid(DiscardReason::DuplicateItem)),
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
            Self::PiiConfigError(_) => Some(Outcome::Invalid(DiscardReason::ProjectStatePii)),
            Self::CouldNotScrub(_) => Some(Outcome::Invalid(DiscardReason::InvalidReplayEventPii)),

            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            Self::Filtered(key) => Some(Outcome::Filtered(key.clone())),
            Self::FailedToSerializeReplayEvent => Some(Outcome::Invalid(DiscardReason::Internal)),
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

// TODO: Q: We seems to use both `processing::Processor` and `Processor` ask about the preference.
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
            .take_items_by(|item| matches!(item.ty(), ItemType::ReplayEvent))
            .into_vec();

        let recordings = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.ty(), ItemType::ReplayRecording))
            .into_vec();

        let videos = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.ty(), ItemType::ReplayVideo))
            .into_vec();

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
        // TODO: Q: Does it make sense to check for ctx.processing here?
        let replays = filter::feature_flag(replays, ctx)?;
        let mut replays = process::expand(replays);

        validate::validate(&mut replays);
        process::normalize(&mut replays, &self.geoip_lookup);
        process::scrub(&mut replays, ctx);
        filter::filter(&mut replays, ctx);
        process::scrub_recording(&mut replays, ctx);

        let replays = self.limiter.enforce_quotas(replays, ctx).await?;
        Ok(Output::just(ReplaysOutput(replays)))
    }
}

/// Serialized replays extracted from an envelope.
#[derive(Debug)]
pub struct SerializedReplays {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// A list of replay events.
    events: Vec<Item>,
    /// A list of replay recordings.
    recordings: Vec<Item>,
    /// A list of replay videos.
    videos: Vec<Item>,
}

impl Counted for SerializedReplays {
    fn quantities(&self) -> crate::managed::Quantities {
        smallvec::smallvec![(
            DataCategory::Replay,
            self.events.len() + self.recordings.len() + self.videos.len()
        )]
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ReplayVideoEvent {
    pub replay_event: Bytes,
    pub replay_recording: Bytes,
    pub replay_video: Bytes,
}

impl Counted for ReplayVideoEvent {
    fn quantities(&self) -> crate::managed::Quantities {
        smallvec::smallvec![(DataCategory::Replay, 1)]
    }
}

/// Replays which have been parsed and expanded from their serialized state.
#[derive(Debug)]
pub struct ExpandedReplays {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    // FIXME: Might not need to be a vec in the future.
    /// Expanded replays
    replays: Vec<ExpandedReplay>,
}

impl Counted for ExpandedReplays {
    fn quantities(&self) -> crate::managed::Quantities {
        let count: usize = self
            .replays
            .iter()
            .map(|r| match r {
                ExpandedReplay::WebReplay { .. } => 2,
                ExpandedReplay::NativeReplay { .. } => 1,
            })
            .sum();
        smallvec::smallvec![(DataCategory::Replay, count)]
    }
}

impl CountRateLimited for Managed<ExpandedReplays> {
    type Error = Error;
}

#[derive(Debug)]
// FIXME: Come up with some better naming here.
// FIXME: Check if we can safely_ignore this warning
/// An expanded Replay.
///
/// Either a web replay, not containing a video or a native replay with a video.
enum ExpandedReplay {
    WebReplay {
        event_header: ItemHeaders,
        recording_header: ItemHeaders,
        event: Annotated<Replay>,
        recording: Bytes,
    },
    NativeReplay {
        video_header: ItemHeaders,
        event: Annotated<Replay>,
        recording: Bytes,
        video: Bytes,
    },
}

impl Counted for ExpandedReplay {
    fn quantities(&self) -> crate::managed::Quantities {
        match self {
            ExpandedReplay::WebReplay { .. } => {
                smallvec::smallvec![(DataCategory::Replay, 2)]
            }
            ExpandedReplay::NativeReplay { .. } => {
                smallvec::smallvec![(DataCategory::Replay, 1)]
            }
        }
    }
}

impl ExpandedReplay {
    fn get_event(&mut self) -> &mut Annotated<Replay> {
        match self {
            ExpandedReplay::WebReplay {
                event_header: _,
                recording_header: _,
                event,
                recording: _,
            } => event,
            ExpandedReplay::NativeReplay {
                video_header: _,
                event,
                recording: _,
                video: _,
            } => event,
        }
    }

    fn get_recording(&mut self) -> &mut Bytes {
        match self {
            ExpandedReplay::WebReplay {
                event_header: _,
                recording_header: _,
                event: _,
                recording,
            } => recording,
            ExpandedReplay::NativeReplay {
                video_header: _,
                event: _,
                recording,
                video: _,
            } => recording,
        }
    }
}

#[derive(Debug)]
pub struct ReplaysOutput(Managed<ExpandedReplays>);
