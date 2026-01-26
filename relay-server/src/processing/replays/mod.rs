use std::sync::Arc;

use relay_dynamic_config::Feature;
use relay_event_normalization::{GeoIpLookup, RawUserAgentInfo, replay};
use relay_event_schema::processor::{self, ProcessingState};
use relay_event_schema::protocol::Replay;
use relay_filter::FilterStatKey;
use relay_pii::{PiiConfigError, PiiProcessor};
use relay_protocol::Annotated;
use relay_quotas::{DataCategory, RateLimits};
use relay_replays::recording::RecordingScrubber;
use relay_statsd::metric;

use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemHeaders, ItemType, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Rejected};

use crate::Envelope;
use crate::processing::{self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::replay::ReplayVideoEvent;
use crate::statsd::{RelayCounters, RelayTimers};

use bytes::Bytes;

pub type Result<T, E = Error> = std::result::Result<T, E>;

mod filter;
mod process;

// FIXME: Come up with more specific error variants and how to map them.
// FIXME: Check if we already have errors that clone
#[derive(Debug, thiserror::Error, Clone)]
pub enum Error {
    #[error("rate limited")]
    RateLimited(RateLimits),
    // Q: When to do this vs: FeatureDisabled(Feature)
    #[error("replay feature flag missing")]
    FilterFeatureFlag,

    /// The Replay event could not be parsed from JSON.
    #[error("invalid json: {0}")]
    CouldNotParse(String),

    #[error("invalid replay video")]
    InvalidReplayVideoEvent,

    #[error("invalid replay")]
    InvalidReplayRecordingEvent,

    /// The Replay event was parsed but did not match the schema.
    #[error("no data found")]
    NoEventContent,

    /// An error occurred during PII scrubbing of the Replay.
    ///
    /// This error is usually returned when the PII configuration fails to parse.
    #[error("failed to scrub PII: {0}")]
    CouldNotScrub(String),

    #[error("invalid item count")]
    InvalidItemCount,

    #[error("replay filtered with reason: {0:?}")]
    ReplayFiltered(FilterStatKey),

    #[error("invalid pii config")]
    PiiConfigError(PiiConfigError),

    #[error("failed to serialize replay")]
    FailedToSerializeReplayEvent,
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        // Q: Is it ok to add some logging here?
        let outcome = match &self {
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            // Q: When to do this vs: Outcome::Invalid(DiscardReason::FeatureDisabled(_))
            Self::FailedToSerializeReplayEvent => None,
            Self::FilterFeatureFlag => None,
            Self::CouldNotParse(_) => Some(Outcome::Invalid(DiscardReason::InvalidReplayEvent)),
            Self::InvalidReplayVideoEvent => {
                Some(Outcome::Invalid(DiscardReason::InvalidReplayVideoEvent))
            }
            Self::NoEventContent => {
                Some(Outcome::Invalid(DiscardReason::InvalidReplayEventNoPayload))
            }
            Self::CouldNotScrub(_) => Some(Outcome::Invalid(DiscardReason::InvalidReplayEventPii)),
            // TODO: Not super happy with this
            // (0,1) (1,0) => mismatch (check how attachments does it)
            // (1,1)
            // (n,n) => duplicate
            Self::InvalidItemCount => Some(Outcome::Invalid(DiscardReason::DuplicateItem)),
            Self::ReplayFiltered(key) => Some(Outcome::Filtered(key.clone())),
            Self::PiiConfigError(_) => Some(Outcome::Invalid(DiscardReason::ProjectStatePii)),
            Self::InvalidReplayRecordingEvent => {
                Some(Outcome::Invalid(DiscardReason::InvalidReplayRecordingEvent))
            }
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
        // FIXME: Does it make sense to check for ctx.processing here?
        let replays = filter::feature_flag(replays, ctx)?;
        let mut replays = process::expand(replays);

        validate(&mut replays);
        normalize(&mut replays, &self.geoip_lookup);
        scrub(&mut replays, ctx);
        filter(&mut replays, ctx);
        processing(&mut replays, ctx);

        let replays = self.limiter.enforce_quotas(replays, ctx).await?;
        Ok(Output::just(ReplaysOutput(replays)))
    }
}

pub fn validate(replays: &mut Managed<ExpandedReplays>) {
    replays.retain(
        |replays| &mut replays.replays,
        |replay, _| {
            // TODO: Update the error on this to be in line with the actual ones.
            match replay.get_event().value() {
                Some(event) => replay::validate(event).map_err(|_| Error::NoEventContent),
                None => Err(Error::NoEventContent),
            }
        },
    )
}

pub fn normalize(replays: &mut Managed<ExpandedReplays>, geoip_lookup: &GeoIpLookup) {
    // Q: We now construct our config here on demand, is that ok, or do we want our own config?
    let meta = replays.headers.meta();
    let client_addr = meta.client_addr();
    let user_agent = RawUserAgentInfo {
        user_agent: meta.user_agent().map(String::from),
        client_hints: meta.client_hints().to_owned(),
    };

    replays.modify(|replay, _| {
        for replay in replay.replays.iter_mut() {
            replay::normalize(
                replay.get_event(),
                client_addr,
                &user_agent.as_deref(), // FIXME: Get rid of this.
                geoip_lookup,
            )
        }
    })
}

pub fn scrub_event(event: &mut Annotated<Replay>, ctx: Context<'_>) -> Result<(), Error> {
    if let Some(ref config) = ctx.project_info.config.pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        processor::process_value(event, &mut processor, ProcessingState::root())
            .map_err(|e| Error::CouldNotScrub(e.to_string()))?;
    }

    let pii_config = ctx
        .project_info
        .config
        .datascrubbing_settings
        .pii_config()
        .map_err(|e| Error::CouldNotScrub(e.to_string()))?;

    if let Some(config) = pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        processor::process_value(event, &mut processor, ProcessingState::root())
            .map_err(|e| Error::CouldNotScrub(e.to_string()))?;
    };
    Ok(())
}

pub fn scrub(replays: &mut Managed<ExpandedReplays>, ctx: Context<'_>) {
    replays.retain(
        |replays| &mut replays.replays,
        |replay, _| scrub_event(replay.get_event(), ctx),
    );
}

pub fn filter(replays: &mut Managed<ExpandedReplays>, ctx: Context<'_>) {
    let client_addr = replays.headers.meta().client_addr();
    let event_id = replays.headers.event_id();

    replays.retain(
        |replays| &mut replays.replays,
        |replay, _| {
            // FIXME: technically should never happen since we already check this earlier.
            let Some(event) = replay.get_event().value() else {
                return Err(Error::NoEventContent);
            };

            relay_filter::should_filter(
                event,
                client_addr,
                &ctx.project_info.config.filter_settings,
                ctx.global_config.filters(),
            )
            .map_err(Error::ReplayFiltered)?;

            // Log segments that exceed the hour limit so we can diagnose errant SDKs
            // or exotic customer implementations.
            if let Some(segment_id) = event.segment_id.value()
                && *segment_id > 720
            {
                metric!(counter(RelayCounters::ReplayExceededSegmentLimit) += 1);

                relay_log::debug!(
                    event_id = ?event_id,
                    project_id = ctx.project_info.project_id.map(|v| v.value()),
                    organization_id = ctx.project_info.organization_id.map(|o| o.value()),
                    segment_id = segment_id,
                    "replay segment-exceeded-limit"
                );
            }

            Ok::<_, Error>(())
        },
    )
}

pub fn processing(replays: &mut Managed<ExpandedReplays>, ctx: Context<'_>) {
    // FIXME: Q: I think ideally we would want to move this check up to the other filter checks no?
    if !ctx
        .project_info
        .has_feature(Feature::SessionReplayRecordingScrubbing)
    {
        return;
    }

    let event_id = replays.headers.event_id();

    replays.retain(
        |replays| &mut replays.replays,
        |replay, _| {
            let datascrubbing_config = ctx
                .project_info
                .config
                .datascrubbing_settings
                .pii_config()
                .map_err(|e| Error::PiiConfigError(e.clone()))?
                .as_ref();

            let mut scrubber = RecordingScrubber::new(
                ctx.config.max_replay_uncompressed_size(),
                ctx.project_info.config.pii_config.as_ref(),
                datascrubbing_config,
            );

            if scrubber.is_empty() {
                return Ok::<(), Error>(());
            }

            let payload = replay.get_recording();
            *payload = metric!(timer(RelayTimers::ReplayRecordingProcessing), {
                scrubber.process_recording(payload)
            })
            .map(Into::into)
            .map_err(|error| {
                relay_log::debug!(
                    error = &error as &dyn std::error::Error,
                    event_id = ?event_id,
                    project_id = ctx.project_info.project_id.map(|v| v.value()),
                    organization_id = ctx.project_info.organization_id.map(|o| o.value()),
                    "invalid replay recording"
                );
                Error::InvalidReplayRecordingEvent
            })?;
            Ok::<(), Error>(())
        },
    );
}

#[derive(Debug)]
pub struct ReplaysOutput(Managed<ExpandedReplays>);

impl Forward for ReplaysOutput {
    fn serialize_envelope(
        self,
        _ctx: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        Ok(self.0.map(|s, r| {
            let ExpandedReplays { headers, replays } = s;
            let mut items = Items::new();
            let event_id = headers.event_id();

            for replay in replays {
                match replay {
                    ExpandedReplay::WebReplay {
                        event_header,
                        recording_header,
                        event,
                        recording,
                    } => {
                        // Parse the replay again.
                        match event.to_json() {
                            Ok(json) => {
                                let event: Bytes = json.into_bytes().into();

                                // items.push(Item::from_parts(event_header, event));
                                let mut item = Item::new(ItemType::ReplayEvent);
                                item.set_payload(ContentType::Json, event);
                                items.push(item);

                                items.push(Item::from_parts(recording_header, recording));
                            }
                            Err(error) => {
                                relay_log::error!(
                                    error = &error as &dyn std::error::Error,
                                    event_id = ?event_id,
                                    "failed to serialize replay"
                                );

                                // FIXME: Not a super fan of this.
                                r.reject_err(
                                    Error::FailedToSerializeReplayEvent,
                                    ExpandedReplay::WebReplay {
                                        event_header,
                                        recording_header,
                                        event,
                                        recording,
                                    },
                                );
                            }
                        }
                    }
                    ExpandedReplay::NativeReplay {
                        video_header,
                        event,
                        recording,
                        video,
                    } => {
                        // FIXME: Kind of duplicate logic
                        let event_bytes: Bytes = match event.to_json() {
                            Ok(json) => json.into_bytes().into(),
                            Err(error) => {
                                relay_log::error!(
                                    error = &error as &dyn std::error::Error,
                                    event_id = ?event_id,
                                    "failed to serialize replay"
                                );

                                r.reject_err(
                                    Error::FailedToSerializeReplayEvent,
                                    ExpandedReplay::NativeReplay {
                                        video_header,
                                        event,
                                        recording,
                                        video,
                                    },
                                );
                                continue;
                            }
                        };

                        let video_event = ReplayVideoEvent {
                            replay_event: event_bytes,
                            replay_recording: recording,
                            replay_video: video,
                        };
                        match rmp_serde::to_vec_named(&video_event) {
                            Ok(payload) => {
                                items.push(Item::from_parts(video_header, payload.into()));
                            }
                            Err(_) => {
                                // FIXME: Come up with a better error here.
                                // Err(ProcessingError::InvalidReplay(DiscardReason::InvalidReplayVideoEvent,))
                                r.reject_err(Error::FailedToSerializeReplayEvent, video_event);
                            }
                        }
                    }
                }
            }

            Envelope::from_parts(headers, items)
        }))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        // FIXME: Already construct the kafka message here?

        let envelope = self.serialize_envelope(ctx)?;
        let envelope = ManagedEnvelope::from(envelope).into_processed();

        s.store(crate::services::store::StoreEnvelope { envelope });

        Ok(())
    }
}

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

#[derive(Debug)]
pub struct ExpandedReplays {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    // FIXME: Might not need to be a vec in the future.
    /// Expanded replays
    replays: Vec<ExpandedReplay>,
}

impl Counted for SerializedReplays {
    fn quantities(&self) -> crate::managed::Quantities {
        smallvec::smallvec![(
            DataCategory::Replay,
            self.events.len() + self.recordings.len() + self.videos.len()
        )]
    }
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

// FIXME: Better understand why this is needed.
impl CountRateLimited for Managed<ExpandedReplays> {
    type Error = Error;
}

#[derive(Debug)]
// FIXME: Come up with some better naming here.
// FIXME: Check if we can safely_ignore this warning
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
