use hashbrown::HashMap;

use crate::perfetto::convert::budget::BudgetExceeded;
use crate::perfetto::convert::consts;
use crate::perfetto::convert::utils::{self, SequenceId};
use crate::perfetto::proto;

/// Tracks the clocks of a Perfetto trace.
///
/// Like the Perfetto trace processor, timestamps are converted with the clock snapshots seen
/// up until the packet. The offsets between clocks change over time, for example `MONOTONIC`
/// does not advance while the device is suspended, which is why only the latest snapshot is
/// used.
///
/// See <https://perfetto.dev/docs/concepts/clock-sync>.
#[derive(Debug, Default)]
pub struct Clocks {
    /// Clock defaulted per sequence via `TracePacketDefaults`.
    defaults: HashMap<SequenceId, ClockId>,
    /// The primary trace clock, as declared by the latest snapshot declaring one.
    primary: Option<ClockId>,
    /// The latest clock snapshot of the trace which can be converted to real time.
    snapshot: Option<Snapshot>,
}

impl Clocks {
    /// Records the sequence defaults contained in the `packet`.
    ///
    /// Like interned data, sequence defaults are part of the incremental state of a sequence and
    /// are dropped when it is cleared.
    pub fn update_defaults(&mut self, packet: &proto::TracePacket) -> Result<(), BudgetExceeded> {
        let seq_id = SequenceId::new(packet);

        // Technically this is off by-one, but significantly easier to handle.
        if self.defaults.len() >= consts::MAX_SEQUENCE_IDS {
            return Err(BudgetExceeded);
        }

        if utils::has_incremental_state_cleared(packet) {
            self.defaults.remove(&seq_id);
        }

        let Some(defaults) = packet.trace_packet_defaults else {
            return Ok(());
        };
        match defaults.timestamp_clock_id {
            Some(clock_id) => self.defaults.insert(seq_id, ClockId::new(clock_id)),
            None => self.defaults.remove(&seq_id),
        };

        Ok(())
    }

    /// Records a [`proto::ClockSnapshot`], replacing the previously recorded snapshot.
    ///
    /// Snapshots without `REALTIME` cannot be converted to real time and are ignored entirely.
    pub fn update_snapshot(&mut self, cs: &proto::ClockSnapshot) {
        let snapshot = Snapshot {
            // Builtin clocks only, see `ClockId::is_builtin`.
            timestamps: cs
                .clocks
                .iter()
                .filter_map(|clock| Some((ClockId::new(clock.clock_id?), clock.timestamp?)))
                .filter(|(clock_id, _)| clock_id.is_builtin())
                .collect(),
        };

        if !snapshot.timestamps.contains_key(&ClockId::REALTIME) {
            return;
        }

        if let Some(primary) = cs.primary_trace_clock {
            self.primary = Some(ClockId::new(primary));
        }
        self.snapshot = Some(snapshot);
    }

    /// Converts the timestamp of the `packet` to real time in nanoseconds.
    ///
    /// Returns `None` if the clock of the `packet` cannot be converted to real time, for example
    /// because no clock snapshot was recorded yet.
    pub fn to_realtime(&self, packet: &proto::TracePacket) -> Option<i128> {
        let timestamp = i128::from(packet.timestamp.unwrap_or(0));

        let clock_id = self.clock_id(packet);
        if clock_id == ClockId::REALTIME {
            return Some(timestamp);
        }

        let snapshot = self.snapshot.as_ref()?;
        let realtime = snapshot.timestamps.get(&ClockId::REALTIME).copied()?;
        let clock = snapshot.timestamps.get(&clock_id).copied()?;

        Some(timestamp + i128::from(realtime) - i128::from(clock))
    }

    /// Returns the clock the timestamp of the `packet` is recorded in.
    ///
    /// The clock of a packet is, in order of precedence, its own `timestamp_clock_id`, the
    /// `timestamp_clock_id` defaulted for its sequence, or the primary trace clock.
    fn clock_id(&self, packet: &proto::TracePacket) -> ClockId {
        match packet.timestamp_clock_id {
            Some(clock_id) => ClockId::new(clock_id),
            None => self
                .defaults
                .get(&SequenceId::new(packet))
                .copied()
                .unwrap_or_else(|| self.primary()),
        }
    }

    /// Returns the primary trace clock, which defaults to `BOOTTIME`.
    fn primary(&self) -> ClockId {
        self.primary.unwrap_or(ClockId::BOOTTIME)
    }
}

/// A Perfetto clock id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClockId(u32);

impl ClockId {
    /// Perfetto builtin real time clock.
    const REALTIME: Self = Self(consts::CLOCK_REALTIME);
    /// Perfetto builtin boot time clock.
    const BOOTTIME: Self = Self(consts::CLOCK_BOOTTIME);

    /// Creates a new [`Self`] from a raw Perfetto clock id.
    fn new(clock_id: u32) -> Self {
        Self(clock_id)
    }

    /// Returns `true` if this is one of the globally known, builtin clocks.
    ///
    /// Clocks beyond the builtin range are custom clocks, either scoped to a single sequence or
    /// global. They may carry a unit multiplier or be incremental, neither of which is decoded
    /// here.
    fn is_builtin(self) -> bool {
        self.0 <= consts::BUILTIN_CLOCK_MAX_ID
    }
}

/// The clocks of a trace at a single point in time.
#[derive(Debug)]
struct Snapshot {
    /// Timestamp of every builtin clock at the time of the snapshot.
    timestamps: HashMap<ClockId, u64>,
}
