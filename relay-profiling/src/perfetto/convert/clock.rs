use hashbrown::HashMap;

use crate::perfetto::convert::consts;
use crate::perfetto::proto;

/// Tracks the clock snapshots of a Perfetto trace.
///
/// Like the Perfetto trace processor, timestamps are converted with the clock snapshots seen
/// up until the packet. The offsets between clocks change over time, for example `MONOTONIC`
/// does not advance while the device is suspended, which is why only the latest snapshot is
/// used.
///
/// See <https://perfetto.dev/docs/concepts/clock-sync>.
#[derive(Debug, Default)]
pub struct Clocks {
    /// The primary trace clock, as declared by the latest snapshot declaring one.
    primary: Option<ClockId>,
    /// The latest clock snapshot of the trace which can be converted to real time.
    snapshot: Option<Snapshot>,
}

impl Clocks {
    /// Records a [`proto::ClockSnapshot`], replacing the previously recorded snapshot if it can
    /// be converted to real time.
    pub fn update_snapshot(&mut self, cs: &proto::ClockSnapshot) {
        if let Some(primary) = cs.primary_trace_clock {
            self.primary = Some(ClockId::new(primary));
        }

        let snapshot = Snapshot {
            // Builtin clocks only, see `ClockId::is_builtin`.
            timestamps: cs
                .clocks
                .iter()
                .filter_map(|clock| Some((ClockId::new(clock.clock_id?), clock.timestamp?)))
                .filter(|(clock_id, _)| clock_id.is_builtin())
                .collect(),
        };

        if self.primary() == ClockId::REALTIME
            || snapshot.timestamps.contains_key(&ClockId::REALTIME)
        {
            self.snapshot = Some(snapshot);
        }
    }

    /// Returns the offset in nanoseconds which converts a timestamp of the `clock_id` clock to
    /// real time.
    ///
    /// `None` for `clock_id` resolves to the primary trace clock.
    ///
    /// Returns `None` if no clock snapshot was recorded yet or if the clock cannot be converted
    /// to real time.
    pub fn offset_to_realtime(&self, clock_id: Option<ClockId>) -> Option<i128> {
        let snapshot = self.snapshot.as_ref()?;
        let clock_id = clock_id.unwrap_or(self.primary());

        if clock_id == ClockId::REALTIME {
            return Some(0);
        }

        let realtime = snapshot.timestamps.get(&ClockId::REALTIME).copied()?;
        let clock = snapshot.timestamps.get(&clock_id).copied()?;

        Some(i128::from(realtime) - i128::from(clock))
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
    pub fn new(clock_id: u32) -> Self {
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
