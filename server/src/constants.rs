include!(concat!(env!("OUT_DIR"), "/constants.gen.rs"));

/// Shutdown timeout before killing all tasks and dropping queued events.
pub const SHUTDOWN_TIMEOUT: u16 = 10;
