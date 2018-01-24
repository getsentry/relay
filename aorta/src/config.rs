use chrono::Duration;

use upstream::UpstreamDescriptor;


/// Holds common config values that affect the aorta behavior.
///
/// This config is typically created by something and then passed down
/// through the aorta functionality to affect how they behave.  This is
/// in turn also used by the trove crate to manage the individual aortas.
#[derive(Debug)]
pub struct AortaConfig {
    /// How long it takes until a snapshot is considered expired.
    pub snapshot_expiry: Duration,
    /// The upstream descriptor for this aorta
    pub upstream: UpstreamDescriptor<'static>,
}

impl Default for AortaConfig {
    fn default() -> AortaConfig {
        AortaConfig {
            snapshot_expiry: Duration::seconds(60),
            upstream: Default::default(),
        }
    }
}
