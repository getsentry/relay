include!(concat!(env!("OUT_DIR"), "/constants.gen.rs"));

/// Timeout in seconds for batched requests.
///
/// Actors such as `KeyManager` and `ProjectManager` will wait this duration before sending an
/// upstream query.
///
/// TODO: Refactor this into config.
pub static BATCH_TIMEOUT: u64 = 1;

/// Timeout in seconds before cached public keys expire.
pub static PUBLIC_KEY_EXPIRY: u64 = 3600;

/// Timeout in seconds before caches expire for missing projects.
pub static MISSING_PUBLIC_KEY_EXPIRY: u64 = 60;
