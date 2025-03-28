use deadpool_redis::redis::Script;
use std::sync::OnceLock;

/// A collection of static methods to load predefined Redis scripts.
pub struct RedisScripts;

impl RedisScripts {
    /// Returns all [`Script`]s.
    pub fn all() -> [&'static Script; 3] {
        [
            Self::load_cardinality(),
            Self::load_global_quota(),
            Self::load_is_rate_limited(),
        ]
    }

    /// Loads the cardinality Redis script.
    pub fn load_cardinality() -> &'static Script {
        static SCRIPT: OnceLock<Script> = OnceLock::new();
        SCRIPT.get_or_init(|| Script::new(include_str!("scripts/cardinality.lua")))
    }

    /// Loads the global quota Redis script.
    pub fn load_global_quota() -> &'static Script {
        static SCRIPT: OnceLock<Script> = OnceLock::new();
        SCRIPT.get_or_init(|| Script::new(include_str!("scripts/global_quota.lua")))
    }

    /// Loads the rate limiting check Redis script.
    pub fn load_is_rate_limited() -> &'static Script {
        static SCRIPT: OnceLock<Script> = OnceLock::new();
        SCRIPT.get_or_init(|| Script::new(include_str!("scripts/is_rate_limited.lua")))
    }
}
