use redis::Script;
use std::sync::OnceLock;

/// A collection of static methods to load predefined Redis scripts.
pub struct RedisScripts;

impl RedisScripts {
    /// Returns all [`Script`]s.
    pub fn all() -> [&'static Script; 1] {
        [Self::load_is_rate_limited()]
    }

    /// Loads the rate limiting check Redis script.
    pub fn load_is_rate_limited() -> &'static Script {
        static SCRIPT: OnceLock<Script> = OnceLock::new();
        SCRIPT.get_or_init(|| Script::new(include_str!("scripts/is_rate_limited.lua")))
    }
}
