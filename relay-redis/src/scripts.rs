use redis::Script;
use std::sync::OnceLock;

/// A collection of static methods to load predefined Redis scripts.
pub struct RedisScripts;

impl RedisScripts {
    /// Loads the cardinality Redis script.
    pub fn load_cardinality() -> &'static RedisScript {
        static SCRIPT: OnceLock<RedisScript> = OnceLock::new();
        SCRIPT.get_or_init(|| RedisScript::new(include_str!("scripts/cardinality.lua")))
    }

    /// Loads the global quota Redis script.
    pub fn load_global_quota() -> &'static RedisScript {
        static SCRIPT: OnceLock<RedisScript> = OnceLock::new();
        SCRIPT.get_or_init(|| RedisScript::new(include_str!("scripts/global_quota.lua")))
    }

    /// Loads the rate limiting check Redis script.
    pub fn load_is_rate_limited() -> &'static RedisScript {
        static SCRIPT: OnceLock<RedisScript> = OnceLock::new();
        SCRIPT.get_or_init(|| RedisScript::new(include_str!("scripts/is_rate_limited.lua")))
    }
}

/// Represents a Redis script with its code and compiled form.
pub struct RedisScript {
    code: String,
    script: Script,
}

impl RedisScript {
    /// Creates a new `RedisScript` instance from the provided Lua code.
    pub fn new(code: &str) -> Self {
        RedisScript {
            code: code.to_string(),
            script: Script::new(code),
        }
    }

    /// Returns the Lua code of the Redis script.
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Returns the SHA1 hash of the Redis script.
    pub fn hash(&self) -> &str {
        self.script.get_hash()
    }

    /// Returns a reference to the compiled Redis script.
    pub fn script(&self) -> &Script {
        &self.script
    }
}
