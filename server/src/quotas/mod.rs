#[cfg(feature = "processing")]
mod redis;
#[cfg(feature = "processing")]
pub use self::redis::*;

#[cfg(not(feature = "processing"))]
mod noop;
#[cfg(not(feature = "processing"))]
pub use self::noop::*;
