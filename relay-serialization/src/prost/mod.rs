//! Deserialization routines for prost.
//! This implements a scanner to record the number of "operations" needed to decode a proto,
//! allowing a caller to enforce a hard limit on how much work to be done.
mod scan;

pub use scan::Error;
pub use scan::MessageDesc;
pub use scan::decode;
pub use scan::scan;
