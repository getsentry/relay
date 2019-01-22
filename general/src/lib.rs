// we use macro_use here because we really consider this to be an internal
// macro which currently cannot be imported.
#[macro_use]
extern crate semaphore_general_derive;

#[macro_use]
extern crate derive_more;

#[macro_use]
mod macros;

#[cfg(test)]
#[macro_use]
mod testutils;

pub mod pii;
pub mod processor;
pub mod protocol;
pub mod store;
pub mod types;
