extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate uuid;
#[macro_use]
extern crate general_derive;
extern crate smallvec;
#[macro_use]
extern crate failure;
extern crate chrono;
extern crate cookie;
#[cfg(test)]
extern crate difference;
extern crate url;

#[macro_use]
mod macros;

#[cfg(test)]
#[macro_use]
mod testutils;

pub mod meta;
pub mod processor;
pub mod protocol;
pub mod types;
