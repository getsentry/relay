extern crate chrono;
extern crate cookie;
extern crate failure;
extern crate serde;
#[macro_use]
extern crate general_derive;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate debugid;
extern crate smallvec;
extern crate url;
extern crate uuid;

#[cfg(test)]
extern crate difference;

#[macro_use]
mod macros;

#[cfg(test)]
#[macro_use]
mod testutils;

pub mod meta;
pub mod processor;
pub mod protocol;
pub mod types;
