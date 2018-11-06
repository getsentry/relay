extern crate chrono;
extern crate cookie;
extern crate debugid;
extern crate failure;
extern crate general_derive;
extern crate itertools;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;
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

mod meta;
mod types;

pub mod processor;
pub mod protocol;
pub mod store;
