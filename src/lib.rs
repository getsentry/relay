extern crate chrono;
extern crate cookie;
extern crate debugid;
extern crate failure;
#[macro_use]
extern crate lazy_static;
extern crate geoip;
extern crate itertools;
extern crate regex;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate smallvec;
extern crate url;
extern crate uuid;

#[cfg(test)]
extern crate difference;

// we use macro_use here because we really consider this to be an internal
// macro which currently cannot be imported.  Same thing with the macros mod
#[macro_use]
extern crate general_derive;

#[macro_use]
mod macros;

#[cfg(test)]
#[macro_use]
mod testutils;

pub mod processor;
pub mod protocol;
pub mod store;
pub mod types;
