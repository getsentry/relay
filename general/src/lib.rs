extern crate chrono;
extern crate cookie;
extern crate debugid;
extern crate failure;
extern crate hmac;
extern crate itertools;
extern crate lazy_static;
extern crate maxminddb;
extern crate num_traits;
extern crate regex;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate serde_urlencoded;
extern crate sha1;
extern crate sha2;
extern crate smallvec;
extern crate url;
extern crate uuid;

#[cfg(test)]
extern crate difference;

// we use macro_use here because we really consider this to be an internal
// macro which currently cannot be imported.  Same thing with the macros mod
#[macro_use]
extern crate semaphore_general_derive;

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
