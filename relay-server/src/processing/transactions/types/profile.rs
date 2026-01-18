use crate::envelope::Item;
use crate::managed::{Counted, Managed, Quantities};
use crate::processing::CountRateLimited;
use crate::processing::transactions::Error;

/// Wrapper for a profile item that can be rate limited.
#[derive(Debug)]
pub struct Profile(pub Box<Item>);

impl Counted for Profile {
    fn quantities(&self) -> Quantities {
        self.0.quantities()
    }
}

impl CountRateLimited for Managed<Profile> {
    type Error = Error;
}
