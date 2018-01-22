use std::collections::HashMap;

use parking_lot::RwLock;

use smith_common::Dsn;
use smith_aorta::{ProjectState, UpstreamDescriptor};

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct StateKey(pub UpstreamDescriptor, pub String);

/// The trove holds project states.
///
/// The goal of the trove is that the server or another component that
/// also wants agent functionality can based on a DSN retrieve the current
/// state of the project. Likewise if a DSN does not exist it will trivially
/// figure out.  The trove can manage concurrent updates in the background
/// automatically.
pub struct Trove {
    state_keys: RwLock<HashMap<Dsn, StateKey>>,
    states: RwLock<HashMap<StateKey, ProjectState>>,
}

impl Trove {
    /// Creates a new empty trove.
    pub fn new() -> Trove {
        Trove {
            state_keys: RwLock::new(HashMap::new()),
            states: RwLock::new(HashMap::new()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_assert_sync() {
        struct Assert<T: Sync> {
            x: Option<T>,
        }
        let val: Assert<Trove> = Assert { x: None };
        assert_eq!(val.x.is_none(), true);
    }
}
