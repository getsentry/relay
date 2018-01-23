use std::sync::Arc;
use std::borrow::Cow;
use std::collections::HashMap;

use parking_lot::RwLock;

use smith_common::Dsn;
use smith_aorta::{ProjectState, UpstreamDescriptor};

/// The trove holds project states.
///
/// The goal of the trove is that the server or another component that
/// also wants agent functionality can based on a DSN retrieve the current
/// state of the project. Likewise if a DSN does not exist it will trivially
/// figure out.  The trove can manage concurrent updates in the background
/// automatically.
pub struct Trove {}

impl Trove {
    /// Creates a new empty trove.
    pub fn new() -> Trove {
        Trove {}
    }

    /// Looks up the project state by dsn.
    ///
    /// This lookup also creates an entry if it does not exist yet.  The
    /// project state might be expired which is why an arc is returned.
    /// Until the arc is dropped the data can be retained.
    pub fn state_for_dsn(&self, dsn: &Dsn) -> Arc<ProjectState> {
        panic!("not implemented");
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
