use relay_base_schema::project::ProjectKey;

use crate::Envelope;

/// Struct that represents two project keys.
#[derive(Debug, Clone, Copy)]
pub struct ProjectKeyPair {
    own_key: ProjectKey,
    sampling_key: Option<ProjectKey>,
}

impl ProjectKeyPair {
    pub fn new(own_key: ProjectKey, sampling_key: ProjectKey) -> Self {
        Self {
            own_key,
            sampling_key: Some(sampling_key),
        }
    }

    pub fn own_key(&self) -> ProjectKey {
        self.own_key
    }

    pub fn sampling_key(&self) -> Option<ProjectKey> {
        self.sampling_key
    }

    pub fn sampling_key_unwrap(&self) -> ProjectKey {
        self.sampling_key.unwrap_or(self.own_key)
    }

    pub fn from_envelope(envelope: &Envelope) -> Self {
        let own_key = envelope.meta().public_key();
        let sampling_key = envelope.sampling_key();
        Self {
            own_key,
            sampling_key,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = ProjectKey> {
        let Self {
            own_key,
            sampling_key,
        } = self;
        std::iter::once(*own_key).chain(sampling_key.filter(|k| k != own_key))
    }
}

impl PartialEq for ProjectKeyPair {
    fn eq(&self, other: &Self) -> bool {
        self.own_key() == other.own_key()
            && self.sampling_key_unwrap() == other.sampling_key_unwrap()
    }
}

impl Eq for ProjectKeyPair {}

impl PartialOrd for ProjectKeyPair {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProjectKeyPair {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let own_comparison = self.own_key().cmp(&other.own_key());
        if own_comparison != std::cmp::Ordering::Equal {
            return own_comparison;
        };
        self.sampling_key_unwrap().cmp(&other.sampling_key_unwrap())
    }
}

impl std::hash::Hash for ProjectKeyPair {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.own_key().hash(state);
        self.sampling_key_unwrap().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_project_key_pair_new() {
        let own = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let sampling = ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let pair = ProjectKeyPair::new(own, sampling);
        assert_eq!(pair.own_key(), own);
        assert_eq!(pair.sampling_key_unwrap(), sampling);
    }

    #[test]
    fn test_project_key_pair_equality() {
        let key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let key2 = ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let pair1 = ProjectKeyPair::new(key1, key2);
        let pair2 = ProjectKeyPair::new(key1, key2);
        let pair3 = ProjectKeyPair::new(key2, key1);

        assert_eq!(pair1, pair2);
        assert_ne!(pair1, pair3);
    }

    #[test]
    fn test_project_key_pair_ordering() {
        let key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let key2 = ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let pair1 = ProjectKeyPair::new(key1, key2);
        let pair2 = ProjectKeyPair::new(key2, key1);
        let pair3 = ProjectKeyPair {
            own_key: key1,
            sampling_key: None,
        };

        assert!(pair1 < pair2);
        assert!(pair3 < pair2);
    }

    #[test]
    fn test_project_key_pair_hash() {
        let key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let key2 = ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let pair1 = ProjectKeyPair::new(key1, key2);
        let pair2 = ProjectKeyPair::new(key1, key2);
        let pair3 = ProjectKeyPair::new(key2, key1);

        let mut set = HashSet::new();
        set.insert(pair1);
        assert!(set.contains(&pair2));
        assert!(!set.contains(&pair3));
    }

    #[test]
    fn test_project_key_pair_iter() {
        let key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let key2 = ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        // Test with different sampling key
        let pair = ProjectKeyPair::new(key1, key2);
        let keys: Vec<_> = pair.iter().collect();
        assert_eq!(keys, vec![key1, key2]);

        // Test with same key (should only yield one key)
        let pair = ProjectKeyPair::new(key1, key1);
        let keys: Vec<_> = pair.iter().collect();
        assert_eq!(keys, vec![key1]);
    }
}
