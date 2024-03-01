use std::marker::PhantomData;

/// Enum mapping type.
pub trait Enum<const LENGTH: usize>: Copy {
    // Unfortunately we need to use const generics here,
    // using the associated consts in const generics is still unstable.
    //
    // There is a potential workaround of using a helper `Array` trait and
    // implementing `for [V; N] where Enum::LENGTH = N`.

    /// Length of the enum.
    const LENGTH: usize = LENGTH;

    /// Converts an index to an enum variant.
    ///
    /// Passed `index` must be in range `0..LENGTH`.
    fn from_index(index: usize) -> Option<Self>;

    /// Converts an enum variant to an index.
    ///
    /// Returned index must be in range `0..LENGTH`.
    fn to_index(value: Self) -> usize;
}

/// An enum map.
#[derive(Clone, Copy)]
pub struct EnumMap<const LENGTH: usize, E: Enum<LENGTH>, V>([Option<V>; LENGTH], PhantomData<E>);

impl<const LENGTH: usize, E: Enum<LENGTH>, V> EnumMap<LENGTH, E, V> {
    /// Inserts a `value` for a `feature` into the map.
    ///
    /// If the map already had a value present for the `feature`,
    /// the old value is returned.
    pub fn insert(&mut self, key: E, value: V) -> Option<V> {
        std::mem::replace(&mut self.0[E::to_index(key)], Some(value))
    }

    /// Removes a `feature` from the map.
    ///
    /// Returns the previously contained value.
    pub fn remove(&mut self, key: E) -> Option<V> {
        std::mem::take(&mut self.0[E::to_index(key)])
    }

    /// Returns the contained value for `key` as mutable reference.
    pub fn get_mut(&mut self, key: E) -> Option<&mut V> {
        self.0[E::to_index(key)].as_mut()
    }
}

impl<const LENGTH: usize, E: Enum<LENGTH>, V> Default for EnumMap<LENGTH, E, V> {
    fn default() -> Self {
        Self([(); LENGTH].map(|_| None), PhantomData)
    }
}

impl<const LENGTH: usize, E: Enum<LENGTH>, V> IntoIterator for EnumMap<LENGTH, E, V> {
    type Item = (E, V);
    type IntoIter = EnumMapIntoIter<LENGTH, E, V>;

    fn into_iter(self) -> Self::IntoIter {
        EnumMapIntoIter::<LENGTH, E, V>::new(self)
    }
}

pub struct EnumMapIntoIter<const LENGTH: usize, E: Enum<LENGTH>, V> {
    index: usize,
    map: EnumMap<LENGTH, E, V>,
}

impl<const LENGTH: usize, E: Enum<LENGTH>, V> EnumMapIntoIter<LENGTH, E, V> {
    fn new(map: EnumMap<LENGTH, E, V>) -> Self {
        Self { index: 0, map }
    }
}

impl<const LENGTH: usize, E: Enum<LENGTH>, V> Iterator for EnumMapIntoIter<LENGTH, E, V> {
    type Item = (E, V);

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.map.0.len() {
            let index = self.index;
            self.index += 1;

            let value = std::mem::take(&mut self.map.0[index]);
            if let Some(value) = value {
                return Some((E::from_index(index)?, value));
            }
        }

        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.map.0.len().saturating_sub(self.index)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppFeature;

    #[test]
    fn test_iter_empty() {
        let map = EnumMap::<16, AppFeature, usize>::default();

        let mut iter = map.into_iter();
        assert_eq!(iter.size_hint(), (0, Some(AppFeature::LENGTH)));
        assert!(iter.next().is_none());
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }

    #[test]
    fn test_iter_with_elements() {
        let mut map = EnumMap::<16, AppFeature, usize>::default();

        map.insert(AppFeature::Spans, 1);
        map.insert(AppFeature::Transactions, 2);
        map.insert(AppFeature::MetricsSpans, 3);
        map.remove(AppFeature::Transactions);

        let mut iter = map.into_iter();
        assert_eq!(iter.size_hint(), (0, Some(AppFeature::LENGTH)));
        assert_eq!(iter.next(), Some((AppFeature::Spans, 1)));
        assert_eq!(
            iter.size_hint(),
            (
                0,
                Some(AppFeature::LENGTH - AppFeature::to_index(AppFeature::Spans) - 1)
            )
        );
        assert_eq!(iter.next(), Some((AppFeature::MetricsSpans, 3)));
        assert_eq!(
            iter.size_hint(),
            (
                0,
                Some(AppFeature::LENGTH - AppFeature::to_index(AppFeature::MetricsSpans) - 1)
            )
        );
        assert!(iter.next().is_none());
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }
}
