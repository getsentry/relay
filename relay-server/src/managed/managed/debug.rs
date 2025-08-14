use std::collections::BTreeMap;

use relay_quotas::DataCategory;

use crate::managed;

#[derive(Debug)]
pub struct Quantities(pub BTreeMap<DataCategory, usize>);

impl Quantities {
    /// Asserts that all categories contained in this instance are also contained in `other` with the same quantities.
    ///
    /// Additional entries in `other` are ignored.
    #[track_caller]
    pub fn assert_only_extra<T>(&self, other: T)
    where
        T: Into<Self>,
    {
        let mut other = other.into();

        for (category, quantity) in &self.0 {
            match other.0.remove(category) {
                None => {
                    panic!("Expected {quantity} outcomes in category '{category}', but got none")
                }
                Some(other) => {
                    assert_eq!(
                        *quantity, other,
                        "Expected {quantity} outcomes in category '{category}', but got '{other}'"
                    );
                }
            }
        }

        if !other.0.is_empty() {
            relay_log::debug!("Additional outcomes created: {:?}", other.0);
        }
    }
}

impl std::ops::Add for Quantities {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        for (category, quantity) in rhs.0 {
            *self.0.entry(category).or_default() += quantity;
        }
        self
    }
}

impl From<&managed::Quantities> for Quantities {
    fn from(value: &managed::Quantities) -> Self {
        Self(
            value
                .iter()
                .fold(Default::default(), |mut acc, (category, quantity)| {
                    *acc.entry(*category).or_default() += *quantity;
                    acc
                }),
        )
    }
}

impl<T> From<&T> for Quantities
where
    T: managed::Counted,
{
    fn from(value: &T) -> Self {
        Self::from(&value.quantities())
    }
}
