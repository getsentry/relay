use std::collections::BTreeMap;

use relay_quotas::DataCategory;

use crate::managed;

#[derive(Debug)]
pub struct Quantities(pub BTreeMap<DataCategory, usize>);

impl std::ops::Add for Quantities {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        for (category, quantity) in rhs.0 {
            *self.0.entry(category).or_default() += quantity;
        }
        self
    }
}

impl<T> From<&T> for Quantities
where
    T: managed::Counted,
{
    fn from(value: &T) -> Self {
        Self(
            value
                .quantities()
                .iter()
                .fold(Default::default(), |mut acc, (category, quantity)| {
                    *acc.entry(*category).or_default() += *quantity;
                    acc
                }),
        )
    }
}
