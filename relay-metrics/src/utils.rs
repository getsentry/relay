use core::fmt;
use std::collections::BTreeMap;
use std::ops::{AddAssign, SubAssign};

use relay_base_schema::metrics::MetricNamespace;

/// Estimates the number of bytes needed to encode the tags.
///
/// Note that this does not necessarily match the exact memory footprint of the tags,
/// because data structures or their serialization have overheads.
pub fn tags_cost(tags: &BTreeMap<String, String>) -> usize {
    tags.iter().map(|(k, v)| k.len() + v.len()).sum()
}

/// Utility to store information for each [`MetricNamespace`].
#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByNamespace<T> {
    /// Value for the [`MetricNamespace::Sessions`] namespace.
    pub sessions: T,
    /// Value for the [`MetricNamespace::Transactions`] namespace.
    pub transactions: T,
    /// Value for the [`MetricNamespace::Spans`] namespace.
    pub spans: T,
    /// Value for the [`MetricNamespace::Custom`] namespace.
    pub custom: T,
    /// Value for the [`MetricNamespace::Stats`] namespace.
    pub stats: T,
    /// Value for the [`MetricNamespace::Unsupported`] namespace.
    pub unsupported: T,
}

impl<T> ByNamespace<T> {
    /// Returns a reference for the value stored for `namespace`.
    pub fn get(&self, namespace: MetricNamespace) -> &T {
        match namespace {
            MetricNamespace::Sessions => &self.sessions,
            MetricNamespace::Transactions => &self.transactions,
            MetricNamespace::Spans => &self.spans,
            MetricNamespace::Custom => &self.custom,
            MetricNamespace::Stats => &self.stats,
            MetricNamespace::Unsupported => &self.unsupported,
        }
    }

    /// Returns a mutable reference for the value stored for `namespace`.
    pub fn get_mut(&mut self, namespace: MetricNamespace) -> &mut T {
        match namespace {
            MetricNamespace::Sessions => &mut self.sessions,
            MetricNamespace::Transactions => &mut self.transactions,
            MetricNamespace::Spans => &mut self.spans,
            MetricNamespace::Custom => &mut self.custom,
            MetricNamespace::Stats => &mut self.stats,
            MetricNamespace::Unsupported => &mut self.unsupported,
        }
    }
}

impl<T> IntoIterator for ByNamespace<T> {
    type Item = (MetricNamespace, T);
    type IntoIter = std::array::IntoIter<(MetricNamespace, T), 6>;

    fn into_iter(self) -> Self::IntoIter {
        let Self {
            sessions,
            transactions,
            spans,
            custom,
            stats,
            unsupported,
        } = self;

        [
            (MetricNamespace::Sessions, sessions),
            (MetricNamespace::Transactions, transactions),
            (MetricNamespace::Spans, spans),
            (MetricNamespace::Custom, custom),
            (MetricNamespace::Stats, stats),
            (MetricNamespace::Unsupported, unsupported),
        ]
        .into_iter()
    }
}

impl<T: fmt::Debug + Default + PartialEq> fmt::Debug for ByNamespace<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A more compact representation. Mainly for snapshot testing.
        write!(f, "(")?;

        let mut values = MetricNamespace::all()
            .into_iter()
            .map(|ns| (ns, self.get(ns)))
            .filter(|(_, v)| v != &&T::default())
            .enumerate()
            .peekable();

        match values.peek() {
            None => write!(f, "{:?}", T::default())?,
            Some(_) => {
                for (i, (namespace, value)) in values {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{namespace}:{value:?}")?;
                }
            }
        }

        write!(f, ")")
    }
}

macro_rules! impl_op {
    ($op:tt, $opfn:ident) => {
        impl<T: $op> $op for ByNamespace<T> {
            fn $opfn(&mut self, rhs: Self) {
                let Self {
                    sessions,
                    transactions,
                    spans,
                    custom,
                    stats,
                    unsupported,
                } = self;

                $op::$opfn(sessions, rhs.sessions);
                $op::$opfn(transactions, rhs.transactions);
                $op::$opfn(spans, rhs.spans);
                $op::$opfn(custom, rhs.custom);
                $op::$opfn(stats, rhs.stats);
                $op::$opfn(unsupported, rhs.unsupported);
            }
        }
    };
}

impl_op!(AddAssign, add_assign);
impl_op!(SubAssign, sub_assign);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get() {
        let mut v = ByNamespace::<usize>::default();

        for (i, namespace) in MetricNamespace::all().into_iter().enumerate() {
            assert_eq!(*v.get(namespace), 0);
            assert_eq!(*v.get_mut(namespace), 0);
            *v.get_mut(namespace) += i;
            assert_eq!(*v.get(namespace), i);
        }

        // Make sure a getter does not override another value by iterating over all values again.
        for (i, namespace) in MetricNamespace::all().into_iter().enumerate() {
            assert_eq!(*v.get(namespace), i);
            assert_eq!(*v.get_mut(namespace), i);
        }
    }

    #[test]
    fn test_add_sub() {
        let mut v = ByNamespace::<usize>::default();
        for (i, namespace) in MetricNamespace::all().into_iter().enumerate() {
            *v.get_mut(namespace) += i;
        }

        let mut v2 = v;
        v2 -= v;
        assert_eq!(v2, Default::default());

        v2 += v;
        assert_eq!(v2, v);
    }
}
