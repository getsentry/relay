use crate::processor::{BagSize, MaxChars, PathItem};

#[derive(Debug, Copy, Clone)]
pub struct TrimmingAttrs {
    pub max_chars: &'static [(PathItem<'static>, MaxChars)],
    pub bag_size: &'static [(PathItem<'static>, BagSize)],
    pub bag_size_inner: &'static [(PathItem<'static>, BagSize)],
}

impl Default for TrimmingAttrs {
    fn default() -> TrimmingAttrs {
        TrimmingAttrs {
            max_chars: &[],
            bag_size: &[],
            bag_size_inner: &[],
        }
    }
}

impl TrimmingAttrs {
    pub fn max_chars(&self, field: &PathItem<'_>) -> Option<MaxChars> {
        for (k, v) in self.max_chars {
            if k == field {
                return Some(*v);
            }
        }

        None
    }

    pub fn bag_size(&self, field: &PathItem<'_>) -> Option<BagSize> {
        for (k, v) in self.bag_size {
            if k == field {
                return Some(*v);
            }
        }

        None
    }

    pub fn bag_size_inner(&self, field: &PathItem<'_>) -> Option<BagSize> {
        for (k, v) in self.bag_size_inner {
            if k == field {
                return Some(*v);
            }
        }

        None
    }
}
