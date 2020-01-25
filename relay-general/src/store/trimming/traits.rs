use crate::processor::PathItem;

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

/// The maximum size of a databag.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum BagSize {
    Small,
    Medium,
    Large,
    Larger,
    Massive,
}

impl BagSize {
    /// Maximum depth of the structure.
    pub fn max_depth(self) -> usize {
        match self {
            BagSize::Small => 3,
            BagSize::Medium => 5,
            BagSize::Large => 7,
            BagSize::Larger => 7,
            BagSize::Massive => 7,
        }
    }

    /// Maximum estimated JSON bytes.
    pub fn max_size(self) -> usize {
        match self {
            BagSize::Small => 1024,
            BagSize::Medium => 2048,
            BagSize::Large => 8192,
            BagSize::Larger => 16384,
            BagSize::Massive => 262_144,
        }
    }
}

/// The maximum length of a field.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum MaxChars {
    Hash,
    EnumLike,
    Summary,
    Message,
    Symbol,
    Path,
    ShortPath,
    Logger,
    Email,
    Culprit,
    TagKey,
    TagValue,
    Environment,
    Hard(usize),
    Soft(usize),
}

impl MaxChars {
    /// The cap in number of unicode characters.
    pub fn limit(self) -> usize {
        match self {
            MaxChars::Hash => 128,
            MaxChars::EnumLike => 128,
            MaxChars::Summary => 1024,
            MaxChars::Message => 8192,
            MaxChars::Symbol => 256,
            MaxChars::Path => 256,
            MaxChars::ShortPath => 128,
            // these are from constants.py or limits imposed by the database
            MaxChars::Logger => 64,
            MaxChars::Email => 75,
            MaxChars::Culprit => 200,
            MaxChars::TagKey => 32,
            MaxChars::TagValue => 200,
            MaxChars::Environment => 64,
            MaxChars::Soft(len) | MaxChars::Hard(len) => len,
        }
    }

    /// The number of extra characters permitted.
    pub fn allowance(self) -> usize {
        match self {
            MaxChars::Hash => 0,
            MaxChars::EnumLike => 0,
            MaxChars::Summary => 100,
            MaxChars::Message => 200,
            MaxChars::Symbol => 20,
            MaxChars::Path => 40,
            MaxChars::ShortPath => 20,
            MaxChars::Logger => 0,
            MaxChars::Email => 0,
            MaxChars::Culprit => 0,
            MaxChars::TagKey => 0,
            MaxChars::TagValue => 0,
            MaxChars::Environment => 0,
            MaxChars::Soft(_) => 10,
            MaxChars::Hard(_) => 0,
        }
    }
}
