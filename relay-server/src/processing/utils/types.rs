/// Data categories a payload counts towards.
///
/// Payloads start out as [`TotalAndIndexed`] and transition to [`Indexed`] once metrics
/// have been extracted, transferring ownership of the total category to the metrics.
pub trait TotalCategory {
    /// Whether the payload still counts towards the total data category.
    const HAS_TOTAL: bool;
}

/// The total and indexed category.
///
/// This category tracks payloads in the total and indexed data categories.
/// Until a payload has metrics extracted it owns both categories.
#[derive(Copy, Clone, Debug)]
pub struct TotalAndIndexed;

impl TotalCategory for TotalAndIndexed {
    const HAS_TOTAL: bool = true;
}

/// The indexed category.
///
/// Once metric extraction happened, payloads no longer track/represent the total category,
/// this was transferred over to the metrics.
///
/// Every stored payload, must have metrics extracted and transferred this ownership.
#[derive(Copy, Clone, Debug)]
pub struct Indexed;

impl TotalCategory for Indexed {
    const HAS_TOTAL: bool = false;
}
