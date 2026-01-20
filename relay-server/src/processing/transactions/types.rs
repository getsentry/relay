mod expanded;
pub use expanded::ExpandedTransaction;
mod serialized;
pub use serialized::SerializedTransaction;
mod output;
pub use output::TransactionOutput;
mod profile;
pub use profile::Profile;

/// Flags extracted from transaction item headers.
///
/// Ideally `metrics_extracted` and `spans_extracted` will not be needed in the future. Unsure
/// about `fully_normalized`.
#[derive(Debug, Default)]
pub struct Flags {
    pub metrics_extracted: bool,
    pub spans_extracted: bool,
    pub fully_normalized: bool,
    pub spans_rate_limited: bool,
    pub spans_killswitched: bool,
}
