use relay_common::time::UnixTimestamp;
use relay_protocol::Getter;

/// Item from which metrics can be extracted.
pub trait Extractable: Getter {
    /// The timestamp to associate with the extracted metrics.
    fn timestamp(&self) -> Option<UnixTimestamp>;
}
