use crate::core::RelayStr;
use relay_metrics::MetricSymbol;

/// Convert a metrics name, tag key or tag value to an integer
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_to_metrics_symbol(value: *const RelayStr) -> u64 {
    MetricSymbol::from((*value).as_str()).as_u64()
}
