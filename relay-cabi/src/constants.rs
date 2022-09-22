use crate::core::RelayStr;

pub use relay_common::{DataCategory, EventType, SpanStatus};

/// Returns the API name of the given `DataCategory`.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_data_category_name(category: DataCategory) -> RelayStr {
    RelayStr::new(category.name())
}

/// Parses a `DataCategory` from its API name.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_data_category_parse(name: *const RelayStr) -> DataCategory {
    (*name).as_str().parse().unwrap_or(DataCategory::Unknown)
}

/// Parses a `DataCategory` from an event type.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_data_category_from_event_type(
    event_type: *const RelayStr,
) -> DataCategory {
    (*event_type)
        .as_str()
        .parse::<EventType>()
        .unwrap_or_default()
        .into()
}
