pub use relay_base_schema::data_category::DataCategory;
pub use relay_base_schema::events::EventType;
pub use relay_base_schema::spans::SpanStatus;

use crate::core::RelayStr;

/// Returns the API name of the given `DataCategory`.
#[unsafe(no_mangle)]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_data_category_name(category: DataCategory) -> RelayStr {
    RelayStr::new(category.name())
}

/// Parses a `DataCategory` from its API name.
#[unsafe(no_mangle)]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_data_category_parse(name: *const RelayStr) -> DataCategory {
    unsafe { (*name).as_str() }
        .parse()
        .unwrap_or(DataCategory::Unknown)
}

/// Parses a `DataCategory` from an event type.
#[unsafe(no_mangle)]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_data_category_from_event_type(
    event_type: *const RelayStr,
) -> DataCategory {
    unsafe { (*event_type).as_str() }
        .parse::<EventType>()
        .unwrap_or_default()
        .into()
}
