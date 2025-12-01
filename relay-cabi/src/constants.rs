pub use relay_base_schema::data_category::DataCategory;
pub use relay_base_schema::events::EventType;
pub use relay_base_schema::spans::SpanStatus;
pub use relay_quotas::CategoryUnit;

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

/// Sentinel value indicating no category unit (used when `Option<CategoryUnit>` is `None`).
const CATEGORY_UNIT_NONE: i8 = -1;

/// Returns the API name of the given `CategoryUnit`.
///
/// If `unit` is `-1` (no unit), returns an empty string.
#[unsafe(no_mangle)]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_category_unit_name(unit: i8) -> RelayStr {
    match unit {
        0 => RelayStr::new("count"),
        1 => RelayStr::new("bytes"),
        2 => RelayStr::new("milliseconds"),
        _ => RelayStr::new(""),
    }
}

/// Parses a `CategoryUnit` from its API name.
///
/// Returns the unit value (0=Count, 1=Bytes, 2=Milliseconds) or `-1` for invalid/unknown names.
#[unsafe(no_mangle)]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_category_unit_parse(name: *const RelayStr) -> i8 {
    let name_str = unsafe { (*name).as_str() };
    match CategoryUnit::from_name(name_str) {
        Some(unit) => unit as i8,
        None => CATEGORY_UNIT_NONE,
    }
}

/// Returns the `CategoryUnit` for a given `DataCategory`.
///
/// Returns the unit value (0=Count, 1=Bytes, 2=Milliseconds) or `-1` if the category
/// has no defined unit (e.g., `DataCategory::Unknown`).
#[unsafe(no_mangle)]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_data_category_unit(category: DataCategory) -> i8 {
    match CategoryUnit::from_category(category) {
        Some(unit) => unit as i8,
        None => CATEGORY_UNIT_NONE,
    }
}
