use crate::core::RelayStr;

pub use relay_common::{DataCategory, EventType, SpanStatus};

ffi_fn! {
    /// Returns the API name of the given `DataCategory`.
    unsafe fn relay_data_category_name(
        category: DataCategory
    ) -> Result<RelayStr> {
        Ok(RelayStr::new(category.name()))
    }
}

ffi_fn! {
    /// Parses a `DataCategory` from its API name.
    unsafe fn relay_data_category_parse(
        name: *const RelayStr
    ) -> Result<DataCategory> {
        Ok((*name).as_str().parse().unwrap_or(DataCategory::Unknown))
    }
}

ffi_fn! {
    /// Parses a `DataCategory` from an event type.
    unsafe fn relay_data_category_from_event_type(
        event_type: *const RelayStr
    ) -> Result<DataCategory> {
        Ok((*event_type).as_str().parse::<EventType>().unwrap_or_default().into())
    }
}
