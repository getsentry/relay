use std::ffi::CStr;
use std::os::raw::c_char;

use serde_json;

// TODO: Migrate this to semaphore_common::processor
use semaphore_common::processor_compat::chunks;

use semaphore_common::processor::process_value;
use semaphore_common::protocol::{Annotated, Event};
use semaphore_common::store::{GeoIpLookup, StoreConfig, StoreNormalizeProcessor};
use semaphore_common::v8_compat::Remark;

use crate::core::SemaphoreStr;

pub struct SemaphoreGeoIpLookup;
pub struct SemaphoreStoreNormalizer;

ffi_fn! {
    unsafe fn semaphore_split_chunks(
        string: *const SemaphoreStr,
        remarks: *const SemaphoreStr
    ) -> Result<SemaphoreStr> {
        let remarks: Vec<Remark> = serde_json::from_str((*remarks).as_str())?;
        let chunks = chunks::split((*string).as_str(), &remarks);
        let json = serde_json::to_string(&chunks)?;
        Ok(json.into())
    }
}

ffi_fn! {
    unsafe fn semaphore_geoip_lookup_new(
        path: *const c_char
    ) -> Result<*mut SemaphoreGeoIpLookup> {
        let path = CStr::from_ptr(path).to_string_lossy();
        let lookup = GeoIpLookup::open(&path)?;
        Ok(Box::into_raw(Box::new(lookup)) as *mut SemaphoreGeoIpLookup)
    }
}

ffi_fn! {
    unsafe fn semaphore_geoip_lookup_free(
        lookup: *mut SemaphoreGeoIpLookup
    ) {
        if !lookup.is_null() {
            let lookup = lookup as *mut GeoIpLookup;
            Box::from_raw(lookup);
        }
    }
}

ffi_fn! {
    unsafe fn semaphore_store_normalizer_new(
        config: *const SemaphoreStr,
        geoip_lookup: *const SemaphoreGeoIpLookup,
    ) -> Result<*mut SemaphoreStoreNormalizer> {
        let config: StoreConfig = serde_json::from_str((*config).as_str())?;
        let geoip_lookup = (geoip_lookup as *const GeoIpLookup).as_ref();
        let normalizer = StoreNormalizeProcessor::new(config, geoip_lookup);
        Ok(Box::into_raw(Box::new(normalizer)) as *mut SemaphoreStoreNormalizer)
    }
}

ffi_fn! {
    unsafe fn semaphore_store_normalizer_free(
        normalizer: *mut SemaphoreStoreNormalizer
    ) {
        if !normalizer.is_null() {
            let normalizer = normalizer as *mut StoreNormalizeProcessor;
            Box::from_raw(normalizer);
        }
    }
}

ffi_fn! {
    unsafe fn semaphore_store_normalizer_normalize_event(
        normalizer: *mut SemaphoreStoreNormalizer,
        event: *const SemaphoreStr,
    ) -> Result<SemaphoreStr> {
        let event = Annotated::<Event>::from_json((*event).as_str())?;
        let processor = normalizer as *mut StoreNormalizeProcessor;
        let processed_event = process_value(event, &mut *processor);
        Ok(SemaphoreStr::from_string(processed_event.to_json()?))
    }
}
