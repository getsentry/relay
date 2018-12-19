// TODO: Fix casts between SemaphoreGeoIpLookup and GeoIpLookup
#![allow(clippy::cast_ptr_alignment)]

use std::ffi::CStr;
use std::os::raw::c_char;

use semaphore_general::processor::{process_value, split_chunks, ProcessingState};
use semaphore_general::protocol::Event;
use semaphore_general::store::{GeoIpLookup, StoreConfig, StoreProcessor};
use semaphore_general::types::{Annotated, Remark};

use crate::core::SemaphoreStr;

pub struct SemaphoreGeoIpLookup;
pub struct SemaphoreStoreNormalizer;

ffi_fn! {
    unsafe fn semaphore_split_chunks(
        string: *const SemaphoreStr,
        remarks: *const SemaphoreStr
    ) -> Result<SemaphoreStr> {
        let remarks: Vec<Remark> = serde_json::from_str((*remarks).as_str())?;
        let chunks = split_chunks((*string).as_str(), &remarks);
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
        let normalizer = StoreProcessor::new(config, geoip_lookup);
        Ok(Box::into_raw(Box::new(normalizer)) as *mut SemaphoreStoreNormalizer)
    }
}

ffi_fn! {
    unsafe fn semaphore_store_normalizer_free(
        normalizer: *mut SemaphoreStoreNormalizer
    ) {
        if !normalizer.is_null() {
            let normalizer = normalizer as *mut StoreProcessor;
            Box::from_raw(normalizer);
        }
    }
}

ffi_fn! {
    unsafe fn semaphore_store_normalizer_normalize_event(
        normalizer: *mut SemaphoreStoreNormalizer,
        event: *const SemaphoreStr,
    ) -> Result<SemaphoreStr> {
        let processor = normalizer as *mut StoreProcessor;
        let mut event = Annotated::<Event>::from_json((*event).as_str())?;
        process_value(&mut event, &mut *processor, ProcessingState::root());
        Ok(SemaphoreStr::from_string(event.to_json()?))
    }
}
