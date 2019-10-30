// TODO: Fix casts between SemaphoreGeoIpLookup and GeoIpLookup
#![allow(clippy::cast_ptr_alignment)]
#![deny(unused_must_use)]

use std::ffi::CStr;
use std::os::raw::c_char;
use std::slice;

use json_forensics;
use semaphore_common::{glob_match_bytes, GlobOptions};
use semaphore_general::datascrubbing::DataScrubbingConfig;
use semaphore_general::pii::PiiProcessor;
use semaphore_general::processor::{process_value, split_chunks, ProcessingState};
use semaphore_general::protocol::Event;
use semaphore_general::store::{GeoIpLookup, StoreConfig, StoreProcessor};
use semaphore_general::types::{Annotated, Remark};

use crate::core::{SemaphoreBuf, SemaphoreStr};

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
        let lookup = GeoIpLookup::open(path.as_ref())?;
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
        process_value(&mut event, &mut *processor, ProcessingState::root())?;
        Ok(SemaphoreStr::from_string(event.to_json()?))
    }
}

ffi_fn! {
    unsafe fn semaphore_translate_legacy_python_json(
        event: *mut SemaphoreStr,
    ) -> Result<bool> {
        let data = slice::from_raw_parts_mut((*event).data as *mut u8, (*event).len);
        json_forensics::translate_slice(data);
        Ok(true)
    }
}

ffi_fn! {
    unsafe fn semaphore_scrub_event(
        config: *const SemaphoreStr,
        event: *const SemaphoreStr,
    ) -> Result<SemaphoreStr> {
        let config: DataScrubbingConfig = serde_json::from_str((*config).as_str())?;
        let mut processor = match config.pii_config() {
            Some(pii_config) => PiiProcessor::new(pii_config),
            None => return Ok(SemaphoreStr::new((*event).as_str())),
        };

        let mut event = Annotated::<Event>::from_json((*event).as_str())?;
        process_value(&mut event, &mut processor, ProcessingState::root())?;

        Ok(SemaphoreStr::from_string(event.to_json()?))
    }
}

ffi_fn! {
    unsafe fn semaphore_test_panic() -> Result<()> {
        panic!("this is a test panic")
    }
}

/// Controls the globbing behaviors.
#[repr(u32)]
pub enum GlobFlags {
    DoubleStar = 1,
    CaseInsensitive = 2,
    PathNormalize = 4,
}

ffi_fn! {
    unsafe fn semaphore_is_glob_match(
        value: *const SemaphoreBuf,
        pat: *const SemaphoreStr,
        flags: GlobFlags,
    ) -> Result<bool> {
        let mut options = GlobOptions::default();
        let flags = flags as u32;
        if (flags & GlobFlags::DoubleStar as u32) != 0 {
            options.double_star = true;
        }
        if (flags & GlobFlags::CaseInsensitive as u32) != 0 {
            options.case_insensitive = true;
        }
        if (flags & GlobFlags::PathNormalize as u32) != 0 {
            options.path_normalize = true;
        }
        Ok(glob_match_bytes((*value).as_bytes(), (*pat).as_str(), options))
    }
}
