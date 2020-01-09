// TODO: Fix casts between RelayGeoIpLookup and GeoIpLookup
#![allow(clippy::cast_ptr_alignment)]
#![deny(unused_must_use)]

use std::ffi::CStr;
use std::os::raw::c_char;
use std::slice;

use json_forensics;
use relay_common::{glob_match_bytes, GlobOptions};
use relay_general::datascrubbing::DataScrubbingConfig;
use relay_general::pii::PiiProcessor;
use relay_general::processor::{process_value, split_chunks, ProcessingState};
use relay_general::protocol::{Event, VALID_PLATFORMS};
use relay_general::store::{GeoIpLookup, StoreConfig, StoreProcessor};
use relay_general::types::{Annotated, Remark};

use crate::core::{RelayBuf, RelayStr};

pub struct RelayGeoIpLookup;
pub struct RelayStoreNormalizer;

lazy_static::lazy_static! {
    static ref VALID_PLATFORM_STRS: Vec<RelayStr> =
        VALID_PLATFORMS.iter().map(|s| RelayStr::new(s)).collect();
}

ffi_fn! {
    unsafe fn relay_split_chunks(
        string: *const RelayStr,
        remarks: *const RelayStr
    ) -> Result<RelayStr> {
        let remarks: Vec<Remark> = serde_json::from_str((*remarks).as_str())?;
        let chunks = split_chunks((*string).as_str(), &remarks);
        let json = serde_json::to_string(&chunks)?;
        Ok(json.into())
    }
}

ffi_fn! {
    unsafe fn relay_geoip_lookup_new(
        path: *const c_char
    ) -> Result<*mut RelayGeoIpLookup> {
        let path = CStr::from_ptr(path).to_string_lossy();
        let lookup = GeoIpLookup::open(path.as_ref())?;
        Ok(Box::into_raw(Box::new(lookup)) as *mut RelayGeoIpLookup)
    }
}

ffi_fn! {
    unsafe fn relay_geoip_lookup_free(
        lookup: *mut RelayGeoIpLookup
    ) {
        if !lookup.is_null() {
            let lookup = lookup as *mut GeoIpLookup;
            Box::from_raw(lookup);
        }
    }
}

ffi_fn! {
    /// Returns a list of all valid platform identifiers.
    unsafe fn relay_valid_platforms(
        size_out: *mut usize,
    ) -> Result<*const RelayStr> {
        if let Some(size_out) = size_out.as_mut() {
            *size_out = VALID_PLATFORM_STRS.len();
        }

        Ok(VALID_PLATFORM_STRS.as_ptr())
    }
}

ffi_fn! {
    unsafe fn relay_store_normalizer_new(
        config: *const RelayStr,
        geoip_lookup: *const RelayGeoIpLookup,
    ) -> Result<*mut RelayStoreNormalizer> {
        let config: StoreConfig = serde_json::from_str((*config).as_str())?;
        let geoip_lookup = (geoip_lookup as *const GeoIpLookup).as_ref();
        let normalizer = StoreProcessor::new(config, geoip_lookup);
        Ok(Box::into_raw(Box::new(normalizer)) as *mut RelayStoreNormalizer)
    }
}

ffi_fn! {
    unsafe fn relay_store_normalizer_free(
        normalizer: *mut RelayStoreNormalizer
    ) {
        if !normalizer.is_null() {
            let normalizer = normalizer as *mut StoreProcessor;
            Box::from_raw(normalizer);
        }
    }
}

ffi_fn! {
    unsafe fn relay_store_normalizer_normalize_event(
        normalizer: *mut RelayStoreNormalizer,
        event: *const RelayStr,
    ) -> Result<RelayStr> {
        let processor = normalizer as *mut StoreProcessor;
        let mut event = Annotated::<Event>::from_json((*event).as_str())?;
        process_value(&mut event, &mut *processor, ProcessingState::root())?;
        Ok(RelayStr::from_string(event.to_json()?))
    }
}

ffi_fn! {
    unsafe fn relay_translate_legacy_python_json(
        event: *mut RelayStr,
    ) -> Result<bool> {
        let data = slice::from_raw_parts_mut((*event).data as *mut u8, (*event).len);
        json_forensics::translate_slice(data);
        Ok(true)
    }
}

ffi_fn! {
    unsafe fn relay_scrub_event(
        config: *const RelayStr,
        event: *const RelayStr,
    ) -> Result<RelayStr> {
        let config: DataScrubbingConfig = serde_json::from_str((*config).as_str())?;
        let mut processor = match config.pii_config() {
            Some(pii_config) => PiiProcessor::new(pii_config),
            None => return Ok(RelayStr::new((*event).as_str())),
        };

        let mut event = Annotated::<Event>::from_json((*event).as_str())?;
        process_value(&mut event, &mut processor, ProcessingState::root())?;

        Ok(RelayStr::from_string(event.to_json()?))
    }
}

ffi_fn! {
    unsafe fn relay_test_panic() -> Result<()> {
        panic!("this is a test panic")
    }
}

/// Controls the globbing behaviors.
#[repr(u32)]
pub enum GlobFlags {
    DoubleStar = 1,
    CaseInsensitive = 2,
    PathNormalize = 4,
    AllowNewline = 8,
}

ffi_fn! {
    unsafe fn relay_is_glob_match(
        value: *const RelayBuf,
        pat: *const RelayStr,
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
        if (flags & GlobFlags::AllowNewline as u32) != 0 {
            options.allow_newline = true;
        }
        Ok(glob_match_bytes((*value).as_bytes(), (*pat).as_str(), options))
    }
}
