use geoip::{self, CityInfo, GeoIp};

use std::cell::RefCell;
use std::net;
use std::path::{Path, PathBuf};

thread_local! {
    static GEOIP_DB: RefCell<Option<(PathBuf, GeoIp)>> = RefCell::new(None);
}

pub fn lookup_ip(geoip_path: &Path, ip_address: &str) -> Option<CityInfo> {
    // XXX: Ugly. Make PR against rust-geoip to get rid of their own IpAddr
    // XXX: Why do we parse the IP again after deserializing?
    let ip_address = match ip_address.parse().ok()? {
        net::IpAddr::V4(x) => geoip::IpAddr::V4(x),
        net::IpAddr::V6(x) => geoip::IpAddr::V6(x),
    };

    // thread local cache because most of the time we only deal with one geoip db
    GEOIP_DB.with(|f| {
        let mut cache = f.borrow_mut();
        let is_valid_cache = match *cache {
            Some((ref path, _)) => path == geoip_path,
            None => false,
        };

        if !is_valid_cache {
            *cache = Some((
                geoip_path.to_owned(),
                GeoIp::open(&geoip_path, geoip::Options::MemoryCache).ok()?,
            ));
        }

        (*cache).as_ref()?.1.city_info_by_ip(ip_address)
    })
}
