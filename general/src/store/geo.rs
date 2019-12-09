use std::fmt;
use std::path::Path;

use crate::protocol::Geo;
use crate::types::Annotated;

#[cfg(feature = "mmap")]
type ReaderType = memmap::Mmap;

#[cfg(not(feature = "mmap"))]
type ReaderType = Vec<u8>;

/// A geo ip lookup helper based on maxmind db files.
pub struct GeoIpLookup(maxminddb::Reader<ReaderType>);

impl GeoIpLookup {
    /// Opens a maxminddb file by path.
    pub fn open<P>(path: P) -> Result<Self, maxminddb::MaxMindDBError>
    where
        P: AsRef<Path>,
    {
        #[cfg(feature = "mmap")]
        let reader = maxminddb::Reader::open_mmap(path)?;
        #[cfg(not(feature = "mmap"))]
        let reader = maxminddb::Reader::open_readfile(path)?;
        Ok(GeoIpLookup(reader))
    }

    /// Looks up an IP address.
    pub fn lookup(&self, ip_address: &str) -> Result<Option<Geo>, maxminddb::MaxMindDBError> {
        // XXX: Why do we parse the IP again after deserializing?
        let ip_address = match ip_address.parse() {
            Ok(x) => x,
            Err(_) => return Ok(None),
        };

        let city: maxminddb::geoip2::City = match self.0.lookup(ip_address) {
            Ok(x) => x,
            Err(maxminddb::MaxMindDBError::AddressNotFoundError(_)) => return Ok(None),
            Err(e) => return Err(e),
        };

        Ok(Some(Geo {
            country_code: Annotated::from(
                city.country
                    .as_ref()
                    .and_then(|country| Some(country.iso_code.as_ref()?.to_string())),
            ),
            city: Annotated::from(
                city.city
                    .as_ref()
                    .and_then(|city| Some(city.names.as_ref()?.get("en")?.to_owned())),
            ),
            region: Annotated::from(
                city.country
                    .as_ref()
                    .and_then(|country| Some(country.names.as_ref()?.get("en")?.to_owned())),
            ),
            ..Default::default()
        }))
    }
}

impl fmt::Debug for GeoIpLookup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GeoIpLookup").finish()
    }
}
