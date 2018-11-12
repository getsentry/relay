use std::fmt;

use maxminddb;

use crate::protocol::Geo;
use crate::types::Annotated;

pub struct GeoIpLookup(maxminddb::Reader);

impl fmt::Debug for GeoIpLookup {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("GeoIpLookup").finish()
    }
}

impl GeoIpLookup {
    pub fn open(path: &str) -> Result<Self, maxminddb::MaxMindDBError> {
        Ok(GeoIpLookup(maxminddb::Reader::open(path)?))
    }

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
            country_code: Annotated(
                city.country
                    .as_ref()
                    .and_then(|country| Some(country.iso_code.as_ref()?.to_string())),
                Default::default(),
            ),
            city: Annotated(
                city.city
                    .as_ref()
                    .and_then(|city| Some(city.names.as_ref()?.get("en")?.to_owned())),
                Default::default(),
            ),
            region: Annotated(
                city.country
                    .as_ref()
                    .and_then(|country| Some(country.names.as_ref()?.get("en")?.to_owned())),
                Default::default(),
            ),
            ..Default::default()
        }))
    }
}
