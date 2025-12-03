use std::fmt;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;

use relay_event_schema::protocol::Geo;
use relay_protocol::Annotated;

#[cfg(feature = "mmap")]
type ReaderType = maxminddb::Mmap;

#[cfg(not(feature = "mmap"))]
type ReaderType = Vec<u8>;

/// An error in the `GeoIpLookup`.
pub type GeoIpError = maxminddb::MaxMindDbError;

/// A geo ip lookup helper based on maxmind db files.
///
/// The helper is internally reference counted and can be cloned cheaply.
#[derive(Clone, Default)]
pub struct GeoIpLookup(Option<Arc<maxminddb::Reader<ReaderType>>>);

impl GeoIpLookup {
    /// Opens a maxminddb file by path.
    pub fn open<P>(path: P) -> Result<Self, GeoIpError>
    where
        P: AsRef<Path>,
    {
        #[cfg(feature = "mmap")]
        let reader = unsafe { maxminddb::Reader::open_mmap(path)? };
        #[cfg(not(feature = "mmap"))]
        let reader = maxminddb::Reader::open_readfile(path)?;
        Ok(GeoIpLookup(Some(Arc::new(reader))))
    }

    /// Creates a new [`GeoIpLookup`] instance without any data loaded.
    pub fn empty() -> Self {
        Self(None)
    }

    /// Looks up an IP address.
    pub fn try_lookup(&self, ip_address: IpAddr) -> Result<Option<Geo>, GeoIpError> {
        let Some(reader) = self.0.as_ref() else {
            return Ok(None);
        };

        let Some(city) = reader
            .lookup(ip_address)?
            .decode::<maxminddb::geoip2::City>()?
        else {
            return Ok(None);
        };

        Ok(Some(Geo {
            country_code: Annotated::from(city.country.iso_code.map(String::from)),
            city: Annotated::from(city.city.names.english.map(String::from)),
            subdivision: Annotated::from(
                city.subdivisions
                    .first()
                    .and_then(|subdivision| subdivision.names.english.map(String::from)),
            ),
            region: Annotated::from(city.country.names.english.map(String::from)),
            ..Default::default()
        }))
    }

    /// Like [`Self::try_lookup`], but swallows errors.
    pub fn lookup(&self, ip_address: IpAddr) -> Option<Geo> {
        self.try_lookup(ip_address).ok().flatten()
    }
}

impl fmt::Debug for GeoIpLookup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GeoIpLookup").finish()
    }
}
