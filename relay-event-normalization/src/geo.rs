use std::fmt;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;

use relay_common::time::UnixTimestamp;
use relay_event_schema::protocol::Geo;
use relay_protocol::Annotated;
use serde::Deserialize;

#[cfg(feature = "mmap")]
type ReaderType = maxminddb::Mmap;

#[cfg(not(feature = "mmap"))]
type ReaderType = Vec<u8>;

/// An error opening or querying a MaxMind database.
pub type GeoIpError = maxminddb::MaxMindDbError;

#[derive(Debug)]
struct Mmdb(maxminddb::Reader<ReaderType>);

impl Mmdb {
    fn open(path: impl AsRef<Path>) -> Result<Self, GeoIpError> {
        #[cfg(feature = "mmap")]
        // The updater replaces the file by rename. A reader keeps its original mapping.
        let reader = unsafe { maxminddb::Reader::open_mmap(path)? };
        #[cfg(not(feature = "mmap"))]
        let reader = maxminddb::Reader::open_readfile(path)?;
        Ok(Self(reader))
    }

    fn lookup<'a, T: Deserialize<'a>>(&'a self, ip: IpAddr) -> Result<Option<T>, GeoIpError> {
        self.0.lookup(ip)?.decode()
    }
}

/// Reader for city and country lookups. Clones share the same database.
#[derive(Clone, Default)]
pub struct CityLookup(Option<Arc<Mmdb>>);

/// Historical name for [`CityLookup`].
pub use CityLookup as GeoIpLookup;

impl CityLookup {
    /// Opens a local database containing MaxMind city records.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, GeoIpError> {
        Ok(Self(Some(Arc::new(Mmdb::open(path)?))))
    }

    /// Creates a new [`CityLookup`] instance without any data loaded.
    pub fn empty() -> Self {
        Self(None)
    }

    /// Unix timestamp when the database was built.
    ///
    /// Returns `None` for an [`Self::empty`] database.
    pub fn build_epoch(&self) -> Option<UnixTimestamp> {
        let reader = self.0.as_ref()?;
        Some(UnixTimestamp::from_secs(reader.0.metadata.build_epoch))
    }

    /// Looks up an IP address.
    pub fn try_lookup(&self, ip_address: IpAddr) -> Result<Option<Geo>, GeoIpError> {
        let Some(reader) = self.0.as_ref() else {
            return Ok(None);
        };

        let Some(city) = reader.lookup::<maxminddb::geoip2::City>(ip_address)? else {
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

impl fmt::Debug for CityLookup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CityLookup").finish()
    }
}

/// Reader for a local MaxMind ASN database.
#[derive(Debug)]
pub struct AsnLookup(Mmdb);

impl AsnLookup {
    /// Opens the ASN database once for subsequent synchronous lookups.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, GeoIpError> {
        Mmdb::open(path).map(Self)
    }

    /// Returns the number and organization if an ASN is known for this IP.
    pub fn lookup(&self, ip: IpAddr) -> Option<(u32, Option<&str>)> {
        let record = self.0.lookup::<maxminddb::geoip2::Asn>(ip).ok()??;
        Some((
            record.autonomous_system_number?,
            record.autonomous_system_organization,
        ))
    }
}
