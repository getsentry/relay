use std::fmt;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;

use maxminddb::geoip2;
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

/// The record to decode and the Relay fields to take from it.
pub trait Edition: Default {
    type Record<'db>: Deserialize<'db>;
    type Fields<'db>;

    fn fields<'db>(&self, record: Self::Record<'db>) -> Option<Self::Fields<'db>>;
}

/// A local MaxMind database. Clones share the same reader.
#[derive(Clone, Default)]
pub struct Lookup<E> {
    reader: Option<Arc<maxminddb::Reader<ReaderType>>>,
    edition: E,
}

impl<E: Edition> Lookup<E> {
    /// Opens a local MMDB file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, GeoIpError> {
        #[cfg(feature = "mmap")]
        // The updater replaces the file by rename. A reader keeps its original mapping.
        let reader = unsafe { maxminddb::Reader::open_mmap(path)? };
        #[cfg(not(feature = "mmap"))]
        let reader = maxminddb::Reader::open_readfile(path)?;
        Ok(Self {
            reader: Some(Arc::new(reader)),
            edition: E::default(),
        })
    }

    /// Creates a lookup without a database.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Whether a database is available for lookups.
    pub fn is_loaded(&self) -> bool {
        self.reader.is_some()
    }

    /// Unix timestamp when the database was built.
    pub fn build_epoch(&self) -> Option<UnixTimestamp> {
        Some(UnixTimestamp::from_secs(
            self.reader.as_ref()?.metadata.build_epoch,
        ))
    }

    /// Looks up an IP and decodes this edition's record.
    pub fn try_lookup(&self, ip: IpAddr) -> Result<Option<E::Fields<'_>>, GeoIpError> {
        let Some(reader) = &self.reader else {
            return Ok(None);
        };
        Ok(reader
            .lookup(ip)?
            .decode::<E::Record<'_>>()?
            .and_then(|record| self.edition.fields(record)))
    }

    /// Like [`Self::try_lookup`], but swallows errors.
    pub fn lookup(&self, ip: IpAddr) -> Option<E::Fields<'_>> {
        self.try_lookup(ip).ok().flatten()
    }
}

impl<E> fmt::Debug for Lookup<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lookup")
            .field("loaded", &self.reader.is_some())
            .finish()
    }
}

/// Reader for city and country lookups.
pub type CityLookup = Lookup<City>;

#[derive(Clone, Default)]
pub struct City;

impl Edition for City {
    type Record<'db> = geoip2::City<'db>;
    type Fields<'db> = Geo;

    fn fields<'db>(&self, city: Self::Record<'db>) -> Option<Self::Fields<'db>> {
        Some(Geo {
            country_code: Annotated::from(city.country.iso_code.map(String::from)),
            city: Annotated::from(city.city.names.english.map(String::from)),
            subdivision: Annotated::from(
                city.subdivisions
                    .first()
                    .and_then(|subdivision| subdivision.names.english.map(String::from)),
            ),
            region: Annotated::from(city.country.names.english.map(String::from)),
            ..Default::default()
        })
    }
}

/// Reader for client autonomous system lookups.
pub type AsnLookup = Lookup<Asn>;

#[derive(Clone, Default)]
pub struct Asn;

impl Edition for Asn {
    type Record<'db> = geoip2::Asn<'db>;
    type Fields<'db> = (u32, Option<&'db str>);

    fn fields<'db>(&self, record: Self::Record<'db>) -> Option<Self::Fields<'db>> {
        Some((
            record.autonomous_system_number?,
            record.autonomous_system_organization,
        ))
    }
}
