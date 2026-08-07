use std::cell::Cell;
use std::fmt::{self, Display};

use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
use opentelemetry_proto::tonic::logs::*;
use opentelemetry_proto::tonic::resource::v1::Resource;
use serde::Deserialize;
use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};

use crate::processing::logs;
use crate::services::outcome::DiscardReason;

struct Meter {
    remaining: Cell<usize>,
}

struct MeterError {}

impl Display for MeterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ran out of meter")
    }
}

impl Meter {
    fn new(max: usize) -> Self {
        Self {
            remaining: Cell::new(max),
        }
    }

    fn spend<E: de::Error>(&self) -> Result<(), E> {
        match self.remaining.get() {
            0 => Err(de::Error::custom(MeterError {})),
            n => {
                self.remaining.set(n - 1);
                Ok(())
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.remaining.get() == 0
    }
}

// A deserializer for generic arrays that can pass deserializer state ("seed") to the
// children elements.
struct Array<S>(S);

impl<'de, S: DeserializeSeed<'de> + Copy> DeserializeSeed<'de> for Array<S> {
    type Value = Vec<S::Value>;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_seq(self)
    }
}

impl<'de, S: DeserializeSeed<'de> + Copy> Visitor<'de> for Array<S> {
    type Value = Vec<S::Value>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("an array")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut out = Vec::new();
        while let Some(item) = seq.next_element_seed(self.0)? {
            out.push(item);
        }
        Ok(out)
    }
}

/// Mapping for OTEL v1::LogsData
#[derive(Copy, Clone)]
struct LogsDataSeed<'b>(&'b Meter);

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "camelCase")]
enum LogsDataField {
    ResourceLogs,
    #[serde(other)]
    Other,
}

impl<'de> DeserializeSeed<'de> for LogsDataSeed<'_> {
    type Value = v1::LogsData;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for LogsDataSeed<'_> {
    type Value = v1::LogsData;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("logs data")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut resource_logs = Vec::new();
        while let Some(field) = map.next_key::<LogsDataField>()? {
            match field {
                LogsDataField::ResourceLogs => {
                    resource_logs = map.next_value_seed(Array(ResourceLogsSeed(self.0)))?;
                }
                LogsDataField::Other => drop(map.next_value::<IgnoredAny>()?),
            }
        }
        Ok(v1::LogsData { resource_logs })
    }
}

/// Mapping for OTEL v1::ResourceLogs
#[derive(Copy, Clone)]
struct ResourceLogsSeed<'b>(&'b Meter);

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "camelCase")]
enum ResourceLogsField {
    Resource,
    ScopeLogs,
    SchemaUrl,
    #[serde(other)]
    Other,
}

impl<'de> DeserializeSeed<'de> for ResourceLogsSeed<'_> {
    type Value = v1::ResourceLogs;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        self.0.spend()?;
        d.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ResourceLogsSeed<'_> {
    type Value = v1::ResourceLogs;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("resource logs")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut out = v1::ResourceLogs::default();
        while let Some(field) = map.next_key::<ResourceLogsField>()? {
            match field {
                // Below this point the derived impls take over unchanged.
                ResourceLogsField::Resource => {
                    out.resource = map.next_value::<Option<Resource>>()?
                }
                ResourceLogsField::SchemaUrl => out.schema_url = map.next_value()?,
                ResourceLogsField::ScopeLogs => {
                    out.scope_logs = map.next_value_seed(Array(ScopeLogsSeed(self.0)))?;
                }
                ResourceLogsField::Other => drop(map.next_value::<IgnoredAny>()?),
            }
        }
        Ok(out)
    }
}

/// Mapping for OTEL v1::ScopeLogs
#[derive(Copy, Clone)]
struct ScopeLogsSeed<'b>(&'b Meter);

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "camelCase")]
enum ScopeLogsField {
    Scope,
    LogRecords,
    SchemaUrl,
    #[serde(other)]
    Other,
}

impl<'de> DeserializeSeed<'de> for ScopeLogsSeed<'_> {
    type Value = v1::ScopeLogs;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        self.0.spend()?;
        d.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for ScopeLogsSeed<'_> {
    type Value = v1::ScopeLogs;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("scope logs")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut out = v1::ScopeLogs::default();
        while let Some(field) = map.next_key::<ScopeLogsField>()? {
            match field {
                ScopeLogsField::Scope => {
                    out.scope = map.next_value::<Option<InstrumentationScope>>()?
                }
                ScopeLogsField::SchemaUrl => out.schema_url = map.next_value()?,
                ScopeLogsField::LogRecords => {
                    out.log_records = map.next_value_seed(LogRecordsSeed(self.0))?;
                }
                ScopeLogsField::Other => drop(map.next_value::<IgnoredAny>()?),
            }
        }
        Ok(out)
    }
}

/// Mapping for OTEL v1::LogRecord
#[derive(Copy, Clone)]
struct LogRecordsSeed<'b>(&'b Meter);

impl<'de> DeserializeSeed<'de> for LogRecordsSeed<'_> {
    type Value = Vec<v1::LogRecord>;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for LogRecordsSeed<'_> {
    type Value = Vec<v1::LogRecord>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("log records")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut out = Vec::new();
        while let Some(record) = seq.next_element::<v1::LogRecord>()? {
            self.0.spend()?;
            out.push(record);
        }
        Ok(out)
    }
}

/// Deserialize the supplied JSON into v1::LogsData, returning an error if the number of logs
/// elements in the payload exceeds the supplied maximum.
pub fn deserialize(payload: &[u8], max: usize) -> Result<v1::LogsData, logs::Error> {
    let budget = Meter::new(max);
    let mut de = serde_json::Deserializer::from_slice(payload);
    let res = LogsDataSeed(&budget).deserialize(&mut de);

    match res {
        Ok(logs) => {
            // Only call 'end' if we're on the Ok path (end expects EOF/trailing whitespace, will
            // error otherwise.)
            de.end()
                .map_err(|_| logs::Error::Invalid(DiscardReason::InvalidJson))?;
            Ok(logs)
        }
        Err(_) if budget.is_empty() => Err(logs::Error::TooManyExpandedLogs),
        Err(_) => Err(logs::Error::Invalid(DiscardReason::InvalidJson)),
    }
}
