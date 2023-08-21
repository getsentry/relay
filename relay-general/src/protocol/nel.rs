//! Contains definitions for the Network Error Logging (NEL) interface.

use std::fmt::{self};

use serde::{Deserialize, Serialize};

use crate::protocol::{Event, LogEntry};
use crate::types::Annotated;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InvalidNelError;

impl fmt::Display for InvalidNelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid nel report")
    }
}

/// Inner (useful) part of a NEL report.
///
/// See `Nel` for meaning of fields.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
struct NelBodyRaw {
    elapsed_time: Option<u64>,
    method: Option<String>,
    phase: Option<String>,
    protocol: Option<String>,
    referrer: Option<String>,
    sampling_fraction: Option<f64>,
    server_ip: Option<String>,
    status_code: Option<u64>,
    #[serde(rename = "type")]
    ty: Option<String>,
}

/// Defines external, RFC-defined schema we accept, while `Nel` defines our own schema.
///
/// See `Nel` for meaning of fields.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
struct NelReportRaw {
    age: Option<u64>,
    #[serde(rename = "type")]
    ty: Option<String>, // always "network-error"
    url: Option<String>,
    user_agent: Option<String>,
    body: NelBodyRaw,
}

impl NelReportRaw {
    fn into_protocol(self) -> Nel {
        let body = NelBody {
            elapsed_time: Annotated::from(self.body.elapsed_time),
            method: Annotated::from(self.body.method),
            phase: Annotated::from(self.body.phase),
            protocol: Annotated::from(self.body.protocol),
            referrer: Annotated::from(self.body.referrer),
            sampling_fraction: Annotated::from(self.body.sampling_fraction),
            server_ip: Annotated::from(self.body.server_ip),
            status_code: Annotated::from(self.body.status_code),
            ty: Annotated::from(self.body.ty),
        };

        Nel {
            age: Annotated::from(self.age),
            ty: Annotated::from(self.ty),
            url: Annotated::from(self.url),
            user_agent: Annotated::from(self.user_agent),
            body: Annotated::from(body),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct NelBody {
    pub elapsed_time: Annotated<u64>,
    pub method: Annotated<String>,
    pub phase: Annotated<String>,
    pub protocol: Annotated<String>,
    pub referrer: Annotated<String>,
    pub sampling_fraction: Annotated<f64>,
    pub server_ip: Annotated<String>,
    pub status_code: Annotated<u64>,
    pub ty: Annotated<String>,
}

/// Models the content of a NEL report.
///
/// NOTE: This is the structure used inside the Event (serialization is based on Annotated
/// infrastructure). We also use a version of this structure to deserialize from raw JSON
/// via serde.
///
///
/// See <https://w3c.github.io/network-error-logging/>
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Nel {
    #[metastructure(pii = "true")]
    pub age: Annotated<u64>,
    #[metastructure(pii = "true")]
    pub ty: Annotated<String>,
    /// The URL of the document in which the error occurred.
    #[metastructure(pii = "true")]
    pub url: Annotated<String>,
    /// The User-Agent HTTP header.
    pub user_agent: Annotated<String>,
    pub body: Annotated<NelBody>,
}

impl Nel {
    pub fn apply_to_event(data: &[u8], event: &mut Event) -> Result<(), serde_json::Error> {
        let raw_report = serde_json::from_slice::<NelReportRaw>(data)?;

        event.logentry = Annotated::new(LogEntry::from(format!(
            "{} / {}",
            raw_report.body.phase.as_ref().unwrap_or(&String::new()),
            raw_report.body.ty.as_ref().unwrap_or(&String::new())
        )));

        event.nel = Annotated::from(raw_report.into_protocol());

        // event.logentry = Annotated::new(LogEntry::from(raw_csp.get_message(effective_directive)));
        // event.culprit = Annotated::new(raw_csp.get_culprit());
        // event.tags = Annotated::new(raw_csp.get_tags(effective_directive));

        Ok(())
    }
}

// TODO: tests
