//! Contains definitions for the security report interfaces.
//!
//! The security interfaces are CSP, HPKP, ExpectCT and ExpectStaple.
use core::fmt;

use serde::de::Error;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as SerdeValue;

use crate::types::{Annotated, Object, Value};

/// Models the content of a CSP report.
///
/// Note this models the older CSP reports (report-uri policy directive).
/// The new CSP reports (using report-to policy directive) are different.
///
/// NOTE: This is the structure used inside the Event (serialization is based on Annotated
/// infrastructure). We also use a version of this structure to deserialize from raw JSON
/// via serde.
///
///
/// See https://www.w3.org/TR/CSP3/
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct Csp {
    /// The directive whose enforcement caused the violation.
    #[metastructure(pii = "true")]
    pub effective_directive: Annotated<String>,
    /// The URI of the resource that was blocked from loading by the Content Security Policy.
    #[metastructure(pii = "true")]
    pub blocked_uri: Annotated<String>,
    /// The URI of the document in which the violation occurred.
    #[metastructure(pii = "true")]
    pub document_uri: Annotated<String>,
    /// The original policy as specified by the Content-Security-Policy HTTP header.
    pub original_policy: Annotated<String>,
    /// The referrer of the document in which the violation occurred.
    #[metastructure(pii = "true")]
    pub referrer: Annotated<String>,
    /// The HTTP status code of the resource on which the global object was instantiated.
    pub status_code: Annotated<u64>,
    /// The name of the policy section that was violated.
    pub violated_directive: Annotated<String>,
    /// The URL of the resource where the violation occurred.
    pub source_file: Annotated<String>,
    /// The line number in source-file on which the violation occurred.
    pub line_number: Annotated<u64>,
    /// The column number in source-file on which the violation occurred.
    pub column_number: Annotated<u64>,
    /// The first 40 characters of the inline script, event handler, or style that caused the
    /// violation.
    pub script_sample: Annotated<String>,
    /// Policy disposition (enforce or report).
    pub disposition: Annotated<String>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(pii = "true", additional_properties)]
    pub other: Object<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct CspRaw {
    /// The directive whose enforcement caused the violation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_directive: Option<String>,
    /// The URI of the resource that was blocked from loading by the Content Security Policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_uri: Option<String>,
    /// The URI of the document in which the violation occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_uri: Option<String>,
    /// The original policy as specified by the Content-Security-Policy HTTP header.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_policy: Option<String>,
    /// The referrer of the document in which the violation occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub referrer: Option<String>,
    /// The HTTP status code of the resource on which the global object was instantiated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u64>,
    /// The name of the policy section that was violated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub violated_directive: Option<String>,
    /// The URL of the resource where the violation occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_file: Option<String>,
    /// The line number in source-file on which the violation occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_number: Option<u64>,
    /// The column number in source-file on which the violation occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_number: Option<u64>,
    /// The first 40 characters of the inline script, event handler, or style that caused the
    /// violation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script_sample: Option<String>,
    /// Policy disposition (enforce or report).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disposition: Option<String>,
    // Additional arbitrary fields for forwards compatibility.
    //pub other: Object<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct CspReportRaw {
    pub csp_report: CspRaw,
}

//#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
// TODO RaduW  implement FromValue ToValue (either for
pub enum CspEffectiveDirective {
    BaseUri,
    ChildSrc,
    ConnectSrc,
    DefaultSrc,
    FontSrc,
    FormAction,
    FrameAncestors,
    FrameSrc,
    ImgSrc,
    ManifestSrc,
    MediaSrc,
    ObjectSrc,
    PluginTypes,
    PrefetchSrc,
    Referrer,
    ScriptSrc,
    ScriptSrcAttr,
    ScriptSrcElem,
    StyleSrc,
    StyleSrcElem,
    StyleSrcAttr,
    UpgradeInsecureRequests,
    WorkerSrc,
}

/// Expect CT security report sent by user agent (browser).
///
/// See https://tools.ietf.org/html/draft-ietf-httpbis-expect-ct-07#section-3.1
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct ExpectCt {
    /// Date time in rfc3339 format YYYY-MM-DDTHH:MM:DD{.FFFFFF}(Z|+/-HH:MM)
    /// UTC time that the UA observed the CT compliance failure
    pub date_time: Annotated<String>,
    /// The hostname to which the UA made the original request that failed the CT compliance check.
    pub host_name: Annotated<String>,
    pub port: Annotated<i64>,
    /// Date time in rfc3339 format
    pub effective_expiration_date: Annotated<String>,
    pub served_certificate_chain: Annotated<String>,
    pub validated_certificate_chain: Annotated<String>,
    pub scts: Annotated<Value>,
    pub failure_mode: Annotated<String>,
    pub test_report: Annotated<String>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(pii = "true", additional_properties)]
    pub other: Object<Value>,
}

//TODO: fill in the types
pub type Hpkp = Value;
pub type ExpectStaple = Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecurityReportType {
    Csp,
    ExpectCt,
    ExpectStaple,
    HpKp,
}

impl<'de> Deserialize<'de> for SecurityReportType {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(SecurityReportTypeVisitor)
    }
}

/// Helper structure to support custom deserialization of SecurityReportType
struct SecurityReportTypeVisitor;

// The deserializer expects a Visitor that can consume input and transform it in the
// deserialized object (this is the visitor for deserializing SecurityReportType objects)
impl<'de> Visitor<'de> for SecurityReportTypeVisitor {
    type Value = SecurityReportType;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Expecting a dictionary.")
    }

    /// SecurityReportType should be an object (JSON object) so we only need to
    /// support map deserialization (anything else is a format error)
    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut message_type: Option<SecurityReportType> = None;

        // look for key attributes that mark the object as being a specific type of report
        while let Some((key, _)) = access.next_entry::<&str, SerdeValue>()? {
            // NOTE: In theory we could stop as soon as we detect the type of security report.
            // In practice if we stop in the middle of the object deserialization the
            // deserialization fails because it expects the visitor to consume the whole object.
            // Maybe there is a cleaner/faster way to do that but I (RaduW) don't know it.
            match key {
                "csp-report" => {
                    message_type = Some(SecurityReportType::Csp);
                }
                "known-pins" => {
                    message_type = Some(SecurityReportType::HpKp);
                }
                "expect-staple-report" => {
                    message_type = Some(SecurityReportType::ExpectStaple);
                }
                "expect-ct-report" => {
                    message_type = Some(SecurityReportType::ExpectCt);
                }
                _ => {}
            }
        }
        message_type.ok_or(M::Error::custom("Invalid  security message type"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_csp_report() {
        let csp_report_text = r#"{
            "csp-report": {
                "document-uri": "https://example.com/foo/bar",
                "referrer": "https://www.google.com/",
                "violated-directive": "default-src self",
                "original-policy": "default-src self; report-uri /csp-hotline.php",
                "blocked-uri": "http://evilhackerscripts.com"
            }
        }"#;

        let report: CspReportRaw = serde_json::from_str(csp_report_text).unwrap();

        ::insta::assert_snapshot!(serde_json::to_string_pretty(&report).unwrap(),
        @r###"
       ⋮{
       ⋮  "csp-report": {
       ⋮    "blocked-uri": "http://evilhackerscripts.com",
       ⋮    "document-uri": "https://example.com/foo/bar",
       ⋮    "original-policy": "default-src self; report-uri /csp-hotline.php",
       ⋮    "referrer": "https://www.google.com/",
       ⋮    "violated-directive": "default-src self"
       ⋮  }
       ⋮}
        "###);
    }
}
