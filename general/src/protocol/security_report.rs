//! Contains definitions for the security report interfaces.
//!
//! The security interfaces are CSP, HPKP, ExpectCT and ExpectStaple.
use std::collections::BTreeMap;

use serde::de::{Error, IgnoredAny};
use serde::{Deserialize, Deserializer, Serialize};

use crate::types::{Annotated, Array, Object, Value};

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

/// Inner (useful) part of a CSP report.
///
/// See `Csp` for meaning of fields.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct CspRaw {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_directive: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub referrer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub violated_directive: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script_sample: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disposition: Option<String>,

    #[serde(flatten)]
    pub other: BTreeMap<String, serde_json::Value>,
}

impl CspRaw {
    pub fn get_message(&self) -> String {
        let directive = self
            .local_script_violation_type()
            .or_else(|| self.effective_directive.as_ref().map(String::as_str))
            .unwrap_or("");

        let is_local = CspRaw::is_local(&self.blocked_uri);

        if is_local {
            match directive {
                "child-src" => "Blocked inline 'child'".to_string(),
                "connect-src" => "Blocked inline 'connect'".to_string(),
                "font-src" => "Blocked inline 'font'".to_string(),
                "form-action" => "".to_string(),
                "img-src" => "Blocked inline 'image'".to_string(),
                "manifest-src" => "Blocked inline 'manifest'".to_string(),
                "media-src" => "Blocked inline 'media'".to_string(),
                "object-src" => "Blocked inline 'object'".to_string(),
                "script-src" => "Blocked unsafe (eval() or inline) 'script'".to_string(),
                "script-src-elem" => "Blocked unsafe 'script' element".to_string(),
                "script-src-attr" => "Blocked inline script attribute".to_string(),
                "style-src" => "Blocked inline 'style'".to_string(),
                "style-src-elem" => "Blocked 'style' or 'link' element".to_string(),
                "style-src-attr" => "Blocked style attribute".to_string(),
                "unsafe-inline" => "Blocked unsafe inline 'script'".to_string(),
                "unsafe-eval" => "Blocked unsafe eval() 'script'".to_string(),
                directive => format!("Blocked inline {}", directive),
            }
        } else {
            let uri = self.blocked_uri.as_ref().map(String::as_str).unwrap_or("");

            match directive {
                "child-src" => format!("Blocked 'child' from '{}'", uri),
                "connect-src" => format!("Blocked 'connect' from '{}'", uri),
                "font-src" => format!("Blocked 'font' from '{}'", uri),
                "form-action" => format!("Blocked 'form' action to '{}'", uri),
                "img-src" => format!("Blocked 'image' from '{}'", uri),
                "manifest-src" => format!("Blocked 'manifest' from '{}'", uri),
                "media-src" => format!("Blocked 'media' from '{}'", uri),
                "object-src" => format!("Blocked 'object' from '{}'", uri),
                "script-src" => format!("Blocked 'script' from '{}'", uri),
                "script-src-elem" => format!("Blocked 'script' from '{}'", uri),
                "script-src-attr" => format!("Blocked inline script attribute from '{}'", uri),
                "style-src" => format!("Blocked 'style' from '{}'", uri),
                "style-src-elem" => format!("Blocked 'style' from '{}'", uri),
                "style-src-attr" => format!("Blocked style attribute from '{}'", uri),
                "unsafe-inline" => "".to_string(), // should not happen
                "unsafe-eval" => "".to_string(),   // should not happen
                directive => format!("Blocked {} from {} ", directive, uri),
            }
        }
    }

    fn local_script_violation_type(&self) -> Option<&'static str> {
        if CspRaw::is_local(&self.blocked_uri) {
            let effective_directive = self.effective_directive.as_ref()?;
            let violated_directive = self.violated_directive.as_ref()?;

            if effective_directive == "script_src" {
                if violated_directive.contains("'unsafe-inline'") {
                    return Some("unsafe-inline");
                }
                if violated_directive.contains("'unsafe-eval'") {
                    return Some("unsafe-eval");
                }
            }
        }
        None
    }

    fn is_local(uri: &Option<String>) -> bool {
        match uri {
            None => true,
            Some(uri) => match uri.as_str() {
                "" | "self" | "'self'" => true,
                _ => false,
            },
        }
    }
}

/// Defines external, RFC-defined schema we accept, while `Csp` defines our own schema.
///
/// See `Csp` for meaning of fields.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct CspReportRaw {
    pub csp_report: CspRaw,
}

impl From<CspRaw> for Csp {
    fn from(raw: CspRaw) -> Csp {
        Csp {
            effective_directive: Annotated::from(raw.effective_directive),
            blocked_uri: Annotated::from(raw.blocked_uri),
            document_uri: Annotated::from(raw.document_uri),
            original_policy: Annotated::from(raw.original_policy),
            referrer: Annotated::from(raw.referrer),
            status_code: Annotated::from(raw.status_code),
            violated_directive: Annotated::from(raw.violated_directive),
            source_file: Annotated::from(raw.source_file),
            line_number: Annotated::from(raw.line_number),
            column_number: Annotated::from(raw.column_number),
            script_sample: Annotated::from(raw.script_sample),
            disposition: Annotated::from(raw.disposition),
            other: raw
                .other
                .into_iter()
                .map(|(k, v)| (k, Annotated::from(v)))
                .collect(),
        }
    }
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
    pub scts: Annotated<Array<SingleCertificateTimestamp>>,
    pub failure_mode: Annotated<String>,
    pub test_report: Annotated<String>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(pii = "true", additional_properties)]
    pub other: Object<Value>,
}

impl From<ExpectCtRaw> for ExpectCt {
    fn from(raw: ExpectCtRaw) -> ExpectCt {
        ExpectCt {
            date_time: Annotated::from(raw.date_time),
            host_name: Annotated::from(raw.host_name),
            port: Annotated::from(raw.port),
            effective_expiration_date: Annotated::from(raw.effective_expiration_date),
            served_certificate_chain: Annotated::from(raw.served_certificate_chain),
            validated_certificate_chain: Annotated::from(raw.validated_certificate_chain),
            scts: Annotated::from(raw.scts.map(|scts| {
                scts.into_iter()
                    .map(|elm| Annotated::from(SingleCertificateTimestamp::from(elm)))
                    .collect()
            })),
            failure_mode: Annotated::from(raw.failure_mode),
            test_report: Annotated::from(raw.test_report),
            other: raw
                .other
                .into_iter()
                .map(|(k, v)| (k, Annotated::from(v)))
                .collect(),
        }
    }
}

/// Object used in ExpectCt reports
///
/// See https://tools.ietf.org/html/draft-ietf-httpbis-expect-ct-07#section-3.1
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct SingleCertificateTimestamp {
    pub version: Annotated<i64>,
    pub status: Annotated<String>,
    pub source: Annotated<String>,
    pub serialized_stc: Annotated<String>,
}

impl From<SingleCertificateTimestampRaw> for SingleCertificateTimestamp {
    fn from(raw: SingleCertificateTimestampRaw) -> SingleCertificateTimestamp {
        SingleCertificateTimestamp {
            version: Annotated::from(raw.version),
            status: Annotated::from(raw.status),
            source: Annotated::from(raw.source),
            serialized_stc: Annotated::from(raw.serialized_stc),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExpectCtReportRaw {
    pub expect_ct_report: ExpectCtRaw,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExpectCtRaw {
    pub date_time: Option<String>,
    pub host_name: Option<String>,
    pub port: Option<i64>,
    pub effective_expiration_date: Option<String>,
    pub served_certificate_chain: Option<String>,
    pub validated_certificate_chain: Option<String>,
    pub scts: Option<Vec<SingleCertificateTimestampRaw>>,
    pub failure_mode: Option<String>,
    pub test_report: Option<String>,
    #[serde(flatten)]
    pub other: BTreeMap<String, serde_json::Value>,
}

impl ExpectCtRaw {
    pub fn get_message(&self) -> String {
        format!("Expect-CT failed for '{}'", "TODO host_name")
    }
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct SingleCertificateTimestampRaw {
    pub version: Option<i64>,
    pub status: Option<String>,
    pub source: Option<String>,
    pub serialized_stc: Option<String>,
}

/// Schema as defined in RFC7469, Section 3
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct Hpkp {
    /// > Indicates the time the UA observed the Pin Validation failure.
    // TODO: Validate (RFC3339)
    pub date_time: Annotated<String>,

    /// > Hostname to which the UA made the original request that failed Pin Validation.
    pub hostname: Annotated<String>,

    /// > The port to which the UA made the original request that failed Pin Validation.
    pub port: Annotated<u64>,

    /// > Effective Expiration Date for the noted pins.
    // TODO: Validate (RFC3339)
    pub effective_expiration_date: Annotated<String>,

    /// > Indicates whether or not the UA has noted the includeSubDomains directive for the Known
    /// Pinned Host.
    pub include_subdomains: Annotated<bool>,

    /// > Indicates the hostname that the UA noted when it noted the Known Pinned Host.  This field
    /// > allows operators to understand why Pin Validation was performed for, e.g., foo.example.com
    /// > when the noted Known Pinned Host was example.com with includeSubDomains set.
    pub noted_hostname: Annotated<String>,

    /// > The certificate chain, as served by the Known Pinned Host during TLS session setup.  It
    /// > is provided as an array of strings; each string pem1, ... pemN is the Privacy-Enhanced Mail
    /// > (PEM) representation of each X.509 certificate as described in [RFC7468].
    pub served_certificate_chain: Annotated<Array<String>>,

    /// > The certificate chain, as constructed by the UA during certificate chain verification.
    pub validated_certificate_chain: Annotated<Array<String>>,

    /// > Pins that the UA has noted for the Known Pinned Host.
    // TODO: regex ths string for 'pin-sha256="ABC123"' syntax
    #[metastructure(required = "true")]
    pub known_pins: Annotated<Array<String>>,

    #[metastructure(pii = "true", additional_properties)]
    pub other: Object<Value>,
}

/// Defines external, RFC-defined schema we accept, while `Hpkp` defines our own schema.
///
/// See `Hpkp` for meaning of fields.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct HpkpRaw {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effective_expiration_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_subdomains: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub noted_hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub served_certificate_chain: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validated_certificate_chain: Option<Vec<String>>,
    pub known_pins: Vec<String>,
    #[serde(flatten)]
    pub other: BTreeMap<String, serde_json::Value>,
}

impl HpkpRaw {
    pub fn get_message(&self) -> String {
        //TODO get the host_name
        format!(
            "Public key pinning validation failed for '{}'",
            "TODO get the host_name"
        )
    }
}

impl From<HpkpRaw> for Hpkp {
    fn from(raw: HpkpRaw) -> Hpkp {
        Hpkp {
            date_time: Annotated::from(raw.date_time),
            hostname: Annotated::from(raw.hostname),
            port: Annotated::from(raw.port),
            effective_expiration_date: Annotated::from(raw.effective_expiration_date),
            include_subdomains: Annotated::from(raw.include_subdomains),
            noted_hostname: Annotated::from(raw.noted_hostname),
            served_certificate_chain: Annotated::from(
                raw.served_certificate_chain
                    .map(|chain| chain.into_iter().map(Annotated::from).collect()),
            ),
            validated_certificate_chain: Annotated::from(
                raw.validated_certificate_chain
                    .map(|chain| chain.into_iter().map(Annotated::from).collect()),
            ),
            known_pins: Annotated::new(raw.known_pins.into_iter().map(Annotated::from).collect()),
            other: raw
                .other
                .into_iter()
                .map(|(k, v)| (k, Annotated::from(v)))
                .collect(),
        }
    }
}

/// Represents an Expect Staple security report
/// See https://scotthelme.co.uk/ocsp-expect-staple/ for specification
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
pub struct ExpectStaple {
    date_time: Annotated<String>,
    hostname: Annotated<String>,
    port: Annotated<i64>,
    effective_expiration_date: Annotated<String>,
    response_status: Annotated<String>,
    cert_status: Annotated<String>,
    served_certificate_chain: Annotated<Array<String>>,
    validated_certificate_chain: Annotated<Array<String>>,
    ocsp_response: Annotated<String>,
}

impl From<ExpectStapleRaw> for ExpectStaple {
    fn from(staple_raw: ExpectStapleRaw) -> ExpectStaple {
        ExpectStaple {
            date_time: Annotated::from(staple_raw.date_time),
            hostname: Annotated::from(staple_raw.hostname),
            port: Annotated::from(staple_raw.port),
            effective_expiration_date: Annotated::from(staple_raw.effective_expiration_date),
            response_status: Annotated::from(staple_raw.response_status),
            cert_status: Annotated::from(staple_raw.cert_status),
            served_certificate_chain: Annotated::from(
                staple_raw
                    .served_certificate_chain
                    .map(|cert_chain| cert_chain.into_iter().map(Annotated::from).collect()),
            ),
            validated_certificate_chain: Annotated::from(
                staple_raw
                    .validated_certificate_chain
                    .map(|cert_chain| cert_chain.into_iter().map(Annotated::from).collect()),
            ),
            ocsp_response: Annotated::from(staple_raw.ocsp_response),
        }
    }
}

/// Defines external, RFC-defined schema we accept, while `ExpectStaple` defines our own schema.
///
/// See `ExpectStaple` for meaning of fields.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExpectStapleReportRaw {
    pub expect_staple_report: ExpectStapleRaw,
}

/// Inner (useful) part of a Expect Stable report as sent by a user agent ( browser)
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ExpectStapleRaw {
    date_time: String,
    hostname: String,
    port: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    effective_expiration_date: Option<String>,
    response_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cert_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    served_certificate_chain: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    validated_certificate_chain: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ocsp_response: Option<String>,
}

impl ExpectStapleRaw {
    pub fn get_message(&self) -> String {
        //TODO implement
        format!("Expect-Staple failed for '{}'", "TODO self.hostname")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecurityReportType {
    Csp,
    ExpectCt,
    ExpectStaple,
    Hpkp,
}

impl<'de> Deserialize<'de> for SecurityReportType {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "kebab-case")]
        struct Helper {
            csp_report: Option<IgnoredAny>,
            known_pins: Option<IgnoredAny>,
            expect_staple_report: Option<IgnoredAny>,
            expect_ct_report: Option<IgnoredAny>,
        }

        let helper = Helper::deserialize(deserializer)?;

        if helper.csp_report.is_some() {
            Ok(SecurityReportType::Csp)
        } else if helper.known_pins.is_some() {
            Ok(SecurityReportType::Hpkp)
        } else if helper.expect_staple_report.is_some() {
            Ok(SecurityReportType::ExpectStaple)
        } else if helper.expect_ct_report.is_some() {
            Ok(SecurityReportType::ExpectCt)
        } else {
            Err(D::Error::custom("Invalid security message type"))
        }
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

    #[test]
    fn test_deserialize_expect_ct_report() {}

    #[test]
    fn test_deserialize_expect_staple_report() {}

    #[test]
    fn test_deserialize_hpkp_report() {}

    #[test]
    fn test_security_report_type_deserializer_recognizes_csp_reports() {
        let csp_report_text = r#"{
            "csp-report": {
                "document-uri": "https://example.com/foo/bar",
                "referrer": "https://www.google.com/",
                "violated-directive": "default-src self",
                "original-policy": "default-src self; report-uri /csp-hotline.php",
                "blocked-uri": "http://evilhackerscripts.com"
            }
        }"#;

        let report_type: SecurityReportType = serde_json::from_str(csp_report_text).unwrap();
        assert_eq!(report_type, SecurityReportType::Csp);
    }

    #[test]
    fn test_security_report_type_deserializer_recognizes_expect_ct_reports() {
        let expect_ct_report_text = r#"{
            "expect-ct-report": {
                "date-time": "2014-04-06T13:00:50Z",
                "hostname": "www.example.com",
                "port": 443,
                "effective-expiration-date": "2014-05-01T12:40:50Z",
                "served-certificate-chain": [
                    "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"
                ],
                "validated-certificate-chain": [
                    "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"
                ],
                "scts": [
                    {
                        "version": 1,
                        "status": "invalid",
                        "source": "embedded",
                        "serialized_sct": "ABCD=="
                    }
                ]
            }
        }"#;

        let report_type: SecurityReportType = serde_json::from_str(expect_ct_report_text).unwrap();
        assert_eq!(report_type, SecurityReportType::ExpectCt);
    }

    #[test]
    fn test_security_report_type_deserializer_recognizes_expect_staple_reports() {
        let expect_staple_report_text = r#"{
             "expect-staple-report": {
                "date-time": "2014-04-06T13:00:50Z",
                "hostname": "www.example.com",
                "port": 443,
                "response-status": "ERROR_RESPONSE",
                "cert-status": "REVOKED",
                "effective-expiration-date": "2014-05-01T12:40:50Z",
                "served-certificate-chain": ["-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"],
                "validated-certificate-chain": ["-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----"]
            }
        }"#;
        let report_type: SecurityReportType =
            serde_json::from_str(expect_staple_report_text).unwrap();
        assert_eq!(report_type, SecurityReportType::ExpectStaple);
    }

    #[test]
    fn test_security_report_type_deserializer_recognizes_hpkp_reports() {
        let hpkp_report_text = r#"{
            "date-time": "2014-04-06T13:00:50Z",
            "hostname": "www.example.com",
            "port": 443,
            "effective-expiration-date": "2014-05-01T12:40:50Z",
            "include-subdomains": false,
            "served-certificate-chain": [
              "-----BEGIN CERTIFICATE-----\n MIIEBDCCAuygAwIBAgIDAjppMA0GCSqGSIb3DQEBBQUAMEIxCzAJBgNVBAYTAlVT\n... -----END CERTIFICATE-----"
            ],
            "validated-certificate-chain": [
              "-----BEGIN CERTIFICATE-----\n MIIEBDCCAuygAwIBAgIDAjppMA0GCSqGSIb3DQEBBQUAMEIxCzAJBgNVBAYTAlVT\n... -----END CERTIFICATE-----"
            ],
            "known-pins": [
              "pin-sha256=\"d6qzRu9zOECb90Uez27xWltNsj0e1Md7GkYYkVoZWmM=\"",
              "pin-sha256=\"E9CZ9INDbd+2eRQozYqqbQ2yXLVKB9+xcprMF+44U1g=\""
            ]
          }"#;

        let report_type: SecurityReportType = serde_json::from_str(hpkp_report_text).unwrap();
        assert_eq!(report_type, SecurityReportType::Hpkp);
    }
}
