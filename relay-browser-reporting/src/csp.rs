//! Content Security Poliy Violation reports.
//!
//! See <https://w3c.github.io/webappsec-csp>.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A CSP directive.
///
/// See <https://w3c.github.io/webappsec-csp/#csp-directives>.
///
/// This enum only has individual variants for directives which we
/// handle specifically. All other values are covered by the `Other`
/// variant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CspDirective {
    // 6.1 Fetch directives
    /// 6.1.1 <https://w3c.github.io/webappsec-csp/#directive-child-src>
    ChildSrc,
    /// 6.1.2 <https://w3c.github.io/webappsec-csp/#directive-connect-src>
    ConnectSrc,
    /// 6.1.4 <https://w3c.github.io/webappsec-csp/#directive-font-src>
    FontSrc,
    /// 6.1.6 <https://w3c.github.io/webappsec-csp/#directive-img-src>
    ImgSrc,
    /// 6.1.7 <https://w3c.github.io/webappsec-csp/#directive-manifest-src>
    ManifestSrc,
    /// 6.1.8 <https://w3c.github.io/webappsec-csp/#directive-media-src>
    MediaSrc,
    /// 6.1.9 <https://w3c.github.io/webappsec-csp/#directive-object-src>
    ObjectSrc,
    /// 6.1.10 <https://w3c.github.io/webappsec-csp/#directive-script-src>
    ScriptSrc,
    /// 6.1.11 <https://w3c.github.io/webappsec-csp/#directive-script-src-elem>
    ScriptSrcElem,
    /// 6.1.12 <https://w3c.github.io/webappsec-csp/#directive-script-src-attr>
    ScriptSrcAttr,
    /// 6.1.13 <https://w3c.github.io/webappsec-csp/#directive-style-src>
    StyleSrc,
    /// 6.1.14 <https://w3c.github.io/webappsec-csp/#directive-style-src-elem>
    StyleSrcElem,
    /// 6.1.15 <https://w3c.github.io/webappsec-csp/#directive-style-src-attr>
    StyleSrcAttr,

    // 6.4 Navigation directives
    /// 6.4.1 <https://w3c.github.io/webappsec-csp/#directive-form-action>
    FormAction,

    /// Other assorted directives.
    #[serde(untagged)]
    Other(String),
}

impl fmt::Display for CspDirective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CspDirective::ChildSrc => f.write_str("child-src"),
            CspDirective::ConnectSrc => f.write_str("`connect-src"),
            CspDirective::FontSrc => f.write_str("font-src"),
            CspDirective::ImgSrc => f.write_str("img-src"),
            CspDirective::ManifestSrc => f.write_str("manifest-src"),
            CspDirective::MediaSrc => f.write_str("media-src"),
            CspDirective::ObjectSrc => f.write_str("object-src"),
            CspDirective::ScriptSrc => f.write_str("script-src"),
            CspDirective::ScriptSrcElem => f.write_str("script-src-elem"),
            CspDirective::ScriptSrcAttr => f.write_str("script-src-attr"),
            CspDirective::StyleSrc => f.write_str("style-src"),
            CspDirective::StyleSrcElem => f.write_str("style-src-elem"),
            CspDirective::StyleSrcAttr => f.write_str("style-src-attr"),
            CspDirective::FormAction => f.write_str("form-action"),
            CspDirective::Other(directive) => f.write_str(directive),
        }
    }
}

/// A CSP policy's disposition.
///
/// See <https://w3c.github.io/webappsec-csp/#framework-policy>.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Disposition {
    /// Enforce security violations.
    Enforce,
    /// Only report security violations.
    Report,
}

/// Source file information in a [`Body`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceFile {
    /// The URL of the resource where the violation occurred.
    pub source_file: String,
    /// The column number in source-file on which the violation occurred.
    pub column_number: u64,
    /// The line number in source-file on which the violation occurred.
    pub line_number: u64,
}

/// The type or URL of a resource that was blocked
/// by a CSP.
///
/// See <https://developer.mozilla.org/en-US/docs/Web/API/CSPViolationReport/blockedURL>.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BlockedUrl {
    /// An inline resource.
    Inline,
    /// An `eval()`.
    Eval,
    /// A WASM evaluation.
    WasmEval,
    /// A resource that violated the `trusted-types` CSP directive.
    TrustedTypesPolicy,
    /// A resource that violated the require-trusted-types-for CSP directive.
    TrustedTypesSink,
    /// The URL of some other resource.
    #[serde(untagged)]
    Url(String),
}

impl fmt::Display for BlockedUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inline => f.write_str("inline"),
            Self::Eval => f.write_str("eval"),
            Self::WasmEval => f.write_str("wasm-eval"),
            Self::TrustedTypesPolicy => f.write_str("trusted-types-policy"),
            Self::TrustedTypesSink => f.write_str("trusted-types-sink"),
            Self::Url(url) => f.write_str(url),
        }
    }
}

/// The body of a CSP Violation Report.
///
/// See <https://w3c.github.io/webappsec-csp/#report-violation>, Point 5.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Body {
    /// The URL of the document or worker in which the violation occurred.
    #[serde(rename = "documentURL")]
    pub document_url: String,
    /// The referrer of the document in which the violation occurred.
    pub referrer: Option<String>,
    /// The type or URL of the resource that was blocked from loading by the Content Security Policy.
    #[serde(rename = "blockedURL")]
    pub blocked_url: BlockedUrl,
    /// The directive whose enforcement caused the violation.
    pub effective_directive: CspDirective,
    /// The original policy as specified by the Content-Security-Policy HTTP header.
    pub original_policy: String,
    /// Information about the resource where the violation occurred.
    #[serde(flatten)]
    pub source_file: Option<SourceFile>,
    /// The first 40 characters of the inline script, event handler, or style that caused the
    /// violation.
    ///
    /// According to <https://w3c.github.io/webappsec-csp/#violation-sample>, this is
    /// "the empty string unless otherwise specified". We use `None` here because the
    /// empty string isn't useful anyway.
    pub sample: Option<String>,
    /// Whether the violation was enforced or only reported.
    pub disposition: Disposition,
    /// The HTTP status code of the resource on which the global object was instantiated.
    pub status_code: u64,
}
