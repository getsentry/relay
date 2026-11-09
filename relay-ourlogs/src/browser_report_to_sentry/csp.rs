use std::borrow::Cow;

use relay_browser_reporting::csp::{BlockedUrl, Body, CspDirective, Disposition, SourceFile};
use relay_conventions::{attributes::*, interpolate};
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel};
use url::Url;

mod attributes {
    // Analogous to `INTEGRITY__BLOCKED_URL`.
    pub const CSP__BLOCKED_URL: &str = "csp.blocked_url";
    // Possibly replace by `code.column.number` (doesn't exist yet)?
    pub const CSP__COLUMN_NUMBER: &str = "csp.column_number";
    // Analogous to `INTEGRITY__DOCUMENT_URL`.
    // This might be redundant with the URL on reports.
    pub const CSP__DOCUMENT_URL: &str = "csp.document_url";
    pub const CSP__EFFECTIVE_DIRECTIVE: &str = "csp.effective_directive";
    // Possibly replace by `code.line.number`?
    pub const CSP__LINE_NUMBER: &str = "csp.line_number";
    pub const CSP__ORIGINAL_POLICY: &str = "csp.original_policy";
    // Analogous to `NEL__REPORT_ONLY`. CSP reports strictly speaking don't
    // have a field for this, they have "disposition" of `enforce` or `report`
    // instead.
    pub const CSP__REPORT_ONLY: &str = "csp.report_only";
    pub const CSP__SAMPLE: &str = "csp.sample";
    // Possibly replace by `code.file.path`?
    pub const CSP__SOURCE_FILE: &str = "csp.source_file";
}

use attributes::*;

fn scheme_uses_host(scheme: &str) -> bool {
    // List of schemes with host (netloc) from Python's urlunsplit:
    // see <https://github.com/python/cpython/blob/1eac437e8da106a626efffe9fce1cb47dbf5be35/Lib/urllib/parse.py#L51>
    //
    // Only modification: "" is set to false, since there is a separate check in the urlunsplit
    // implementation that omits the leading "//" in that case.
    matches!(
        scheme,
        "ftp"
            | "http"
            | "gopher"
            | "nntp"
            | "telnet"
            | "imap"
            | "wais"
            | "file"
            | "mms"
            | "https"
            | "shttp"
            | "snews"
            | "prospero"
            | "rtsp"
            | "rtspu"
            | "rsync"
            | "svn"
            | "svn+ssh"
            | "sftp"
            | "nfs"
            | "git"
            | "git+ssh"
            | "ws"
            | "wss"
    )
}

/// Mimicks Python's urlunsplit with all its quirks.
fn unsplit_url(scheme: &str, host: &str) -> String {
    if !host.is_empty() || scheme_uses_host(scheme) {
        format!("{scheme}://{host}")
    } else if !scheme.is_empty() {
        format!("{scheme}:{host}")
    } else {
        String::new()
    }
}

fn normalize_url(value: &str) -> Cow<'_, str> {
    // A lot of these values get reported as literally just the scheme (see
    // <https://w3c.github.io/webappsec-csp/#strip-url-for-use-in-reports>). So a value like 'data'
    // or 'blob', which are valid schemes, just not a uri. So we want to normalize it into a
    // URL.

    if !value.contains(':') {
        return Cow::Owned(unsplit_url(value, ""));
    }

    let url = match Url::parse(value) {
        Ok(url) => url,
        Err(_) => return Cow::Borrowed(value),
    };

    let normalized = match url.scheme() {
        "http" | "https" => Cow::Borrowed(url.host_str().unwrap_or_default()),
        scheme => Cow::Owned(unsplit_url(scheme, url.host_str().unwrap_or_default())),
    };

    Cow::Owned(match url.port() {
        Some(port) => format!("{normalized}:{port}"),
        None => normalized.into_owned(),
    })
}

fn get_message(blocked_url: &BlockedUrl, directive: &CspDirective) -> String {
    match blocked_url {
        BlockedUrl::Inline => "Blocked unsafe inline 'script'".to_owned(),
        BlockedUrl::Eval => "Blocked unsafe eval() 'script'".to_owned(),
        BlockedUrl::WasmEval => "Blocked unsafe WASM eval() 'script'".to_owned(),
        BlockedUrl::TrustedTypesPolicy => "Blocked creation of trusted types policy".to_owned(),
        BlockedUrl::TrustedTypesSink => "Blocked sending of unsanitized data to sink".to_owned(),
        BlockedUrl::Url(url) => {
            let url = normalize_url(url);

            match directive {
                CspDirective::ChildSrc => format!("Blocked 'child' from '{url}'"),
                CspDirective::ConnectSrc => format!("Blocked 'connect' from '{url}'"),
                CspDirective::FontSrc => format!("Blocked 'font' from '{url}'"),
                CspDirective::FormAction => format!("Blocked 'form' action to '{url}'"),
                CspDirective::ImgSrc => format!("Blocked 'image' from '{url}'"),
                CspDirective::ManifestSrc => format!("Blocked 'manifest' from '{url}'"),
                CspDirective::MediaSrc => format!("Blocked 'media' from '{url}'"),
                CspDirective::ObjectSrc => format!("Blocked 'object' from '{url}'"),
                CspDirective::ScriptSrc => format!("Blocked 'script' from '{url}'"),
                CspDirective::ScriptSrcAttr => {
                    format!("Blocked inline script attribute from '{url}'")
                }
                CspDirective::ScriptSrcElem => format!("Blocked 'script' from '{url}'"),
                CspDirective::StyleSrc => format!("Blocked 'style' from '{url}'"),
                CspDirective::StyleSrcElem => format!("Blocked 'style' from '{url}'"),
                CspDirective::StyleSrcAttr => format!("Blocked style attribute from '{url}'"),
                CspDirective::Other(_) => format!("Blocked '{directive}' from '{url}'"),
            }
        }
    }
}

pub fn create_log_from_csp_body(body: Body, mut attributes: Attributes) -> OurLog {
    let Body {
        document_url,
        referrer,
        blocked_url,
        effective_directive,
        original_policy,
        source_file,
        sample,
        disposition,
        status_code,
    } = body;

    add_string_attribute!(attributes, CSP__ORIGINAL_POLICY, original_policy);
    add_string_attribute!(attributes, CSP__EFFECTIVE_DIRECTIVE, effective_directive);
    add_string_attribute!(attributes, CSP__BLOCKED_URL, blocked_url);
    add_string_attribute!(attributes, CSP__DOCUMENT_URL, document_url);
    add_attribute!(
        attributes,
        CSP__REPORT_ONLY,
        matches!(disposition, Disposition::Report)
    );

    if let Some(sample) = sample {
        add_string_attribute!(attributes, CSP__SAMPLE, sample);
    }

    if let Some(SourceFile {
        source_file,
        column_number,
        line_number,
    }) = source_file
    {
        add_string_attribute!(attributes, CSP__SOURCE_FILE, source_file);
        add_attribute!(attributes, CSP__COLUMN_NUMBER, column_number as i64);
        add_attribute!(attributes, CSP__LINE_NUMBER, line_number as i64);
    }

    add_attribute!(attributes, HTTP__RESPONSE__STATUS_CODE, status_code as i64);

    if let Some(referrer) = referrer {
        let referrer_key = interpolate::http__request__header__key("referer");
        add_attribute!(attributes, referrer_key, referrer.clone());
    }

    OurLog {
        body: get_message(&blocked_url, &effective_directive).into(),
        level: OurLogLevel::Warn.into(),
        attributes: attributes.into(),
        ..Default::default()
    }
}
