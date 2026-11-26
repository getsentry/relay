use std::borrow::Cow;

use relay_browser_reporting::SourceFile;
use relay_browser_reporting::csp::{BlockedUrl, Body, CspDirective, Disposition};
use relay_conventions::{attributes::*, interpolate};
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel};
use url::Url;

use crate::browser_report_to_sentry::attributes::*;

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
        BlockedUrl::Inline => format!("Blocked unsafe inline '{directive}'"),
        BlockedUrl::Eval => format!("Blocked unsafe eval() '{directive}'"),
        BlockedUrl::WasmEval => format!("Blocked unsafe WASM eval() '{directive}'"),
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

    add_string_attribute!(attributes, BROWSER__REPORT__BLOCKED_URL, blocked_url);
    add_string_attribute!(attributes, BROWSER__REPORT__DOCUMENT_URL, document_url);
    add_attribute!(
        attributes,
        BROWSER__REPORT__REPORT_ONLY,
        matches!(disposition, Disposition::Report)
    );
    add_string_attribute!(
        attributes,
        BROWSER__REPORT__CSP__ORIGINAL_POLICY,
        original_policy
    );
    add_string_attribute!(
        attributes,
        BROWSER__REPORT__CSP__EFFECTIVE_DIRECTIVE,
        effective_directive
    );

    if let Some(sample) = sample {
        add_string_attribute!(attributes, BROWSER__REPORT__CSP__SAMPLE, sample);
    }

    if let Some(SourceFile {
        source_file,
        column_number,
        line_number,
    }) = source_file
    {
        add_string_attribute!(attributes, CODE__FILE__PATH, source_file);
        if let Some(column_number) = column_number {
            add_attribute!(attributes, CODE__COLUMN__NUMBER, column_number as i64);
        }
        if let Some(line_number) = line_number {
            add_attribute!(attributes, CODE__LINE__NUMBER, line_number as i64);
        }
    }

    add_attribute!(attributes, HTTP__RESPONSE__STATUS_CODE, status_code as i64);

    if let Some(referrer) = referrer
        && !referrer.is_empty()
    {
        let referrer_key = interpolate::http__request__header__key("referer");
        add_attribute!(attributes, referrer_key, vec![referrer.clone()]);
    }

    OurLog {
        body: get_message(&blocked_url, &effective_directive).into(),
        level: OurLogLevel::Warn.into(),
        attributes: attributes.into(),
        ..Default::default()
    }
}
