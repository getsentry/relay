use std::collections::BTreeMap;

use itertools::Itertools;
use relay_filter::csp::SchemeDomainPort;
use relay_general::protocol::Span;

use crate::metrics_extraction::spans::types::SpanTagKey;

pub(crate) fn extract_http_span_tags(span: &Span) -> BTreeMap<SpanTagKey, String> {
    let mut tags = BTreeMap::new();

    tags.insert(SpanTagKey::Module, "http".to_owned());

    // TODO(iker): we're relying on the existance of `http.method`
    // or `db.operation`. This is not guaranteed, and we'll need to
    // parse the span description in that case.
    if let Some(a) = span
        .data
        .value()
        // TODO(iker): some SDKs extract this as method
        .and_then(|v| v.get("http.method"))
        .and_then(|method| method.as_str())
        .map(|s| s.to_uppercase())
    {
        tags.insert(SpanTagKey::Action, a);
    }

    if let Some(d) = span
        .description
        .value()
        .and_then(|url| domain_from_http_url(url))
        .map(|d| d.to_lowercase())
    {
        tags.insert(SpanTagKey::Domain, d);
    }

    tags
}

fn domain_from_http_url(url: &str) -> Option<String> {
    match url.split_once(' ') {
        Some((_method, url)) => {
            let domain_port = SchemeDomainPort::from(url);
            match (domain_port.domain, domain_port.port) {
                (Some(domain), port) => normalize_domain(&domain, port.as_ref()),
                _ => None,
            }
        }
        _ => None,
    }
}

fn normalize_domain(domain: &str, port: Option<&String>) -> Option<String> {
    if let Some(allow_listed) = normalized_domain_from_allowlist(domain, port) {
        return Some(allow_listed);
    }

    let mut tokens = domain.rsplitn(3, '.');
    let tld = tokens.next();
    let domain = tokens.next();
    let prefix = tokens.next().map(|_| "*");

    let mut replaced = prefix
        .iter()
        .chain(domain.iter())
        .chain(tld.iter())
        .join(".");

    if let Some(port) = port {
        replaced = format!("{replaced}:{port}");
    }

    if replaced.is_empty() {
        return None;
    }
    Some(replaced)
}

/// Allow list of domains to not get subdomains scrubbed.
const DOMAIN_ALLOW_LIST: &[&str] = &["127.0.0.1", "localhost"];

fn normalized_domain_from_allowlist(domain: &str, port: Option<&String>) -> Option<String> {
    if let Some(domain) = DOMAIN_ALLOW_LIST.iter().find(|allowed| **allowed == domain) {
        let with_port = port.map_or_else(|| (*domain).to_owned(), |p| format!("{}:{}", domain, p));
        return Some(with_port);
    }
    None
}
