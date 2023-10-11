//! Span description scrubbing logic.
mod sql;
pub use sql::parse_query;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::Path;

use itertools::Itertools;
use relay_event_schema::processor::{self, ProcessingResult};
use relay_event_schema::protocol::Span;
use relay_protocol::{Annotated, Remark, RemarkType, Value};
use url::Url;

use crate::regexes::{
    DB_SQL_TRANSACTION_CORE_DATA_REGEX, REDIS_COMMAND_REGEX, RESOURCE_NORMALIZER_REGEX,
};
use crate::span::tag_extraction::HTTP_METHOD_EXTRACTOR_REGEX;
use crate::transactions::SpanDescriptionRule;

/// Attempts to replace identifiers in the span description with placeholders.
///
/// The resulting scrubbed description is stored in `data.description.scrubbed`, and serves as input
/// for the span group hash.
pub(crate) fn scrub_span_description(span: &mut Span, rules: &Vec<SpanDescriptionRule>) {
    let Some(description) = span.description.as_str() else {
        return;
    };

    let db_system = span
        .data
        .value()
        .and_then(|v| v.get("db.system"))
        .and_then(|system| system.as_str());
    let span_origin = span.origin.as_str();

    let scrubbed = span
        .op
        .as_str()
        .map(|op| op.split_once('.').unwrap_or((op, "")))
        .and_then(|(op, sub)| match (op, sub) {
            ("http", _) => scrub_http(description),
            ("cache", _) | ("db", "redis") => scrub_redis_keys(description),
            ("db", sub) => {
                if sub.contains("clickhouse")
                    || sub.contains("mongodb")
                    || sub.contains("redis")
                    || is_legacy_activerecord(sub, db_system)
                    || is_sql_mongodb(description, db_system)
                {
                    None
                // `db.sql.transaction` coming from CoreData need to be scrubbed differently.
                } else if sub == "sql.transaction" && span_origin == Some("auto.db.core_data") {
                    scrub_core_data(description)
                } else {
                    sql::scrub_queries(db_system, description)
                }
            }
            ("resource", _) => scrub_resource_identifiers(description),
            ("ui", "load") => {
                // `ui.load` spans contain component names like `ListAppViewController`, so
                // they _should_ be low-cardinality.
                // At the moment the metrics from this module
                // are still filtered by the `SpanMetricsExtractionAllModules` feature, so it should
                // be low-risk to start adding the description.
                Some(description.to_owned())
            }
            ("app", _) => {
                // `app.*` has static descriptions, like `Cold Start`
                // or `Pre Runtime Init`.
                // They are low-cardinality.
                Some(description.to_owned())
            }
            ("file", _) => scrub_file(description),
            _ => None,
        });

    if let Some(scrubbed) = scrubbed {
        span.data
            .get_or_insert_with(BTreeMap::new)
            // We don't care what the cause of scrubbing was, since we assume
            // that after scrubbing the value is sanitized.
            .insert(
                "description.scrubbed".to_owned(),
                Annotated::new(Value::String(scrubbed)),
            );
    }

    apply_span_rename_rules(span, rules).ok(); // Only fails on InvalidTransaction
}

/// A span declares `op: db.sql.query`, but contains mongodb.
fn is_sql_mongodb(description: &str, db_system: Option<&str>) -> bool {
    description.contains("\"$")
        || description.contains("({")
        || description.contains("[{")
        || description.starts_with('{')
        || db_system == Some("mongodb")
}

/// We are unable to parse active record when we do not know which database is being used.
fn is_legacy_activerecord(sub_op: &str, db_system: Option<&str>) -> bool {
    db_system.is_none() && (sub_op.contains("active_record") || sub_op.contains("activerecord"))
}

fn scrub_core_data(string: &str) -> Option<String> {
    match DB_SQL_TRANSACTION_CORE_DATA_REGEX.replace_all(string, "*") {
        Cow::Owned(scrubbed) => Some(scrubbed),
        Cow::Borrowed(_) => Some("".into()),
    }
}

fn scrub_http(string: &str) -> Option<String> {
    let (method, url) = string.split_once(' ')?;
    if !HTTP_METHOD_EXTRACTOR_REGEX.is_match(method) {
        return None;
    };

    if url.starts_with("data:image/") {
        return Some(format!("{method} data:image/*"));
    }

    let scrubbed = match Url::parse(url) {
        Ok(url) => {
            let host = url.host().map(|h| h.to_string())?;
            let domain = normalize_domain(host.as_str(), url.port())?;
            let scheme = url.scheme();

            format!("{method} {scheme}://{domain}")
        }
        Err(_) => {
            format!("{method} *")
        }
    };

    Some(scrubbed)
}

fn scrub_file(description: &str) -> Option<String> {
    let filename = match description.split_once(' ') {
        Some((filename, _)) => filename,
        _ => description,
    };
    match Path::new(filename).extension() {
        Some(extension) => {
            let ext = extension.to_str()?;
            Some(format!("*.{ext}"))
        }
        _ => Some("*".to_owned()),
    }
}

fn normalize_domain(domain: &str, port: Option<u16>) -> Option<String> {
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

fn normalized_domain_from_allowlist(domain: &str, port: Option<u16>) -> Option<String> {
    if let Some(domain) = DOMAIN_ALLOW_LIST.iter().find(|allowed| **allowed == domain) {
        let with_port = port.map_or_else(|| (*domain).to_owned(), |p| format!("{}:{}", domain, p));
        return Some(with_port);
    }
    None
}

fn scrub_redis_keys(string: &str) -> Option<String> {
    let parts = REDIS_COMMAND_REGEX
        .captures(string)
        .map(|caps| (caps.name("command"), caps.name("args")));
    let scrubbed = match parts {
        Some((Some(command), Some(_args))) => command.as_str().to_owned() + " *",
        Some((Some(command), None)) => command.as_str().into(),
        None | Some((None, _)) => "*".into(),
    };
    Some(scrubbed)
}

fn scrub_resource_identifiers(mut string: &str) -> Option<String> {
    // Remove query parameters or the fragment.
    if string.starts_with("data:") {
        if let Some(pos) = string.find(';') {
            return Some(string[..pos].into());
        }
        return Some("data:*/*".into());
    } else if let Some(pos) = string.find('?') {
        string = &string[..pos];
    } else if let Some(pos) = string.find('#') {
        string = &string[..pos];
    }
    match RESOURCE_NORMALIZER_REGEX.replace_all(string, "$pre*$post") {
        Cow::Owned(scrubbed) => Some(scrubbed),
        Cow::Borrowed(string) => {
            // No IDs scrubbed, but we still want to set something.
            // If we managed to parse the URL, we'll try to get an extension.
            if let Ok(url) = Url::parse(string) {
                let domain = normalize_domain(&url.host()?.to_string(), url.port())?;
                // If there is an extension, we add it to the domain.
                if let Some(extension) = url.path().rsplit_once('.') {
                    return Some(format!("{domain}/*.{}", extension.1));
                }
                return Some(domain);
            }
            Some(string.into())
        }
    }
}

/// Applies rules to the span description.
///
/// For now, rules are only generated from transaction names, and the
/// scrubbed value is stored in `span.data[description.scrubbed]` instead of
/// `span.description` (which remains intact).
fn apply_span_rename_rules(span: &mut Span, rules: &Vec<SpanDescriptionRule>) -> ProcessingResult {
    if let Some(op) = span.op.value() {
        if !op.starts_with("http") {
            return Ok(());
        }
    }

    if rules.is_empty() {
        return Ok(());
    }

    // HACK(iker): work-around to scrub the description, in a
    // context-manager-like approach.
    //
    // If data[description.scrubbed] isn't present, we want to scrub
    // span.description. However, they have different types:
    // Annotated<Value> vs Annotated<String>. The simplest and fastest
    // solution I found is to add span.description to span.data if it
    // doesn't exist already, scrub it, and remove it if we did nothing.
    let previously_scrubbed = span
        .data
        .value()
        .map(|d| d.get("description.scrubbed"))
        .is_some();
    if !previously_scrubbed {
        if let Some(description) = span.description.clone().value() {
            span.data
                .value_mut()
                .get_or_insert_with(BTreeMap::new)
                .insert(
                    "description.scrubbed".to_owned(),
                    Annotated::new(Value::String(description.to_owned())),
                );
        }
    }

    let mut scrubbed = false;

    if let Some(data) = span.data.value_mut() {
        if let Some(description) = data.get_mut("description.scrubbed") {
            processor::apply(description, |name, meta| {
                if let Value::String(s) = name {
                    let result = rules.iter().find_map(|rule| {
                        rule.match_and_apply(Cow::Borrowed(s))
                            .map(|new_name| (rule.pattern.compiled().pattern(), new_name))
                    });

                    if let Some((applied_rule, new_name)) = result {
                        scrubbed = true;
                        if *s != new_name {
                            meta.add_remark(Remark::new(
                                RemarkType::Substituted,
                                // Setting a different format to not get
                                // confused by the actual `span.description`.
                                format!("description.scrubbed:{}", applied_rule),
                            ));
                            *name = Value::String(new_name);
                        }
                    }
                }

                Ok(())
            })?;
        }
    }

    if !previously_scrubbed && !scrubbed {
        span.data
            .value_mut()
            .as_mut()
            .and_then(|data| data.remove("description.scrubbed"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use relay_protocol::get_value;
    use similar_asserts::assert_eq;

    use super::*;

    macro_rules! span_description_test {
        // Tests the scrubbed span description for the given op.

        // Same output and input means the input was already scrubbed.
        // An empty output `""` means the input wasn't scrubbed and Relay didn't scrub it.
        ($name:ident, $description_in:literal, $op_in:literal, $output:literal) => {
            #[test]
            fn $name() {
                let json = format!(
                    r#"
                    {{
                        "description": "",
                        "span_id": "bd2eb23da2beb459",
                        "start_timestamp": 1597976393.4619668,
                        "timestamp": 1597976393.4718769,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "op": "{}"
                    }}
                "#,
                    $op_in
                );

                let mut span = Annotated::<Span>::from_json(&json).unwrap();
                span.value_mut()
                    .as_mut()
                    .unwrap()
                    .description
                    .set_value(Some($description_in.into()));

                scrub_span_description(span.value_mut().as_mut().unwrap(), &vec![]);

                if $output == "" {
                    assert!(span
                        .value()
                        .and_then(|span| span.data.value())
                        .and_then(|data| data.get("description.scrubbed"))
                        .is_none());
                } else {
                    assert_eq!(
                        $output,
                        span.value()
                            .and_then(|span| span.data.value())
                            .and_then(|data| data.get("description.scrubbed"))
                            .and_then(|an_value| an_value.as_str())
                            .unwrap()
                    );
                }
            }
        };
    }

    span_description_test!(empty, "", "http.client", "");

    span_description_test!(
        only_domain,
        "GET http://service.io",
        "http.client",
        "GET http://service.io"
    );

    span_description_test!(
        only_urllike_on_http_ops,
        "GET https://www.service.io/resources/01234",
        "http.client",
        "GET https://*.service.io"
    );

    span_description_test!(
        path_ids_end,
        "GET https://www.service.io/resources/01234",
        "http.client",
        "GET https://*.service.io"
    );

    span_description_test!(
        path_ids_middle,
        "GET https://www.service.io/resources/01234/details",
        "http.client",
        "GET https://*.service.io"
    );

    span_description_test!(
        path_multiple_ids,
        "GET https://www.service.io/users/01234-qwerty/settings/98765-adfghj",
        "http.client",
        "GET https://*.service.io"
    );

    span_description_test!(
        path_md5_hashes,
        "GET /clients/563712f9722fb0996ac8f3905b40786f/project/01234",
        "http.client",
        "GET *"
    );

    span_description_test!(
        path_sha_hashes,
        "GET /clients/403926033d001b5279df37cbbe5287b7c7c267fa/project/01234",
        "http.client",
        "GET *"
    );

    span_description_test!(
        hex,
        "GET /shop/de/f43/beef/3D6/my-beef",
        "http.client",
        "GET *"
    );

    span_description_test!(
        path_uuids,
        "GET /clients/8ff81d74-606d-4c75-ac5e-cee65cbbc866/project/01234",
        "http.client",
        "GET *"
    );

    span_description_test!(
        data_images,
        "GET data:image/png;base64,drtfghaksjfdhaeh/blah/blah/blah",
        "http.client",
        "GET data:image/*"
    );

    span_description_test!(
        only_dblike_on_db_ops,
        "SELECT count() FROM table WHERE id IN (%s, %s)",
        "http.client",
        ""
    );

    span_description_test!(
        cache,
        "GET abc:12:{def}:{34}:{fg56}:EAB38:zookeeper",
        "cache.get_item",
        "GET *"
    );

    span_description_test!(redis_set, "SET mykey myvalue", "db.redis", "SET *");

    span_description_test!(
        redis_set_quoted,
        r#"SET mykey 'multi: part, value'"#,
        "db.redis",
        "SET *"
    );

    span_description_test!(redis_whitespace, " GET  asdf:123", "db.redis", "GET *");

    span_description_test!(redis_no_args, "EXEC", "db.redis", "EXEC");

    span_description_test!(redis_invalid, "What a beautiful day!", "db.redis", "*");

    span_description_test!(
        redis_long_command,
        "ACL SETUSER jane",
        "db.redis",
        "ACL SETUSER *"
    );

    span_description_test!(
        nothing_cache,
        "abc-dontscrubme-meneither:stillno:ohplsstop",
        "cache.get_item",
        "*"
    );

    span_description_test!(
        resource_script,
        "https://example.com/static/chunks/vendors-node_modules_somemodule_v1.2.3_mini-dist_index_js-client_dist-6c733292-f3cd-11ed-a05b-0242ac120003-0dc369dcf3d311eda05b0242ac120003.[hash].abcd1234.chunk.js-0242ac120003.map",
        "resource.script",
        "https://example.com/static/chunks/vendors-node_modules_somemodule_*_mini-dist_index_js-client_dist-*-*.[hash].*.chunk.js-*.map"
    );

    span_description_test!(
        resource_script_numeric_filename,
        "https://example.com/static/chunks/09876543211234567890",
        "resource.script",
        "https://example.com/static/chunks/*"
    );

    span_description_test!(
        resource_css,
        "https://example.com/assets/dark_high_contrast-764fa7c8-f3cd-11ed-a05b-0242ac120003.css",
        "resource.css",
        "https://example.com/assets/dark_high_contrast-*.css"
    );

    span_description_test!(
        integer_in_resource,
        "https://example.com/assets/this_is-a_good_resource-123-scrub_me.js",
        "resource.css",
        "https://example.com/assets/this_is-a_good_resource-*-scrub_me.js"
    );

    span_description_test!(
        resource_query_params,
        "/organization-avatar/123/?s=120",
        "resource.img",
        "/organization-avatar/*/"
    );

    span_description_test!(
        resource_query_params2,
        "https://data.domain.com/data/guide123.gif?jzb=3f535634H467g5-2f256f&ct=1234567890&v=1.203.0_prod",
        "resource.img",
        "https://data.domain.com/data/guide*.gif"
    );

    span_description_test!(
        resource_no_ids,
        "https://data.domain.com/data/guide.gif",
        "resource.img",
        "*.domain.com/*.gif"
    );

    span_description_test!(
        resource_webpack,
        "https://domain.com/path/to/app-1f90d5.f012d11690e188c96fe6.js",
        "resource.js",
        "https://domain.com/path/to/app-*.*.js"
    );

    span_description_test!(
        resource_vite,
        "webroot/assets/Profile-73f6525d.js",
        "resource.js",
        "webroot/assets/Profile-*.js"
    );

    span_description_test!(
        resource_vite_css,
        "webroot/assets/Shop-1aff80f7.css",
        "resource.css",
        "webroot/assets/Shop-*.css"
    );

    span_description_test!(
        ui_load,
        "ListAppViewController",
        "ui.load",
        "ListAppViewController"
    );

    span_description_test!(
        span_description_file_write_keep_extension_only,
        "data.data (42 KB)",
        "file.write",
        "*.data"
    );

    span_description_test!(
        span_description_file_read_keep_extension_only,
        "Info.plist",
        "file.read",
        "*.plist"
    );

    span_description_test!(
        span_description_file_with_no_extension,
        "somefilenamewithnoextension",
        "file.read",
        "*"
    );

    span_description_test!(
        resource_url_with_fragment,
        "https://data.domain.com/data/guide123.gif#url=someotherurl",
        "resource.img",
        "https://data.domain.com/data/guide*.gif"
    );

    span_description_test!(
        resource_script_with_no_extension,
        "https://www.domain.com/page?id=1234567890",
        "resource.script",
        "*.domain.com"
    );

    span_description_test!(
        resource_script_with_no_domain,
        "/page.js?action=name",
        "resource.script",
        "/page.js"
    );

    span_description_test!(
        resource_script_with_no_domain_no_extension,
        "/page?action=name",
        "resource.script",
        "/page"
    );

    span_description_test!(
        resource_img_embedded,
        "data:image/svg+xml;base64,PHN2ZyB4bW",
        "resource.img",
        "data:image/svg+xml"
    );

    span_description_test!(
        db_category_with_mongodb_query,
        "find({some_id:1234567890},{limit:100})",
        "db",
        ""
    );

    span_description_test!(db_category_with_not_sql, "{someField:someValue}", "db", "");

    #[test]
    fn informed_sql_parser() {
        let json = r#"
            {
                "description": "SELECT \"not an identifier\"",
                "span_id": "bd2eb23da2beb459",
                "start_timestamp": 1597976393.4619668,
                "timestamp": 1597976393.4718769,
                "trace_id": "ff62a8b040f340bda5d830223def1d81",
                "op": "db",
                "data": {"db.system": "mysql"}
            }
        "#;

        let mut span = Annotated::<Span>::from_json(json).unwrap();
        let span = span.value_mut().as_mut().unwrap();
        scrub_span_description(span, &vec![]);
        let scrubbed = span
            .data
            .value()
            .and_then(|d| d.get("description.scrubbed"))
            .and_then(|v| v.value())
            .and_then(|v| v.as_str());
        assert_eq!(scrubbed, Some("SELECT %s"));
    }

    #[test]
    fn active_record() {
        let json = r#"{
            "description": "/*some comment `my_function'*/ SELECT `a` FROM `b`",
            "op": "db.sql.activerecord"
        }"#;

        let mut span = Annotated::<Span>::from_json(json).unwrap();

        scrub_span_description(span.value_mut().as_mut().unwrap(), &vec![]);
        let scrubbed = get_value!(span.data["description.scrubbed"]);

        // When db.system is missing, no scrubbed description (i.e. no group) is set.
        assert!(scrubbed.is_none());
    }

    #[test]
    fn active_record_with_db_system() {
        let json = r#"{
            "description": "/*some comment `my_function'*/ SELECT `a` FROM `b`",
            "op": "db.sql.activerecord",
            "data": {
                "db.system": "mysql"
            }
        }"#;

        let mut span = Annotated::<Span>::from_json(json).unwrap();

        scrub_span_description(span.value_mut().as_mut().unwrap(), &vec![]);
        let scrubbed = get_value!(span.data["description.scrubbed"]!);

        // Can be scrubbed with db system.
        assert_eq!(scrubbed.as_str(), Some("SELECT a FROM b"));
    }

    #[test]
    fn core_data() {
        let json = r#"{
            "description": "INSERTED 1 'UAEventData'",
            "op": "db.sql.transaction",
            "origin": "auto.db.core_data"
        }"#;

        let mut span = Annotated::<Span>::from_json(json).unwrap();

        scrub_span_description(span.value_mut().as_mut().unwrap(), &vec![]);
        let scrubbed = get_value!(span.data["description.scrubbed"]!);

        assert_eq!(scrubbed.as_str(), Some("INSERTED * 'UAEventData'"));
    }

    #[test]
    fn multiple_core_data() {
        let json = r#"{
            "description": "UPDATED 1 'QueuedRequest', DELETED 1 'QueuedRequest'",
            "op": "db.sql.transaction",
            "origin": "auto.db.core_data"
        }"#;

        let mut span = Annotated::<Span>::from_json(json).unwrap();

        scrub_span_description(span.value_mut().as_mut().unwrap(), &vec![]);
        let scrubbed = get_value!(span.data["description.scrubbed"]!);

        assert_eq!(
            scrubbed.as_str(),
            Some("UPDATED * 'QueuedRequest', DELETED * 'QueuedRequest'")
        );
    }
}
