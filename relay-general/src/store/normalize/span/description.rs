//! Span description scrubbing logic.
use std::borrow::Cow;
use std::collections::BTreeMap;

use once_cell::sync::Lazy;
use regex::Regex;

use crate::protocol::Span;
use crate::store::regexes::{REDIS_COMMAND_REGEX, RESOURCE_NORMALIZER_REGEX};
use crate::store::{scrub_identifiers, scrub_identifiers_with_regex, SpanDescriptionRule};
use crate::types::{Annotated, ProcessingAction, ProcessingResult, Remark, RemarkType, Value};

/// Regex with multiple capture groups for SQL tokens we should scrub.
///
/// Slightly modified from
/// <https://github.com/getsentry/sentry/blob/65fb6fdaa0080b824ab71559ce025a9ec6818b3e/src/sentry/spans/grouping/strategy/base.py#L170>
/// <https://github.com/getsentry/sentry/blob/17af7efe869007f85c5322e48aa9f80a8515bde4/src/sentry/spans/grouping/strategy/base.py#L163>
static SQL_NORMALIZER_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?xi)
        # Capture `SAVEPOINT` savepoints.
        ((?-x)SAVEPOINT (?P<savepoint>(?:(?:"[^"]+")|(?:'[^']+')|(?:`[^`]+`)|(?:[a-z]\w+)))) |
        # Capture single-quoted strings, including the remaining substring if `\'` is found.
        ((?-x)(?P<single_quoted_strs>'(?:\\'|[^'])*(?:'|$))) |
        # Capture placeholders.
        ((?-x)(?P<placeholder>(?:\?+|\$\d+))) |
        # Capture numbers.
        ((?-x)(?P<number>(-?\b(?:[0-9]+\.)?[0-9]+(?:[eE][+-]?[0-9]+)?\b))) |
        # Capture booleans (as full tokens, not as substrings of other tokens).
        ((?-x)(?P<bool>(\b(?:true|false)\b)))
        "#,
    )
    .unwrap()
});

/// Regex to make multiple placeholders collapse into one.
/// This can be used as a second pass after [`SQL_NORMALIZER_REGEX`].
static SQL_COLLAPSE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?xi)
        (VALUES|IN) \s+ \( (?P<values> ( %s ( \)\s*,\s*\(\s*%s | \s*,\s*%s )* )) \)?
        "#,
    )
    .unwrap()
});

/// Regex to identify SQL queries that are already normalized.
///
/// Looks for `?`, `$1` or `%s` identifiers, commonly used identifiers in
/// Python, Ruby on Rails and PHP platforms.
static SQL_ALREADY_NORMALIZED_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"/\?|\$1|%s"#).unwrap());

/// Attempt to replace identifiers in the span description with placeholders.
///
/// The resulting scrubbed description is stored in `data.description.scrubbed`, and serves as input
/// for the span group hash.
pub(crate) fn scrub_span_description(
    span: &mut Span,
    rules: &Vec<SpanDescriptionRule>,
) -> Result<(), ProcessingAction> {
    if span.description.value().is_none() {
        return Ok(());
    }

    let mut scrubbed = span.description.clone();

    let did_scrub = match span.op.value() {
        Some(op) if op.starts_with("http") => scrub_identifiers(&mut scrubbed)?,
        Some(op) if op.starts_with("cache") || op == "db.redis" => scrub_redis_keys(&mut scrubbed)?,
        Some(op) if op.starts_with("db") && op != "db.redis" => scrub_sql_queries(&mut scrubbed)?,
        Some(op) if op.starts_with("resource") => scrub_resource_identifiers(&mut scrubbed)?,
        _ => false,
    };

    if did_scrub {
        if let Some(new_desc) = scrubbed.into_value() {
            span.data
                .get_or_insert_with(BTreeMap::new)
                // We don't care what the cause of scrubbing was, since we assume
                // that after scrubbing the value is sanitized.
                .insert(
                    "description.scrubbed".to_owned(),
                    Annotated::new(Value::String(new_desc)),
                );
        };
    }

    apply_span_rename_rules(span, rules)?;

    Ok(())
}

/// Normalize the given SQL-query-like string.
fn scrub_sql_queries(string: &mut Annotated<String>) -> Result<bool, ProcessingAction> {
    let mut mark_as_scrubbed = is_sql_query_scrubbed(string);
    mark_as_scrubbed |= scrub_identifiers_with_regex(string, &SQL_NORMALIZER_REGEX, "%s")?;
    mark_as_scrubbed |= scrub_identifiers_with_regex(string, &SQL_COLLAPSE_REGEX, "%s")?;

    Ok(mark_as_scrubbed)
}

fn is_sql_query_scrubbed(query: &Annotated<String>) -> bool {
    query
        .value()
        .map_or(false, |q| SQL_ALREADY_NORMALIZED_REGEX.is_match(q))
}

fn scrub_redis_keys(string: &mut Annotated<String>) -> Result<bool, ProcessingAction> {
    let parts = string
        .as_str()
        .and_then(|s| REDIS_COMMAND_REGEX.captures(s))
        .map(|caps| (caps.name("command"), caps.name("args")));
    *string = Annotated::new(match parts {
        Some((Some(command), Some(_args))) => command.as_str().to_owned() + " *",
        Some((Some(command), None)) => command.as_str().into(),
        None | Some((None, _)) => "*".into(),
    });
    Ok(true)
}

fn scrub_resource_identifiers(string: &mut Annotated<String>) -> Result<bool, ProcessingAction> {
    scrub_identifiers_with_regex(string, &RESOURCE_NORMALIZER_REGEX, "*")
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
            description.apply(|name, meta| {
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
                        "description": "{}",
                        "span_id": "bd2eb23da2beb459",
                        "start_timestamp": 1597976393.4619668,
                        "timestamp": 1597976393.4718769,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "op": "{}"
                    }}
                "#,
                    $description_in, $op_in
                );

                let mut span = Annotated::<Span>::from_json(&json).unwrap();

                scrub_span_description(span.value_mut().as_mut().unwrap(), &vec![]).unwrap();

                // The input description may contain escaped characters, and the
                // default formatter (when taking the value from the span
                // description) automatically escapes them. The goal is to
                // compute raw values, so we want to get rid of character
                // escaping, and the debug formatter does that. The debug
                // formatter doesn't remove the leading and trailing `"`s, so we
                // manually add them to the input literal.
                assert_eq!(
                    format!("\"{}\"", $description_in),
                    format!("{:?}", span.value().unwrap().description.value().unwrap())
                );

                if $output == "" {
                    assert!(span
                        .value()
                        .and_then(|span| span.data.value())
                        .and_then(|data| data.get("description.scrubbed"))
                        .is_none());
                } else {
                    assert_eq!(
                        format!("\"{}\"", $output),
                        format!(
                            "{:?}",
                            span.value()
                                .and_then(|span| span.data.value())
                                .and_then(|data| data.get("description.scrubbed"))
                                .and_then(|an_value| an_value.as_str())
                                .unwrap()
                        )
                    );
                }
            }
        };
    }

    span_description_test!(span_description_scrub_empty, "", "http.client", "");

    span_description_test!(
        span_description_scrub_only_domain,
        "GET http://service.io",
        "http.client",
        ""
    );

    span_description_test!(
        span_description_scrub_only_urllike_on_http_ops,
        "GET https://www.service.io/resources/01234",
        "http.client",
        "GET https://www.service.io/resources/*"
    );

    span_description_test!(
        span_description_scrub_path_ids_end,
        "GET https://www.service.io/resources/01234",
        "http.client",
        "GET https://www.service.io/resources/*"
    );

    span_description_test!(
        span_description_scrub_path_ids_middle,
        "GET https://www.service.io/resources/01234/details",
        "http.client",
        "GET https://www.service.io/resources/*/details"
    );

    span_description_test!(
        span_description_scrub_path_multiple_ids,
        "GET https://www.service.io/users/01234-qwerty/settings/98765-adfghj",
        "http.client",
        "GET https://www.service.io/users/*/settings/*"
    );

    span_description_test!(
        span_description_scrub_path_md5_hashes,
        "GET /clients/563712f9722fb0996ac8f3905b40786f/project/01234",
        "http.client",
        "GET /clients/*/project/*"
    );

    span_description_test!(
        span_description_scrub_path_sha_hashes,
        "GET /clients/403926033d001b5279df37cbbe5287b7c7c267fa/project/01234",
        "http.client",
        "GET /clients/*/project/*"
    );

    span_description_test!(
        span_description_scrub_path_uuids,
        "GET /clients/8ff81d74-606d-4c75-ac5e-cee65cbbc866/project/01234",
        "http.client",
        "GET /clients/*/project/*"
    );

    // TODO(iker): Add span description test for URLs with paths

    span_description_test!(
        span_description_scrub_only_dblike_on_db_ops,
        "SELECT count() FROM table WHERE id IN (%s, %s)",
        "http.client",
        ""
    );

    span_description_test!(
        span_description_scrub_various_parameterized_ins_percentage,
        "SELECT count() FROM table WHERE id IN (%s, %s) AND id IN (%s, %s, %s)",
        "db.sql.query",
        "SELECT count() FROM table WHERE id IN (%s) AND id IN (%s)"
    );

    span_description_test!(
        span_description_scrub_various_parameterized_ins_dollar,
        "SELECT count() FROM table WHERE id IN ($1, $2, $3)",
        "db.sql.query",
        "SELECT count() FROM table WHERE id IN (%s)"
    );

    span_description_test!(
        span_description_scrub_various_parameterized_questionmarks,
        "SELECT count() FROM table WHERE id IN (?, ?, ?)",
        "db.sql.query",
        "SELECT count() FROM table WHERE id IN (%s)"
    );

    span_description_test!(
        span_description_scrub_unparameterized_ins_uppercase,
        "SELECT count() FROM table WHERE id IN (100, 101, 102)",
        "db.sql.query",
        "SELECT count() FROM table WHERE id IN (%s)"
    );

    span_description_test!(
        span_description_scrub_various_parameterized_ins_lowercase,
        "select count() from table where id in (100, 101, 102)",
        "db.sql.query",
        "select count() from table where id in (%s)"
    );

    span_description_test!(
        span_description_scrub_various_parameterized_strings,
        "select count() from table_1 where name in ('foo', %s, 1)",
        "db.sql.query",
        "select count() from table_1 where name in (%s)"
    );

    span_description_test!(
        span_description_scrub_various_parameterized_cutoff,
        "select count() from table where name in ('foo', 'bar', 'ba",
        "db.sql.query",
        "select count() from table where name in (%s"
    );

    span_description_test!(
        span_description_scrub_mixed,
        "UPDATE foo SET a = %s, b = log(e + 5) * 600 + 12345 WHERE true",
        "db.sql.query",
        "UPDATE foo SET a = %s, b = log(e + %s) * %s + %s WHERE %s"
    );

    span_description_test!(
        span_description_scrub_savepoint_uppercase,
        "SAVEPOINT unquoted_identifier",
        "db.sql.query",
        "SAVEPOINT %s"
    );

    span_description_test!(
        span_description_scrub_savepoint_uppercase_semicolon,
        "SAVEPOINT unquoted_identifier;",
        "db.sql.query",
        "SAVEPOINT %s;"
    );

    span_description_test!(
        span_description_scrub_savepoint_lowercase,
        "savepoint unquoted_identifier",
        "db.sql.query",
        "savepoint %s"
    );

    span_description_test!(
        span_description_scrub_savepoint_quoted,
        "SAVEPOINT 'single_quoted_identifier'",
        "db.sql.query",
        "SAVEPOINT %s"
    );

    span_description_test!(
        span_description_scrub_savepoint_quoted_backtick,
        "SAVEPOINT `backtick_quoted_identifier`",
        "db.sql.query",
        "SAVEPOINT %s"
    );

    span_description_test!(
        span_description_scrub_single_quoted_string,
        "SELECT * FROM table WHERE sku = 'foo'",
        "db.sql.query",
        "SELECT * FROM table WHERE sku = %s"
    );

    span_description_test!(
        span_description_scrub_single_quoted_string_finished,
        r#"SELECT * FROM table WHERE quote = 'it\\'s a string'"#,
        "db.sql.query",
        "SELECT * FROM table WHERE quote = %s"
    );

    span_description_test!(
        span_description_scrub_single_quoted_string_unfinished,
        r#"SELECT * FROM table WHERE quote = 'it\\'s a string"#,
        "db.sql.query",
        "SELECT * FROM table WHERE quote = %s"
    );

    span_description_test!(
        span_description_dont_scrub_double_quoted_strings_format_postgres,
        r#"SELECT * from \"table\" WHERE sku = %s"#,
        "db.sql.query",
        r#"SELECT * from \"table\" WHERE sku = %s"#
    );

    span_description_test!(
        span_description_dont_scrub_double_quoted_strings_format_mysql,
        r#"SELECT * from table WHERE sku = \"foo\""#,
        "db.sql.query",
        ""
    );

    span_description_test!(
        span_description_scrub_num_where,
        "SELECT * FROM table WHERE id = 1",
        "db.sql.query",
        "SELECT * FROM table WHERE id = %s"
    );

    span_description_test!(
        span_description_scrub_num_limit,
        "SELECT * FROM table LIMIT 1",
        "db.sql.query",
        "SELECT * FROM table LIMIT %s"
    );

    span_description_test!(
        span_description_scrub_num_negative_where,
        "SELECT * FROM table WHERE temperature > -100",
        "db.sql.query",
        "SELECT * FROM table WHERE temperature > %s"
    );

    span_description_test!(
        span_description_scrub_num_e_where,
        "SELECT * FROM table WHERE salary > 1e7",
        "db.sql.query",
        "SELECT * FROM table WHERE salary > %s"
    );

    span_description_test!(
        span_description_already_scrubbed,
        "SELECT * FROM table123 WHERE id = %s",
        "db.sql.query",
        "SELECT * FROM table123 WHERE id = %s"
    );

    span_description_test!(
        span_description_scrub_boolean_where_true,
        "SELECT * FROM table WHERE deleted = true",
        "db.sql.query",
        "SELECT * FROM table WHERE deleted = %s"
    );

    span_description_test!(
        span_description_scrub_boolean_where_false,
        "SELECT * FROM table WHERE deleted = false",
        "db.sql.query",
        "SELECT * FROM table WHERE deleted = %s"
    );

    span_description_test!(
        span_description_scrub_boolean_where_bool_insensitive,
        "SELECT * FROM table WHERE deleted = FaLsE",
        "db.sql.query",
        "SELECT * FROM table WHERE deleted = %s"
    );

    span_description_test!(
        span_description_scrub_boolean_not_in_tablename_true,
        "SELECT * FROM table_true WHERE deleted = %s",
        "db.sql.query",
        "SELECT * FROM table_true WHERE deleted = %s"
    );

    span_description_test!(
        span_description_scrub_boolean_not_in_tablename_false,
        "SELECT * FROM table_false WHERE deleted = %s",
        "db.sql.query",
        "SELECT * FROM table_false WHERE deleted = %s"
    );

    span_description_test!(
        span_description_scrub_boolean_not_in_mid_tablename_true,
        "SELECT * FROM tatrueble WHERE deleted = %s",
        "db.sql.query",
        "SELECT * FROM tatrueble WHERE deleted = %s"
    );

    span_description_test!(
        span_description_scrub_boolean_not_in_mid_tablename_false,
        "SELECT * FROM tafalseble WHERE deleted = %s",
        "db.sql.query",
        "SELECT * FROM tafalseble WHERE deleted = %s"
    );

    span_description_test!(
        span_description_dont_scrub_nulls,
        "SELECT * FROM table WHERE deleted_at IS NULL",
        "db.sql.query",
        ""
    );

    span_description_test!(
        span_description_scrub_values,
        "INSERT INTO a (b, c, d, e) VALUES (%s, %s, %s, %s)",
        "db.sql.query",
        "INSERT INTO a (b, c, d, e) VALUES (%s)"
    );

    span_description_test!(
        span_description_scrub_values_multi,
        "INSERT INTO a (b, c, d, e) VALuES (%s, %s, %s, %s), (%s, %s, %s, %s), (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
        "db.sql.query",
        "INSERT INTO a (b, c, d, e) VALuES (%s) ON CONFLICT DO NOTHING"
    );

    span_description_test!(
        span_description_scrub_clickhouse,
        "SELECT (toStartOfHour(finish_ts, 'Universal') AS _snuba_time), (uniqIf((nullIf(user, '') AS _snuba_user), greater(multiIf(equals(tupleElement(('duration', 300), 1), 'lcp'), (if(has(measurements.key, 'lcp'), arrayElement(measurements.value, indexOf(measurements.key, 'lcp')), NULL) AS `_snuba_measurements[lcp]`), (duration AS _snuba_duration)), multiply(tupleElement(('duration', 300), 2), 4))) AS _snuba_count_miserable_user), (ifNull(divide(plus(_snuba_count_miserable_user, 4.56), plus(nullIf(uniqIf(_snuba_user, greater(multiIf(equals(tupleElement(('duration', 300), 1), 'lcp'), `_snuba_measurements[lcp]`, _snuba_duration), 0)), 0), 113.45)), 0) AS _snuba_user_misery), _snuba_count_miserable_user, (divide(countIf(notEquals(transaction_status, 0) AND notEquals(transaction_status, 1) AND notEquals(transaction_status, 2)), count()) AS _snuba_failure_rate), (divide(count(), divide(3600.0, 60)) AS _snuba_tpm_3600) FROM transactions_dist WHERE equals(('transaction' AS _snuba_type), 'transaction') AND greaterOrEquals((finish_ts AS _snuba_finish_ts), toDateTime('2023-06-13T09:08:51', 'Universal')) AND less(_snuba_finish_ts, toDateTime('2023-07-11T09:08:51', 'Universal')) AND in((project_id AS _snuba_project_id), [123, 456, 789]) AND equals((environment AS _snuba_environment), 'production') GROUP BY _snuba_time ORDER BY _snuba_time ASC LIMIT 10000 OFFSET 0",
        "db.clickhouse",
        "SELECT (toStartOfHour(finish_ts, %s) AS _snuba_time), (uniqIf((nullIf(user, %s) AS _snuba_user), greater(multiIf(equals(tupleElement((%s, %s), %s), %s), (if(has(measurements.key, %s), arrayElement(measurements.value, indexOf(measurements.key, %s)), NULL) AS `_snuba_measurements[lcp]`), (duration AS _snuba_duration)), multiply(tupleElement((%s, %s), %s), %s))) AS _snuba_count_miserable_user), (ifNull(divide(plus(_snuba_count_miserable_user, %s), plus(nullIf(uniqIf(_snuba_user, greater(multiIf(equals(tupleElement((%s, %s), %s), %s), `_snuba_measurements[lcp]`, _snuba_duration), %s)), %s), %s)), %s) AS _snuba_user_misery), _snuba_count_miserable_user, (divide(countIf(notEquals(transaction_status, %s) AND notEquals(transaction_status, %s) AND notEquals(transaction_status, %s)), count()) AS _snuba_failure_rate), (divide(count(), divide(%s, %s)) AS _snuba_tpm_3600) FROM transactions_dist WHERE equals((%s AS _snuba_type), %s) AND greaterOrEquals((finish_ts AS _snuba_finish_ts), toDateTime(%s, %s)) AND less(_snuba_finish_ts, toDateTime(%s, %s)) AND in((project_id AS _snuba_project_id), [%s, %s, %s]) AND equals((environment AS _snuba_environment), %s) GROUP BY _snuba_time ORDER BY _snuba_time ASC LIMIT %s OFFSET %s"
    );

    span_description_test!(
        span_description_scrub_cache,
        "GET abc:12:{def}:{34}:{fg56}:EAB38:zookeeper",
        "cache.get_item",
        "GET *"
    );

    span_description_test!(
        span_description_scrub_redis_set,
        "SET mykey myvalue",
        "db.redis",
        "SET *"
    );

    span_description_test!(
        span_description_scrub_redis_set_quoted,
        r#"SET mykey 'multi: part, value'"#,
        "db.redis",
        "SET *"
    );

    span_description_test!(
        span_description_scrub_redis_whitespace,
        " GET  asdf:123",
        "db.redis",
        "GET *"
    );

    span_description_test!(
        span_description_scrub_redis_no_args,
        "EXEC",
        "db.redis",
        "EXEC"
    );

    span_description_test!(
        span_description_scrub_redis_invalid,
        "What a beautiful day!",
        "db.redis",
        "*"
    );

    span_description_test!(
        span_description_scrub_redis_long_command,
        "ACL SETUSER jane",
        "db.redis",
        "ACL SETUSER *"
    );

    span_description_test!(
        span_description_scrub_nothing_cache,
        "abc-dontscrubme-meneither:stillno:ohplsstop",
        "cache.get_item",
        "*"
    );

    span_description_test!(
        span_description_scrub_resource_script,
        "https://example.com/static/chunks/vendors-node_modules_somemodule_v1.2.3_mini-dist_index_js-client_dist-6c733292-f3cd-11ed-a05b-0242ac120003-0dc369dcf3d311eda05b0242ac120003.[hash].abcd1234.chunk.js-0242ac120003.map",
        "resource.script",
        "https://example.com/static/chunks/vendors-node_modules_somemodule_*_mini-dist_index_js-client_dist-*-*.[hash].*.js-*.map"
    );

    span_description_test!(
        span_description_scrub_resource_script_numeric_filename,
        "https://example.com/static/chunks/09876543211234567890",
        "resource.script",
        "https://example.com/static/chunks/*"
    );

    span_description_test!(
        span_description_scrub_resource_css,
        "https://example.com/assets/dark_high_contrast-764fa7c8-f3cd-11ed-a05b-0242ac120003.css",
        "resource.css",
        "https://example.com/assets/dark_high_contrast-*.css"
    );

    span_description_test!(
        span_description_scrub_nothing_in_resource,
        "https://example.com/assets/this_is-a_good_resource-123-dont_scrub_me.js",
        "resource.css",
        ""
    );
}
