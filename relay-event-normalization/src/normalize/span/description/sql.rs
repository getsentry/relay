//! Logic for scrubbing and normalizing span descriptions that contain SQL queries.

use std::fmt::Write;
use std::ops::ControlFlow;

use once_cell::sync::Lazy;
use regex::Regex;
use sqlparser::ast::{Value, VisitMut, VisitorMut};
use sqlparser::dialect::GenericDialect;

/// Removes SQL comments starting with "--" or "#".
static COMMENTS: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?:--|#).*(?P<newline>\n)").unwrap());

/// Removes MySQL inline comments.
static INLINE_COMMENTS: Lazy<Regex> = Lazy::new(|| Regex::new(r"/\*(?:.|\n)*?\*/").unwrap());

/// Regex with multiple capture groups for SQL tokens we should scrub.
///
/// Slightly modified from
/// <https://github.com/getsentry/sentry/blob/65fb6fdaa0080b824ab71559ce025a9ec6818b3e/src/sentry/spans/grouping/strategy/base.py#L170>
/// <https://github.com/getsentry/sentry/blob/17af7efe869007f85c5322e48aa9f80a8515bde4/src/sentry/spans/grouping/strategy/base.py#L163>
static NORMALIZER_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?xi)
        # Capture `SAVEPOINT` savepoints.
        ((?-x)(?P<pre>SAVEPOINT )(?P<savepoint>(?:(?:"[^"]+")|(?:'[^']+')|(?:`[^`]+`)|(?:[a-z]\w+)))) |
        # Capture single-quoted strings, including the remaining substring if `\'` is found.
        ((?-x)(?P<single_quoted_strs>'(?:\\'|[^'])*(?:'|$)(::\w+(\[\]?)?)?)) |
        # Capture placeholders.
        (   (?P<placeholder> (?:\?+|\$\d+|%(?:\(\w+\))?s|:\w+) (::\w+(\[\]?)?)? )   ) |
        # Capture numbers.
        ((?-x)(?P<number>(-?\b(?:[0-9]+\.)?[0-9]+(?:[eE][+-]?[0-9]+)?\b)(::\w+(\[\]?)?)?)) |
        # Capture booleans (as full tokens, not as substrings of other tokens).
        ((?-x)(?P<bool>(\b(?:true|false)\b)))
        "#,
    )
    .unwrap()
});

/// Removes extra whitespace and newlines.
static WHITESPACE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(\s*\n\s*)|(\s\s+)").unwrap());

/// Removes whitespace around parentheses.
static PARENS: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"((?P<pre>\()\s+)|(\s+(?P<post>\)))").unwrap());

static STRIP_QUOTES: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"["`](?P<entity_name>\w+)($|["`])"#).unwrap());

/// Regex to shorten table or column references, e.g. `"table1"."col1"` -> `col1`.
static COLLAPSE_ENTITIES: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?:\w+\.)+(?P<entity_name>\w+)").unwrap());

/// Regex to make multiple placeholders collapse into one.
/// This can be used as a second pass after [`NORMALIZER_REGEX`].
static COLLAPSE_PLACEHOLDERS: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?xi)
        (?P<pre>(?:VALUES|IN) \s+\() (?P<values> ( %s ( \)\s*,\s*\(\s*%s | \s*,\s*%s )* )) (?P<post>\s*\)?)
        ",
    )
    .unwrap()
});

/// Collapse simple lists of column names.
/// For example:
///   SELECT a, b, count(*) FROM x -> SELECT .., count(*) FROM x
///   SELECT "a.b" AS a__b, "a.c" AS a__c FROM x -> SELECT .. FROM x
///
/// Assumes that column names have already been normalized.
static COLLAPSE_COLUMNS: Lazy<Regex> = Lazy::new(|| {
    let col = r"\w+(\s+AS\s+\w+)?";
    Regex::new(
        format!(
            r"(?ix)
        (?P<pre>(SELECT(\s+(DISTINCT|ALL))?\s|,|\())
        \s*
        (?P<columns>({col}(?:\s*,\s*{col})+))
        (?P<post>\s*(,|\s+|\)))"
        )
        .as_str(),
    )
    .unwrap()
});

/// Regex to identify SQL queries that are already normalized.
///
/// Looks for `?`, `$1` or `%s` identifiers, commonly used identifiers in
/// Python, Ruby on Rails and PHP platforms.
static ALREADY_NORMALIZED_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"/\?|\$1|%s").unwrap());

/// Normalizes the given SQL-query-like string.
pub fn scrub_queries(db_system: Option<&str>, string: &str) -> Option<String> {
    let mut parsed = parse_query(db_system, string).ok()?; // TODO: fallback to regexes
    let mut visitor = NormalizeVisitor::default();
    dbg!("VISITING");
    parsed.visit(&mut visitor);
    Some(parsed[0].to_string()) // TODO: if empty? // FIXME: [0]
                                // let mark_as_scrubbed = ALREADY_NORMALIZED_REGEX.is_match(string);

    // let mut string = Cow::from(string.trim());

    // for (regex, replacement) in [
    //     (&NORMALIZER_REGEX, "$pre%s"),
    //     (&COMMENTS, "\n"),
    //     (&INLINE_COMMENTS, ""),
    //     (&WHITESPACE, " "),
    //     (&PARENS, "$pre$post"),
    //     (&COLLAPSE_PLACEHOLDERS, "$pre%s$post"),
    //     (&STRIP_QUOTES, "$entity_name"),
    //     (&COLLAPSE_ENTITIES, "$entity_name"),
    //     (&COLLAPSE_COLUMNS, "$pre..$post"),
    // ] {
    //     let replaced = regex.replace_all(&string, replacement);
    //     if let Cow::Owned(s) = replaced {
    //         string = Cow::Owned(s);
    //     }
    // }

    // match string {
    //     Cow::Owned(scrubbed) => Some(scrubbed),
    //     Cow::Borrowed(s) if mark_as_scrubbed => Some(s.to_owned()),
    //     Cow::Borrowed(_) => None,
    // }
}

#[derive(Debug, Default)]
struct NormalizeVisitor {
    result: String,
}

impl VisitorMut for NormalizeVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut sqlparser::ast::Expr) -> ControlFlow<Self::Break> {
        *expr = sqlparser::ast::Expr::Value(Value::Number("666".into(), false));
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, _expr: &mut sqlparser::ast::Expr) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
}

/// Used to replace placeholders (parameters) by dummy values such that the query can be parsed.
static SQL_PLACEHOLDER_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?:\?+|\$\d+|%(?:\(\w+\))?s|:\w+)").unwrap());

/// Derive the SQL dialect from `span.system` and try to parse the query into an AST.
pub fn parse_query(
    db_system: Option<&str>,
    query: &str,
) -> Result<Vec<sqlparser::ast::Statement>, sqlparser::parser::ParserError> {
    // See https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/database.md#notes-and-well-known-identifiers-for-dbsystem
    //     https://docs.rs/sqlparser/latest/sqlparser/dialect/fn.dialect_from_str.html
    let dialect = db_system
        .and_then(sqlparser::dialect::dialect_from_str)
        .unwrap_or_else(|| Box::new(GenericDialect {}));

    let parsable_query = SQL_PLACEHOLDER_REGEX.replace_all(query, "1");
    sqlparser::parser::Parser::parse_sql(&*dialect, &parsable_query)
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! scrub_sql_test {
        ($name:ident, $description_in:literal, $output:literal) => {
            #[test]
            fn $name() {
                let scrubbed = scrub_queries(None, $description_in);
                assert_eq!(scrubbed.as_deref().unwrap_or_default(), $output);
            }
        };
    }

    scrub_sql_test!(
        various_parameterized_ins_percentage,
        "SELECT count() FROM table WHERE id IN (%s, %s) AND id IN (%s, %s, %s)",
        "SELECT count() FROM table WHERE id IN (%s) AND id IN (%s)"
    );

    scrub_sql_test!(
        various_parameterized_ins_dollar,
        "SELECT count() FROM table WHERE id IN ($1, $2, $3)",
        "SELECT count() FROM table WHERE id IN (%s)"
    );

    scrub_sql_test!(
        various_parameterized_questionmarks,
        "SELECT count() FROM table WHERE id IN (?, ?, ?)",
        "SELECT count() FROM table WHERE id IN (%s)"
    );

    scrub_sql_test!(
        unparameterized_ins_uppercase,
        "SELECT count() FROM table WHERE id IN (100, 101, 102)",
        "SELECT count() FROM table WHERE id IN (%s)"
    );

    scrub_sql_test!(
        php_placeholders,
        r"SELECT x FROM y WHERE (z = :c0 AND w = :c1) LIMIT 1",
        "SELECT x FROM y WHERE (z = %s AND w = %s) LIMIT %s"
    );

    scrub_sql_test!(
        named_placeholders,
        r#"SELECT some_func(col, %(my_param1)s)"#,
        "SELECT some_func(col, %s)"
    );

    scrub_sql_test!(
        various_parameterized_ins_lowercase,
        "select count() from table where id in (100, 101, 102)",
        "select count() from table where id in (%s)"
    );

    scrub_sql_test!(
        various_parameterized_strings,
        "select count() from table_1 where name in ('foo', %s, 1)",
        "select count() from table_1 where name in (%s)"
    );

    scrub_sql_test!(
        various_parameterized_cutoff,
        "select count() from table where name in ('foo', 'bar', 'ba",
        "select count() from table where name in (%s"
    );

    scrub_sql_test!(
        mixed,
        "UPDATE foo SET a = %s, b = log(e + 5) * 600 + 12345 WHERE true",
        "UPDATE foo SET a = %s, b = log(e + %s) * %s + %s WHERE %s"
    );

    scrub_sql_test!(
        savepoint_uppercase,
        "SAVEPOINT unquoted_identifier",
        "SAVEPOINT %s"
    );

    scrub_sql_test!(
        savepoint_uppercase_semicolon,
        "SAVEPOINT unquoted_identifier;",
        "SAVEPOINT %s;"
    );

    scrub_sql_test!(
        savepoint_lowercase,
        "savepoint unquoted_identifier",
        "savepoint %s"
    );

    scrub_sql_test!(
        savepoint_quoted,
        "SAVEPOINT 'single_quoted_identifier'",
        "SAVEPOINT %s"
    );

    scrub_sql_test!(
        savepoint_quoted_backtick,
        "SAVEPOINT `backtick_quoted_identifier`",
        "SAVEPOINT %s"
    );

    scrub_sql_test!(
        single_quoted_string,
        "SELECT * FROM table WHERE sku = 'foo'",
        "SELECT * FROM table WHERE sku = %s"
    );

    scrub_sql_test!(
        single_quoted_string_finished,
        r"SELECT * FROM table WHERE quote = 'it\\'s a string'",
        "SELECT * FROM table WHERE quote = %s"
    );

    scrub_sql_test!(
        single_quoted_string_unfinished,
        r"SELECT * FROM table WHERE quote = 'it\\'s a string",
        "SELECT * FROM table WHERE quote = %s"
    );

    scrub_sql_test!(
        span_description_dont_scrub_double_quoted_strings_format_postgres,
        r#"SELECT * from "table" WHERE sku = %s"#,
        r#"SELECT * from table WHERE sku = %s"#
    );

    scrub_sql_test!(
        span_description_strip_prefixes,
        r#"SELECT table.foo, count(*) from table WHERE sku = %s"#,
        r#"SELECT foo, count(*) from table WHERE sku = %s"#
    );

    scrub_sql_test!(
        span_description_strip_prefixes_ansi,
        r#"SELECT "table"."foo", count(*) from "table" WHERE sku = %s"#,
        r#"SELECT foo, count(*) from table WHERE sku = %s"#
    );

    scrub_sql_test!(
        span_description_strip_prefixes_mysql,
        r#"SELECT `table`.`foo`, count(*) from `table` WHERE sku = %s"#,
        r#"SELECT foo, count(*) from table WHERE sku = %s"#
    );

    scrub_sql_test!(
        span_description_strip_prefixes_truncated,
        r#"SELECT foo = %s FROM "db"."ba"#,
        r#"SELECT foo = %s FROM ba"#
    );

    scrub_sql_test!(
        span_description_dont_scrub_double_quoted_strings_format_mysql,
        r#"SELECT * from table WHERE sku = "foo""#,
        "SELECT * from table WHERE sku = foo"
    );

    scrub_sql_test!(
        num_where,
        "SELECT * FROM table WHERE id = 1",
        "SELECT * FROM table WHERE id = %s"
    );

    scrub_sql_test!(
        num_limit,
        "SELECT * FROM table LIMIT 1",
        "SELECT * FROM table LIMIT %s"
    );

    scrub_sql_test!(
        num_negative_where,
        "SELECT * FROM table WHERE temperature > -100",
        "SELECT * FROM table WHERE temperature > %s"
    );

    scrub_sql_test!(
        num_e_where,
        "SELECT * FROM table WHERE salary > 1e7",
        "SELECT * FROM table WHERE salary > %s"
    );

    scrub_sql_test!(
        span_description_already_scrubbed,
        "SELECT * FROM table123 WHERE id = %s",
        "SELECT * FROM table123 WHERE id = %s"
    );

    scrub_sql_test!(
        boolean_where_true,
        "SELECT * FROM table WHERE deleted = true",
        "SELECT * FROM table WHERE deleted = %s"
    );

    scrub_sql_test!(
        boolean_where_false,
        "SELECT * FROM table WHERE deleted = false",
        "SELECT * FROM table WHERE deleted = %s"
    );

    scrub_sql_test!(
        boolean_where_bool_insensitive,
        "SELECT * FROM table WHERE deleted = FaLsE",
        "SELECT * FROM table WHERE deleted = %s"
    );

    scrub_sql_test!(
        boolean_not_in_tablename_true,
        "SELECT * FROM table_true WHERE deleted = %s",
        "SELECT * FROM table_true WHERE deleted = %s"
    );

    scrub_sql_test!(
        boolean_not_in_tablename_false,
        "SELECT * FROM table_false WHERE deleted = %s",
        "SELECT * FROM table_false WHERE deleted = %s"
    );

    scrub_sql_test!(
        boolean_not_in_mid_tablename_true,
        "SELECT * FROM tatrueble WHERE deleted = %s",
        "SELECT * FROM tatrueble WHERE deleted = %s"
    );

    scrub_sql_test!(
        boolean_not_in_mid_tablename_false,
        "SELECT * FROM tafalseble WHERE deleted = %s",
        "SELECT * FROM tafalseble WHERE deleted = %s"
    );

    scrub_sql_test!(
        span_description_dont_scrub_nulls,
        "SELECT * FROM table WHERE deleted_at IS NULL",
        ""
    );

    scrub_sql_test!(
        span_description_collapse_columns,
        // Simple lists of columns will be collapsed
        r#"SELECT myfield1, "a"."b", another_field FROM table WHERE %s"#,
        "SELECT .. FROM table WHERE %s"
    );

    scrub_sql_test!(
        span_description_do_not_collapse_single_column,
        // Single columns remain intact
        r#"SELECT a FROM table WHERE %s"#,
        "SELECT a FROM table WHERE %s"
    );

    scrub_sql_test!(
        span_description_collapse_columns_nested,
        // Simple lists of columns will be collapsed
        r#"SELECT a, b FROM (SELECT c, d FROM t) AS s WHERE %s"#,
        "SELECT .. FROM (SELECT .. FROM t) AS s WHERE %s"
    );

    scrub_sql_test!(
        span_description_collapse_partial_column_lists,
        r#"SELECT myfield1, "a"."b", count(*) AS c, another_field, another_field2 FROM table WHERE %s"#,
        "SELECT .., count(*) AS c,.. FROM table WHERE %s"
    );

    scrub_sql_test!(
        span_description_collapse_partial_column_lists_2,
        r#"SELECT DISTINCT a, b,c ,d , e, f, g, h, COALESCE(foo, %s) AS "id" FROM x"#,
        "SELECT DISTINCT .., COALESCE(foo, %s) AS id FROM x"
    );

    scrub_sql_test!(
        span_description_collapse_columns_distinct,
        r#"SELECT DISTINCT a, b, c FROM table WHERE %s"#,
        "SELECT DISTINCT .. FROM table WHERE %s"
    );

    scrub_sql_test!(
        span_description_collapse_columns_with_as,
        // Simple lists of columns will be collapsed.
        r#"SELECT myfield1, "a"."b" AS a__b, another_field as bar FROM table WHERE %s"#,
        "SELECT .. FROM table WHERE %s"
    );

    scrub_sql_test!(
        span_description_collapse_columns_with_as_without_quotes,
        // Simple lists of columns will be collapsed.
        r#"SELECT myfield1, a.b AS a__b, another_field as bar FROM table WHERE %s"#,
        "SELECT .. FROM table WHERE %s"
    );

    scrub_sql_test!(
        parameters_values,
        "INSERT INTO a (b, c, d, e) VALUES (%s, %s, %s, %s)",
        "INSERT INTO a (..) VALUES (%s)"
    );

    scrub_sql_test!(
        parameters_in,
        "select column FROM table WHERE id IN (1, 2, 3)",
        "select column FROM table WHERE id IN (%s)"
    );

    scrub_sql_test!(
        values_multi,
        "INSERT INTO a (b, c, d, e) VALuES (%s, %s, %s, %s), (%s, %s, %s, %s), (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
        "INSERT INTO a (..) VALuES (%s) ON CONFLICT DO NOTHING"
    );

    scrub_sql_test!(
        type_casts,
        "INSERT INTO a (b, c, d) VALUES ('foo'::date, 123::bigint[], %s::bigint[])",
        "INSERT INTO a (..) VALUES (%s)"
    );

    scrub_sql_test!(
        whitespace_and_comments,
        "
            select a,  b, c, d
            from (
                select *
                -- Some comment here.
                from (
                    -- Another comment.
                    select a, b, c, d from x where foo = 1
                ) srpe
                where x = %s --inline comment
            ) srpe
            inner join foo on foo.id = foo_id
        ",
        "select .. from (select * from (select .. from x where foo = %s) srpe where x = %s) srpe inner join foo on id = foo_id"
    );

    scrub_sql_test!(
        not_a_comment,
        "SELECT * from comments WHERE comment LIKE '-- NOTE%s'
        AND foo > 0",
        "SELECT * from comments WHERE comment LIKE %s AND foo > %s"
    );

    scrub_sql_test!(
        mysql_comment,
        "
            DELETE  # end-of-line
            FROM /* inline comment */ `some_table`
            /*
                multi
                line
                comment
            */
            WHERE `some_table`.`id` IN (%s)
        ",
        "DELETE FROM some_table WHERE id IN (%s)"
    );

    scrub_sql_test!(
        bytesa,
        r#"SELECT "t"."x", "t"."arr"::bytea, "t"."c", WHERE "t"."id" IN (%s, %s)"#,
        "SELECT x, arr, c WHERE id IN (%s)"
    );

    scrub_sql_test!(
        collapse_list_cutoff,
        r#"SELECT "t"."a", "t"."b", "t..."#,
        "SELECT .."
    );

    scrub_sql_test!(
        clickhouse,
        "SELECT (toStartOfHour(finish_ts, 'Universal') AS _snuba_time), (uniqIf((nullIf(user, '') AS _snuba_user), greater(multiIf(equals(tupleElement(('duration', 300), 1), 'lcp'), (if(has(measurements.key, 'lcp'), arrayElement(measurements.value, indexOf(measurements.key, 'lcp')), NULL) AS `_snuba_measurements[lcp]`), (duration AS _snuba_duration)), multiply(tupleElement(('duration', 300), 2), 4))) AS _snuba_count_miserable_user), (ifNull(divide(plus(_snuba_count_miserable_user, 4.56), plus(nullIf(uniqIf(_snuba_user, greater(multiIf(equals(tupleElement(('duration', 300), 1), 'lcp'), `_snuba_measurements[lcp]`, _snuba_duration), 0)), 0), 113.45)), 0) AS _snuba_user_misery), _snuba_count_miserable_user, (divide(countIf(notEquals(transaction_status, 0) AND notEquals(transaction_status, 1) AND notEquals(transaction_status, 2)), count()) AS _snuba_failure_rate), (divide(count(), divide(3600.0, 60)) AS _snuba_tpm_3600) FROM transactions_dist WHERE equals(('transaction' AS _snuba_type), 'transaction') AND greaterOrEquals((finish_ts AS _snuba_finish_ts), toDateTime('2023-06-13T09:08:51', 'Universal')) AND less(_snuba_finish_ts, toDateTime('2023-07-11T09:08:51', 'Universal')) AND in((project_id AS _snuba_project_id), [123, 456, 789]) AND equals((environment AS _snuba_environment), 'production') GROUP BY _snuba_time ORDER BY _snuba_time ASC LIMIT 10000 OFFSET 0",
        "SELECT (toStartOfHour(finish_ts, %s) AS _snuba_time), (uniqIf((nullIf(user, %s) AS _snuba_user), greater(multiIf(equals(tupleElement((%s, %s), %s), %s), (if(has(key, %s), arrayElement(value, indexOf(key, %s)), NULL) AS `_snuba_measurements[lcp]`), (duration AS _snuba_duration)), multiply(tupleElement((%s, %s), %s), %s))) AS _snuba_count_miserable_user), (ifNull(divide(plus(_snuba_count_miserable_user, %s), plus(nullIf(uniqIf(_snuba_user, greater(multiIf(equals(tupleElement((%s, %s), %s), %s), `_snuba_measurements[lcp]`, _snuba_duration), %s)), %s), %s)), %s) AS _snuba_user_misery), _snuba_count_miserable_user, (divide(countIf(notEquals(transaction_status, %s) AND notEquals(transaction_status, %s) AND notEquals(transaction_status, %s)), count()) AS _snuba_failure_rate), (divide(count(), divide(%s, %s)) AS _snuba_tpm_3600) FROM transactions_dist WHERE equals((%s AS _snuba_type), %s) AND greaterOrEquals((finish_ts AS _snuba_finish_ts), toDateTime(%s, %s)) AND less(_snuba_finish_ts, toDateTime(%s, %s)) AND in((project_id AS _snuba_project_id), [%s, %s, %s]) AND equals((environment AS _snuba_environment), %s) GROUP BY _snuba_time ORDER BY _snuba_time ASC LIMIT %s OFFSET %s"
    );

    #[test]
    fn find_placeholders() {
        let s = "? NULL ?? 'str' %s %(name1)s :c0 $4 xx";
        assert_eq!(
            SQL_PLACEHOLDER_REGEX.replace_all(s, "1"),
            "1 NULL 1 'str' 1 1 1 1 xx"
        );
    }
}
