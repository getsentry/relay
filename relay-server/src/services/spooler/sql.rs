//! This module contains the helper functions wrapping the SQL queries which will be run against
//! the on-disk spool (currently backed by SQLite).

use futures::stream::{Stream, StreamExt};
use itertools::Itertools;
use sqlx::query::Query;
use sqlx::sqlite::SqliteArguments;
use sqlx::{Pool, QueryBuilder, Sqlite};

use crate::services::spooler::QueueKey;
use crate::statsd::RelayCounters;

/// SQLite allocates space to hold all host parameters between 1 and the largest host parameter number used.
///
/// To prevent excessive memory allocations, the maximum value of a host parameter number is SQLITE_MAX_VARIABLE_NUMBER,
/// which defaults to 999 for SQLite versions prior to 3.32.0 (2020-05-22) or 32766 for SQLite versions after 3.32.0.
///
/// Keep it on the lower side for now.
const SQLITE_LIMIT_VARIABLE_NUMBER: usize = 999;

/// Prepares a DELETE query, by properly genering IN clauses for provided keys.
pub fn prepare_delete_query(keys: Vec<QueueKey>) -> String {
    let (own_keys, sampling_keys) = keys.iter().fold(
        (Vec::new(), Vec::new()),
        |(mut own_keys, mut sampling_keys), key| {
            own_keys.push(format!(r#""{}""#, key.own_key));
            sampling_keys.push(format!(r#""{}""#, key.sampling_key));
            (own_keys, sampling_keys)
        },
    );

    let own_keys = own_keys.into_iter().join(",");
    let sampling_keys = sampling_keys.into_iter().join(",");

    format!(
        "DELETE FROM
            envelopes
         WHERE id IN (SELECT id FROM envelopes WHERE own_key in ({}) AND sampling_key in ({}) LIMIT ?)
         RETURNING
            received_at, own_key, sampling_key, envelope", own_keys, sampling_keys
    )
}

/// Creates a DELETE query binding to the provided [`QueueKey`] which returns the envelopes and
/// timestamp.
///
/// The query will perform the delete once executed returning deleted envelope and timestamp when
/// the envelope was received. This will create a prepared statement which is cached and re-used.
pub fn delete_and_fetch(query: &str, batch_size: i64) -> Query<'_, Sqlite, SqliteArguments<'_>> {
    sqlx::query(query).bind(batch_size)
}

/// Creates a DELETE query which returns the requested batch of the envelopes with the timestamp
/// and designated keys.
pub fn delete_and_fetch_all<'a>(batch_size: i64) -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query(
        "DELETE FROM
            envelopes
         WHERE id IN (SELECT id FROM envelopes LIMIT ?)
         RETURNING
            received_at, own_key, sampling_key, envelope",
    )
    .bind(batch_size)
}

/// Creates a DELETE query, which silently removes the data from the database.
pub fn delete<'a>(key: QueueKey) -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query("DELETE FROM envelopes where own_key = ? AND sampling_key = ?")
        .bind(key.own_key.to_string())
        .bind(key.sampling_key.to_string())
}

/// Creates a query which fetches the `envelopes` table size.
///
/// This info used to calculate the current allocated database size.
pub fn current_size<'a>() -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query(r#"SELECT SUM(pgsize - unused) FROM dbstat WHERE name="envelopes""#)
}

/// Creates the query to select only 1 record's `received_at` from the database.
///
/// It is useful and very fast for checking if the table is empty.
pub fn select_one<'a>() -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query("SELECT received_at FROM envelopes LIMIT 1;")
}

/// Returns the query to select all the unique combinations of own and sampling keys.
pub fn get_keys<'a>() -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query("SELECT DISTINCT own_key, sampling_key FROM envelopes;")
}

/// Creates the INSERT query.
pub fn insert<'a>(
    key: QueueKey,
    managed_envelope: Vec<u8>,
    received_at: i64,
) -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query(
        "INSERT INTO envelopes (received_at, own_key, sampling_key, envelope) VALUES (?, ?, ?, ?);",
    )
    .bind(received_at)
    .bind(key.own_key.to_string())
    .bind(key.sampling_key.to_string())
    .bind(managed_envelope)
}

/// Describes the chunk item which is handled by insert statement.
type ChunkItem = (QueueKey, Vec<u8>, i64);

/// Creates an INSERT query for the chunk of provided data.
fn build_insert<'a>(
    builder: &'a mut QueryBuilder<Sqlite>,
    chunk: Vec<ChunkItem>,
) -> Query<'a, Sqlite, SqliteArguments<'a>> {
    builder.push_values(chunk, |mut b, (key, value, received_at)| {
        b.push_bind(received_at)
            .push_bind(key.own_key.to_string())
            .push_bind(key.sampling_key.to_string())
            .push_bind(value);
    });

    builder.build()
}

/// Creates INSERT statements from the stream and execute them on provided database pool.
///
/// This function internally will split the provided stream into chunks and will prepare the
/// insert statement for each chunk.
///
/// Returns the number of inserted rows on success.
pub async fn do_insert(
    stream: impl Stream<Item = ChunkItem> + std::marker::Unpin,
    db: &Pool<Sqlite>,
) -> Result<u64, sqlx::Error> {
    // Since we have 3 variables we have to bind, we divide the SQLite limit by 3
    // here to prepare the chunks which will be preparing the batch inserts.
    let mut envelopes = stream.chunks(SQLITE_LIMIT_VARIABLE_NUMBER / 3);

    // A builder type for constructing queries at runtime.
    // This by default creates a prepared SQL statement, which is cached and
    // re-used for sequential queries.
    let mut query_builder: QueryBuilder<Sqlite> =
        QueryBuilder::new("INSERT INTO envelopes (received_at, own_key, sampling_key, envelope) ");

    let mut count = 0;
    while let Some(chunk) = envelopes.next().await {
        let result = build_insert(&mut query_builder, chunk).execute(db).await?;
        count += result.rows_affected();
        relay_statsd::metric!(counter(RelayCounters::BufferWrites) += 1);

        // Reset the builder to initial state set by `QueryBuilder::new` function,
        // so it can be reused for another chunk.
        query_builder.reset();
    }

    Ok(count)
}

/// Creates DELETE statement which truncates the entire `envelopes` table.
///
/// When the DELETE statement runs without WHERE clause SQLite uses an optimization (so called "truncate" optimization)
/// to erase the entire table content without having to visit each row.
pub fn truncate<'a>() -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query("DELETE FROM envelopes;")
}
