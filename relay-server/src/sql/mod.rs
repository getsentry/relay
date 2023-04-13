use futures::stream::{Stream, StreamExt};
use sqlx::query::Query;
use sqlx::sqlite::SqliteArguments;
use sqlx::{Pool, QueryBuilder, Sqlite};

use crate::actors::project_buffer::QueueKey;
use crate::statsd::RelayCounters;

/// SQLite allocates space to hold all host parameters between 1 and the largest host parameter number used.
///
/// To prevent excessive memory allocations, the maximum value of a host parameter number is SQLITE_MAX_VARIABLE_NUMBER,
/// which defaults to 999 for SQLite versions prior to 3.32.0 (2020-05-22) or 32766 for SQLite versions after 3.32.0.
///
/// Keep it on the lower side for now.
const SQLITE_LIMIT_VARIABLE_NUMBER: usize = 999;

/// Creates a DELETE query binding to the provided [`QueueKey`] which returns the envelopes and
/// timestamp.
///
/// The query will perform the delete once executed returning deleted envelope and timestamp when
/// the envelope was received. This will create a prepared statement which is cached and re-used.
pub fn delete_and_fetch<'a>(
    key: QueueKey,
    batch_size: u32,
) -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query(
        "DELETE FROM
            envelopes
         WHERE id IN (SELECT id FROM envelopes WHERE own_key = ? AND sampling_key = ? LIMIT ?)
         RETURNING
            envelope, received_at",
    )
    .bind(key.own_key.to_string())
    .bind(key.sampling_key.to_string())
    .bind(batch_size)
}

/// Creates a DELETE query, which silently removes the data from the database.
pub fn delete<'a>(key: QueueKey) -> Query<'a, Sqlite, SqliteArguments<'a>> {
    sqlx::query("DELETE FROM envelopes where own_key = ? AND sampling_key = ?")
        .bind(key.own_key.to_string())
        .bind(key.sampling_key.to_string())
}

/// Descibes the chunk item which is handled by insert statement.
type ChunkItem = (QueueKey, Vec<u8>, i64);

/// Creates an INSERT query for the chunk of provided data.
pub fn insert_with_builder<'a>(
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
pub async fn insert_with_exec(
    stream: impl Stream<Item = ChunkItem> + std::marker::Unpin,
    db: &Pool<Sqlite>,
) -> Result<(), sqlx::Error> {
    // Since we have 3 variables we have to bind, we devide the SQLite limit by 3
    // here to prepare the chunks which will be preparing the batch inserts.
    let mut envelopes = stream.chunks(SQLITE_LIMIT_VARIABLE_NUMBER / 3);

    // A builder type for constructing queries at runtime.
    // This by default creates a prepared sql statement, which is cached and
    // re-used for sequential queries.
    let mut query_builder: QueryBuilder<Sqlite> =
        QueryBuilder::new("INSERT INTO envelopes (received_at, own_key, sampling_key, envelope) ");

    while let Some(chunk) = envelopes.next().await {
        insert_with_builder(&mut query_builder, chunk)
            .execute(db)
            .await?;
        relay_statsd::metric!(counter(RelayCounters::BufferWrites) += 1);

        // Reset the builder to initial state set by `QueryBuilder::new` function,
        // so it can be reused for another chunk.
        query_builder.reset();
    }

    Ok(())
}
