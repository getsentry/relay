use futures::future::join;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use redis::aio::ConnectionLike;
use redis::{Cmd, Pipeline, RedisFuture, RedisResult, Value};

use crate::AsyncRedisConnection;

/// Commands which are duplicated to the secondaries.
///
/// Scripts are treated as writes. Read-only variants like `EVALSHA_RO` are not included.
const SECONDARY_COMMANDS: &[&str] = &[
    "EVAL", "EVALSHA", "SCRIPT", "SET", "SETEX", "DEL", "INCR", "INCRBY", "DECR", "DECRBY",
    "EXPIRE", "EXPIREAT", "HSET", "HDEL", "HINCRBY",
];

/// Returns `true` if `cmd` should be duplicated to the secondaries.
fn is_secondary_command(cmd: &Cmd) -> bool {
    let Some(redis::Arg::Simple(name)) = cmd.args_iter().next() else {
        return false;
    };
    SECONDARY_COMMANDS
        .iter()
        .any(|c| c.as_bytes().eq_ignore_ascii_case(name))
}

/// Returns a pipeline with only the secondary commands, `None` if there are none.
fn secondary_pipeline(pipeline: &Pipeline) -> Option<Pipeline> {
    let mut filtered = Pipeline::new();
    if pipeline.is_transaction() {
        filtered.atomic();
    }
    for cmd in pipeline.cmd_iter().filter(|cmd| is_secondary_command(cmd)) {
        filtered.add_command(cmd.clone());
    }
    (!filtered.is_empty()).then_some(filtered)
}

/// Sends the filtered pipeline to all secondaries, logging failures.
async fn write_secondary_pipeline(secondaries: &mut [AsyncRedisConnection], pipeline: Pipeline) {
    // Mirrors the offset and count of `Pipeline::query_async`.
    let (offset, count) = match pipeline.is_transaction() {
        true => (pipeline.len() + 1, 1),
        false => (0, pipeline.len()),
    };
    secondaries
        .iter_mut()
        .map(|secondary| secondary.req_packed_commands(&pipeline, offset, count))
        .collect::<FuturesUnordered<_>>()
        .for_each(log_secondary_error)
        .await;
}

/// Logs a failed command sent to a secondary.
async fn log_secondary_error<T>(result: RedisResult<T>) {
    if let Err(error) = result {
        relay_log::warn!(
            error = &error as &dyn std::error::Error,
            "failed to send command to secondary redis",
        );
    }
}

/// A connection which sends commands to the primary and write commands to the secondaries.
///
/// Only the primary's result is returned, errors from the secondaries are only logged.
pub struct MultiWriteConnection {
    primary: Box<AsyncRedisConnection>,
    secondaries: Vec<AsyncRedisConnection>,
}

impl MultiWriteConnection {
    /// Creates a connection from a `primary` and the `secondaries` which could be acquired.
    pub(crate) fn new(
        primary: AsyncRedisConnection,
        secondaries: Vec<AsyncRedisConnection>,
    ) -> Self {
        Self {
            primary: Box::new(primary),
            secondaries,
        }
    }
}

impl ConnectionLike for MultiWriteConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        let Self {
            primary,
            secondaries,
        } = self;

        if !is_secondary_command(cmd) {
            return primary.req_packed_command(cmd);
        }

        let secondaries = secondaries
            .iter_mut()
            .map(|r| r.req_packed_command(cmd))
            .collect::<FuturesUnordered<_>>()
            .for_each(log_secondary_error);

        join(primary.req_packed_command(cmd), secondaries)
            .map(|(primary, ())| primary)
            .boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        let Self {
            primary,
            secondaries,
        } = self;

        let Some(filtered) = secondary_pipeline(cmd) else {
            return primary.req_packed_commands(cmd, offset, count);
        };

        let secondaries = write_secondary_pipeline(secondaries, filtered);

        join(primary.req_packed_commands(cmd, offset, count), secondaries)
            .map(|(primary, ())| primary)
            .boxed()
    }

    fn get_db(&self) -> i64 {
        self.primary.get_db()
    }
}

#[cfg(test)]
mod tests {
    use redis::AsyncCommands;

    use super::*;
    use crate::{AsyncRedisClient, RedisConfigOptions};

    #[test]
    fn test_is_secondary_command() {
        assert!(is_secondary_command(&redis::cmd("EVALSHA")));
        assert!(is_secondary_command(&redis::cmd("evalsha")));
        assert!(is_secondary_command(&redis::cmd("SCRIPT")));
        assert!(is_secondary_command(&redis::cmd("SET")));

        assert!(!is_secondary_command(&redis::cmd("GET")));
        assert!(!is_secondary_command(&redis::cmd("MGET")));
        assert!(!is_secondary_command(&redis::cmd("EVALSHA_RO")));
    }

    #[test]
    fn test_secondary_pipeline() {
        let mut pipeline = redis::pipe();
        pipeline
            .atomic()
            .cmd("GET")
            .arg("a")
            .cmd("SET")
            .arg("b")
            .arg(1);
        let filtered = secondary_pipeline(&pipeline).unwrap();
        assert!(filtered.is_transaction());
        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered.get_packed_pipeline(),
            redis::pipe()
                .atomic()
                .cmd("SET")
                .arg("b")
                .arg(1)
                .get_packed_pipeline()
        );

        let mut reads = redis::pipe();
        reads.cmd("GET").arg("a").cmd("MGET").arg("b");
        assert!(secondary_pipeline(&reads).is_none());
    }

    #[tokio::test]
    async fn test_multi_write() {
        let url = std::env::var("RELAY_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
        let client = |db: u8| {
            let options = RedisConfigOptions::default();
            AsyncRedisClient::single("test", &format!("{url}/{db}"), &options).unwrap()
        };
        let key = format!("multi_write:{}", std::process::id());

        let multi_write = AsyncRedisClient::multi_write(client(0), vec![client(1), client(2)]);
        let mut connection = multi_write.get_connection().await.unwrap();
        let _: () = connection.set_ex(&key, "primary", 60).await.unwrap();

        // Diverge the secondaries, reads must only be answered by the primary.
        for db in [1, 2] {
            let mut secondary = client(db).get_connection().await.unwrap();
            let value: String = secondary.get(&key).await.unwrap();
            assert_eq!(value, "primary");
            let _: () = secondary.set_ex(&key, "secondary", 60).await.unwrap();
        }

        let value: String = connection.get(&key).await.unwrap();
        assert_eq!(value, "primary");
    }
}
