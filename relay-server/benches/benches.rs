use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::runtime::Runtime;

use relay_base_schema::project::ProjectKey;
use relay_server::{Envelope, EnvelopeStack, SQLiteEnvelopeStack};

fn setup_db(path: &PathBuf) -> Pool<Sqlite> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .journal_mode(SqliteJournalMode::Wal)
        .create_if_missing(true);

    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        let db = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .unwrap();

        sqlx::migrate!("../migrations").run(&db).await.unwrap();

        db
    })
}

async fn reset_db(db: Pool<Sqlite>) {
    sqlx::query("DELETE FROM envelopes")
        .execute(&db)
        .await
        .unwrap();
}

fn mock_envelope(size: &str) -> Box<Envelope> {
    let payload = match size {
        "small" => "small_payload".to_string(),
        "medium" => "medium_payload".repeat(100),
        "big" => "big_payload".repeat(1000),
        "huge" => "huge_payload".repeat(10000),
        _ => "default_payload".to_string(),
    };

    let bytes = Bytes::from(format!(
        "\
         {{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}}\n\
         {{\"type\":\"attachment\"}}\n\
         {}\n\
         ",
        payload
    ));

    Envelope::parse_bytes(bytes).unwrap()
}

fn benchmark_sqlite_envelope_stack(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = setup_db(&db_path);

    let runtime = Runtime::new().unwrap();

    let mut group = c.benchmark_group("sqlite_envelope_stack");
    group.measurement_time(Duration::from_secs(60));

    let disk_batch_size = 1000;
    for size in [1_000, 10_000, 100_000].iter() {
        for envelope_size in &["small", "medium", "big", "huge"] {
            group.throughput(Throughput::Elements(*size as u64));

            // Benchmark push operations
            group.bench_with_input(
                BenchmarkId::new(format!("push_{}", envelope_size), size),
                size,
                |b, &size| {
                    b.iter_with_setup(
                        || {
                            runtime.block_on(async {
                                reset_db(db.clone()).await;
                            });

                            let stack = SQLiteEnvelopeStack::new(
                                db.clone(),
                                disk_batch_size,
                                2,
                                ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
                                ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
                            );

                            let mut envelopes = Vec::with_capacity(size);
                            for _ in 0..size {
                                envelopes.push(mock_envelope(envelope_size));
                            }

                            (stack, envelopes)
                        },
                        |(mut stack, envelopes)| {
                            runtime.block_on(async {
                                for envelope in envelopes {
                                    stack.push(envelope).await.unwrap();
                                }
                            });
                        },
                    );
                },
            );

            // Benchmark pop operations
            group.bench_with_input(
                BenchmarkId::new(format!("pop_{}", envelope_size), size),
                size,
                |b, &size| {
                    b.iter_with_setup(
                        || {
                            runtime.block_on(async {
                                reset_db(db.clone()).await;

                                let mut stack = SQLiteEnvelopeStack::new(
                                    db.clone(),
                                    disk_batch_size,
                                    2,
                                    ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
                                    ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
                                );

                                // Pre-fill the stack
                                for _ in 0..size {
                                    let envelope = mock_envelope(envelope_size);
                                    stack.push(envelope).await.unwrap();
                                }

                                stack
                            })
                        },
                        |mut stack| {
                            runtime.block_on(async {
                                // Benchmark popping
                                for _ in 0..size {
                                    stack.pop().await.unwrap();
                                }
                            });
                        },
                    );
                },
            );

            // Benchmark mixed push and pop operations
            group.bench_with_input(
                BenchmarkId::new(format!("mixed_{}", envelope_size), size),
                size,
                |b, &size| {
                    b.iter_with_setup(
                        || {
                            runtime.block_on(async {
                                reset_db(db.clone()).await;
                            });

                            SQLiteEnvelopeStack::new(
                                db.clone(),
                                disk_batch_size,
                                2,
                                ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
                                ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
                            )
                        },
                        |mut stack| {
                            runtime.block_on(async {
                                for _ in 0..size {
                                    if rand::random::<bool>() {
                                        let envelope = mock_envelope(envelope_size);
                                        stack.push(envelope).await.unwrap();
                                    } else if stack.pop().await.is_err() {
                                        // If pop fails (empty stack), push instead
                                        let envelope = mock_envelope(envelope_size);
                                        stack.push(envelope).await.unwrap();
                                    }
                                }
                            });
                        },
                    );
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, benchmark_sqlite_envelope_stack);
criterion_main!(benches);
